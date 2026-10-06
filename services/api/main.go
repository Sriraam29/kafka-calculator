package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultAPIPort = ":8080"

	servicePublishTimeout  = 5 * time.Second
	serviceResponseTimeout = 15 * time.Second
	tokenLookupTimeout     = 2 * time.Second
)

type Server struct {
	kafka *kgo.Client
	db    *pgxpool.Pool

	pendingMu sync.Mutex
	pending   map[string]chan ServiceResponse
}

type CredentialsRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type CalculationRequest struct {
	Operation string  `json:"operation"`
	A         float64 `json:"a"`
	B         float64 `json:"b"`
}

type APIResponse struct {
	Success bool            `json:"success"`
	Code    string          `json:"code"`
	Data    json.RawMessage `json:"data"`
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	// PostgreSQL is required by the API for access-token validation.
	db, err := connectDatabase()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Kafka is required for communication with Auth and Math services.
	kafkaClient, err := newKafkaClient()
	if err != nil {
		log.Fatal(err)
	}
	defer kafkaClient.Close()

	server := &Server{
		kafka:   kafkaClient,
		db:      db,
		pending: make(map[string]chan ServiceResponse),
	}

	// Continuously consume Auth and Math responses.
	go server.consumeServiceResponses(
		ctx,
		kafkaClient,
	)

	mux := http.NewServeMux()

	// Operational endpoints.
	mux.HandleFunc(
		"/health",
		server.healthHandler,
	)

	mux.HandleFunc(
		"/health/db",
		server.databaseHealthHandler,
	)

	mux.HandleFunc(
		"/health/kafka",
		server.kafkaHealthHandler,
	)

	// Authentication endpoints.
	mux.HandleFunc(
		"/api/auth/register",
		server.registerHandler,
	)

	mux.HandleFunc(
		"/api/auth/login",
		server.loginHandler,
	)

	mux.HandleFunc(
		"/api/auth/me",
		server.meHandler,
	)

	// Calculator endpoints.
	mux.HandleFunc(
		"/api/calculator/calculate",
		server.calculateHandler,
	)

	mux.HandleFunc(
		"DELETE /api/calculator/history",
		server.historyHandler,
	)

	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           corsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Bind first. Only log "listening" after Windows successfully
	// gives us the port.
	listener, err := net.Listen(
		"tcp",
		httpServer.Addr,
	)
	if err != nil {
		log.Fatalf(
			"failed to listen on %s: %v",
			httpServer.Addr,
			err,
		)
	}

	log.Printf(
		"API Handler listening on %s",
		httpServer.Addr,
	)

	serverErrors := make(chan error, 1)

	go func() {
		serverErrors <- httpServer.Serve(listener)
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(
			err,
			http.ErrServerClosed,
		) {
			log.Fatalf(
				"API Handler failed: %v",
				err,
			)
		}

	case <-ctx.Done():
		log.Println("shutting down API Handler")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := httpServer.Shutdown(
			shutdownCtx,
		); err != nil {
			log.Printf(
				"API Handler shutdown failed: %v",
				err,
			)
		}
	}
}

func (server *Server) healthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		writeJSON(
			w,
			http.StatusMethodNotAllowed,
			APIResponse{
				Success: false,
				Code:    "METHOD_NOT_ALLOWED",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		APIResponse{
			Success: true,
			Code:    "API_HANDLER_OK",
		},
	)
}

func (server *Server) databaseHealthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		writeJSON(
			w,
			http.StatusMethodNotAllowed,
			APIResponse{
				Success: false,
				Code:    "METHOD_NOT_ALLOWED",
			},
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		2*time.Second,
	)
	defer cancel()

	if err := server.db.Ping(ctx); err != nil {
		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "DATABASE_UNAVAILABLE",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		APIResponse{
			Success: true,
			Code:    "DATABASE_CONNECTED",
		},
	)
}

func (server *Server) kafkaHealthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		writeJSON(
			w,
			http.StatusMethodNotAllowed,
			APIResponse{
				Success: false,
				Code:    "METHOD_NOT_ALLOWED",
			},
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		2*time.Second,
	)
	defer cancel()

	if err := server.kafka.Ping(ctx); err != nil {
		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "KAFKA_UNAVAILABLE",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		APIResponse{
			Success: true,
			Code:    "KAFKA_CONNECTED",
		},
	)
}

// ------------------------------------------------------------
// Authentication
// ------------------------------------------------------------

func (server *Server) registerHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	request, ok := decodeCredentials(w, r)
	if !ok {
		return
	}

	if request.Username == "" ||
		request.Password == "" {
		writeJSON(
			w,
			http.StatusBadRequest,
			APIResponse{
				Success: false,
				Code:    "INVALID_REQUEST",
			},
		)
		return
	}

	data, err := json.Marshal(request)
	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			APIResponse{
				Success: false,
				Code:    "INTERNAL_ERROR",
			},
		)
		return
	}

	response, err := server.requestAuthService(
		r.Context(),
		"REGISTER",
		data,
	)
	if err != nil {
		log.Printf(
			"registration request failed: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "AUTH_SERVICE_UNAVAILABLE",
			},
		)
		return
	}

	writeJSON(
		w,
		authResponseHTTPStatus(response),
		APIResponse{
			Success: response.Success,
			Code:    response.Code,
			Data:    response.Data,
		},
	)
}

func (server *Server) loginHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	request, ok := decodeCredentials(w, r)
	if !ok {
		return
	}

	if request.Username == "" ||
		request.Password == "" {
		writeJSON(
			w,
			http.StatusBadRequest,
			APIResponse{
				Success: false,
				Code:    "INVALID_REQUEST",
			},
		)
		return
	}

	data, err := json.Marshal(request)
	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			APIResponse{
				Success: false,
				Code:    "INTERNAL_ERROR",
			},
		)
		return
	}

	response, err := server.requestAuthService(
		r.Context(),
		"LOGIN",
		data,
	)
	if err != nil {
		log.Printf(
			"login request failed: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "AUTH_SERVICE_UNAVAILABLE",
			},
		)
		return
	}

	writeJSON(
		w,
		authResponseHTTPStatus(response),
		APIResponse{
			Success: response.Success,
			Code:    response.Code,
			Data:    response.Data,
		},
	)
}

func (server *Server) meHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		writeJSON(
			w,
			http.StatusMethodNotAllowed,
			APIResponse{
				Success: false,
				Code:    "METHOD_NOT_ALLOWED",
			},
		)
		return
	}

	userID, authCode, err := server.authenticateRequest(r)
	if err != nil {
		log.Printf(
			"authentication lookup failed: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "AUTHENTICATION_UNAVAILABLE",
			},
		)
		return
	}

	if authCode != "" {
		writeJSON(
			w,
			http.StatusUnauthorized,
			APIResponse{
				Success: false,
				Code:    authCode,
			},
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		tokenLookupTimeout,
	)
	defer cancel()

	aliasName, err := lookupAliasByUserID(
		ctx,
		server.db,
		userID,
	)
	if err != nil {
		log.Printf(
			"failed to lookup authenticated user: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusInternalServerError,
			APIResponse{
				Success: false,
				Code:    "INTERNAL_ERROR",
			},
		)
		return
	}

	data, err := json.Marshal(
		map[string]string{
			"alias_name": aliasName,
		},
	)
	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			APIResponse{
				Success: false,
				Code:    "INTERNAL_ERROR",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		APIResponse{
			Success: true,
			Code:    "AUTHENTICATED",
			Data:    data,
		},
	)
}

// ------------------------------------------------------------
// Calculator
// ------------------------------------------------------------

func (server *Server) calculateHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		writeJSON(
			w,
			http.StatusMethodNotAllowed,
			APIResponse{
				Success: false,
				Code:    "METHOD_NOT_ALLOWED",
			},
		)
		return
	}

	userID, authCode, err := server.authenticateRequest(r)
	if err != nil {
		log.Printf(
			"authentication lookup failed: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "AUTHENTICATION_UNAVAILABLE",
			},
		)
		return
	}

	if authCode != "" {
		writeJSON(
			w,
			http.StatusUnauthorized,
			APIResponse{
				Success: false,
				Code:    authCode,
			},
		)
		return
	}

	var request CalculationRequest

	if !decodeJSONBody(
		w,
		r,
		&request,
	) {
		return
	}

	request.Operation = strings.ToLower(
		strings.TrimSpace(request.Operation),
	)

	if request.Operation == "" {
		writeJSON(
			w,
			http.StatusBadRequest,
			APIResponse{
				Success: false,
				Code:    "INVALID_OPERATION",
			},
		)
		return
	}

	data, err := json.Marshal(request)
	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			APIResponse{
				Success: false,
				Code:    "INTERNAL_ERROR",
			},
		)
		return
	}

	response, err := server.requestMathService(
		r.Context(),
		"CALCULATE",
		userID,
		data,
	)
	if err != nil {
		log.Printf(
			"calculation request failed: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "MATH_SERVICE_UNAVAILABLE",
			},
		)
		return
	}

	writeJSON(
		w,
		mathResponseHTTPStatus(response),
		APIResponse{
			Success: response.Success,
			Code:    response.Code,
			Data:    response.Data,
		},
	)
}

func (server *Server) historyHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		writeJSON(
			w,
			http.StatusMethodNotAllowed,
			APIResponse{
				Success: false,
				Code:    "METHOD_NOT_ALLOWED",
			},
		)
		return
	}

	userID, authCode, err := server.authenticateRequest(r)
	if err != nil {
		log.Printf(
			"authentication lookup failed: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "AUTHENTICATION_UNAVAILABLE",
			},
		)
		return
	}

	if authCode != "" {
		writeJSON(
			w,
			http.StatusUnauthorized,
			APIResponse{
				Success: false,
				Code:    authCode,
			},
		)
		return
	}

	limit := 20

	rawLimit := r.URL.Query().Get("limit")

	if rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil ||
			parsedLimit < 1 ||
			parsedLimit > 100 {
			writeJSON(
				w,
				http.StatusBadRequest,
				APIResponse{
					Success: false,
					Code:    "INVALID_LIMIT",
				},
			)
			return
		}

		limit = parsedLimit
	}

	data, err := json.Marshal(
		map[string]int{
			"limit": limit,
		},
	)
	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			APIResponse{
				Success: false,
				Code:    "INTERNAL_ERROR",
			},
		)
		return
	}

	response, err := server.requestMathService(
		r.Context(),
		"HISTORY",
		userID,
		data,
	)
	if err != nil {
		log.Printf(
			"history request failed: %v",
			err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "MATH_SERVICE_UNAVAILABLE",
			},
		)
		return
	}

	writeJSON(
		w,
		mathResponseHTTPStatus(response),
		APIResponse{
			Success: response.Success,
			Code:    response.Code,
			Data:    response.Data,
		},
	)
}

// ------------------------------------------------------------
// Request validation
// ------------------------------------------------------------

func decodeCredentials(
	w http.ResponseWriter,
	r *http.Request,
) (CredentialsRequest, bool) {

	if r.Method != http.MethodPost {
		writeJSON(
			w,
			http.StatusMethodNotAllowed,
			APIResponse{
				Success: false,
				Code:    "METHOD_NOT_ALLOWED",
			},
		)
		return CredentialsRequest{}, false
	}

	if !strings.HasPrefix(
		r.Header.Get("Content-Type"),
		"application/json",
	) {
		writeJSON(
			w,
			http.StatusUnsupportedMediaType,
			APIResponse{
				Success: false,
				Code:    "UNSUPPORTED_MEDIA_TYPE",
			},
		)
		return CredentialsRequest{}, false
	}

	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		4096,
	)

	var request CredentialsRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeJSON(
			w,
			http.StatusBadRequest,
			APIResponse{
				Success: false,
				Code:    "INVALID_REQUEST",
			},
		)
		return CredentialsRequest{}, false
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		writeJSON(
			w,
			http.StatusBadRequest,
			APIResponse{
				Success: false,
				Code:    "INVALID_REQUEST",
			},
		)
		return CredentialsRequest{}, false
	}

	request.Username = strings.TrimSpace(
		request.Username,
	)

	return request, true
}

func decodeJSONBody(
	w http.ResponseWriter,
	r *http.Request,
	target any,
) bool {
	if !strings.HasPrefix(
		r.Header.Get("Content-Type"),
		"application/json",
	) {
		writeJSON(
			w,
			http.StatusUnsupportedMediaType,
			APIResponse{
				Success: false,
				Code:    "UNSUPPORTED_MEDIA_TYPE",
			},
		)
		return false
	}

	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		4096,
	)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		writeJSON(
			w,
			http.StatusBadRequest,
			APIResponse{
				Success: false,
				Code:    "INVALID_REQUEST",
			},
		)
		return false
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		writeJSON(
			w,
			http.StatusBadRequest,
			APIResponse{
				Success: false,
				Code:    "INVALID_REQUEST",
			},
		)
		return false
	}

	return true
}

// ------------------------------------------------------------
// Token authentication
// ------------------------------------------------------------

func (server *Server) authenticateRequest(
	r *http.Request,
) (string, string, error) {

	token, code := extractBearerToken(
		r.Header.Get("Authorization"),
	)

	if code != "" {
		return "", code, nil
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		tokenLookupTimeout,
	)
	defer cancel()

	userID, status, err := lookupAccessToken(
		ctx,
		server.db,
		token,
	)

	if err != nil {
		return "", "", err
	}

	switch status {

	case tokenInvalid:
		return "", "INVALID_ACCESS_TOKEN", nil

	case tokenExpired:
		return "", "ACCESS_TOKEN_EXPIRED", nil

	case tokenValid:
		return userID, "", nil

	default:
		return "", "INVALID_ACCESS_TOKEN", nil
	}
}

func extractBearerToken(
	authorization string,
) (string, string) {

	authorization = strings.TrimSpace(
		authorization,
	)

	if authorization == "" {
		return "", "AUTHENTICATION_REQUIRED"
	}

	parts := strings.Fields(
		authorization,
	)

	if len(parts) != 2 ||
		!strings.EqualFold(
			parts[0],
			"Bearer",
		) ||
		parts[1] == "" {
		return "", "INVALID_ACCESS_TOKEN"
	}

	return parts[1], ""
}

// ------------------------------------------------------------
// Response mapping
// ------------------------------------------------------------

func authResponseHTTPStatus(
	response ServiceResponse,
) int {
	if response.Success {
		switch response.Code {

		case "REGISTRATION_SUCCESS":
			return http.StatusCreated

		case "LOGIN_SUCCESS":
			return http.StatusOK

		default:
			return http.StatusOK
		}
	}

	switch response.Code {

	case "INVALID_REQUEST",
		"INVALID_USERNAME",
		"INVALID_PASSWORD":
		return http.StatusBadRequest

	case "USERNAME_ALREADY_EXISTS":
		return http.StatusConflict

	case "AUTHENTICATION_FAILED":
		return http.StatusUnauthorized

	case "LOGIN_FAILED":
		return http.StatusBadGateway

	default:
		return http.StatusBadGateway
	}
}

func mathResponseHTTPStatus(
	response ServiceResponse,
) int {
	if response.Success {
		return http.StatusOK
	}

	switch response.Code {

	case "INVALID_REQUEST",
		"INVALID_OPERATION",
		"DIVISION_BY_ZERO",
		"INVALID_LIMIT":
		return http.StatusBadRequest

	default:
		return http.StatusBadGateway
	}
}

func writeJSON(
	w http.ResponseWriter,
	statusCode int,
	response APIResponse,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(statusCode)

	if err := json.NewEncoder(
		w,
	).Encode(response); err != nil {
		log.Printf(
			"failed to write JSON response: %v",
			err,
		)
	}
}
