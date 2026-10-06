package main

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	db             *pgxpool.Pool
	encryptionKey  []byte
	accessTokenTTL time.Duration
}

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RegisterData struct {
	AliasName string `json:"alias_name"`
}

type LoginData struct {
	AliasName   string `json:"alias_name"`
	AccessToken string `json:"access_token"`
}

type APIResponse struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Data    any    `json:"data"`
}

var (
	errInvalidUsername      = errors.New("invalid username")
	errInvalidPassword      = errors.New("invalid password")
	errUsernameExists       = errors.New("username already exists")
	errAliasCollision       = errors.New("alias collision")
	errInvalidLoginRequest  = errors.New("invalid login request")
	errAuthenticationFailed = errors.New("authentication failed")
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	db, err := connectDatabase()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	encryptionKey, err := loadEncryptionKey()
	if err != nil {
		log.Fatal(err)
	}

	accessTokenTTL, err := loadAccessTokenTTL()
	if err != nil {
		log.Fatal(err)
	}

	server := &Server{
		db:             db,
		encryptionKey:  encryptionKey,
		accessTokenTTL: accessTokenTTL,
	}

	kafkaClient, err := newKafkaClient()
	if err != nil {
		log.Fatal(err)
	}
	defer kafkaClient.Close()

	go server.consumeKafka(ctx, kafkaClient)

	mux := http.NewServeMux()

	mux.HandleFunc(
		"/health",
		server.healthHandler,
	)

	mux.HandleFunc(
		"/health/db",
		server.databaseHealthHandler,
	)

	httpServer := &http.Server{
		Addr:              ":8081",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

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
		"Auth Service listening on %s",
		httpServer.Addr,
	)

	serverErrors := make(chan error, 1)

	go func() {
		serverErrors <- httpServer.Serve(listener)
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf(
				"HTTP server failed: %v",
				err,
			)
		}

	case <-ctx.Done():
		log.Println("shutting down Auth Service")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := httpServer.Shutdown(
			shutdownCtx,
		); err != nil {
			log.Printf(
				"shutdown failed: %v",
				err,
			)
		}
	}
}

func loadAccessTokenTTL() (time.Duration, error) {
	value := os.Getenv("ACCESS_TOKEN_TTL")

	if value == "" {
		return time.Hour, nil
	}

	ttl, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid ACCESS_TOKEN_TTL: %w",
			err,
		)
	}

	if ttl <= 0 {
		return 0, fmt.Errorf(
			"ACCESS_TOKEN_TTL must be greater than zero",
		)
	}

	return ttl, nil
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
				Data:    nil,
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		APIResponse{
			Success: true,
			Code:    "AUTH_SERVICE_OK",
			Data:    nil,
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
				Data:    nil,
			},
		)
		return
	}

	if err := server.db.Ping(r.Context()); err != nil {
		writeJSON(
			w,
			http.StatusServiceUnavailable,
			APIResponse{
				Success: false,
				Code:    "DATABASE_UNAVAILABLE",
				Data:    nil,
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
			Data:    nil,
		},
	)
}

func (server *Server) registerUser(
	ctx context.Context,
	request RegisterRequest,
) (RegisterData, error) {

	username := strings.TrimSpace(
		request.Username,
	)

	if len(username) < 3 || len(username) > 100 {
		return RegisterData{}, errInvalidUsername
	}

	if len(request.Password) < 8 ||
		len(request.Password) > 128 {
		return RegisterData{}, errInvalidPassword
	}

	encryptedPassword, passwordNonce, err :=
		encryptPassword(
			request.Password,
			server.encryptionKey,
		)

	if err != nil {
		return RegisterData{}, fmt.Errorf(
			"password encryption failed: %w",
			err,
		)
	}

	aliasName, err := server.createUser(
		ctx,
		username,
		encryptedPassword,
		passwordNonce,
	)
	if err != nil {
		return RegisterData{}, err
	}

	return RegisterData{
		AliasName: aliasName,
	}, nil
}

func (server *Server) loginUser(
	ctx context.Context,
	request LoginRequest,
) (LoginData, error) {

	username := strings.TrimSpace(
		request.Username,
	)

	if len(username) < 3 ||
		len(username) > 100 ||
		len(request.Password) < 8 ||
		len(request.Password) > 128 {
		return LoginData{}, errInvalidLoginRequest
	}

	const query = `
		SELECT
			id::text,
			alias_name,
			encrypted_password,
			password_nonce
		FROM users
		WHERE LOWER(username) = LOWER($1)
		LIMIT 1
	`

	var (
		userID            string
		aliasName         string
		encryptedPassword string
		passwordNonce     string
	)

	err := server.db.QueryRow(
		ctx,
		query,
		username,
	).Scan(
		&userID,
		&aliasName,
		&encryptedPassword,
		&passwordNonce,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return LoginData{}, errAuthenticationFailed
		}

		return LoginData{}, fmt.Errorf(
			"failed to query user: %w",
			err,
		)
	}

	storedPassword, err := decryptPassword(
		encryptedPassword,
		passwordNonce,
		server.encryptionKey,
	)
	if err != nil {
		return LoginData{}, fmt.Errorf(
			"failed to decrypt stored password: %w",
			err,
		)
	}

	if !passwordsMatch(
		request.Password,
		storedPassword,
	) {
		return LoginData{}, errAuthenticationFailed
	}

	const maxTokenAttempts = 3

	const insertTokenQuery = `
		INSERT INTO access_tokens (
			token_hash,
			user_id,
			expires_at
		)
		VALUES ($1, $2::uuid, $3)
	`

	for attempt := 0; attempt < maxTokenAttempts; attempt++ {
		rawToken, err := generateAccessToken()
		if err != nil {
			return LoginData{}, err
		}

		tokenHash := hashAccessToken(rawToken)

		expiresAt := time.Now().
			UTC().
			Add(server.accessTokenTTL)

		_, err = server.db.Exec(
			ctx,
			insertTokenQuery,
			tokenHash,
			userID,
			expiresAt,
		)

		if err == nil {
			return LoginData{
				AliasName:   aliasName,
				AccessToken: rawToken,
			}, nil
		}

		var pgErr *pgconn.PgError

		if errors.As(err, &pgErr) &&
			pgErr.Code == "23505" {
			// Extremely unlikely random-token collision.
			continue
		}

		return LoginData{}, fmt.Errorf(
			"failed to persist access token: %w",
			err,
		)
	}

	return LoginData{}, fmt.Errorf(
		"failed to generate unique access token",
	)
}

func (server *Server) createUser(
	ctx context.Context,
	username string,
	encryptedPassword string,
	passwordNonce string,
) (string, error) {

	const maxAliasAttempts = 5

	const query = `
		INSERT INTO users (
			username,
			alias_name,
			encrypted_password,
			password_nonce
		)
		VALUES ($1, $2, $3, $4)
		RETURNING alias_name
	`

	for attempt := 0; attempt < maxAliasAttempts; attempt++ {
		aliasName, err := generateAlias()
		if err != nil {
			return "", err
		}

		var savedAlias string

		err = server.db.QueryRow(
			ctx,
			query,
			username,
			aliasName,
			encryptedPassword,
			passwordNonce,
		).Scan(&savedAlias)

		if err == nil {
			return savedAlias, nil
		}

		var pgErr *pgconn.PgError

		if errors.As(err, &pgErr) &&
			pgErr.Code == "23505" {

			switch pgErr.ConstraintName {

			case "users_username_lower_key":
				return "", errUsernameExists

			case "users_alias_name_key":
				continue
			}
		}

		return "", fmt.Errorf(
			"failed to insert user: %w",
			err,
		)
	}

	return "", errAliasCollision
}

func generateAlias() (string, error) {
	const randomBytes = 10

	buffer := make([]byte, randomBytes)

	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf(
			"failed to generate alias: %w",
			err,
		)
	}

	encoded := base32.StdEncoding.
		WithPadding(base32.NoPadding).
		EncodeToString(buffer)

	return "usr_" + encoded, nil
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

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf(
			"failed to write JSON response: %v",
			err,
		)
	}
}
