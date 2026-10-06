package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

type MathServer struct {
	mongo *MongoStore
}

type CalculationRequest struct {
	Operation string  `json:"operation"`
	A         float64 `json:"a"`
	B         float64 `json:"b"`
}

type CalculationData struct {
	Operation string  `json:"operation"`
	A         float64 `json:"a"`
	B         float64 `json:"b"`
	Result    float64 `json:"result"`
}

type HistoryRequest struct {
	Limit int `json:"limit"`
}

type HistoryItem struct {
	Operation string    `json:"operation"`
	A         float64   `json:"a"`
	B         float64   `json:"b"`
	Result    float64   `json:"result"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	mongoStore, err := connectMongo()
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		disconnectCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := mongoStore.client.Disconnect(
			disconnectCtx,
		); err != nil {
			log.Printf(
				"MongoDB disconnect failed: %v",
				err,
			)
		}
	}()

	server := &MathServer{
		mongo: mongoStore,
	}

	kafkaClient, err := newMathKafkaClient()
	if err != nil {
		log.Fatal(err)
	}
	defer kafkaClient.Close()

	go server.consumeMathRequests(
		ctx,
		kafkaClient,
	)

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
		Addr:              ":8082",
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

	log.Println(
		"Math Service listening on :8082",
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
				"Math Service failed: %v",
				err,
			)
		}

	case <-ctx.Done():
		log.Println(
			"shutting down Math Service",
		)

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := httpServer.Shutdown(
			shutdownCtx,
		); err != nil {
			log.Printf(
				"Math HTTP shutdown failed: %v",
				err,
			)
		}
	}
}

func (server *MathServer) healthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusOK)

	_, _ = w.Write([]byte(
		`{"success":true,"code":"MATH_SERVICE_OK","data":null}` +
			"\n",
	))
}

func (server *MathServer) databaseHealthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx, cancel := context.WithTimeout(
		r.Context(),
		2*time.Second,
	)
	defer cancel()

	if err := server.mongo.client.Ping(
		ctx,
		nil,
	); err != nil {
		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		w.WriteHeader(
			http.StatusServiceUnavailable,
		)

		_, _ = w.Write([]byte(
			`{"success":false,"code":"DATABASE_UNAVAILABLE","data":null}` +
				"\n",
		))

		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusOK)

	_, _ = w.Write([]byte(
		`{"success":true,"code":"DATABASE_CONNECTED","data":null}` +
			"\n",
	))
}
