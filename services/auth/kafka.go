package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	authRequestsTopic  = "auth.requests"
	authResponsesTopic = "auth.responses"
	authConsumerGroup  = "auth-service"
	defaultKafkaBroker = "localhost:9092"
)

type AuthRequest struct {
	RequestID string          `json:"request_id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

type AuthResponse struct {
	RequestID string `json:"request_id"`
	Success   bool   `json:"success"`
	Code      string `json:"code"`
	Data      any    `json:"data"`
}

func newKafkaClient() (*kgo.Client, error) {
	broker := os.Getenv("KAFKA_BROKER")

	if broker == "" {
		broker = defaultKafkaBroker
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(authConsumerGroup),
		kgo.ConsumeTopics(authRequestsTopic),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create Kafka client: %w",
			err,
		)
	}

	return client, nil
}

func (server *Server) consumeKafka(
	ctx context.Context,
	client *kgo.Client,
) {
	log.Printf(
		"Auth Kafka consumer started: topic=%s group=%s",
		authRequestsTopic,
		authConsumerGroup,
	)

	for {
		fetches := client.PollFetches(ctx)

		if ctx.Err() != nil {
			return
		}

		if fetches.IsClientClosed() {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				log.Printf(
					"Kafka consumer error: topic=%s partition=%d error=%v",
					err.Topic,
					err.Partition,
					err.Err,
				)
			}

			continue
		}

		fetches.EachRecord(
			func(record *kgo.Record) {
				if err := server.processAuthRecord(
					ctx,
					client,
					record,
				); err != nil {
					log.Printf(
						"failed to process Kafka message: topic=%s partition=%d offset=%d error=%v",
						record.Topic,
						record.Partition,
						record.Offset,
						err,
					)
				}
			},
		)
	}
}

func (server *Server) processAuthRecord(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
) error {

	var request AuthRequest

	if err := json.Unmarshal(
		record.Value,
		&request,
	); err != nil {
		return fmt.Errorf(
			"invalid Kafka JSON: %w",
			err,
		)
	}

	if request.RequestID == "" {
		return errors.New("missing request_id")
	}

	if request.Type == "" {
		return server.sendAuthResponse(
			ctx,
			client,
			AuthResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "INVALID_REQUEST",
				Data:      nil,
			},
		)
	}

	switch request.Type {

	case "REGISTER":
		return server.handleRegisterKafkaRequest(
			ctx,
			client,
			request,
		)

	case "LOGIN":
		return server.handleLoginKafkaRequest(
			ctx,
			client,
			request,
		)

	default:
		return server.sendAuthResponse(
			ctx,
			client,
			AuthResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "UNSUPPORTED_REQUEST",
				Data:      nil,
			},
		)
	}
}

func (server *Server) handleRegisterKafkaRequest(
	ctx context.Context,
	client *kgo.Client,
	request AuthRequest,
) error {

	var registerRequest RegisterRequest

	if err := json.Unmarshal(
		request.Data,
		&registerRequest,
	); err != nil {
		return server.sendAuthResponse(
			ctx,
			client,
			AuthResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "INVALID_REQUEST",
				Data:      nil,
			},
		)
	}

	registerData, err := server.registerUser(
		ctx,
		registerRequest,
	)
	if err != nil {
		code := registrationErrorCode(err)

		if code == "REGISTRATION_FAILED" {
			log.Printf(
				"registration failed: request_id=%s error=%v",
				request.RequestID,
				err,
			)
		}

		return server.sendAuthResponse(
			ctx,
			client,
			AuthResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      code,
				Data:      nil,
			},
		)
	}

	return server.sendAuthResponse(
		ctx,
		client,
		AuthResponse{
			RequestID: request.RequestID,
			Success:   true,
			Code:      "REGISTRATION_SUCCESS",
			Data:      registerData,
		},
	)
}

func (server *Server) handleLoginKafkaRequest(
	ctx context.Context,
	client *kgo.Client,
	request AuthRequest,
) error {

	var loginRequest LoginRequest

	if err := json.Unmarshal(
		request.Data,
		&loginRequest,
	); err != nil {
		return server.sendAuthResponse(
			ctx,
			client,
			AuthResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "INVALID_REQUEST",
				Data:      nil,
			},
		)
	}

	loginData, err := server.loginUser(
		ctx,
		loginRequest,
	)
	if err != nil {
		code := loginErrorCode(err)

		if code == "LOGIN_FAILED" {
			log.Printf(
				"login failed internally: request_id=%s error=%v",
				request.RequestID,
				err,
			)
		}

		return server.sendAuthResponse(
			ctx,
			client,
			AuthResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      code,
				Data:      nil,
			},
		)
	}

	return server.sendAuthResponse(
		ctx,
		client,
		AuthResponse{
			RequestID: request.RequestID,
			Success:   true,
			Code:      "LOGIN_SUCCESS",
			Data:      loginData,
		},
	)
}

func registrationErrorCode(err error) string {
	switch {
	case errors.Is(err, errInvalidUsername):
		return "INVALID_USERNAME"

	case errors.Is(err, errInvalidPassword):
		return "INVALID_PASSWORD"

	case errors.Is(err, errUsernameExists):
		return "USERNAME_ALREADY_EXISTS"

	case errors.Is(err, errAliasCollision):
		return "REGISTRATION_FAILED"

	default:
		return "REGISTRATION_FAILED"
	}
}

func loginErrorCode(err error) string {
	switch {
	case errors.Is(err, errInvalidLoginRequest):
		return "INVALID_REQUEST"

	case errors.Is(err, errAuthenticationFailed):
		return "AUTHENTICATION_FAILED"

	default:
		return "LOGIN_FAILED"
	}
}

func (server *Server) sendAuthResponse(
	ctx context.Context,
	client *kgo.Client,
	response AuthResponse,
) error {

	value, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf(
			"failed to encode Kafka response: %w",
			err,
		)
	}

	responseCtx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()

	record := &kgo.Record{
		Topic: authResponsesTopic,
		Key:   []byte(response.RequestID),
		Value: value,
	}

	if err := client.ProduceSync(
		responseCtx,
		record,
	).FirstErr(); err != nil {
		return fmt.Errorf(
			"failed to produce auth response: %w",
			err,
		)
	}

	return nil
}
