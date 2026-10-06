package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	//"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultKafkaBroker = "localhost:9092"

	authRequestsTopic  = "auth.requests"
	authResponsesTopic = "auth.responses"

	mathRequestsTopic  = "math.requests"
	mathResponsesTopic = "math.responses"
)

type ServiceRequest struct {
	RequestID string          `json:"request_id"`
	Type      string          `json:"type"`
	UserID    string          `json:"user_id,omitempty"`
	Data      json.RawMessage `json:"data"`
}

type ServiceResponse struct {
	RequestID string          `json:"request_id"`
	Success   bool            `json:"success"`
	Code      string          `json:"code"`
	Data      json.RawMessage `json:"data"`
}

func newKafkaClient() (*kgo.Client, error) {
	broker := os.Getenv("KAFKA_BROKER")

	if broker == "" {
		broker = defaultKafkaBroker
	}

	instanceID := os.Getenv("API_INSTANCE_ID")

	if instanceID == "" {
		instanceID = generateInstanceID()
	}

	consumerGroup := "api-handler-" + instanceID

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ClientID("api-handler"),
		kgo.ConsumerGroup(consumerGroup),

		kgo.ConsumeTopics(
			authResponsesTopic,
			mathResponsesTopic,
		),

		// The pending-request map exists only in this process.
		// Therefore an API instance should not consume old responses
		// that cannot belong to its current requests.
		kgo.ConsumeResetOffset(
			kgo.NewOffset().AtEnd(),
		),

		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return nil, fmt.Errorf(
			"failed to create API Kafka client: %w",
			err,
		)
	}

	log.Printf(
		"API Kafka client started: broker=%s group=%s",
		broker,
		consumerGroup,
	)

	return client, nil
}

func generateInstanceID() string {
	buffer := make([]byte, 4)

	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf(
			"unknown-%d",
			time.Now().UnixNano(),
		)
	}

	return hex.EncodeToString(buffer)
}

func generateRequestID() (string, error) {
	buffer := make([]byte, 16)

	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf(
			"failed to generate request ID: %w",
			err,
		)
	}

	return hex.EncodeToString(buffer), nil
}

// requestService is the generic Kafka request/response mechanism
// used by both Auth and Math.
func (server *Server) requestService(
	ctx context.Context,
	topic string,
	requestType string,
	userID string,
	data json.RawMessage,
) (ServiceResponse, error) {

	requestID, err := generateRequestID()
	if err != nil {
		return ServiceResponse{}, err
	}

	responseChannel := make(chan ServiceResponse, 1)

	server.pendingMu.Lock()
	server.pending[requestID] = responseChannel
	server.pendingMu.Unlock()

	// Always remove the pending request when this HTTP request finishes.
	defer func() {
		server.pendingMu.Lock()
		delete(server.pending, requestID)
		server.pendingMu.Unlock()
	}()

	request := ServiceRequest{
		RequestID: requestID,
		Type:      requestType,
		UserID:    userID,
		Data:      data,
	}

	publishCtx, cancel := context.WithTimeout(
		ctx,
		5*time.Second,
	)
	defer cancel()

	if err := server.publishServiceRequest(
		publishCtx,
		topic,
		request,
	); err != nil {
		return ServiceResponse{}, err
	}

	responseCtx, cancel := context.WithTimeout(
		ctx,
		15*time.Second,
	)
	defer cancel()

	select {
	case response := <-responseChannel:
		return response, nil

	case <-responseCtx.Done():
		return ServiceResponse{}, fmt.Errorf(
			"service response timeout: request_id=%s: %w",
			requestID,
			responseCtx.Err(),
		)
	}
}

// Auth-specific wrapper.
func (server *Server) requestAuthService(
	ctx context.Context,
	requestType string,
	data json.RawMessage,
) (ServiceResponse, error) {

	return server.requestService(
		ctx,
		authRequestsTopic,
		requestType,
		"",
		data,
	)
}

// Math-specific wrapper.
func (server *Server) requestMathService(
	ctx context.Context,
	requestType string,
	userID string,
	data json.RawMessage,
) (ServiceResponse, error) {

	return server.requestService(
		ctx,
		mathRequestsTopic,
		requestType,
		userID,
		data,
	)
}

func (server *Server) publishServiceRequest(
	ctx context.Context,
	topic string,
	request ServiceRequest,
) error {

	value, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf(
			"failed to encode service request: %w",
			err,
		)
	}

	record := &kgo.Record{
		Topic: topic,

		// request_id is used as the Kafka key so all messages
		// for one request use the same partitioning key.
		Key: []byte(request.RequestID),

		Value: value,
	}

	if err := server.kafka.ProduceSync(
		ctx,
		record,
	).FirstErr(); err != nil {
		return fmt.Errorf(
			"failed to publish service request: %w",
			err,
		)
	}

	return nil
}

func (server *Server) consumeServiceResponses(
	ctx context.Context,
	client *kgo.Client,
) {

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
					"Kafka response consumer error: topic=%s partition=%d error=%v",
					err.Topic,
					err.Partition,
					err.Err,
				)
			}

			continue
		}

		fetches.EachRecord(
			func(record *kgo.Record) {

				if err := server.processServiceResponse(
					ctx,
					client,
					record,
				); err != nil {
					log.Printf(
						"failed to process service response: topic=%s partition=%d offset=%d error=%v",
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

func (server *Server) processServiceResponse(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
) error {

	var response ServiceResponse

	if err := json.Unmarshal(
		record.Value,
		&response,
	); err != nil {

		log.Printf(
			"invalid service response JSON: topic=%s partition=%d offset=%d",
			record.Topic,
			record.Partition,
			record.Offset,
		)

		// Commit malformed messages so one poison message
		// does not block the consumer forever.
		return server.commitServiceResponse(
			client,
			record,
		)
	}

	if response.RequestID == "" {
		log.Printf(
			"service response missing request_id: topic=%s partition=%d offset=%d",
			record.Topic,
			record.Partition,
			record.Offset,
		)

		return server.commitServiceResponse(
			client,
			record,
		)
	}

	server.deliverServiceResponse(response)

	return server.commitServiceResponse(
		client,
		record,
	)
}

func (server *Server) deliverServiceResponse(
	response ServiceResponse,
) {

	server.pendingMu.Lock()

	responseChannel, exists :=
		server.pending[response.RequestID]

	server.pendingMu.Unlock()

	if !exists {
		// The HTTP request may have already timed out, or the
		// response may be from an older request.
		log.Printf(
			"no pending HTTP request for request_id=%s",
			response.RequestID,
		)

		return
	}

	// Buffered channel means this never blocks the Kafka consumer.
	select {
	case responseChannel <- response:

	default:
		log.Printf(
			"response channel already contains a response: request_id=%s",
			response.RequestID,
		)
	}
}

func (server *Server) commitServiceResponse(
	client *kgo.Client,
	record *kgo.Record,
) error {

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := client.CommitRecords(
		ctx,
		record,
	); err != nil {
		return fmt.Errorf(
			"failed to commit service response: %w",
			err,
		)
	}

	return nil
}
