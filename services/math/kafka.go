package main

import (
	"context"
	"encoding/json"

	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultKafkaBroker = "localhost:9092"

	mathRequestsTopic  = "math.requests"
	mathResponsesTopic = "math.responses"
	mathConsumerGroup  = "math-service"
)

type MathRequest struct {
	RequestID string          `json:"request_id"`
	Type      string          `json:"type"`
	UserID    string          `json:"user_id"`
	Data      json.RawMessage `json:"data"`
}

type MathResponse struct {
	RequestID string `json:"request_id"`
	Success   bool   `json:"success"`
	Code      string `json:"code"`
	Data      any    `json:"data"`
}

func newMathKafkaClient() (*kgo.Client, error) {
	broker := os.Getenv("KAFKA_BROKER")

	if broker == "" {
		broker = defaultKafkaBroker
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(mathConsumerGroup),
		kgo.ConsumeTopics(mathRequestsTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create Math Kafka client: %w",
			err,
		)
	}

	log.Printf(
		"Math Kafka consumer started: topic=%s group=%s",
		mathRequestsTopic,
		mathConsumerGroup,
	)

	return client, nil
}

func (server *MathServer) consumeMathRequests(
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
					"Math Kafka consumer error: topic=%s partition=%d error=%v",
					err.Topic,
					err.Partition,
					err.Err,
				)
			}

			continue
		}

		fetches.EachRecord(
			func(record *kgo.Record) {
				if err := server.processMathRequest(
					ctx,
					client,
					record,
				); err != nil {
					log.Printf(
						"failed to process math request: topic=%s partition=%d offset=%d error=%v",
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

func (server *MathServer) processMathRequest(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
) error {
	var request MathRequest

	if err := json.Unmarshal(
		record.Value,
		&request,
	); err != nil {
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				Success: false,
				Code:    "INVALID_REQUEST",
				Data:    nil,
			},
		)
	}

	if request.RequestID == "" ||
		request.UserID == "" {
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "INVALID_REQUEST",
				Data:      nil,
			},
		)
	}

	switch request.Type {

	case "CALCULATE":
		return server.handleCalculate(
			ctx,
			client,
			record,
			request,
		)

	case "HISTORY":
		return server.handleHistory(
			ctx,
			client,
			record,
			request,
		)
	case "DELETE_HISTORY":
    return server.handleDeleteHistory(
        ctx,
        client,
        record,
        request,
    )


	default:
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "UNSUPPORTED_REQUEST",
				Data:      nil,
			},
		)
	}
}

func (server *MathServer) handleCalculate(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
	request MathRequest,
) error {
	var calculationRequest CalculationRequest

	if err := json.Unmarshal(
		request.Data,
		&calculationRequest,
	); err != nil {
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "INVALID_REQUEST",
				Data:      nil,
			},
		)
	}

	result, code := calculate(
		calculationRequest,
	)

	if code != "" {
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      code,
				Data:      nil,
			},
		)
	}

	createdAt := time.Now().UTC()

	saved, err := server.mongo.saveCalculation(
		ctx,
		CalculationRecord{
			RequestID: request.RequestID,
			UserID:    request.UserID,
			Operation: calculationRequest.Operation,
			A:         calculationRequest.A,
			B:         calculationRequest.B,
			Result:    result,
			CreatedAt: createdAt,
		},
	)

	if err != nil {
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "HISTORY_SAVE_FAILED",
				Data:      nil,
			},
		)
	}

	return server.respondAndCommit(
		ctx,
		client,
		record,
		MathResponse{
			RequestID: request.RequestID,
			Success:   true,
			Code:      "CALCULATION_SUCCESS",
			Data: CalculationData{
				Operation: saved.Operation,
				A:         saved.A,
				B:         saved.B,
				Result:    saved.Result,
			},
		},
	)
}

func (server *MathServer) handleHistory(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
	request MathRequest,
) error {
	var historyRequest HistoryRequest

	if len(request.Data) > 0 {
		if err := json.Unmarshal(
			request.Data,
			&historyRequest,
		); err != nil {
			return server.respondAndCommit(
				ctx,
				client,
				record,
				MathResponse{
					RequestID: request.RequestID,
					Success:   false,
					Code:      "INVALID_REQUEST",
					Data:      nil,
				},
			)
		}
	}

	limit := historyRequest.Limit

	if limit <= 0 {
		limit = 20
	}

	if limit > 100 {
		limit = 100
	}

	records, err := server.mongo.listCalculations(
		ctx,
		request.UserID,
		int64(limit),
	)
	if err != nil {
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "HISTORY_READ_FAILED",
				Data:      nil,
			},
		)
	}

	items := make(
		[]HistoryItem,
		0,
		len(records),
	)

	for _, record := range records {
		items = append(
			items,
			HistoryItem{
				Operation: record.Operation,
				A:         record.A,
				B:         record.B,
				Result:    record.Result,
				CreatedAt: record.CreatedAt,
			},
		)
	}

	return server.respondAndCommit(
		ctx,
		client,
		record,
		MathResponse{
			RequestID: request.RequestID,
			Success:   true,
			Code:      "HISTORY_SUCCESS",
			Data:      items,
		},
	)
}

func calculate(
	request CalculationRequest,
) (float64, string) {
	switch request.Operation {

	case "add":
		return request.A + request.B, ""

	case "subtract":
		return request.A - request.B, ""

	case "multiply":
		return request.A * request.B, ""

	case "divide":
		if request.B == 0 {
			return 0, "DIVISION_BY_ZERO"
		}

		return request.A / request.B, ""

	default:
		return 0, "INVALID_OPERATION"
	}
}

func (server *MathServer) respondAndCommit(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
	response MathResponse,
) error {

	value, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf(
			"failed to encode Math response: %w",
			err,
		)
	}

	responseCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	outgoing := &kgo.Record{
		Topic: mathResponsesTopic,
		Key:   []byte(response.RequestID),
		Value: value,
	}

	if err := client.ProduceSync(
		responseCtx,
		outgoing,
	).FirstErr(); err != nil {
		return fmt.Errorf(
			"failed to produce Math response: %w",
			err,
		)
	}

	commitCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := client.CommitRecords(
		commitCtx,
		record,
	); err != nil {
		return fmt.Errorf(
			"failed to commit Math request: %w",
			err,
		)
	}

	return nil
}
func (server *MathServer) handleDeleteHistory(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
	request MathRequest,
) error {

	_, err := server.mongo.deleteCalculations(
		ctx,
		request.UserID,
	)

	if err != nil {
		return server.respondAndCommit(
			ctx,
			client,
			record,
			MathResponse{
				RequestID: request.RequestID,
				Success:   false,
				Code:      "HISTORY_DELETE_FAILED",
				Data:      nil,
			},
		)
	}

	return server.respondAndCommit(
		ctx,
		client,
		record,
		MathResponse{
			RequestID: request.RequestID,
			Success:   true,
			Code:      "HISTORY_CLEARED",
			Data:      nil,
		},
	)
}