package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoStore struct {
	client     *mongo.Client
	collection *mongo.Collection
}

type CalculationRecord struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	RequestID string        `bson:"request_id"`
	UserID    string        `bson:"user_id"`
	Operation string        `bson:"operation"`
	A         float64       `bson:"a"`
	B         float64       `bson:"b"`
	Result    float64       `bson:"result"`
	CreatedAt time.Time     `bson:"created_at"`
}

func connectMongo() (*MongoStore, error) {
	uri := os.Getenv("MONGODB_URI")

	if uri == "" {
		uri = "mongodb://127.0.0.1:27018"
	}

	databaseName := os.Getenv("MONGODB_DATABASE")

	if databaseName == "" {
		databaseName = "calculator_db"
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	serverAPI := options.ServerAPI(
		options.ServerAPIVersion1,
	)

	client, err := mongo.Connect(
		options.Client().
			ApplyURI(uri).
			SetServerAPIOptions(serverAPI),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create MongoDB client: %w",
			err,
		)
	}

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())

		return nil, fmt.Errorf(
			"failed to ping MongoDB: %w",
			err,
		)
	}

	collection := client.
		Database(databaseName).
		Collection("calculation_history")

	store := &MongoStore{
		client:     client,
		collection: collection,
	}

	if err := store.createIndexes(ctx); err != nil {
		_ = client.Disconnect(context.Background())

		return nil, err
	}

	return store, nil
}

func (store *MongoStore) createIndexes(
	ctx context.Context,
) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "request_id", Value: 1},
			},
			Options: options.Index().
				SetUnique(true),
		},
		{
			Keys: bson.D{
				{Key: "user_id", Value: 1},
				{Key: "created_at", Value: -1},
			},
		},
	}

	_, err := store.collection.Indexes().CreateMany(
		ctx,
		indexes,
	)

	if err != nil {
		return fmt.Errorf(
			"failed to create MongoDB indexes: %w",
			err,
		)
	}

	return nil
}

func (store *MongoStore) saveCalculation(
	ctx context.Context,
	record CalculationRecord,
) (CalculationRecord, error) {
	filter := bson.M{
		"request_id": record.RequestID,
	}

	update := bson.M{
		"$setOnInsert": record,
	}

	_, err := store.collection.UpdateOne(
		ctx,
		filter,
		update,
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return CalculationRecord{}, fmt.Errorf(
			"failed to save calculation: %w",
			err,
		)
	}

	var saved CalculationRecord

	err = store.collection.FindOne(
		ctx,
		filter,
	).Decode(&saved)

	if err != nil {
		return CalculationRecord{}, fmt.Errorf(
			"failed to load saved calculation: %w",
			err,
		)
	}

	return saved, nil
}

func (store *MongoStore) listCalculations(
	ctx context.Context,
	userID string,
	limit int64,
) ([]CalculationRecord, error) {
	cursor, err := store.collection.Find(
		ctx,
		bson.M{
			"user_id": userID,
		},
		options.Find().
			SetSort(
				bson.D{
					{Key: "created_at", Value: -1},
				},
			).
			SetLimit(limit),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to query calculation history: %w",
			err,
		)
	}

	defer cursor.Close(ctx)

	var records []CalculationRecord

	if err := cursor.All(
		ctx,
		&records,
	); err != nil {
		return nil, fmt.Errorf(
			"failed to decode calculation history: %w",
			err,
		)
	}

	return records, nil
}
func (store *MongoStore) deleteCalculations(
	ctx context.Context,
	userID string,
) (int64, error) {

	result, err := store.collection.DeleteMany(
		ctx,
		bson.M{
			"user_id": userID,
		},
	)

	if err != nil {
		return 0, fmt.Errorf(
			"failed to delete calculations: %w",
			err,
		)
	}

	return result.DeletedCount, nil
}
