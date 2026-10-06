package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func connectDatabase() (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")

	if databaseURL == "" {
		return nil, fmt.Errorf(
			"DATABASE_URL is not set",
		)
	}

	config, err := pgxpool.ParseConfig(
		databaseURL,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid DATABASE_URL: %w",
			err,
		)
	}

	config.MinConns = 1
	config.MaxConns = 10

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(
		ctx,
		config,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create PostgreSQL pool: %w",
			err,
		)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf(
			"failed to connect to PostgreSQL: %w",
			err,
		)
	}

	return pool, nil
}

type tokenStatus string

const (
	tokenValid   tokenStatus = "VALID"
	tokenInvalid tokenStatus = "INVALID"
	tokenExpired tokenStatus = "EXPIRED"
)

func lookupAccessToken(
	ctx context.Context,
	db *pgxpool.Pool,
	token string,
) (string, tokenStatus, error) {

	tokenHash := hashAccessToken(token)

	const query = `
		SELECT user_id::text, expires_at
		FROM access_tokens
		WHERE token_hash = $1
		LIMIT 1
	`

	var (
		userID    string
		expiresAt time.Time
	)

	err := db.QueryRow(
		ctx,
		query,
		tokenHash,
	).Scan(
		&userID,
		&expiresAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", tokenInvalid, nil
		}

		return "", tokenInvalid, fmt.Errorf(
			"failed to validate access token: %w",
			err,
		)
	}

	if !time.Now().UTC().Before(expiresAt) {
		return "", tokenExpired, nil
	}

	return userID, tokenValid, nil
}

func lookupAliasByUserID(
	ctx context.Context,
	db *pgxpool.Pool,
	userID string,
) (string, error) {

	const query = `
		SELECT alias_name
		FROM users
		WHERE id = $1::uuid
	`

	var aliasName string

	if err := db.QueryRow(
		ctx,
		query,
		userID,
	).Scan(&aliasName); err != nil {
		return "", fmt.Errorf(
			"failed to lookup user alias: %w",
			err,
		)
	}

	return aliasName, nil
}

func hashAccessToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))

	return sum[:]
}
