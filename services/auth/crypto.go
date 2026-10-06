package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

func loadEncryptionKey() ([]byte, error) {
	keyString := os.Getenv("AUTH_ENCRYPTION_KEY")

	if keyString == "" {
		return nil, fmt.Errorf("AUTH_ENCRYPTION_KEY is not set")
	}

	key, err := base64.StdEncoding.DecodeString(keyString)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid AUTH_ENCRYPTION_KEY: %w",
			err,
		)
	}

	if len(key) != 32 {
		return nil, fmt.Errorf(
			"AUTH_ENCRYPTION_KEY must decode to exactly 32 bytes",
		)
	}

	return key, nil
}

func encryptPassword(
	password string,
	key []byte,
) (string, string, error) {

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", "", fmt.Errorf(
			"failed to create AES cipher: %w",
			err,
		)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", "", fmt.Errorf(
			"failed to create GCM: %w",
			err,
		)
	}

	nonce := make([]byte, gcm.NonceSize())

	if _, err := io.ReadFull(
		rand.Reader,
		nonce,
	); err != nil {
		return "", "", fmt.Errorf(
			"failed to generate nonce: %w",
			err,
		)
	}

	ciphertext := gcm.Seal(
		nil,
		nonce,
		[]byte(password),
		nil,
	)

	return base64.StdEncoding.EncodeToString(ciphertext),
		base64.StdEncoding.EncodeToString(nonce),
		nil
}

func decryptPassword(
	ciphertextBase64 string,
	nonceBase64 string,
	key []byte,
) (string, error) {

	ciphertext, err := base64.StdEncoding.DecodeString(
		ciphertextBase64,
	)
	if err != nil {
		return "", fmt.Errorf(
			"invalid encrypted password",
		)
	}

	nonce, err := base64.StdEncoding.DecodeString(
		nonceBase64,
	)
	if err != nil {
		return "", fmt.Errorf(
			"invalid password nonce",
		)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf(
			"failed to create AES cipher: %w",
			err,
		)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf(
			"failed to create GCM: %w",
			err,
		)
	}

	plaintext, err := gcm.Open(
		nil,
		nonce,
		ciphertext,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf(
			"failed to decrypt password",
		)
	}

	return string(plaintext), nil
}

func generateAccessToken() (string, error) {
	const tokenBytes = 32

	buffer := make([]byte, tokenBytes)

	if _, err := io.ReadFull(
		rand.Reader,
		buffer,
	); err != nil {
		return "", fmt.Errorf(
			"failed to generate access token: %w",
			err,
		)
	}

	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func hashAccessToken(token string) []byte {
	sum := sha256.Sum256([]byte(token))

	return sum[:]
}

func passwordsMatch(
	provided string,
	stored string,
) bool {
	return subtle.ConstantTimeCompare(
		[]byte(provided),
		[]byte(stored),
	) == 1
}
