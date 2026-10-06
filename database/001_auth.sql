CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    username VARCHAR(100) NOT NULL,

    alias_name VARCHAR(100) NOT NULL UNIQUE,

    encrypted_password TEXT NOT NULL,

    password_nonce TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_key
    ON users (LOWER(username));