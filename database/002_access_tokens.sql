CREATE TABLE IF NOT EXISTS access_tokens (
    token_hash BYTEA PRIMARY KEY,

    user_id UUID NOT NULL
        REFERENCES users(id)
        ON DELETE CASCADE,

    expires_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS access_tokens_user_id_idx
    ON access_tokens(user_id);

CREATE INDEX IF NOT EXISTS access_tokens_expires_at_idx
    ON access_tokens(expires_at);