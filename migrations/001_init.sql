CREATE TABLE IF NOT EXISTS payments (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    amount          BIGINT NOT NULL CHECK (amount > 0),
    currency        TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    provider_ref    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);