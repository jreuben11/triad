CREATE SCHEMA IF NOT EXISTS triad;

CREATE TABLE triad.idempotency_keys (
    idempotency_key TEXT         PRIMARY KEY,
    pattern_name    TEXT         NOT NULL,
    response        JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ  NOT NULL
);
CREATE INDEX ON triad.idempotency_keys (expires_at);
