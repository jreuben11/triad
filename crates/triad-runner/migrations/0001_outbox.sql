CREATE SCHEMA IF NOT EXISTS triad;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE triad.triad_outbox (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        UUID         NOT NULL DEFAULT gen_random_uuid(),
    aggregate_type  TEXT,
    aggregate_id    TEXT,
    event_type      TEXT         NOT NULL,
    payload         JSONB        NOT NULL,
    status          TEXT         NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending', 'published')),
    kafka_topic     TEXT         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    attempt_count   INT          NOT NULL DEFAULT 0
);
CREATE INDEX ON triad.triad_outbox (status, id)
    WHERE status = 'pending';
