CREATE SCHEMA IF NOT EXISTS triad;

CREATE TABLE triad.webhook_subscriptions (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_name    TEXT         NOT NULL,
    endpoint_url    TEXT         NOT NULL,
    event_types     TEXT[]       NOT NULL DEFAULT '{}',
    secret          TEXT,
    enabled         BOOLEAN      NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE triad.webhook_deliveries (
    id              BIGSERIAL    PRIMARY KEY,
    subscription_id UUID         NOT NULL REFERENCES triad.webhook_subscriptions (id),
    event_id        UUID         NOT NULL,
    attempt         INT          NOT NULL DEFAULT 1,
    status_code     INT,
    outcome         TEXT         NOT NULL,
    duration_ms     BIGINT,
    delivered_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ON triad.webhook_deliveries (subscription_id, delivered_at DESC);
