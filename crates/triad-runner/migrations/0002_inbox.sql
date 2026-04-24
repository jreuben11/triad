CREATE SCHEMA IF NOT EXISTS triad;

CREATE TABLE triad.triad_inbox (
    event_id        UUID         PRIMARY KEY,
    pattern_name    TEXT         NOT NULL,
    pipeline_name   TEXT         NOT NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ON triad.triad_inbox (pattern_name, received_at DESC);
