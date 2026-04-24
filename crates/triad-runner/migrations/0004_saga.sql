CREATE SCHEMA IF NOT EXISTS triad;

CREATE TABLE triad.triad_saga_checkpoints (
    saga_id           UUID         PRIMARY KEY,
    saga_name         TEXT         NOT NULL,
    current_step      INT          NOT NULL DEFAULT 0,
    status            TEXT         NOT NULL DEFAULT 'Started',
    state             JSONB        NOT NULL DEFAULT '{}',
    compensation_mode BOOLEAN      NOT NULL DEFAULT false,
    version           BIGINT       NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ON triad.triad_saga_checkpoints (saga_name, updated_at DESC);

CREATE TABLE triad.triad_saga_steps (
    id          BIGSERIAL    PRIMARY KEY,
    saga_id     UUID         NOT NULL REFERENCES triad.triad_saga_checkpoints (saga_id),
    step_index  INT          NOT NULL,
    step_name   TEXT         NOT NULL,
    outcome     TEXT         NOT NULL,
    duration_ms BIGINT,
    recorded_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX ON triad.triad_saga_steps (saga_id, step_index);
