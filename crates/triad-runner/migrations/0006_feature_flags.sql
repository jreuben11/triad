CREATE SCHEMA IF NOT EXISTS triad;

CREATE TABLE triad.feature_flags (
    name            TEXT         PRIMARY KEY,
    enabled         BOOLEAN      NOT NULL DEFAULT false,
    rollout_pct     INT          NOT NULL DEFAULT 0 CHECK (rollout_pct BETWEEN 0 AND 100),
    config          JSONB        NOT NULL DEFAULT '{}',
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE triad.flag_audit (
    id          BIGSERIAL    PRIMARY KEY,
    flag_name   TEXT         NOT NULL REFERENCES triad.feature_flags (name),
    changed_by  TEXT,
    old_value   JSONB,
    new_value   JSONB,
    changed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
