CREATE SCHEMA IF NOT EXISTS triad;

CREATE TABLE triad_feature_flags (
    name                TEXT         PRIMARY KEY,
    enabled             BOOLEAN      NOT NULL DEFAULT false,
    rollout_percentage  SMALLINT     NOT NULL DEFAULT 0
                                     CHECK (rollout_percentage BETWEEN 0 AND 100),
    targeting_rules     JSONB        NOT NULL DEFAULT '{}',
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE triad.flag_audit (
    id          BIGSERIAL    PRIMARY KEY,
    flag_name   TEXT         NOT NULL REFERENCES triad_feature_flags (name),
    changed_by  TEXT,
    old_value   JSONB,
    new_value   JSONB,
    changed_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
