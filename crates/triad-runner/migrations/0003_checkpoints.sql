CREATE SCHEMA IF NOT EXISTS triad;

CREATE TABLE triad.triad_checkpoints (
    pattern_name        TEXT        NOT NULL,
    pipeline_name       TEXT        NOT NULL,
    owner_instance_id   TEXT        NOT NULL,
    version             BIGINT      NOT NULL DEFAULT 0,
    pg_lsn              PG_LSN,
    kafka_offsets       JSONB,
    redis_watermark     BIGINT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pattern_name, pipeline_name)
);
CREATE INDEX ON triad.triad_checkpoints (owner_instance_id);
