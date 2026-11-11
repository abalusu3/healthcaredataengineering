-- Idempotent DDL for operational tables (applied by the transform_silver DAG).
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS ops.dq_results (
    job            TEXT        NOT NULL,
    table_name     TEXT        NOT NULL,
    valid_rows     BIGINT      NOT NULL,
    rejected_rows  BIGINT      NOT NULL,
    run_ts         TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dq_results_table_time ON ops.dq_results (table_name, run_ts DESC);
