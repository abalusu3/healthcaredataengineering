#!/usr/bin/env bash
# Runs once, on first container start, as the postgres superuser.
# Creates two isolated databases on one server:
#   airflow    -> Airflow metadata
#   readmitiq  -> analytics warehouse (raw/staging/marts schemas + ops audit)
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
  CREATE ROLE airflow LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
  CREATE DATABASE airflow OWNER airflow;

  CREATE ROLE ${WAREHOUSE_USER} LOGIN PASSWORD '${WAREHOUSE_PASSWORD}';
  CREATE DATABASE ${WAREHOUSE_DB} OWNER ${WAREHOUSE_USER};
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$WAREHOUSE_DB" <<-SQL
  SET ROLE ${WAREHOUSE_USER};

  -- Medallion-aligned schemas. Bronze lives in object storage; silver lands in raw,
  -- dbt builds staging -> intermediate -> marts.
  CREATE SCHEMA IF NOT EXISTS ops;
  CREATE SCHEMA IF NOT EXISTS raw;
  CREATE SCHEMA IF NOT EXISTS staging;
  CREATE SCHEMA IF NOT EXISTS intermediate;
  CREATE SCHEMA IF NOT EXISTS marts;

  -- Lineage/audit trail for every file landed in the bronze layer.
  CREATE TABLE IF NOT EXISTS ops.ingestion_log (
      ingestion_id    BIGSERIAL PRIMARY KEY,
      run_id          TEXT        NOT NULL,
      source          TEXT        NOT NULL,
      dataset         TEXT        NOT NULL,
      object_key      TEXT        NOT NULL,
      source_uri      TEXT        NOT NULL,
      sha256          CHAR(64)    NOT NULL,
      size_bytes      BIGINT      NOT NULL,
      line_count      BIGINT,
      status          TEXT        NOT NULL CHECK (status IN ('loaded', 'unchanged', 'failed')),
      error_message   TEXT,
      ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS ix_ingestion_log_dataset_time
      ON ops.ingestion_log (source, dataset, ingested_at DESC);
SQL
