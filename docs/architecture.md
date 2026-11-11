# Architecture

## Layers

| Layer | Where | Format | Owner | Contract |
|---|---|---|---|---|
| **Bronze** | `s3://readmitiq-lake/bronze/` | Source files as-is (CSV) | `ingest_bronze` DAG | Immutable, append-only; every file audited in `ops.ingestion_log` |
| **Silver** | `s3://readmitiq-lake/silver/` → `raw.*` in Postgres | Delta Lake | PySpark jobs | Typed, deduplicated, conformed; readmission flags computed |
| **Gold** | Postgres `staging` → `intermediate` → `marts` | Tables/views | dbt | Kimball star schema + business marts; tested |
| **Serving** | Neon Postgres (cloud) | Tables | `publish` DAG | Marts only, member IDs HMAC-tokenized |

## Bronze key layout
```
bronze/<source>/<dataset>/[<partition>=<value>/]ingest_date=YYYY-MM-DD/<file>
e.g. bronze/synthea/encounters/state=missouri/ingest_date=2026-11-10/encounters.csv
```
Hive-style partitions let Spark prune by state and by load date. The same layout works unchanged on S3, ADLS, or a Databricks volume.

## Ingestion guarantees
- **Lineage:** each file records its source URI, run ID, sha256, size, and line count in `ops.ingestion_log`. The same values are written to the object's S3 metadata.
- **Idempotency:** a file whose sha256 matches the last successful load is skipped and logged as `unchanged`.
- **Failure visibility:** failed uploads are logged with `status='failed'` before the task retries (exponential backoff).
- **Parallelism:** Synthea files are fanned out with Airflow dynamic task mapping, one task instance per (table, state).

## Local services
| Service | Port | Purpose |
|---|---|---|
| Airflow API server / UI | 8080 | Orchestration |
| Postgres 16 | 5432 | Airflow metadata DB + `readmitiq` warehouse DB |
| SeaweedFS S3 | 8333 | Object storage for the lake |
