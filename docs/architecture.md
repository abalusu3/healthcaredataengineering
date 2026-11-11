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

## Silver contract (Phase 2)
`transform_silver` runs automatically when `ingest_bronze` updates the bronze **asset** (Airflow 3 data-aware scheduling).

```
ensure_ops_schema -> [silver_synthea, silver_reference] -> hospital_xref
                  -> readmission_flags -> load_warehouse -> smoke_checks
```

- **Schema contracts:** each bronze input has a `TableSpec` (`readmitiq/spark/schemas.py`). A missing source column raises `SchemaDriftError` and the job fails fast.
- **Typing:** bronze is read as strings and cast with `try_cast`. Values that fail to cast become quarantine reasons (`invalid_<col>`) instead of crashing the job.
- **Quarantine:** rows that break a rule go to `silver/_quarantine/<table>` with a `reject_reason`. Rules: required keys, unparseable values, `stop < start`, negative cost, FK orphans (child → member). Counts are appended to `ops.dq_results`, and the DAG fails if any table rejects more than 1%.
- **Data minimization:** direct identifiers (SSN, license, passport, names, street address, lat/lon) are dropped on the way into silver and never leave bronze.
- **Idempotent writes:** Delta `replaceWhere` per `source_state` partition, so reloading one state never touches the others.
- **Lineage columns:** `_source_object`, `_ingest_date`, `_processed_at`.

### Readmission definition (`readmitiq/spark/transforms/readmissions.py`)
A simplified version of the CMS Hospital-Wide Readmission measure:
- **Admission episodes (gaps-and-islands):** a member's encounters are merged into one hospitalization when the next admit starts less than 6 hours after the latest discharge so far, or overlaps it. This covers ICU → ward at the same hospital and transfers between hospitals. An episode is attributed to the *discharging* hospital, as CMS does for transfers. Without this step, 77% of flagged "readmissions" were actually day-0 level-of-care changes.
- **Planned episodes** (chemo/radiation, transplant, rehab, elective sterilization) never count as readmissions.
- **Index-eligible episode:** discharged alive, not a cancer-treatment episode, and not right-censored (discharged at least 30 days before the end of the data).
- **Readmission:** the next *unplanned* episode admitted 0–30 days (calendar days, inclusive) after discharge, at any hospital. It is computed with one window pass (a `min(struct)` over following rows), with no self-join.
- **HRRP cohort:** a procedure (CABG, THA/TKA) takes precedence over the admission reason (AMI, HF, PN, COPD).

### Entity resolution (`hospital_xref`)
Synthea organization → CMS CCN. Candidates are blocked by state + ZIP5 or state + city, then scored as 0.6 × name similarity + 0.4 × address similarity (normalized Levenshtein, Spark built-ins only). Tiers: `exact` ≥ 0.90; `probable` ≥ 0.75, **or** same ZIP5 with address similarity ≥ 0.85; otherwise `unmatched`. The address rule covers a legal-entity name differing from the public "doing business as" name, e.g. *Prime Healthcare Services – Blue Springs* = *St Mary's Medical Center*. `match_method` records which rule matched.

Current coverage: 41.6% of inpatient stays match a CMS hospital (22.0% exact, 19.6% probable). Most of the rest occur at Synthea facilities that aren't CMS acute-care hospitals (nursing centers, clinics, VA). Only 65% of stays are even in a ZIP code that has a CMS hospital.

## Local services
| Service | Port | Purpose |
|---|---|---|
| Airflow API server / UI | 8080 | Orchestration |
| Spark master UI | 8081 | Standalone cluster (1 worker, 4 cores / 3 GB) |
| Postgres 16 | 5432 | Airflow metadata DB + `readmitiq` warehouse DB |
| SeaweedFS S3 | 8333 | Object storage for the lake |
