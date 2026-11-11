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

## Gold contract (Phase 3): dbt + Great Expectations
`build_marts` runs automatically when `transform_silver` reloads `raw.*` (asset-triggered): `dbt deps → dbt build → gx_checks → dbt docs generate`.

```
raw.* ─► staging (views, 1:1 typed/renamed)
       ─► intermediate (LACE components: Charlson index, ED use, LACE points)
       ─► marts: dim_member · dim_hospital · dim_payer · dim_date · fct_admissions
                 mart_hospital_scorecard · mart_readmission_trends · mart_member_outreach
                 mart_sdoh_readmission · mart_savings_scenario
seeds/charlson_condition_map ─┘     snapshots/cms_hospitals_snapshot (SCD2) ─► dim_hospital
```

| Model | Grain | Business question |
|---|---|---|
| `fct_admissions` | admission episode | Every readmission KPI; LACE score (0–19) + risk tier |
| `mart_hospital_scorecard` | hospital | Which network hospitals readmit more than their case mix predicts? |
| `mart_readmission_trends` | month × LOB × state × cohort | Where and when are we losing money? |
| `mart_member_outreach` | member | Who should care management call today? |
| `mart_sdoh_readmission` | county (FIPS) | Does social vulnerability drive readmissions? |
| `mart_savings_scenario` | LACE risk tier | Does a transitional-care program pay for itself? |

**Risk adjustment:** the scorecard uses *indirect standardization*. Expected readmissions = Σ over a hospital's index admissions of the plan-wide rate for that admission's LACE tier. The scorecard reports O/E (observed / expected) instead of a raw rate, so hospitals treating sicker members aren't penalized for case mix. O/E is suppressed below 20 index admissions, because ratios are meaningless at tiny volume. Great Expectations caught that issue on its first run.

**LACE index** (van Walraven 2010): **L**ength of stay points + **A**cuity (ED visit within 24 hours before admit = 3) + **C**harlson comorbidity points (from conditions active on the admit date; the code→category map is a dbt seed, with hierarchy rules) + **E**D visits in the prior 6 months. It is validated in the data: readmission rate rises monotonically from the low to the moderate to the high tier.

**Two kinds of tests:**
- **dbt tests (structure):** unique/not-null keys, FK relationships fact→dims, accepted values, value ranges, and 4 singular business-rule tests (a readmission implies the stay was index-eligible, the readmission falls after discharge, planned stays are never counted as readmissions, LACE components stay within their point ranges).
- **Great Expectations (plausibility, `readmitiq/quality/gx_checks.py`):** volumes, distributions (mean LACE), and value ranges on marts, with entity-resolution regression guards. Every result is appended to `ops.gx_results`, so data-quality history can be queried.

## Local services
| Service | Port | Purpose |
|---|---|---|
| Airflow API server / UI | 8080 | Orchestration |
| Spark master UI | 8081 | Standalone cluster (1 worker, 4 cores / 3 GB) |
| Postgres 16 | 5432 | Airflow metadata DB + `readmitiq` warehouse DB |
| SeaweedFS S3 | 8333 | Object storage for the lake |
