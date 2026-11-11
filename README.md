# ReadmitIQ — Payer-Side Readmission Analytics

An end-to-end data platform that helps a health plan cut the cost of **avoidable 30-day hospital readmissions**. It joins a synthetic member population (claims and EHR) with public CMS hospital-penalty data and CDC social-vulnerability data to answer four questions:

| Business question | Output |
|---|---|
| Where are we losing money to readmissions? | Readmission rate and cost by hospital, condition, and region |
| Which network hospitals underperform? | Hospital scorecard: member outcomes + CMS Excess Readmission Ratio |
| Which members should care management call today? | LACE-scored risk tiers and an outreach list |
| Do social determinants drive readmissions? | Readmission rate vs. county Social Vulnerability Index |
| *(real-time)* Which high-risk member was just discharged? | Streaming ADT alerts for 48-hour follow-up |

## Architecture

```
LOCAL (docker compose)                                              CLOUD
 Synthea ─┐
 CMS API ─┼─► S3 lake (bronze) ─► PySpark/Delta (silver) ─► Postgres ─► dbt marts
 CDC SVI ─┘          ▲                                          │   Great Expectations
                     └──────────── Airflow orchestrates ────────┘
                                                                │ publish (de-identified)
                                                                ▼
                                    Neon Postgres ◄── FastAPI ◄── Next.js  (Vercel)
```

Heavy compute runs locally. Only curated, tokenized marts are published to the cloud. See [docs/architecture.md](docs/architecture.md) and the [ADRs](docs/adr/).

## Stack
**Orchestration** Airflow 3.3 · **Processing** PySpark 4.1 + Delta Lake 4.4 · **Storage** S3-compatible lake (SeaweedFS locally), PostgreSQL 16 · **Modeling** dbt · **Quality** Great Expectations, dbt tests · **Serving** FastAPI, Next.js on Vercel, Neon · **Infra** Docker Compose, Terraform, GitHub Actions

## Quickstart

Prerequisites: Docker Desktop (8 GB+ RAM allocated) and Python 3.11.

```bash
make venv && make test     # unit tests (no Docker needed)
make up                    # postgres, seaweedfs, spark, airflow
                           #   Airflow http://localhost:8080 (admin/admin) · Spark http://localhost:8081
make synthea               # generate synthetic members (3 states x 3,000 by default)
make ingest                # ingest_bronze -> transform_silver runs automatically (asset-triggered)
make transform             # re-run bronze -> silver -> warehouse only
make marts                 # re-run dbt + Great Expectations only
make venv-analytics && make dbt   # optional: run dbt locally against localhost:5432
make psql                  # e.g. SELECT * FROM ops.dq_results; SELECT * FROM raw.inpatient_stays;
```

## Pipeline (current)
| DAG | What it does |
|---|---|
| `ingest_bronze` | Lands CMS, CDC, and Synthea files in the S3 lake, sha256-audited and idempotent |
| `transform_silver` | PySpark on a Spark cluster: schema contracts, typed Delta tables, quarantine, PII minimization, hospital entity resolution, readmission flags, then loads Postgres `raw.*` and runs smoke checks |
| `build_marts` | dbt (seeds, SCD2 snapshot, staging → intermediate → marts, 99 tests), then Great Expectations checks and dbt docs |

**Readmission logic:** contiguous stays (ICU → ward, transfers) are merged into admission episodes with a gaps-and-islands step. Planned readmissions (chemo, transplant, rehab, elective procedures) are excluded, and index stays near the end of the data are censored. The result is a simplified version of the CMS Hospital-Wide Readmission measure. See [docs/architecture.md](docs/architecture.md).

> **Data caveat:** Synthea generates far fewer unplanned readmissions than real claims do. After correct exclusions, the synthetic 30-day rate is ~1.3%, versus ~14% all-cause in Medicare FFS. The pipeline logic is unit-tested independently of this rate.

## Data sources
| Source | What | Volume |
|---|---|---|
| [Synthea](https://github.com/synthetichealth/synthea) | Synthetic members: encounters, conditions, procedures, meds, payers | configurable (default 9k members) |
| [CMS HRRP](https://data.cms.gov/provider-data/dataset/9n3s-kdb3) | Excess readmission ratio, hospital × condition | ~18k rows |
| [CMS Hospital General Info](https://data.cms.gov/provider-data/dataset/xubh-q36u) | Hospital attributes, star ratings | ~5.4k hospitals |
| [CDC/ATSDR SVI](https://www.atsdr.cdc.gov/place-health/php/svi/) | County social vulnerability | ~3.1k counties |

All member data is **synthetic**. No real PHI is used anywhere.

## Status
- [x] **Phase 1 — Foundation:** Compose stack, Synthea generator, audited and idempotent bronze ingestion DAG
- [x] **Phase 2 — Bronze → silver:** Spark 4.1 + Delta cluster, schema contracts, quarantine + DQ metrics, entity resolution, episode-based readmission flags, Postgres load (30 unit tests)
- [x] **Phase 3 — Gold layer:** dbt star schema + 5 business marts, LACE risk scoring, SCD2 hospital snapshot, risk-adjusted (O/E) hospital scorecard, 99 dbt tests + 17 Great Expectations checks
- [ ] Phase 4 — FastAPI + Next.js on Vercel, Terraform (Neon)
- [ ] Phase 5 — CI/CD (GitHub Actions, security scans)
- [ ] Phase 6 — Real-time ADT streaming
- [ ] Phase 7 — ML risk model, Databricks/Snowflake portability, findings memo
