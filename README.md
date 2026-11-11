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
**Orchestration** Airflow 3.3 · **Processing** PySpark + Delta Lake · **Storage** S3-compatible lake (SeaweedFS locally), PostgreSQL 16 · **Modeling** dbt · **Quality** Great Expectations, dbt tests · **Serving** FastAPI, Next.js on Vercel, Neon · **Infra** Docker Compose, Terraform, GitHub Actions

## Quickstart

Prerequisites: Docker Desktop (8 GB+ RAM allocated) and Python 3.11.

```bash
make venv && make test     # unit tests (no Docker needed)
make up                    # postgres, seaweedfs, airflow  -> http://localhost:8080 (admin/admin)
make synthea               # generate synthetic members (3 states x 3,000 by default)
make ingest                # trigger the ingest_bronze DAG
make psql                  # then: SELECT * FROM ops.ingestion_log;
```

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
- [ ] Phase 2 — PySpark bronze→silver, readmission flags
- [ ] Phase 3 — dbt star schema, Great Expectations
- [ ] Phase 4 — FastAPI + Next.js on Vercel, Terraform (Neon)
- [ ] Phase 5 — CI/CD (GitHub Actions, security scans)
- [ ] Phase 6 — Real-time ADT streaming
- [ ] Phase 7 — ML risk model, Databricks/Snowflake portability, findings memo
