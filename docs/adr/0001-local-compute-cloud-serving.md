# ADR 0001: Run compute locally, serve curated marts from the cloud

**Status:** Accepted · 2026-11-10

## Context
The project must be publicly viewable on Vercel. Vercel runs static and serverless workloads only. It cannot host Airflow, Spark, or Kafka, and its function limits rule out long-running jobs.

## Decision
Run all processing (Airflow, Spark, streaming) in Docker Compose. A `publish` DAG copies only the de-identified marts to a managed serverless Postgres (Neon), provisioned with Terraform. FastAPI runs as Vercel Python functions next to a Next.js dashboard.

## Consequences
- The hosted demo costs nothing (free tiers) and is always on. The pipeline runs on demand.
- The cloud database contains no member-level identifiers, only tokenized IDs. This mirrors a production PHI boundary.
- The "real-time" view on the hosted site is a periodic snapshot. It is live only when the stack runs locally.
