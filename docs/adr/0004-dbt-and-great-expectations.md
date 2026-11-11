# ADR 0004: dbt for the gold layer; dbt tests for structure, Great Expectations for plausibility

**Status:** Accepted · 2026-11-11

## Context
Silver is computed in Spark because the readmission logic needs windowed, high-volume transforms. Business-facing models (dimensions, facts, scorecards) change often, need to be readable by analysts, and need lineage and documentation. They also need two kinds of checks. One is structural integrity: keys and relationships. The other is plausibility: are the volumes and distributions believable?

## Decision
- **dbt (Postgres adapter)** owns gold. The layers are staging → intermediate → marts, with a Kimball star schema around `fct_admissions`. Hospital attributes use an SCD2 snapshot, and clinical code mappings (Charlson) are seeds.
- **dbt tests** cover structure: unique/not-null keys, relationships, accepted values, value ranges, and singular business-rule tests. `dbt build` runs them in DAG order, so a failing test stops downstream models from building on bad data.
- **Great Expectations** (suites defined as code) covers plausibility on the marts: volumes, distribution means, and value ranges. Every result is persisted to `ops.gx_results`.
- dbt and GX run from an **isolated virtualenv** inside the Airflow image, so their dependency trees never fight Airflow's pinned constraints.

## Consequences
- Analysts can change marts with SQL alone. `dbt docs` provides lineage.
- On its first run, GX caught a real modeling flaw: O/E ratios exploding at low hospital volume. The fix was a minimum-volume rule.
- Because the dbt project uses only standard SQL and `dbt_utils`, moving to Snowflake mainly means adding a profile target (Phase 7).
