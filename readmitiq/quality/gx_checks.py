"""Business-level data-quality checks on the marts, defined as code (Great Expectations 1.x).

dbt tests guard *structure* (keys, relationships, accepted values). These suites guard
*plausibility*: volumes, distributions and value ranges a reviewer would sanity-check before
trusting a dashboard. Every expectation result is appended to ops.gx_results so data-quality
history is queryable (and can be charted next to the business KPIs).

Run:  python -m readmitiq.quality.gx_checks        (exit code 1 if any suite fails)
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

import great_expectations as gx
import great_expectations.expectations as gxe

from readmitiq.config import Settings

# (schema, table) -> expectations
SUITES: dict[tuple[str, str], list] = {
    ("marts", "fct_admissions"): [
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000),
        gxe.ExpectColumnValuesToNotBeNull(column="lace_score"),
        gxe.ExpectColumnValuesToBeBetween(
            column="los_days", min_value=0, max_value=365, mostly=0.999
        ),
        gxe.ExpectColumnValuesToBeBetween(
            column="total_claim_cost", min_value=0, max_value=5_000_000, mostly=0.999
        ),
        gxe.ExpectColumnMeanToBeBetween(column="lace_score", min_value=1, max_value=12),
        gxe.ExpectColumnValuesToBeInSet(
            column="payer_type",
            value_set=["Medicare", "Medicaid", "Dual", "Uninsured", "Commercial", "Unknown"],
        ),
        gxe.ExpectColumnValuesToBeInSet(
            column="hrrp_cohort", value_set=["AMI", "HF", "PN", "COPD", "CABG", "THA_TKA", "Other"]
        ),
    ],
    ("marts", "mart_hospital_scorecard"): [
        gxe.ExpectTableRowCountToBeBetween(min_value=10),
        gxe.ExpectColumnValuesToBeBetween(column="observed_rate", min_value=0, max_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="oe_ratio", min_value=0, max_value=25),
        gxe.ExpectColumnValuesToBeBetween(column="cms_star_rating", min_value=1, max_value=5),
    ],
    ("marts", "mart_savings_scenario"): [
        gxe.ExpectTableRowCountToEqual(value=3),
        # Synthea under-generates readmissions; this band catches broken logic, not clinical drift.
        gxe.ExpectColumnValuesToBeBetween(column="readmit_rate", min_value=0.001, max_value=0.35),
    ],
    ("marts", "mart_readmission_trends"): [
        gxe.ExpectColumnValuesToBeBetween(column="readmit_rate", min_value=0, max_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="index_admissions", min_value=1),
    ],
    ("marts", "dim_hospital"): [
        gxe.ExpectColumnValuesToBeInSet(
            column="match_tier", value_set=["exact", "probable", "unmatched"]
        ),
        # Entity resolution regression guard: at least some orgs must resolve to a CCN.
        gxe.ExpectColumnValuesToNotBeNull(column="ccn", mostly=0.03),
    ],
}

RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS ops.gx_results (
    run_ts        TIMESTAMPTZ NOT NULL,
    table_name    TEXT        NOT NULL,
    expectation   TEXT        NOT NULL,
    column_name   TEXT,
    success       BOOLEAN     NOT NULL,
    observed      TEXT
)
"""


def _connection_string(s: Settings) -> str:
    return (
        f"postgresql+psycopg2://{s.warehouse_user}:{s.warehouse_password}"
        f"@{s.warehouse_host}:{s.warehouse_port}/{s.warehouse_db}"
    )


def run() -> bool:
    settings = Settings.from_env()
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_postgres(
        name="warehouse", connection_string=_connection_string(settings)
    )
    run_ts = datetime.now(UTC)
    rows: list[tuple] = []
    all_ok = True

    for (schema, table), expectations in SUITES.items():
        asset = source.add_table_asset(name=table, table_name=table, schema_name=schema)
        batch = asset.add_batch_definition_whole_table(name=f"{table}_all").get_batch()
        suite = context.suites.add(gx.ExpectationSuite(name=f"{table}_suite"))
        for exp in expectations:
            suite.add_expectation(exp)
        result = batch.validate(suite)
        all_ok &= result.success

        for r in result.results:
            cfg = r.expectation_config
            observed = r.result.get("observed_value", r.result.get("unexpected_percent"))
            rows.append(
                (
                    run_ts,
                    f"{schema}.{table}",
                    cfg.type,
                    cfg.kwargs.get("column"),
                    bool(r.success),
                    None if observed is None else str(observed)[:500],
                )
            )
            mark = "PASS" if r.success else "FAIL"
            col = cfg.kwargs.get("column") or "<table>"
            print(f"  {mark}  {schema}.{table:<26} {cfg.type:<45} {col:<20} observed={observed}")

    _persist(settings, rows)
    print(f"Great Expectations: {sum(r[4] for r in rows)}/{len(rows)} expectations passed")
    return all_ok


def _persist(settings: Settings, rows: list[tuple]) -> None:
    import psycopg2
    from psycopg2.extras import execute_values

    with psycopg2.connect(settings.warehouse_dsn()) as conn, conn.cursor() as cur:
        cur.execute(RESULTS_DDL)
        execute_values(
            cur,
            "INSERT INTO ops.gx_results (run_ts, table_name, expectation, column_name, success,"
            " observed) VALUES %s",
            rows,
        )


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
