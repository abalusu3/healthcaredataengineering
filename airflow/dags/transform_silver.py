"""Bronze -> silver (PySpark + Delta) -> Postgres raw.*

Triggered automatically when `ingest_bronze` updates the bronze asset (data-aware scheduling).

  ensure_ops_schema
        |
  [silver_synthea, silver_reference]   (parallel Spark jobs)
        |
  hospital_xref -> readmission_flags -> load_warehouse -> smoke_checks
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset, dag, task

BRONZE = Asset("s3://readmitiq-lake/bronze")
# Asset URIs for postgres must name db/schema/table; the fact table marks "raw is loaded".
WAREHOUSE_RAW = Asset("postgres://postgres:5432/readmitiq/raw/inpatient_stays")
JOBS = "/opt/airflow/spark/jobs"

# Driver runs in this Airflow container (client mode); executors must be able to call back.
SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",  # noqa: S104 (inside docker network)
    "spark.driver.memory": "1g",
    "spark.sql.adaptive.enabled": "true",
}


def spark_job(task_id: str, script: str, cores: int = 2, memory: str = "1500m", **kw):
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{JOBS}/{script}",
        conn_id="spark_default",
        name=f"readmitiq-{task_id}",
        conf=SPARK_CONF,
        total_executor_cores=cores,
        executor_cores=cores,
        executor_memory=memory,
        verbose=False,
        **kw,
    )


@dag(
    dag_id="transform_silver",
    schedule=[BRONZE],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["readmitiq", "silver", "spark"],
    doc_md=__doc__,
)
def transform_silver():
    @task
    def ensure_ops_schema() -> None:
        import psycopg2

        import readmitiq
        from readmitiq.config import Settings

        sql = (Path(readmitiq.__file__).parent / "sql" / "ops.sql").read_text()
        with psycopg2.connect(Settings.from_env().warehouse_dsn()) as conn, conn.cursor() as cur:
            cur.execute(sql)

    # The worker has 4 cores / 3 GB: the two parallel jobs split it.
    synthea = spark_job("silver_synthea", "silver_synthea.py", cores=2, memory="1500m")
    reference = spark_job("silver_reference", "silver_reference.py", cores=1, memory="1g")
    xref = spark_job("hospital_xref", "hospital_xref.py")
    flags = spark_job("readmission_flags", "readmission_flags.py", cores=3, memory="2g")
    load = spark_job(
        "load_warehouse", "load_warehouse.py", cores=2, memory="1500m", outlets=[WAREHOUSE_RAW]
    )

    @task
    def smoke_checks() -> None:
        """Cheap invariants that must hold before anything downstream runs."""
        import psycopg2

        from readmitiq.config import Settings

        with psycopg2.connect(Settings.from_env().warehouse_dsn()) as conn, conn.cursor() as cur:
            failures: list[str] = []
            for table in (
                "members",
                "encounters",
                "inpatient_stays",
                "hrrp",
                "cms_hospitals",
                "svi_county",
                "hospital_xref",
            ):
                cur.execute(f"SELECT count(*) FROM raw.{table}")  # noqa: S608 (fixed list)
                n = cur.fetchone()[0]
                print(f"raw.{table}: {n:,} rows")
                if n == 0:
                    failures.append(f"raw.{table} is empty")

            # Quarantine rate per table from the latest run of each job.
            cur.execute("""
                SELECT DISTINCT ON (table_name) table_name, valid_rows, rejected_rows
                FROM ops.dq_results ORDER BY table_name, run_ts DESC
            """)
            for table, valid, rejected in cur.fetchall():
                rate = rejected / max(valid + rejected, 1)
                print(f"dq {table}: valid={valid:,} rejected={rejected:,} ({rate:.2%})")
                if rate > 0.01:
                    failures.append(f"{table} quarantine rate {rate:.2%} > 1%")

            cur.execute("""
                SELECT avg(readmit_30d::int) FILTER (WHERE is_index_eligible)
                FROM raw.inpatient_stays
            """)
            rate = float(cur.fetchone()[0] or 0)
            print(f"30-day readmission rate: {rate:.2%}")
            # Guards against broken logic (0% = flags never set, >25% = episodes not merged),
            # not a clinical benchmark: Synthea under-generates unplanned readmissions vs the
            # ~14% real-world all-cause rate, so the lower bound is deliberately loose.
            if not 0.005 <= rate <= 0.25:
                failures.append(f"readmission rate {rate:.2%} outside 0.5-25% sanity band")

            if failures:
                raise ValueError("Smoke checks failed: " + "; ".join(failures))

    ensure_ops_schema() >> [synthea, reference] >> xref >> flags >> load >> smoke_checks()


transform_silver()
