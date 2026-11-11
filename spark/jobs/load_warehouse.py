"""Load silver Delta tables into Postgres `raw.*` for dbt (Phase 3).

High-volume tables only needed for Spark-side logic (procedures, medications) stay in the
lake; everything the dimensional model needs is loaded.
"""

from __future__ import annotations

from readmitiq.spark.runtime import jdbc_write, start_job

TABLES = [
    "members",
    "encounters",
    "conditions",
    "payers",
    "payer_transitions",
    "organizations",
    "providers",
    "hrrp",
    "cms_hospitals",
    "svi_county",
    "hospital_xref",
    "inpatient_stays",
]


def main() -> None:
    job = start_job("load_warehouse")
    for table in TABLES:
        df = job.read_silver(table)
        # Drop the object-path lineage column; ingest date + processed_at remain.
        df = df.drop("_source_object")
        jdbc_write(df.repartition(4), job.settings, f"raw.{table}")
        rows = (
            job.spark.read.format("jdbc")
            .option("url", job.settings.warehouse_jdbc_url())
            .option("query", f"select count(*) as n from raw.{table}")  # noqa: S608 (fixed list)
            .option("user", job.settings.warehouse_user)
            .option("password", job.settings.warehouse_password)
            .option("driver", "org.postgresql.Driver")
            .load()
            .first()
            .n
        )
        job.metrics.append({"table": f"raw.{table}", "valid_rows": rows, "rejected_rows": 0})
        print(f"loaded raw.{table}: {rows} rows")
    job.record_metrics()
    job.spark.stop()


if __name__ == "__main__":
    main()
