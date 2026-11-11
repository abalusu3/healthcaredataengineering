"""Shared plumbing for spark-submit job entrypoints."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession

from readmitiq.config import Settings
from readmitiq.spark.io import latest_bronze_objects, read_bronze_csv, write_delta
from readmitiq.spark.schemas import TableSpec
from readmitiq.spark.session import build_spark
from readmitiq.storage import Lake

log = logging.getLogger("readmitiq.job")


@dataclass
class JobContext:
    name: str
    settings: Settings
    spark: SparkSession
    lake: Lake
    metrics: list[dict] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def read_bronze(self, source: str, dataset: str, spec: TableSpec) -> DataFrame:
        keys = self.lake.list_keys(f"bronze/{source}/{dataset}/")
        objects = latest_bronze_objects(keys)
        log.info(
            "%s: reading %d bronze file(s) for %s/%s", self.name, len(objects), source, dataset
        )
        return read_bronze_csv(self.spark, self.lake.bucket, objects, spec)

    def read_silver(self, table: str) -> DataFrame:
        return self.spark.read.format("delta").load(self.settings.silver_path(table))

    def publish(
        self,
        table: str,
        valid: DataFrame,
        rejected: DataFrame | None = None,
        partition_by: tuple[str, ...] = (),
    ) -> None:
        """Write a silver table (+ its quarantine) and record DQ metrics."""
        valid_rows = write_delta(valid, self.settings.silver_path(table), partition_by)
        rejected_rows = 0
        if rejected is not None:
            rejected = rejected.cache()
            rejected_rows = rejected.count()
            # Quarantine is fully replaced each run so it always reflects the current load.
            rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
                self.settings.quarantine_path(table)
            )
            rejected.unpersist()
        self.metrics.append(
            {"table": table, "valid_rows": valid_rows, "rejected_rows": rejected_rows}
        )
        log.info("%s: %s valid=%d rejected=%d", self.name, table, valid_rows, rejected_rows)

    def record_metrics(self) -> None:
        """Append this run's DQ metrics to ops.dq_results in the warehouse."""
        if not self.metrics:
            return
        rows = [
            (self.name, m["table"], m["valid_rows"], m["rejected_rows"], self.started_at)
            for m in self.metrics
        ]
        df = self.spark.createDataFrame(
            rows,
            "job string, table_name string, valid_rows long, rejected_rows long, run_ts timestamp",
        )
        jdbc_write(df, self.settings, "ops.dq_results", mode="append")
        print("METRICS " + json.dumps(self.metrics))  # surfaced in the Airflow task log


def _jdbc_reader(spark: SparkSession, settings: Settings):
    return (
        spark.read.format("jdbc")
        .option("url", settings.warehouse_jdbc_url())
        .option("user", settings.warehouse_user)
        .option("password", settings.warehouse_password)
        .option("driver", "org.postgresql.Driver")
    )


def existing_columns(spark: SparkSession, settings: Settings, table: str) -> list[str] | None:
    """Column names of an existing Postgres table (None if it doesn't exist)."""
    schema, name = table.split(".")
    rows = (
        _jdbc_reader(spark, settings)
        .option(
            "query",
            "select column_name from information_schema.columns "  # noqa: S608 (internal names)
            f"where table_schema = '{schema}' and table_name = '{name}' order by ordinal_position",
        )
        .load()
        .collect()
    )
    return [r.column_name for r in rows] or None


def jdbc_write(df: DataFrame, settings: Settings, table: str, mode: str = "overwrite") -> None:
    # TRUNCATE keeps the table (and any dependent dbt views) instead of DROP/CREATE, but only
    # works while columns are unchanged; after a schema change, recreate the table.
    truncate = True
    if mode == "overwrite":
        current = existing_columns(df.sparkSession, settings, table)
        if current is not None and current != [c.lower() for c in df.columns]:
            log.warning("schema change for %s: dropping and recreating", table)
            truncate = False
    (
        df.write.format("jdbc")
        .option("url", settings.warehouse_jdbc_url())
        .option("dbtable", table)
        .option("user", settings.warehouse_user)
        .option("password", settings.warehouse_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", str(truncate).lower())
        .option("batchsize", "10000")
        .option("rewriteBatchedStatements", "true")
        .option("reWriteBatchedInserts", "true")
        .mode(mode)
        .save()
    )


def start_job(name: str) -> JobContext:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()
    spark = build_spark(f"readmitiq-{name}", settings)
    spark.sparkContext.setLogLevel("WARN")
    return JobContext(name=name, settings=settings, spark=spark, lake=Lake.from_settings(settings))
