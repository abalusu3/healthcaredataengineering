"""Lake I/O: locate latest bronze files, read them with lineage, write Delta idempotently."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from readmitiq.spark.schemas import TableSpec, check_contract

log = logging.getLogger(__name__)

_PART_RE = re.compile(r"([A-Za-z_]+)=([^/]+)")


@dataclass(frozen=True)
class BronzeObject:
    key: str
    partitions: dict[str, str]

    @property
    def ingest_date(self) -> str:
        return self.partitions["ingest_date"]


def latest_bronze_objects(keys: list[str]) -> list[BronzeObject]:
    """Given all keys under bronze/<source>/<dataset>/, keep the newest ingest_date for each
    combination of the other partitions (e.g. one file per state)."""
    latest: dict[tuple, BronzeObject] = {}
    for key in keys:
        if not key.endswith(".csv"):
            continue
        parts = dict(_PART_RE.findall(key))
        if "ingest_date" not in parts:
            continue
        group = tuple(sorted((k, v) for k, v in parts.items() if k != "ingest_date"))
        obj = BronzeObject(key, parts)
        if group not in latest or obj.ingest_date > latest[group].ingest_date:
            latest[group] = obj
    return sorted(latest.values(), key=lambda o: o.key)


def read_bronze_csv(
    spark: SparkSession, bucket: str, objects: list[BronzeObject], spec: TableSpec
) -> DataFrame:
    """Read the given bronze CSVs as all-string columns and attach lineage columns.

    The `state=` path partition becomes `source_state` (Synthea's own STATE column would
    otherwise collide with it under Spark's case-insensitive resolution).
    """
    if not objects:
        raise FileNotFoundError(f"No bronze files found for {spec.name}")
    paths = [f"s3a://{bucket}/{o.key}" for o in objects]
    df = (
        spark.read.option("header", "true")
        .option("multiLine", "true")  # quoted fields may contain newlines
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(paths)
    )
    check_contract(df.columns, spec)

    # Lineage first: the hidden _metadata column only resolves directly on the file scan.
    df = df.withColumn("_source_object", F.col("_metadata.file_path"))
    path = F.col("_source_object")
    df = df.withColumn(
        "_ingest_date", F.regexp_extract(path, r"ingest_date=([0-9-]+)", 1).cast("date")
    )
    if "source_state" in spec.partition_by:
        df = df.withColumn("source_state", F.regexp_extract(path, r"/state=([^/]+)/", 1))
    # Normalise a BOM-prefixed first header so spec lookups by name work.
    df = df.toDF(*[c.lstrip("\ufeff") for c in df.columns])
    return df.withColumn("_processed_at", F.current_timestamp())


def write_delta(df: DataFrame, path: str, partition_by: tuple[str, ...] = ()) -> int:
    """Overwrite a Delta table. For partitioned tables only the partitions present in `df`
    are replaced (replaceWhere), so reloading one state never touches the others.

    If the table's schema changed (new transform version), replaceWhere can't be used: the
    whole table is rewritten with the new schema instead, and that is logged.
    """
    from delta.tables import DeltaTable

    spark = df.sparkSession
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    exists = DeltaTable.isDeltaTable(spark, path)

    def shape(schema):  # names + types only: Delta reports every column as nullable
        return [(f.name, f.dataType.simpleString()) for f in schema.fields]

    same_schema = exists and shape(spark.read.format("delta").load(path).schema) == shape(df.schema)
    if exists and not same_schema:
        log.warning("schema change detected for %s: full rewrite instead of replaceWhere", path)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
        if same_schema and len(partition_by) == 1:
            col = partition_by[0]
            values = [r[0] for r in df.select(col).distinct().collect()]
            quoted = ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)
            writer = writer.option("replaceWhere", f"{col} IN ({quoted})").option(
                "overwriteSchema", "false"
            )
    writer.save(path)
    count = spark.read.format("delta").load(path).count()
    log.info("wrote %s (%d rows total)", path, count)
    return count
