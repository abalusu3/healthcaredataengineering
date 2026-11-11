"""Synthea bronze -> silver cleaning."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from readmitiq.spark.schemas import TableSpec
from readmitiq.spark.transforms.quality import add_rule, conform, dedupe, split_valid


def _table_rules(df: DataFrame, spec: TableSpec) -> DataFrame:
    name = spec.name
    if name == "encounters":
        df = add_rule(df, "stop_before_start", F.col("stop_ts") < F.col("start_ts"))
        df = add_rule(
            df, "negative_cost", (F.col("total_claim_cost") < 0) | (F.col("base_cost") < 0)
        )
        df = df.withColumn("encounter_class", F.lower("encounter_class"))
    elif name == "members":
        df = add_rule(df, "death_before_birth", F.col("death_date") < F.col("birth_date"))
        df = df.withColumn("zip", F.substring(F.lpad("zip", 5, "0"), 1, 5))
        df = df.withColumn("county_fips", F.lpad("county_fips", 5, "0"))
    elif name in ("procedures", "medications", "conditions"):
        stop, start = (
            ("stop_date", "start_date") if name == "conditions" else ("stop_ts", "start_ts")
        )
        df = add_rule(df, "stop_before_start", F.col(stop) < F.col(start))
    elif name in ("organizations", "providers"):
        df = df.withColumn("zip5", F.substring(F.lpad("zip", 5, "0"), 1, 5))
    return df


def clean_synthea(df: DataFrame, spec: TableSpec) -> tuple[DataFrame, DataFrame]:
    """Conform + validate one Synthea table. Returns (valid, quarantined)."""
    typed = _table_rules(conform(df, spec), spec)
    valid, rejected = split_valid(typed)
    if spec.primary_key:
        valid = dedupe(valid, spec.primary_key)
    return valid, rejected
