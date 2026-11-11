"""Row-level validation: route bad rows to quarantine with explicit reasons."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from readmitiq.spark.schemas import TableSpec

REJECT_COL = "_reject_reasons"


def conform(df: DataFrame, spec: TableSpec) -> DataFrame:
    """Rename + type-cast bronze string columns per spec.

    Adds `_reject_reasons` (array<string>) listing every rule a row violates:
      * missing_<col>  for required columns that are null/blank
      * invalid_<col>  for non-blank values that failed to cast to the declared type
    Lineage / partition columns already on `df` (prefixed `_` or in partition_by) are kept.
    """
    keep = [c for c in df.columns if c.startswith("_") or c in spec.partition_by]
    selected: list[Column] = []
    reasons: list[Column] = []

    for col in spec.columns:
        raw = F.trim(F.col(f"`{col.src}`"))
        is_blank = raw.isNull() | raw.isin(*spec.null_tokens)
        cleaned = F.when(is_blank, F.lit(None)).otherwise(raw)
        typed = cleaned if col.dtype == "string" else cleaned.try_cast(col.dtype)
        selected.append(typed.alias(col.dst))

        if col.dtype != "string":
            reasons.append(
                F.when(cleaned.isNotNull() & typed.isNull(), F.lit(f"invalid_{col.dst}"))
            )
        if col.dst in spec.required:
            reasons.append(F.when(typed.isNull(), F.lit(f"missing_{col.dst}")))

    reason_arr = F.filter(F.array(*reasons), lambda x: x.isNotNull()) if reasons else F.array()
    return df.select(*[F.col(c) for c in keep], *selected, reason_arr.alias(REJECT_COL))


def add_rule(df: DataFrame, reason: str, is_bad: Column) -> DataFrame:
    """Append a custom rule. Null predicates count as 'not bad' (rule can't be evaluated)."""
    return df.withColumn(
        REJECT_COL,
        F.when(
            F.coalesce(is_bad, F.lit(False)),
            F.array_union(F.col(REJECT_COL), F.array(F.lit(reason))),
        ).otherwise(F.col(REJECT_COL)),
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows without the reasons column, quarantined rows with reasons)."""
    is_valid = F.size(REJECT_COL) == 0
    valid = df.filter(is_valid).drop(REJECT_COL)
    rejected = df.filter(~is_valid).withColumn("reject_reason", F.array_join(REJECT_COL, ","))
    return valid, rejected.drop(REJECT_COL)


def quarantine_orphans(
    child: DataFrame, parent: DataFrame, fk: str, pk: str, reason: str
) -> tuple[DataFrame, DataFrame]:
    """Split child rows whose FK has no matching parent PK (null FKs are left alone)."""
    keys = parent.select(F.col(pk).alias("__pk")).distinct()
    joined = child.join(keys, child[fk] == keys["__pk"], "left")
    orphan = F.col(fk).isNotNull() & F.col("__pk").isNull()
    valid = joined.filter(~orphan).drop("__pk")
    rejected = joined.filter(orphan).drop("__pk").withColumn("reject_reason", F.lit(reason))
    return valid, rejected


def dedupe(df: DataFrame, key: str, order_by: str = "_ingest_date") -> DataFrame:
    """Keep one row per key, preferring the most recently ingested."""
    from pyspark.sql.window import Window

    order = F.col(order_by).desc_nulls_last() if order_by in df.columns else F.lit(1)
    w = Window.partitionBy(key).orderBy(order)
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")
