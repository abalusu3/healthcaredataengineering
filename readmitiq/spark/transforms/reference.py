"""CMS / CDC reference data bronze -> silver cleaning."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from readmitiq.spark.codes import HRRP_MEASURE_COHORTS
from readmitiq.spark.schemas import CMS_HOSPITALS, HRRP, SVI_COUNTY
from readmitiq.spark.transforms.quality import conform, dedupe, split_valid


def _us_date(col: str):
    return F.try_to_timestamp(F.col(col), F.lit("MM/dd/yyyy")).cast("date")


def clean_hrrp(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    # Capture CMS suppression *before* conform() turns the token into null.
    df = df.withColumn(
        "_readmissions_suppressed",
        F.trim(F.col("`Number of Readmissions`")) == F.lit("Too Few to Report"),
    )
    typed = conform(df, HRRP)

    token = F.regexp_extract("measure_name", r"^READM-30-(.+)-HRRP$", 1)
    cohort = F.lit(None).cast("string")
    for src, dst in HRRP_MEASURE_COHORTS.items():
        cohort = F.when(token == src, F.lit(dst)).otherwise(cohort)

    typed = (
        typed.withColumn("ccn", F.lpad("ccn", 6, "0"))
        .withColumn("cohort", cohort)
        .withColumn("period_start", _us_date("period_start_raw"))
        .withColumn("period_end", _us_date("period_end_raw"))
        .withColumnRenamed("_readmissions_suppressed", "readmissions_suppressed")
        .drop("period_start_raw", "period_end_raw")
    )
    return split_valid(typed)


def clean_cms_hospitals(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = (
        conform(df, CMS_HOSPITALS)
        .withColumn("ccn", F.lpad("ccn", 6, "0"))
        .withColumn("zip5", F.substring(F.lpad("zip", 5, "0"), 1, 5))
        .withColumn("emergency_services", F.col("emergency_services_raw") == F.lit("Yes"))
        .drop("emergency_services_raw")
    )
    valid, rejected = split_valid(typed)
    return dedupe(valid, "ccn"), rejected


def clean_svi(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = conform(df, SVI_COUNTY).withColumn("county_fips", F.lpad("county_fips", 5, "0"))
    valid, rejected = split_valid(typed)
    return dedupe(valid, "county_fips"), rejected
