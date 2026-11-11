"""Build silver.inpatient_stays with CMS-style 30-day readmission flags."""

from __future__ import annotations

from pyspark.sql import functions as F

from readmitiq.spark.runtime import start_job
from readmitiq.spark.transforms.readmissions import build_inpatient_stays


def main() -> None:
    job = start_job("readmission_flags")
    encounters = job.read_silver("encounters")
    # Data end = last observed encounter; computed once (scalar) rather than via a global window.
    data_end = encounters.agg(F.max("stop_ts")).first()[0]

    stays = build_inpatient_stays(
        encounters=encounters,
        procedures=job.read_silver("procedures"),
        members=job.read_silver("members"),
        payers=job.read_silver("payers"),
        data_end_ts=F.lit(data_end).cast("timestamp"),
    )
    job.publish("inpatient_stays", stays, partition_by=("source_state",))

    s = job.read_silver("inpatient_stays")
    summary = s.agg(
        F.count("*").alias("stays"),
        F.sum(F.col("is_planned").cast("int")).alias("planned"),
        # NB: don't alias "index"/"count" — they shadow tuple methods on pyspark Row.
        F.sum(F.col("is_index_eligible").cast("int")).alias("index_stays"),
        F.sum(F.col("readmit_30d").cast("int")).alias("readmits"),
    ).first()
    rate = summary.readmits / summary.index_stays if summary.index_stays else 0
    print(
        f"READMISSION SUMMARY data_end={data_end} stays={summary.stays} planned={summary.planned} "
        f"index={summary.index_stays} readmits={summary.readmits} rate={rate:.2%}"
    )
    job.record_metrics()
    job.spark.stop()


if __name__ == "__main__":
    main()
