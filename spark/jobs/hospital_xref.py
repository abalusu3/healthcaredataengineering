"""Entity resolution: Synthea organizations -> CMS hospitals (CCN), with a match report."""

from __future__ import annotations

from pyspark.sql import functions as F

from readmitiq.spark.runtime import start_job
from readmitiq.spark.transforms.entity_resolution import resolve_hospitals


def main() -> None:
    job = start_job("hospital_xref")
    orgs = job.read_silver("organizations")
    hospitals = job.read_silver("cms_hospitals")
    xref = resolve_hospitals(orgs, hospitals).cache()
    job.publish("hospital_xref", xref)

    # Report coverage weighted by inpatient volume: that's what matters for the scorecard.
    stays = (
        job.read_silver("encounters")
        .filter(F.col("encounter_class") == "inpatient")
        .groupBy("organization_id")
        .count()
    )
    report = (
        xref.join(stays, "organization_id", "left")
        .groupBy("match_tier")
        .agg(
            F.count("*").alias("orgs"),
            F.coalesce(F.sum("count"), F.lit(0)).alias("inpatient_stays"),
        )
        .orderBy("match_tier")
        .collect()
    )
    total_stays = sum(r.inpatient_stays for r in report) or 1
    print("ENTITY RESOLUTION REPORT")
    for r in report:
        print(
            f"  {r.match_tier:<10} orgs={r.orgs:>6}  inpatient_stays={r.inpatient_stays:>7} "
            f"({r.inpatient_stays / total_stays:.1%})"
        )
    job.record_metrics()
    job.spark.stop()


if __name__ == "__main__":
    main()
