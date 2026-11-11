"""Bronze CMS / CDC reference files -> silver Delta tables."""

from __future__ import annotations

from readmitiq.spark.runtime import start_job
from readmitiq.spark.schemas import CMS_HOSPITALS, HRRP, SVI_COUNTY
from readmitiq.spark.transforms.reference import clean_cms_hospitals, clean_hrrp, clean_svi


def main() -> None:
    job = start_job("silver_reference")
    for source, dataset, spec, clean in (
        ("cms", "hrrp", HRRP, clean_hrrp),
        ("cms", "hospital_general_info", CMS_HOSPITALS, clean_cms_hospitals),
        ("cdc", "svi_county", SVI_COUNTY, clean_svi),
    ):
        valid, rejected = clean(job.read_bronze(source, dataset, spec))
        job.publish(spec.name, valid, rejected)
    job.record_metrics()
    job.spark.stop()


if __name__ == "__main__":
    main()
