"""Bronze Synthea CSVs -> silver Delta tables (typed, validated, deduped, PII-minimized)."""

from __future__ import annotations

import argparse

from readmitiq.spark.runtime import start_job
from readmitiq.spark.schemas import SYNTHEA_SPECS
from readmitiq.spark.transforms.quality import quarantine_orphans
from readmitiq.spark.transforms.synthea import clean_synthea

# Members first: child tables are FK-checked against it.
ORDER = [
    "patients",
    "payers",
    "organizations",
    "providers",
    "encounters",
    "conditions",
    "procedures",
    "medications",
    "payer_transitions",
]
MEMBER_CHILDREN = {"encounters", "conditions", "procedures", "medications", "payer_transitions"}


def main(tables: list[str]) -> None:
    job = start_job("silver_synthea")
    members = None
    for table in [t for t in ORDER if t in tables]:
        spec = SYNTHEA_SPECS[table]
        raw = job.read_bronze("synthea", table, spec)
        valid, rejected = clean_synthea(raw, spec)

        if table in MEMBER_CHILDREN:
            members = members if members is not None else job.read_silver("members")
            valid, orphans = quarantine_orphans(
                valid, members, "member_id", "member_id", "orphan_member"
            )
            rejected = rejected.unionByName(orphans, allowMissingColumns=True)

        job.publish(spec.name, valid, rejected, spec.partition_by)
        if table == "patients":
            members = job.read_silver("members")
    job.record_metrics()
    job.spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--tables", nargs="*", default=ORDER)
    main(p.parse_args().tables)
