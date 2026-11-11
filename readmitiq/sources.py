"""Registry of upstream data sources landed in the bronze layer."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CmsDataset:
    """A dataset on the CMS Provider Data Catalog (data.cms.gov/provider-data)."""

    name: str
    dataset_id: str
    description: str

    @property
    def metastore_url(self) -> str:
        return (
            "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/"
            f"{self.dataset_id}"
        )


CMS_DATASETS: dict[str, CmsDataset] = {
    d.name: d
    for d in (
        CmsDataset(
            name="hrrp",
            dataset_id="9n3s-kdb3",
            description="Hospital Readmissions Reduction Program: excess readmission "
            "ratio by hospital x condition (AMI, HF, PN, COPD, CABG, THA/TKA).",
        ),
        CmsDataset(
            name="hospital_general_info",
            dataset_id="xubh-q36u",
            description="Hospital attributes: CCN, address, type, ownership, star rating.",
        ),
    )
}

# CDC/ATSDR Social Vulnerability Index, county level (most recent release).
SVI_COUNTY_URL = (
    "https://svi.cdc.gov/Documents/Data/2022/csv/states_counties/SVI_2022_US_county.csv"
)

# Synthea CSV tables needed downstream. High-volume tables we don't use
# (observations, claims_transactions, imaging) are intentionally skipped.
SYNTHEA_TABLES: tuple[str, ...] = (
    "patients",
    "encounters",
    "conditions",
    "procedures",
    "medications",
    "organizations",
    "providers",
    "payers",
    "payer_transitions",
)
