"""Schema contracts for bronze inputs and the typed silver outputs.

Each TableSpec declares the source columns we depend on, their silver name and type.
Bronze is read as all-strings; `conform()` casts with try-semantics so bad values become
quarantinable nulls instead of crashing the job (Spark 4 runs in ANSI mode).
"""

from __future__ import annotations

from dataclasses import dataclass, field


class SchemaDriftError(ValueError):
    """Raised when a bronze file no longer contains a column the pipeline depends on."""


@dataclass(frozen=True)
class Col:
    src: str
    dst: str
    dtype: str = "string"


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Col, ...]
    primary_key: str | None = None
    required: tuple[str, ...] = ()  # silver columns that must be non-null
    null_tokens: tuple[str, ...] = ("",)  # source strings that mean "no value"
    partition_by: tuple[str, ...] = ()
    extra: dict = field(default_factory=dict)

    @property
    def source_columns(self) -> list[str]:
        return [c.src for c in self.columns]


def check_contract(actual_columns: list[str], spec: TableSpec) -> None:
    # Strip a UTF-8 BOM some publishers (e.g. CDC) put on the first header.
    actual = {c.lstrip("﻿") for c in actual_columns}
    missing = [c for c in spec.source_columns if c not in actual]
    if missing:
        raise SchemaDriftError(f"{spec.name}: missing source columns {missing}")


TS = "timestamp"
DATE = "date"
MONEY = "decimal(18,2)"

# ------------------------------------------------------------------------------- Synthea
_SYN = ("source_state",)

PATIENTS = TableSpec(
    name="members",
    primary_key="member_id",
    required=("member_id", "birth_date"),
    partition_by=_SYN,
    # Data minimization: SSN, DRIVERS, PASSPORT, names, street ADDRESS and LAT/LON are
    # deliberately NOT listed, so they never leave bronze.
    columns=(
        Col("Id", "member_id"),
        Col("BIRTHDATE", "birth_date", DATE),
        Col("DEATHDATE", "death_date", DATE),
        Col("MARITAL", "marital_status"),
        Col("RACE", "race"),
        Col("ETHNICITY", "ethnicity"),
        Col("GENDER", "gender"),
        Col("CITY", "city"),
        Col("STATE", "member_state"),
        Col("COUNTY", "county"),
        Col("FIPS", "county_fips"),
        Col("ZIP", "zip"),
        Col("HEALTHCARE_EXPENSES", "healthcare_expenses", MONEY),
        Col("HEALTHCARE_COVERAGE", "healthcare_coverage", MONEY),
        Col("INCOME", "income", MONEY),
    ),
)

ENCOUNTERS = TableSpec(
    name="encounters",
    primary_key="encounter_id",
    required=("encounter_id", "member_id", "start_ts", "encounter_class"),
    partition_by=_SYN,
    columns=(
        Col("Id", "encounter_id"),
        Col("START", "start_ts", TS),
        Col("STOP", "stop_ts", TS),
        Col("PATIENT", "member_id"),
        Col("ORGANIZATION", "organization_id"),
        Col("PROVIDER", "provider_id"),
        Col("PAYER", "payer_id"),
        Col("ENCOUNTERCLASS", "encounter_class"),
        Col("CODE", "encounter_code"),
        Col("DESCRIPTION", "encounter_description"),
        Col("BASE_ENCOUNTER_COST", "base_cost", MONEY),
        Col("TOTAL_CLAIM_COST", "total_claim_cost", MONEY),
        Col("PAYER_COVERAGE", "payer_coverage", MONEY),
        Col("REASONCODE", "reason_code"),
        Col("REASONDESCRIPTION", "reason_description"),
    ),
)

CONDITIONS = TableSpec(
    name="conditions",
    required=("member_id", "encounter_id", "code", "start_date"),
    partition_by=_SYN,
    columns=(
        Col("START", "start_date", DATE),
        Col("STOP", "stop_date", DATE),
        Col("PATIENT", "member_id"),
        Col("ENCOUNTER", "encounter_id"),
        Col("SYSTEM", "code_system"),
        Col("CODE", "code"),
        Col("DESCRIPTION", "description"),
    ),
)

PROCEDURES = TableSpec(
    name="procedures",
    required=("member_id", "encounter_id", "code", "start_ts"),
    partition_by=_SYN,
    columns=(
        Col("START", "start_ts", TS),
        Col("STOP", "stop_ts", TS),
        Col("PATIENT", "member_id"),
        Col("ENCOUNTER", "encounter_id"),
        Col("SYSTEM", "code_system"),
        Col("CODE", "code"),
        Col("DESCRIPTION", "description"),
        Col("BASE_COST", "base_cost", MONEY),
        Col("REASONCODE", "reason_code"),
    ),
)

MEDICATIONS = TableSpec(
    name="medications",
    required=("member_id", "encounter_id", "code", "start_ts"),
    partition_by=_SYN,
    columns=(
        Col("START", "start_ts", TS),
        Col("STOP", "stop_ts", TS),
        Col("PATIENT", "member_id"),
        Col("PAYER", "payer_id"),
        Col("ENCOUNTER", "encounter_id"),
        Col("CODE", "code"),
        Col("DESCRIPTION", "description"),
        Col("BASE_COST", "base_cost", MONEY),
        Col("PAYER_COVERAGE", "payer_coverage", MONEY),
        Col("DISPENSES", "dispenses", "int"),
        Col("TOTALCOST", "total_cost", MONEY),
        Col("REASONCODE", "reason_code"),
    ),
)

ORGANIZATIONS = TableSpec(
    name="organizations",
    primary_key="organization_id",
    required=("organization_id", "name"),
    columns=(
        Col("Id", "organization_id"),
        Col("NAME", "name"),
        Col("ADDRESS", "address"),
        Col("CITY", "city"),
        Col("STATE", "state_code"),
        Col("ZIP", "zip"),
        Col("LAT", "lat", "double"),
        Col("LON", "lon", "double"),
        Col("REVENUE", "revenue", MONEY),
        Col("UTILIZATION", "utilization", "int"),
    ),
)

PROVIDERS = TableSpec(
    name="providers",
    primary_key="provider_id",
    required=("provider_id",),
    columns=(
        Col("Id", "provider_id"),
        Col("ORGANIZATION", "organization_id"),
        Col("GENDER", "gender"),
        Col("SPECIALITY", "specialty"),
        Col("STATE", "state_code"),
        Col("ZIP", "zip"),
    ),
)

PAYERS = TableSpec(
    name="payers",
    primary_key="payer_id",
    required=("payer_id", "name"),
    columns=(
        Col("Id", "payer_id"),
        Col("NAME", "name"),
        Col("OWNERSHIP", "ownership"),
    ),
)

PAYER_TRANSITIONS = TableSpec(
    name="payer_transitions",
    required=("member_id", "start_date", "payer_id"),
    partition_by=_SYN,
    columns=(
        Col("PATIENT", "member_id"),
        Col("MEMBERID", "plan_member_id"),
        Col("START_DATE", "start_date", DATE),
        Col("END_DATE", "end_date", DATE),
        Col("PAYER", "payer_id"),
        Col("SECONDARY_PAYER", "secondary_payer_id"),
        Col("PLAN_OWNERSHIP", "plan_ownership"),
    ),
)

SYNTHEA_SPECS: dict[str, TableSpec] = {
    "patients": PATIENTS,
    "encounters": ENCOUNTERS,
    "conditions": CONDITIONS,
    "procedures": PROCEDURES,
    "medications": MEDICATIONS,
    "organizations": ORGANIZATIONS,
    "providers": PROVIDERS,
    "payers": PAYERS,
    "payer_transitions": PAYER_TRANSITIONS,
}

# ------------------------------------------------------------------------------- Reference
_CMS_NULLS = ("", "N/A", "Not Available", "Too Few to Report")

HRRP = TableSpec(
    name="hrrp",
    required=("ccn", "measure_name"),
    null_tokens=_CMS_NULLS,
    columns=(
        Col("Facility ID", "ccn"),
        Col("Facility Name", "facility_name"),
        Col("State", "state_code"),
        Col("Measure Name", "measure_name"),
        Col("Number of Discharges", "discharges", "int"),
        Col("Footnote", "footnote"),
        Col("Excess Readmission Ratio", "excess_readmission_ratio", "double"),
        Col("Predicted Readmission Rate", "predicted_readmission_rate", "double"),
        Col("Expected Readmission Rate", "expected_readmission_rate", "double"),
        Col("Number of Readmissions", "readmissions", "int"),
        Col("Start Date", "period_start_raw"),
        Col("End Date", "period_end_raw"),
    ),
)

CMS_HOSPITALS = TableSpec(
    name="cms_hospitals",
    primary_key="ccn",
    required=("ccn", "facility_name"),
    null_tokens=_CMS_NULLS,
    columns=(
        Col("Facility ID", "ccn"),
        Col("Facility Name", "facility_name"),
        Col("Address", "address"),
        Col("City/Town", "city"),
        Col("State", "state_code"),
        Col("ZIP Code", "zip"),
        Col("County/Parish", "county"),
        Col("Hospital Type", "hospital_type"),
        Col("Hospital Ownership", "ownership"),
        Col("Emergency Services", "emergency_services_raw"),
        Col("Hospital overall rating", "overall_rating", "int"),
        Col("Count of READM Measures Better", "readm_measures_better", "int"),
        Col("Count of READM Measures Worse", "readm_measures_worse", "int"),
    ),
)

SVI_COUNTY = TableSpec(
    name="svi_county",
    primary_key="county_fips",
    required=("county_fips",),
    null_tokens=("", "-999"),
    columns=(
        Col("FIPS", "county_fips"),
        Col("ST_ABBR", "state_code"),
        Col("COUNTY", "county"),
        Col("E_TOTPOP", "population", "int"),
        Col("RPL_THEMES", "svi_overall", "double"),
        Col("RPL_THEME1", "svi_socioeconomic", "double"),
        Col("RPL_THEME2", "svi_household", "double"),
        Col("RPL_THEME3", "svi_minority_language", "double"),
        Col("RPL_THEME4", "svi_housing_transport", "double"),
    ),
)

REFERENCE_SPECS: dict[str, TableSpec] = {
    "hrrp": HRRP,
    "hospital_general_info": CMS_HOSPITALS,
    "svi_county": SVI_COUNTY,
}
