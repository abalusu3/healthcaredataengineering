"""Readmission logic edge cases. Each member below isolates exactly one rule."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from readmitiq.spark.transforms.readmissions import build_inpatient_stays

ENC_SCHEMA = (
    "encounter_id string, member_id string, organization_id string, provider_id string, "
    "payer_id string, start_ts string, stop_ts string, encounter_class string, "
    "encounter_code string, encounter_description string, reason_code string, "
    "reason_description string, total_claim_cost double, payer_coverage double, "
    "source_state string"
)


def stay(eid, member, admit, discharge, org="H1", reason=None, code="32485007", cls="inpatient"):
    return (
        eid,
        member,
        org,
        "P1",
        "PAY_MCD",
        admit,
        discharge,
        cls,
        code,
        "Admission",
        reason,
        None,
        1000.0,
        800.0,
        "missouri",
    )


@pytest.fixture
def result(spark):
    rows = [
        # m1: day-30 boundary -> readmission
        stay("m1a", "m1", "2025-01-01 08:00:00", "2025-01-03 12:00:00", reason="88805009"),
        stay("m1b", "m1", "2025-02-02 09:00:00", "2025-02-04 10:00:00"),
        # m2: day 31 -> not a readmission
        stay("m2a", "m2", "2025-01-01 08:00:00", "2025-01-03 12:00:00"),
        stay("m2b", "m2", "2025-02-03 09:00:00", "2025-02-05 10:00:00"),
        # m3: planned rehab stay at day 5 ignored, unplanned stay at day 20 counts
        stay("m3a", "m3", "2025-01-01 08:00:00", "2025-01-03 12:00:00"),
        stay("m3b", "m3", "2025-01-08 08:00:00", "2025-01-10 12:00:00", code="56876005"),
        stay("m3c", "m3", "2025-01-23 08:00:00", "2025-01-25 12:00:00"),
        # m4: died in hospital -> not index eligible even though re-admitted (data quirk)
        stay("m4a", "m4", "2025-01-01 08:00:00", "2025-01-03 12:00:00"),
        # m5: readmitted to a different hospital -> counts
        stay("m5a", "m5", "2025-01-01 08:00:00", "2025-01-03 12:00:00", org="H1"),
        stay("m5b", "m5", "2025-01-10 08:00:00", "2025-01-12 12:00:00", org="H2"),
        # m6: chain A -> B -> C; B is both a readmission and an index with its own readmission
        stay("m6a", "m6", "2025-01-01 08:00:00", "2025-01-03 12:00:00"),
        stay("m6b", "m6", "2025-01-15 08:00:00", "2025-01-17 12:00:00"),
        stay("m6c", "m6", "2025-02-01 08:00:00", "2025-02-03 12:00:00"),
        # m7: inter-hospital transfer (2h gap) -> ONE episode attributed to discharging H2
        stay("m7a", "m7", "2025-01-01 08:00:00", "2025-01-03 12:00:00", org="H1"),
        stay("m7b", "m7", "2025-01-03 14:00:00", "2025-01-06 12:00:00", org="H2"),
        # m10: ICU -> ward at same hospital (0h gap) is one episode, NOT a day-0 readmission;
        # a real unplanned readmission follows 10 days later.
        stay("m10a", "m10", "2025-03-01 08:00:00", "2025-03-03 08:00:00", reason="88805009"),
        stay("m10b", "m10", "2025-03-03 08:00:00", "2025-03-05 12:00:00"),
        stay("m10c", "m10", "2025-03-15 08:00:00", "2025-03-16 12:00:00"),
        # m8: discharged 10 days before data end -> censored, not index
        stay("m8a", "m8", "2025-06-15 08:00:00", "2025-06-20 12:00:00"),
        # m9: CABG procedure makes cohort CABG even though reason says AMI
        stay("m9a", "m9", "2025-01-01 08:00:00", "2025-01-05 12:00:00", reason="401314000"),
        # an ambulatory visit must be ignored entirely
        stay("x1", "m1", "2025-01-10 08:00:00", "2025-01-10 09:00:00", cls="ambulatory"),
    ]
    encounters = (
        spark.createDataFrame(rows, ENC_SCHEMA)
        .withColumn("start_ts", F.to_timestamp("start_ts"))
        .withColumn("stop_ts", F.to_timestamp("stop_ts"))
    )
    procedures = spark.createDataFrame(
        [("m9a", "232717009"), ("m3b", "430193006")], ["encounter_id", "code"]
    )
    members = spark.createDataFrame(
        [(f"m{i}", "1950-01-01", "2025-01-03" if i == 4 else None) for i in range(1, 11)],
        "member_id string, birth_date string, death_date string",
    ).select(
        "member_id",
        F.to_date("birth_date").alias("birth_date"),
        F.to_date("death_date").alias("death_date"),
    )
    payers = spark.createDataFrame([("PAY_MCD", "Medicaid")], ["payer_id", "name"])

    out = build_inpatient_stays(
        encounters,
        procedures,
        members,
        payers,
        data_end_ts=F.lit("2025-06-30 00:00:00").cast("timestamp"),
    )
    return {r.stay_id: r for r in out.collect()}


def test_one_row_per_episode_inpatient_only(result):
    assert "x1" not in result
    # 20 inpatient encounters; m7 (2) and m10a+b (2) each collapse into one episode.
    assert len(result) == 18
    assert {"m7b", "m10b"}.isdisjoint(result)


def test_day_30_is_inclusive(result):
    assert result["m1a"].readmit_30d is True
    assert result["m1a"].days_to_readmit == 30
    assert result["m1a"].readmit_stay_id == "m1b"
    assert result["m1a"].hrrp_cohort == "HF"


def test_day_31_is_not_a_readmission(result):
    assert result["m2a"].readmit_30d is False
    assert result["m2a"].days_to_readmit is None


def test_planned_stay_skipped_next_unplanned_counts(result):
    assert result["m3b"].is_planned is True
    assert result["m3a"].readmit_30d is True
    assert result["m3a"].readmit_stay_id == "m3c"
    assert result["m3a"].days_to_readmit == 20


def test_death_in_hospital_not_index(result):
    assert result["m4a"].died_in_hospital is True
    assert result["m4a"].is_index_eligible is False


def test_readmission_at_other_hospital_counts(result):
    assert result["m5a"].readmit_30d is True


def test_chained_readmissions(result):
    assert result["m6a"].readmit_stay_id == "m6b"
    assert result["m6b"].readmit_30d is True
    assert result["m6b"].readmit_stay_id == "m6c"
    assert result["m6c"].readmit_30d is False


def test_transfer_merged_and_attributed_to_discharging_hospital(result):
    ep = result["m7a"]
    assert ep.encounter_count == 2
    assert ep.is_multi_facility is True
    assert (ep.admitting_org_id, ep.organization_id) == ("H1", "H2")
    # Span covers both encounters: Jan 1 08:00 -> Jan 6 12:00 = 5.17 days (TZ-independent).
    assert ep.los_days == pytest.approx(5.17, abs=0.01)
    assert ep.is_index_eligible is True
    assert ep.readmit_30d is False


def test_contiguous_same_hospital_stays_are_not_a_readmission(result):
    ep = result["m10a"]
    assert ep.encounter_count == 2
    assert ep.is_multi_facility is False
    assert ep.hrrp_cohort == "HF"  # first non-null cohort in the episode
    assert float(ep.total_claim_cost) == 2000.0  # costs summed across the episode
    assert ep.readmit_stay_id == "m10c"
    assert ep.days_to_readmit == 10  # measured from the episode's final discharge


def test_right_censoring(result):
    assert result["m8a"].is_censored is True
    assert result["m8a"].is_index_eligible is False


def test_procedure_cohort_beats_reason_cohort(result):
    assert result["m9a"].hrrp_cohort == "CABG"


def test_derived_fields(result):
    r = result["m1a"]
    assert r.los_days == pytest.approx(2.17, abs=0.01)
    assert r.age_at_admit == 75
    assert r.payer_type == "Medicaid"
    assert float(r.readmit_cost) == 1000.0
