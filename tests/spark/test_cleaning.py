from __future__ import annotations

from readmitiq.spark.schemas import ENCOUNTERS, PATIENTS
from readmitiq.spark.transforms.entity_resolution import resolve_hospitals
from readmitiq.spark.transforms.quality import quarantine_orphans
from readmitiq.spark.transforms.reference import clean_hrrp, clean_svi
from readmitiq.spark.transforms.synthea import clean_synthea

ENC_SRC = [c.src for c in ENCOUNTERS.columns]


def _enc(eid, start, stop, cost="100.00", patient="p1"):
    vals = {
        "Id": eid,
        "START": start,
        "STOP": stop,
        "PATIENT": patient,
        "ORGANIZATION": "o1",
        "PROVIDER": "pr1",
        "PAYER": "pay1",
        "ENCOUNTERCLASS": "Inpatient",
        "CODE": "1",
        "DESCRIPTION": "d",
        "BASE_ENCOUNTER_COST": cost,
        "TOTAL_CLAIM_COST": cost,
        "PAYER_COVERAGE": "0",
        "REASONCODE": "",
        "REASONDESCRIPTION": "",
    }
    return tuple(vals[c] for c in ENC_SRC) + ("2026-11-10", "missouri")


def test_encounters_quarantine_rules(spark):
    rows = [
        _enc("ok", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"),
        _enc("bad_ts", "not-a-date", "2025-01-02T00:00:00Z"),
        _enc("reversed", "2025-01-05T00:00:00Z", "2025-01-02T00:00:00Z"),
        _enc("neg", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z", cost="-5"),
        _enc("", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"),
        _enc("ok", "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"),  # duplicate
    ]
    df = spark.createDataFrame(rows, ENC_SRC + ["_ingest_date", "source_state"])
    valid, rejected = clean_synthea(df, ENCOUNTERS)

    assert [r.encounter_id for r in valid.collect()] == ["ok"]
    assert valid.first().encounter_class == "inpatient"
    reasons = {r.encounter_id: r.reject_reason for r in rejected.collect()}
    assert reasons["bad_ts"] == "invalid_start_ts,missing_start_ts"
    assert reasons["reversed"] == "stop_before_start"
    assert reasons["neg"] == "negative_cost"
    assert reasons[None] == "missing_encounter_id"


def test_members_drop_direct_identifiers(spark):
    cols = [c.src for c in PATIENTS.columns] + ["SSN", "FIRST", "LAST", "ADDRESS", "LAT"]
    row = {c: "x" for c in cols}
    row.update(
        Id="p1",
        BIRTHDATE="1950-02-03",
        DEATHDATE="",
        ZIP="2134",
        FIPS="1001",
        HEALTHCARE_EXPENSES="1",
        HEALTHCARE_COVERAGE="1",
        INCOME="50000",
    )
    df = spark.createDataFrame([tuple(row[c] for c in cols)], cols)
    valid, rejected = clean_synthea(df, PATIENTS)
    out = valid.first().asDict()
    assert rejected.count() == 0
    for pii in ("SSN", "FIRST", "LAST", "ADDRESS", "LAT"):
        assert pii not in out
    assert out["zip"] == "02134"
    assert out["county_fips"] == "01001"
    assert out["death_date"] is None


def test_quarantine_orphans(spark):
    child = spark.createDataFrame([("e1", "p1"), ("e2", "ghost"), ("e3", None)], ["eid", "pid"])
    parent = spark.createDataFrame([("p1",)], ["member_id"])
    valid, rejected = quarantine_orphans(child, parent, "pid", "member_id", "orphan_member")
    assert sorted(r.eid for r in valid.collect()) == ["e1", "e3"]
    assert [(r.eid, r.reject_reason) for r in rejected.collect()] == [("e2", "orphan_member")]


def test_clean_hrrp(spark):
    cols = [
        "Facility Name",
        "Facility ID",
        "State",
        "Measure Name",
        "Number of Discharges",
        "Footnote",
        "Excess Readmission Ratio",
        "Predicted Readmission Rate",
        "Expected Readmission Rate",
        "Number of Readmissions",
        "Start Date",
        "End Date",
    ]
    rows = [
        (
            "H",
            "10001",
            "AL",
            "READM-30-HIP-KNEE-HRRP",
            "N/A",
            "",
            "0.9875",
            "4.57",
            "4.63",
            "Too Few to Report",
            "07/01/2021",
            "06/30/2024",
        ),
        (
            "H",
            "10001",
            "AL",
            "READM-30-HF-HRRP",
            "512",
            "",
            "1.0712",
            "21.1",
            "19.7",
            "108",
            "07/01/2021",
            "06/30/2024",
        ),
    ]
    valid, rejected = clean_hrrp(spark.createDataFrame(rows, cols))
    got = {r.cohort: r for r in valid.collect()}
    assert rejected.count() == 0
    assert got["THA_TKA"].ccn == "010001"
    assert got["THA_TKA"].discharges is None
    assert got["THA_TKA"].readmissions_suppressed is True
    assert got["HF"].readmissions == 108
    assert got["HF"].excess_readmission_ratio == 1.0712
    assert str(got["HF"].period_end) == "2024-06-30"


def test_clean_svi_nulls_and_fips(spark):
    cols = [
        "﻿ST",
        "FIPS",
        "ST_ABBR",
        "COUNTY",
        "E_TOTPOP",
        "RPL_THEMES",
        "RPL_THEME1",
        "RPL_THEME2",
        "RPL_THEME3",
        "RPL_THEME4",
    ]
    df = spark.createDataFrame(
        [("01", "1001", "AL", "Autauga", "58761", "-999", "0.2", "0.3", "0.4", "0.5")], cols
    )
    df = df.toDF(*[c.lstrip("﻿") for c in df.columns])
    valid, _ = clean_svi(df)
    r = valid.first()
    assert r.county_fips == "01001"
    assert r.svi_overall is None
    assert r.svi_socioeconomic == 0.2


def test_entity_resolution_tiers(spark):
    orgs = spark.createDataFrame(
        [
            ("o1", "Liberty Hospital", "2525 Glenn Hendren Drive", "LIBERTY", "MO", "64068"),
            (
                "o2",
                "ST. MARY'S MEDICAL CENTER, INC.",
                "201 NW R D MOWERY DR",
                "BLUE SPRINGS",
                "MO",
                "64014",
            ),
            ("o3", "Tiny Clinic LLC", "1 Main Street", "NOWHERE", "MO", "65000"),
        ],
        ["organization_id", "name", "address", "city", "state_code", "zip5"],
    )
    hospitals = spark.createDataFrame(
        [
            ("260177", "LIBERTY HOSPITAL", "2525 GLENN HENDREN DR", "LIBERTY", "MO", "64068"),
            (
                "260193",
                "SAINT MARYS MEDICAL CENTER",
                "201 NW R D MOWERY DRIVE",
                "BLUE SPRINGS",
                "MO",
                "64014",
            ),
            ("260999", "OTHER HOSPITAL", "9 ELM ST", "LIBERTY", "MO", "64068"),
        ],
        ["ccn", "facility_name", "address", "city", "state_code", "zip5"],
    )

    got = {r.organization_id: r for r in resolve_hospitals(orgs, hospitals).collect()}
    assert (got["o1"].ccn, got["o1"].match_tier) == ("260177", "exact")
    assert got["o2"].ccn == "260193"
    assert got["o2"].match_tier in ("exact", "probable")
    assert (got["o3"].ccn, got["o3"].match_tier, got["o3"].block_key) == (
        None,
        "unmatched",
        "no_candidate",
    )


def test_entity_resolution_address_led_match(spark):
    """Legal-entity name differs from the CMS DBA name, but it's the same building."""
    orgs = spark.createDataFrame(
        [
            (
                "o1",
                "ST MARY'S SACRED HEART HOSPITAL INC",
                "367 CLEAR CREEK PARKWAY",
                "LAVONIA",
                "GA",
                "30553",
            )
        ],
        ["organization_id", "name", "address", "city", "state_code", "zip5"],
    )
    hospitals = spark.createDataFrame(
        [
            (
                "110027",
                "TY COBB REGIONAL MEDICAL CENTER, LLC",
                "367 CLEAR CREEK PKWY",
                "LAVONIA",
                "GA",
                "30553",
            ),
            ("110999", "ST MARYS HOSPITAL", "1 OTHER ROAD", "LAVONIA", "GA", "30553"),
        ],
        ["ccn", "facility_name", "address", "city", "state_code", "zip5"],
    )
    r = resolve_hospitals(orgs, hospitals).first()
    assert (r.ccn, r.match_tier, r.match_method) == ("110027", "probable", "address")
