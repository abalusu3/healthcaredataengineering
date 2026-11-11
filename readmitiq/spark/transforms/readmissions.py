"""Build `inpatient_stays` (one row per *admission episode*) with CMS-style 30-day
readmission flags (simplified).

Definitions (mirroring the CMS Hospital-Wide Readmission methodology, simplified for Synthea):
  * Encounter: one inpatient encounter row from the source system.
  * Admission episode: encounters for the same member that are contiguous (next admit starts
    < 6h after the previous discharge, or overlaps it) are ONE hospitalization -- e.g. ICU ->
    ward at the same hospital, or an inter-hospital transfer. Built with gaps-and-islands.
    The episode is attributed to the *discharging* hospital, as CMS does for transfers.
  * Planned episode: contains a planned procedure / admission type / reason. Planned episodes
    never count as readmissions.
  * Index-eligible episode: discharged alive, not a cancer-treatment episode, and discharged at
    least 30 days before the end of the data (no right-censoring).
  * Readmission: the member's next *unplanned* episode admitted 0-30 days (inclusive, by
    calendar date) after an index episode's discharge, at any hospital.
An episode can be both a readmission (for an earlier index) and an index episode itself.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from readmitiq.spark.codes import (
    CANCER_TREATMENT_PROCEDURE_CODES,
    DEFAULT_PAYER_TYPE,
    HRRP_PROCEDURE_COHORTS,
    HRRP_REASON_COHORTS,
    PAYER_TYPES,
    PLANNED_ENCOUNTER_CODES,
    PLANNED_PROCEDURE_CODES,
    PLANNED_REASON_CODES,
)

READMIT_WINDOW_DAYS = 30
CONTIGUOUS_GAP_HOURS = 6


def _in(col: str, codes) -> Column:
    return F.col(col).isin(*sorted(codes))


def procedure_flags(procedures: DataFrame) -> DataFrame:
    """Aggregate procedures to one row of boolean flags per encounter."""
    aggs = [
        F.max(_in("code", PLANNED_PROCEDURE_CODES)).alias("has_planned_procedure"),
        F.max(_in("code", CANCER_TREATMENT_PROCEDURE_CODES)).alias("has_cancer_treatment"),
    ]
    aggs += [
        F.max(_in("code", codes)).alias(f"has_{cohort.lower()}_procedure")
        for cohort, codes in HRRP_PROCEDURE_COHORTS.items()
    ]
    return procedures.groupBy("encounter_id").agg(*aggs)


def payer_type(payer_name: Column) -> Column:
    expr = F.lit(DEFAULT_PAYER_TYPE)
    for name, lob in PAYER_TYPES.items():
        expr = F.when(payer_name == name, F.lit(lob)).otherwise(expr)
    return F.when(payer_name.isNull(), F.lit("Unknown")).otherwise(expr)


def hrrp_cohort() -> Column:
    expr = F.lit(None).cast("string")
    for cohort, codes in HRRP_REASON_COHORTS.items():
        expr = F.when(_in("reason_code", codes), F.lit(cohort)).otherwise(expr)
    # Procedure cohorts win over diagnosis cohorts.
    for cohort in reversed(list(HRRP_PROCEDURE_COHORTS)):
        expr = F.when(F.col(f"has_{cohort.lower()}_procedure"), F.lit(cohort)).otherwise(expr)
    return expr


def encounter_level(
    encounters: DataFrame, procedures: DataFrame, members: DataFrame, payers: DataFrame
) -> DataFrame:
    """One row per inpatient encounter with clinical / payer attributes."""
    enc = encounters.filter(F.col("encounter_class") == "inpatient").select(
        "encounter_id",
        "member_id",
        "organization_id",
        "provider_id",
        "payer_id",
        F.col("start_ts").alias("admit_ts"),
        F.col("stop_ts").alias("discharge_ts"),
        "encounter_code",
        "encounter_description",
        "reason_code",
        "reason_description",
        "total_claim_cost",
        "payer_coverage",
        "source_state",
    )
    flags = procedure_flags(procedures)
    flag_cols = [c for c in flags.columns if c != "encounter_id"]
    enc = enc.join(flags, "encounter_id", "left").fillna(False, subset=flag_cols)
    enc = enc.join(
        members.select("member_id", "birth_date", "death_date"), "member_id", "left"
    ).join(payers.select("payer_id", F.col("name").alias("payer_name")), "payer_id", "left")
    return (
        enc.withColumn("payer_type", payer_type(F.col("payer_name")))
        .withColumn("hrrp_cohort", hrrp_cohort())
        .withColumn(
            "is_planned",
            F.col("has_planned_procedure")
            | _in("encounter_code", PLANNED_ENCOUNTER_CODES)
            | F.coalesce(_in("reason_code", PLANNED_REASON_CODES), F.lit(False)),
        )
    )


def to_episodes(enc: DataFrame) -> DataFrame:
    """Gaps-and-islands: merge contiguous encounters into admission episodes."""
    w = Window.partitionBy("member_id").orderBy("admit_ts", "encounter_id")
    running_max_discharge = F.max("discharge_ts").over(w.rowsBetween(Window.unboundedPreceding, -1))
    gap_seconds = F.unix_timestamp("admit_ts") - F.unix_timestamp(running_max_discharge)
    new_episode = running_max_discharge.isNull() | (gap_seconds >= CONTIGUOUS_GAP_HOURS * 3600)
    enc = enc.withColumn("_new", new_episode.cast("int")).withColumn(
        "_episode_seq", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0))
    )

    first = lambda c: F.min_by(c, "admit_ts")  # noqa: E731  (value from earliest encounter)
    last = lambda c: F.max_by(c, "discharge_ts")  # noqa: E731  (value from discharging enc.)
    return enc.groupBy("member_id", "_episode_seq").agg(
        first("encounter_id").alias("stay_id"),
        F.count("*").alias("encounter_count"),
        F.min("admit_ts").alias("admit_ts"),
        F.max("discharge_ts").alias("discharge_ts"),
        first("organization_id").alias("admitting_org_id"),
        last("organization_id").alias("organization_id"),
        (F.countDistinct("organization_id") > 1).alias("is_multi_facility"),
        last("provider_id").alias("provider_id"),
        first("payer_id").alias("payer_id"),
        first("payer_name").alias("payer_name"),
        first("payer_type").alias("payer_type"),
        first("source_state").alias("source_state"),
        first("encounter_code").alias("encounter_code"),
        first("encounter_description").alias("encounter_description"),
        first("reason_code").alias("reason_code"),
        first("reason_description").alias("reason_description"),
        # Earliest non-null cohort (min_by ignores rows whose ordering value is null).
        F.min_by("hrrp_cohort", F.when(F.col("hrrp_cohort").isNotNull(), F.col("admit_ts"))).alias(
            "hrrp_cohort"
        ),
        # sum() widens decimal(18,2) -> (28,2); keep the silver money type stable.
        F.sum("total_claim_cost").cast("decimal(18,2)").alias("total_claim_cost"),
        F.sum("payer_coverage").cast("decimal(18,2)").alias("payer_coverage"),
        F.max("is_planned").alias("is_planned"),
        F.max("has_cancer_treatment").alias("has_cancer_treatment"),
        F.first("birth_date").alias("birth_date"),
        F.first("death_date").alias("death_date"),
    )


def build_inpatient_stays(
    encounters: DataFrame,
    procedures: DataFrame,
    members: DataFrame,
    payers: DataFrame,
    data_end_ts: Column | None = None,
) -> DataFrame:
    """encounters/procedures/members/payers are silver tables. Returns one row per episode."""
    eps = to_episodes(encounter_level(encounters, procedures, members, payers))

    if data_end_ts is None:
        data_end_ts = F.max("discharge_ts").over(Window.partitionBy())

    eps = (
        eps.withColumn(
            "los_days",
            F.round((F.unix_timestamp("discharge_ts") - F.unix_timestamp("admit_ts")) / 86400, 2),
        )
        .withColumn(
            "age_at_admit", F.floor(F.months_between("admit_ts", "birth_date") / 12).cast("int")
        )
        .withColumn(
            "died_in_hospital",
            F.coalesce(F.col("death_date") <= F.to_date("discharge_ts"), F.lit(False)),
        )
        .withColumn("data_end_ts", data_end_ts)
    )

    # Next *unplanned* episode in a single windowed pass: min over a struct orders by admit_ts.
    following = (
        Window.partitionBy("member_id")
        .orderBy("admit_ts", "stay_id")
        .rowsBetween(1, Window.unboundedFollowing)
    )
    eps = eps.withColumn(
        "_nu",
        F.min(
            F.when(~F.col("is_planned"), F.struct("admit_ts", "stay_id", "total_claim_cost"))
        ).over(following),
    )
    censored = F.datediff(F.to_date("data_end_ts"), F.to_date("discharge_ts")) < READMIT_WINDOW_DAYS
    days_to_next = F.datediff(F.to_date(F.col("_nu.admit_ts")), F.to_date("discharge_ts"))

    eps = (
        eps.withColumn("is_censored", censored)
        .withColumn(
            "is_index_eligible",
            ~F.col("died_in_hospital") & ~F.col("has_cancer_treatment") & ~F.col("is_censored"),
        )
        .withColumn(
            "readmit_30d",
            F.col("is_index_eligible")
            & F.coalesce(days_to_next.between(0, READMIT_WINDOW_DAYS), F.lit(False)),
        )
    )

    readmit = F.col("readmit_30d")
    return eps.select(
        "stay_id",
        "member_id",
        "encounter_count",
        "admitting_org_id",
        "organization_id",
        "is_multi_facility",
        "provider_id",
        "payer_id",
        "payer_name",
        "payer_type",
        "source_state",
        "admit_ts",
        "discharge_ts",
        "los_days",
        "age_at_admit",
        "encounter_code",
        "encounter_description",
        "reason_code",
        "reason_description",
        "hrrp_cohort",
        "total_claim_cost",
        "payer_coverage",
        "is_planned",
        "has_cancer_treatment",
        "died_in_hospital",
        "is_censored",
        "is_index_eligible",
        "readmit_30d",
        F.when(readmit, F.col("_nu.stay_id")).alias("readmit_stay_id"),
        F.when(readmit, F.col("_nu.admit_ts")).alias("readmit_admit_ts"),
        F.when(readmit, days_to_next).alias("days_to_readmit"),
        F.when(readmit, F.col("_nu.total_claim_cost")).alias("readmit_cost"),
    )
