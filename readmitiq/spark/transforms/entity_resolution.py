"""Resolve Synthea organizations to CMS hospitals (CCN).

Blocking: same state AND (same ZIP5 OR same city) keeps the candidate set small.
Scoring: 0.6 * name similarity + 0.4 * address similarity, where similarity is
1 - levenshtein / max_len on normalized strings. Only Spark built-ins are used (no Python
UDFs), so the job scales with the cluster and executors never need a Python worker.

Tiers: exact (score >= 0.90), probable (score >= 0.75, OR same ZIP5 with address similarity
>= 0.85 -- legal-entity vs "doing business as" names), else unmatched. `match_method` records
which rule matched so reviewers can audit address-only matches.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

EXACT_THRESHOLD = 0.90
PROBABLE_THRESHOLD = 0.75
ADDRESS_LED_THRESHOLD = 0.85
NAME_WEIGHT = 0.6
ADDRESS_WEIGHT = 0.4

_NAME_NOISE = r"\b(THE|INC|LLC|LLP|PLLC|PC|CORP|CORPORATION|CO|LTD|OF|AND)\b"
_ADDRESS_ABBREV = {
    r"\bSTREET\b": "ST",
    r"\bAVENUE\b": "AVE",
    r"\bROAD\b": "RD",
    r"\bDRIVE\b": "DR",
    r"\bBOULEVARD\b": "BLVD",
    r"\bHIGHWAY\b": "HWY",
    r"\bPARKWAY\b": "PKWY",
    r"\bLANE\b": "LN",
    r"\bNORTH\b": "N",
    r"\bSOUTH\b": "S",
    r"\bEAST\b": "E",
    r"\bWEST\b": "W",
}


def _squash(col: Column) -> Column:
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def normalize_name(col: Column) -> Column:
    c = F.upper(F.regexp_replace(col, r"[^A-Za-z0-9 ]", " "))
    c = F.regexp_replace(c, r"\bST\b", "SAINT")
    return _squash(F.regexp_replace(c, _NAME_NOISE, " "))


def normalize_address(col: Column) -> Column:
    c = F.upper(F.regexp_replace(col, r"[^A-Za-z0-9 ]", " "))
    for pattern, repl in _ADDRESS_ABBREV.items():
        c = F.regexp_replace(c, pattern, repl)
    return _squash(c)


def similarity(a: Column, b: Column) -> Column:
    longest = F.greatest(F.length(a), F.length(b))
    return F.when(longest == 0, F.lit(0.0)).otherwise(1 - F.levenshtein(a, b) / longest)


def resolve_hospitals(orgs: DataFrame, hospitals: DataFrame) -> DataFrame:
    """orgs: organization_id, name, address, city, state_code, zip5
    hospitals: ccn, facility_name, address, city, state_code, zip5
    Returns one row per org with best CCN candidate, scores and match_tier.
    """
    o = orgs.select(
        "organization_id",
        F.col("name").alias("org_name"),
        F.upper("city").alias("o_city"),
        F.col("state_code").alias("o_state"),
        F.col("zip5").alias("o_zip5"),
        normalize_name(F.col("name")).alias("o_name_n"),
        normalize_address(F.col("address")).alias("o_addr_n"),
    )
    h = hospitals.select(
        "ccn",
        F.col("facility_name").alias("cms_name"),
        F.upper("city").alias("h_city"),
        F.col("state_code").alias("h_state"),
        F.col("zip5").alias("h_zip5"),
        normalize_name(F.col("facility_name")).alias("h_name_n"),
        normalize_address(F.col("address")).alias("h_addr_n"),
    )

    blocked = o.join(
        h,
        (o.o_state == h.h_state) & ((o.o_zip5 == h.h_zip5) | (o.o_city == h.h_city)),
        "left",
    )
    scored = (
        blocked.withColumn("name_score", similarity("o_name_n", "h_name_n"))
        .withColumn("address_score", similarity("o_addr_n", "h_addr_n"))
        .withColumn(
            "match_score",
            F.round(NAME_WEIGHT * F.col("name_score") + ADDRESS_WEIGHT * F.col("address_score"), 4),
        )
    )
    # Address-led rule: same ZIP5 + near-identical street address is the same facility even
    # when names differ (legal entity "PRIME HEALTHCARE SERVICES" vs DBA "ST MARY'S MEDICAL
    # CENTER"). Rank such candidates by address so they beat a weak name-only lookalike.
    address_led = (F.col("o_zip5") == F.col("h_zip5")) & (
        F.col("address_score") >= ADDRESS_LED_THRESHOLD
    )
    scored = scored.withColumn("address_led", F.coalesce(address_led, F.lit(False))).withColumn(
        "rank_score",
        F.greatest(
            F.col("match_score"),
            F.when(F.col("address_led"), F.col("address_score") * PROBABLE_THRESHOLD),
        ),
    )
    best = Window.partitionBy("organization_id").orderBy(
        F.col("rank_score").desc_nulls_last(), F.col("ccn")
    )
    top = scored.withColumn("__rn", F.row_number().over(best)).filter("__rn = 1")

    by_name = F.col("match_score") >= PROBABLE_THRESHOLD
    tier = (
        F.when(F.col("match_score") >= EXACT_THRESHOLD, "exact")
        .when(by_name | F.col("address_led"), "probable")
        .otherwise("unmatched")
    )
    method = F.when(by_name, "name+address").when(F.col("address_led"), "address").otherwise(None)
    matched = by_name | F.col("address_led")
    return top.select(
        "organization_id",
        "org_name",
        F.when(matched, F.col("ccn")).alias("ccn"),
        F.when(matched, F.col("cms_name")).alias("cms_name"),
        F.round("name_score", 4).alias("name_score"),
        F.round("address_score", 4).alias("address_score"),
        F.coalesce("match_score", F.lit(0.0)).alias("match_score"),
        tier.alias("match_tier"),
        method.alias("match_method"),
        F.when(F.col("o_zip5") == F.col("h_zip5"), "state+zip5")
        .when(F.col("ccn").isNotNull(), "state+city")
        .otherwise("no_candidate")
        .alias("block_key"),
    )
