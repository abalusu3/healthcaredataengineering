from readmitiq.spark.io import latest_bronze_objects


def test_latest_per_state_partition():
    keys = [
        "bronze/synthea/encounters/state=missouri/ingest_date=2026-11-01/encounters.csv",
        "bronze/synthea/encounters/state=missouri/ingest_date=2026-11-10/encounters.csv",
        "bronze/synthea/encounters/state=texas/ingest_date=2026-11-01/encounters.csv",
        "bronze/synthea/encounters/state=texas/ingest_date=2026-11-01/_SUCCESS",
    ]
    got = latest_bronze_objects(keys)
    assert [(o.partitions["state"], o.ingest_date) for o in got] == [
        ("missouri", "2026-11-10"),
        ("texas", "2026-11-01"),
    ]


def test_latest_unpartitioned():
    keys = [
        "bronze/cms/hrrp/ingest_date=2026-10-01/hrrp.csv",
        "bronze/cms/hrrp/ingest_date=2026-11-10/hrrp.csv",
    ]
    assert [o.key for o in latest_bronze_objects(keys)] == [keys[1]]
