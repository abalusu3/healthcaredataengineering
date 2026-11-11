from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from readmitiq.audit import InMemoryAuditLog
from readmitiq.ingest import (
    bronze_key,
    discover_synthea_files,
    fingerprint,
    ingest_cms_dataset,
    ingest_synthea_file,
    land_file,
    resolve_cms_download_url,
)
from readmitiq.sources import CMS_DATASETS
from readmitiq.storage import Lake

BUCKET = "test-lake"
D = date(2026, 11, 10)


@pytest.fixture
def lake():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        lake = Lake(client, BUCKET)
        lake.ensure_bucket()
        lake.ensure_bucket()  # idempotent
        yield lake


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    p = tmp_path / "data.csv"
    p.write_text("id,value\n1,a\n2,b\n")
    return p


# ------------------------------------------------------------------ pure helpers


def test_bronze_key_is_hive_partitioned():
    assert (
        bronze_key("synthea", "encounters", D, "encounters.csv", {"state": "missouri"})
        == "bronze/synthea/encounters/state=missouri/ingest_date=2026-11-10/encounters.csv"
    )
    assert bronze_key("cms", "hrrp", D, "hrrp.csv") == (
        "bronze/cms/hrrp/ingest_date=2026-11-10/hrrp.csv"
    )


def test_fingerprint(csv_file: Path):
    fp = fingerprint(csv_file)
    assert fp.sha256 == hashlib.sha256(csv_file.read_bytes()).hexdigest()
    assert fp.size_bytes == csv_file.stat().st_size
    assert fp.line_count == 3


def test_discover_synthea_files(tmp_path: Path):
    for state in ("missouri", "texas", "georgia"):
        d = tmp_path / state / "csv"
        d.mkdir(parents=True)
        (d / "patients.csv").write_text("Id\n")
        (d / "encounters.csv").write_text("Id\n")
        (d / "observations.csv").write_text("Id\n")  # not in the allow-list
        if state != "georgia":  # georgia is still generating: no marker
            (tmp_path / state / "_SUCCESS").touch()
    found = discover_synthea_files(tmp_path)
    assert {(f.state, f.table) for f in found} == {
        ("missouri", "patients"),
        ("missouri", "encounters"),
        ("texas", "patients"),
        ("texas", "encounters"),
    }
    assert discover_synthea_files(tmp_path / "missing") == []


# ------------------------------------------------------------------ landing + idempotency


def test_land_file_uploads_then_skips_unchanged(lake: Lake, csv_file: Path):
    audit = InMemoryAuditLog()
    kwargs = dict(
        lake=lake,
        audit=audit,
        source="cms",
        dataset="hrrp",
        local_path=csv_file,
        source_uri="https://example/hrrp.csv",
    )

    first = land_file(run_id="r1", object_key="bronze/a.csv", **kwargs)
    assert first.status == "loaded"
    assert lake.list_keys("bronze/") == ["bronze/a.csv"]
    head = lake.client.head_object(Bucket=BUCKET, Key="bronze/a.csv")
    assert head["Metadata"]["sha256"] == first.sha256

    second = land_file(run_id="r2", object_key="bronze/b.csv", **kwargs)
    assert second.status == "unchanged"
    assert lake.list_keys("bronze/") == ["bronze/a.csv"]  # nothing re-uploaded

    csv_file.write_text("id,value\n1,changed\n")
    third = land_file(run_id="r3", object_key="bronze/c.csv", **kwargs)
    assert third.status == "loaded"
    assert [r.status for r in audit.records] == ["loaded", "unchanged", "loaded"]


def test_land_file_records_failure(csv_file: Path):
    class BrokenLake:
        bucket = "x"

        def put_file(self, *a, **k):
            raise RuntimeError("s3 down")

    audit = InMemoryAuditLog()
    with pytest.raises(RuntimeError):
        land_file(
            lake=BrokenLake(),
            audit=audit,
            run_id="r1",
            source="cms",
            dataset="hrrp",
            local_path=csv_file,
            source_uri="u",
            object_key="k",
        )
    assert audit.records[-1].status == "failed"
    assert "s3 down" in audit.records[-1].error_message


def test_ingest_synthea_file(lake: Lake, tmp_path: Path):
    from readmitiq.ingest import SyntheaFile

    p = tmp_path / "encounters.csv"
    p.write_text("Id,START\n1,2020-01-01\n")
    rec = ingest_synthea_file(
        lake=lake,
        audit=InMemoryAuditLog(),
        run_id="r1",
        item=SyntheaFile("missouri", "encounters", p),
        ingest_date=D,
    )
    assert rec.dataset == "encounters:missouri"
    assert lake.list_keys("bronze/synthea/") == [
        "bronze/synthea/encounters/state=missouri/ingest_date=2026-11-10/encounters.csv"
    ]


# ------------------------------------------------------------------ CMS (HTTP mocked)


class FakeResponse:
    def __init__(self, *, json_data=None, content: bytes = b""):
        self._json = json_data
        self._content = content

    def raise_for_status(self):
        pass

    def json(self):
        return self._json

    def iter_content(self, _size):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, routes: dict[str, FakeResponse]):
        self.routes = routes

    def get(self, url, **_):
        return self.routes[url]


def test_resolve_cms_download_url_picks_csv():
    ds = CMS_DATASETS["hrrp"]
    session = FakeSession(
        {
            ds.metastore_url: FakeResponse(
                json_data={
                    "distribution": [
                        {"downloadURL": "https://cms/x.json"},
                        {"downloadURL": "https://cms/FY_2026_HRRP.csv"},
                    ]
                }
            )
        }
    )
    assert resolve_cms_download_url(ds, session) == "https://cms/FY_2026_HRRP.csv"


def test_resolve_cms_download_url_raises_without_csv():
    ds = CMS_DATASETS["hrrp"]
    session = FakeSession({ds.metastore_url: FakeResponse(json_data={"distribution": []})})
    with pytest.raises(ValueError):
        resolve_cms_download_url(ds, session)


def test_ingest_cms_dataset_end_to_end(lake: Lake):
    ds = CMS_DATASETS["hrrp"]
    csv_url = "https://cms/FY_2026_HRRP.csv"
    session = FakeSession(
        {
            ds.metastore_url: FakeResponse(json_data={"distribution": [{"downloadURL": csv_url}]}),
            csv_url: FakeResponse(content=b"Facility ID,Measure Name\n010001,READM-30-HF-HRRP\n"),
        }
    )
    audit = InMemoryAuditLog()
    rec = ingest_cms_dataset(
        lake=lake, audit=audit, run_id="r1", ds=ds, ingest_date=D, session=session
    )
    assert rec.status == "loaded"
    assert rec.source_uri == csv_url
    assert rec.line_count == 2
    assert lake.list_keys("bronze/cms/") == ["bronze/cms/hrrp/ingest_date=2026-11-10/hrrp.csv"]
