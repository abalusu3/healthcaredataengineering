"""Bronze-layer ingestion: land source files unchanged in the lake, with audit + dedupe.

Design rules:
  * Bronze is an immutable, append-only copy of the source. No parsing happens here.
  * Every landed file is fingerprinted (sha256) and logged to ops.ingestion_log.
  * If a source file is byte-identical to the last successful load, it is not re-uploaded
    (status='unchanged'). This makes re-runs and backfills idempotent and cheap.
"""

from __future__ import annotations

import hashlib
import logging
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests

from readmitiq.audit import AuditLog, IngestionRecord
from readmitiq.sources import SYNTHEA_TABLES, CmsDataset
from readmitiq.storage import Lake

log = logging.getLogger(__name__)

HTTP_TIMEOUT = (10, 300)  # (connect, read) seconds
CHUNK = 1024 * 1024
SUCCESS_MARKER = "_SUCCESS"


@dataclass(frozen=True)
class Fingerprint:
    sha256: str
    size_bytes: int
    line_count: int


@dataclass(frozen=True)
class SyntheaFile:
    state: str
    table: str
    path: Path


def bronze_key(
    source: str,
    dataset: str,
    ingest_date: date,
    filename: str,
    partitions: dict[str, str] | None = None,
) -> str:
    """Hive-style partitioned key, e.g.
    bronze/synthea/encounters/state=missouri/ingest_date=2026-11-10/encounters.csv
    """
    parts = ["bronze", source, dataset]
    parts += [f"{k}={v}" for k, v in (partitions or {}).items()]
    parts += [f"ingest_date={ingest_date.isoformat()}", filename]
    return "/".join(parts)


def fingerprint(path: Path) -> Fingerprint:
    """Stream the file once to get hash, size and line count (memory-safe for GB files)."""
    digest = hashlib.sha256()
    size = 0
    lines = 0
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK):
            digest.update(chunk)
            size += len(chunk)
            lines += chunk.count(b"\n")
    return Fingerprint(digest.hexdigest(), size, lines)


def land_file(
    *,
    lake: Lake,
    audit: AuditLog,
    run_id: str,
    source: str,
    dataset: str,
    local_path: Path,
    source_uri: str,
    object_key: str,
) -> IngestionRecord:
    """Upload one file to bronze unless unchanged since the last load; always audit."""
    fp = fingerprint(local_path)
    base = dict(
        run_id=run_id,
        source=source,
        dataset=dataset,
        object_key=object_key,
        source_uri=source_uri,
        sha256=fp.sha256,
        size_bytes=fp.size_bytes,
        line_count=fp.line_count,
    )

    if audit.latest_loaded_sha(source, dataset) == fp.sha256:
        rec = IngestionRecord(**base, status="unchanged")
        audit.record(rec)
        log.info("unchanged: %s/%s (sha256=%s…)", source, dataset, fp.sha256[:12])
        return rec

    try:
        lake.put_file(
            object_key,
            local_path,
            metadata={"sha256": fp.sha256, "source-uri": source_uri, "run-id": run_id},
        )
    except Exception as exc:
        audit.record(IngestionRecord(**base, status="failed", error_message=str(exc)[:2000]))
        raise

    rec = IngestionRecord(**base, status="loaded")
    audit.record(rec)
    log.info(
        "loaded: %s/%s -> s3://%s/%s (%d bytes, %d lines)",
        source, dataset, lake.bucket, object_key, fp.size_bytes, fp.line_count,
    )
    return rec


# --------------------------------------------------------------------------- HTTP sources


def _download(url: str, dest: Path, session: requests.Session) -> None:
    with session.get(url, stream=True, timeout=HTTP_TIMEOUT) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(CHUNK):
                fh.write(chunk)


def resolve_cms_download_url(ds: CmsDataset, session: requests.Session) -> str:
    """CMS rotates file URLs on every refresh; resolve the current one via the metastore."""
    resp = session.get(ds.metastore_url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    meta = resp.json()
    for dist in meta.get("distribution", []):
        url = dist.get("downloadURL", "")
        if url.lower().endswith(".csv"):
            return url
    raise ValueError(f"No CSV distribution found for CMS dataset {ds.dataset_id}")


def ingest_http_csv(
    *,
    lake: Lake,
    audit: AuditLog,
    run_id: str,
    source: str,
    dataset: str,
    url: str,
    ingest_date: date,
    session: requests.Session | None = None,
) -> IngestionRecord:
    session = session or requests.Session()
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / f"{dataset}.csv"
        _download(url, local, session)
        return land_file(
            lake=lake,
            audit=audit,
            run_id=run_id,
            source=source,
            dataset=dataset,
            local_path=local,
            source_uri=url,
            object_key=bronze_key(source, dataset, ingest_date, f"{dataset}.csv"),
        )


def ingest_cms_dataset(
    *,
    lake: Lake,
    audit: AuditLog,
    run_id: str,
    ds: CmsDataset,
    ingest_date: date,
    session: requests.Session | None = None,
) -> IngestionRecord:
    session = session or requests.Session()
    url = resolve_cms_download_url(ds, session)
    return ingest_http_csv(
        lake=lake, audit=audit, run_id=run_id, source="cms", dataset=ds.name,
        url=url, ingest_date=ingest_date, session=session,
    )


# --------------------------------------------------------------------------- Synthea


def discover_synthea_files(
    root: Path, tables: tuple[str, ...] = SYNTHEA_TABLES
) -> list[SyntheaFile]:
    """Find <root>/<state>/csv/<table>.csv for every *completed* generated state.

    A state is complete only when the generator has written <root>/<state>/_SUCCESS.
    Without this, a run that overlaps generation would land half-written CSVs.
    """
    found: list[SyntheaFile] = []
    if not root.exists():
        return found
    for state_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if not (state_dir / SUCCESS_MARKER).is_file():
            log.warning("skipping %s: no %s marker (incomplete)", state_dir, SUCCESS_MARKER)
            continue
        csv_dir = state_dir / "csv"
        for table in tables:
            path = csv_dir / f"{table}.csv"
            if path.is_file():
                found.append(SyntheaFile(state=state_dir.name, table=table, path=path))
    return found


def ingest_synthea_file(
    *,
    lake: Lake,
    audit: AuditLog,
    run_id: str,
    item: SyntheaFile,
    ingest_date: date,
) -> IngestionRecord:
    return land_file(
        lake=lake,
        audit=audit,
        run_id=run_id,
        source="synthea",
        dataset=f"{item.table}:{item.state}",
        local_path=item.path,
        source_uri=f"file://{item.path}",
        object_key=bronze_key(
            "synthea", item.table, ingest_date, f"{item.table}.csv",
            partitions={"state": item.state},
        ),
    )
