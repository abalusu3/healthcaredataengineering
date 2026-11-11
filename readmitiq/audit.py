"""Ingestion audit log (ops.ingestion_log) for lineage and idempotency."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class IngestionRecord:
    run_id: str
    source: str
    dataset: str
    object_key: str
    source_uri: str
    sha256: str
    size_bytes: int
    line_count: int | None
    status: str  # loaded | unchanged | failed
    error_message: str | None = None


class AuditLog(Protocol):
    def latest_loaded_sha(self, source: str, dataset: str) -> str | None: ...

    def record(self, rec: IngestionRecord) -> None: ...


class PostgresAuditLog:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def _connect(self):
        import psycopg2  # imported lazily so unit tests don't need it

        return psycopg2.connect(self.dsn)

    def latest_loaded_sha(self, source: str, dataset: str) -> str | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT sha256 FROM ops.ingestion_log
                WHERE source = %s AND dataset = %s AND status = 'loaded'
                ORDER BY ingested_at DESC LIMIT 1
                """,
                (source, dataset),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def record(self, rec: IngestionRecord) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.ingestion_log
                    (run_id, source, dataset, object_key, source_uri, sha256,
                     size_bytes, line_count, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    rec.run_id,
                    rec.source,
                    rec.dataset,
                    rec.object_key,
                    rec.source_uri,
                    rec.sha256,
                    rec.size_bytes,
                    rec.line_count,
                    rec.status,
                    rec.error_message,
                ),
            )


class InMemoryAuditLog:
    """Test double / local dry-run implementation."""

    def __init__(self) -> None:
        self.records: list[IngestionRecord] = []

    def latest_loaded_sha(self, source: str, dataset: str) -> str | None:
        for rec in reversed(self.records):
            if rec.source == source and rec.dataset == dataset and rec.status == "loaded":
                return rec.sha256
        return None

    def record(self, rec: IngestionRecord) -> None:
        self.records.append(rec)
