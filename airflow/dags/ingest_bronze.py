"""Land all upstream sources in the bronze layer of the lake.

Sources:
  * CMS Provider Data (HRRP readmission penalties, hospital general info)
  * CDC Social Vulnerability Index (county)
  * Synthea synthetic member population (one partition per state)

Synthea files are fanned out with dynamic task mapping, one task per (state, table),
so retries and parallelism are per file. Every load is audited in ops.ingestion_log and
is idempotent: byte-identical files are skipped.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

# Downstream DAGs (transform_silver) are scheduled on this asset.
BRONZE = Asset("s3://readmitiq-lake/bronze")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


def _deps():
    """Build lake + audit clients inside tasks (never at DAG-parse time)."""
    from readmitiq.audit import PostgresAuditLog
    from readmitiq.config import Settings
    from readmitiq.storage import Lake

    settings = Settings.from_env()
    return settings, Lake.from_settings(settings), PostgresAuditLog(settings.warehouse_dsn())


def _run_id() -> str:
    return get_current_context()["run_id"]


def _today():
    # Partition by actual landing date: manual runs in Airflow 3 have no logical date.
    return datetime.now(UTC).date()


@dag(
    dag_id="ingest_bronze",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["readmitiq", "bronze", "ingestion"],
    doc_md=__doc__,
)
def ingest_bronze():
    @task
    def ensure_lake() -> None:
        _, lake, _ = _deps()
        lake.ensure_bucket()

    @task
    def ingest_cms(dataset_name: str) -> dict:
        from readmitiq.ingest import ingest_cms_dataset
        from readmitiq.sources import CMS_DATASETS

        _, lake, audit = _deps()
        rec = ingest_cms_dataset(
            lake=lake,
            audit=audit,
            run_id=_run_id(),
            ds=CMS_DATASETS[dataset_name],
            ingest_date=_today(),
        )
        return {"dataset": rec.dataset, "status": rec.status, "lines": rec.line_count}

    @task
    def ingest_svi() -> dict:
        from readmitiq.ingest import ingest_http_csv
        from readmitiq.sources import SVI_COUNTY_URL

        _, lake, audit = _deps()
        rec = ingest_http_csv(
            lake=lake,
            audit=audit,
            run_id=_run_id(),
            source="cdc",
            dataset="svi_county",
            url=SVI_COUNTY_URL,
            ingest_date=_today(),
        )
        return {"dataset": rec.dataset, "status": rec.status, "lines": rec.line_count}

    @task
    def list_synthea_files() -> list[dict]:
        from readmitiq.ingest import discover_synthea_files

        settings, _, _ = _deps()
        files = discover_synthea_files(settings.synthea_data_dir)
        if not files:
            raise FileNotFoundError(
                f"No Synthea CSVs under {settings.synthea_data_dir}. Run: make synthea"
            )
        return [{"state": f.state, "table": f.table, "path": str(f.path)} for f in files]

    @task(map_index_template="{{ synthea_file_label }}")
    def ingest_synthea(item: dict) -> dict:
        from readmitiq.ingest import SyntheaFile, ingest_synthea_file

        # Human-readable label for each mapped task instance in the UI.
        get_current_context()["synthea_file_label"] = f"{item['table']}:{item['state']}"
        _, lake, audit = _deps()
        rec = ingest_synthea_file(
            lake=lake,
            audit=audit,
            run_id=_run_id(),
            item=SyntheaFile(state=item["state"], table=item["table"], path=Path(item["path"])),
            ingest_date=_today(),
        )
        return {"dataset": rec.dataset, "status": rec.status, "lines": rec.line_count}

    @task(outlets=[BRONZE])
    def summarize(cms: list[dict], svi: dict, synthea: list[dict]) -> None:
        results = [*cms, svi, *synthea]
        by_status: dict[str, int] = {}
        for r in results:
            by_status[r["status"]] = by_status.get(r["status"], 0) + 1
        print(f"Bronze ingestion complete: {by_status}")
        for r in results:
            print(f"  {r['status']:<9} {r['dataset']:<40} {r['lines']:>12,} lines")

    lake_ready = ensure_lake()
    cms = ingest_cms.expand(dataset_name=["hrrp", "hospital_general_info"])
    svi = ingest_svi()
    synthea = ingest_synthea.expand(item=list_synthea_files())
    lake_ready >> [cms, svi, synthea]
    summarize(cms, svi, synthea)


ingest_bronze()
