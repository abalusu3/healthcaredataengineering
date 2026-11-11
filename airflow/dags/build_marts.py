"""Gold layer: dbt (seeds, SCD2 snapshot, staging -> intermediate -> marts, tests) then
Great Expectations plausibility checks. Triggered when transform_silver reloads raw.*.

  dbt_deps -> dbt_build -> gx_checks -> dbt_docs

dbt and GX run from an isolated virtualenv in the image (/home/airflow/analytics-venv) so
their dependency trees never conflict with Airflow's pinned constraints.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag

WAREHOUSE_RAW = Asset("postgres://postgres:5432/readmitiq/raw/inpatient_stays")
MARTS = Asset("postgres://postgres:5432/readmitiq/marts/fct_admissions")

VENV = "/home/airflow/analytics-venv/bin"
DBT = f"{VENV}/dbt --no-use-colors"
DBT_DIR = "/opt/airflow/dbt"
ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "PYTHONPATH": "/opt/airflow/project",
}


@dag(
    dag_id="build_marts",
    schedule=[WAREHOUSE_RAW],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "analytics-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["readmitiq", "gold", "dbt", "data-quality"],
    doc_md=__doc__,
)
def build_marts():
    common = dict(cwd=DBT_DIR, env=ENV, append_env=True)

    deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT} deps", **common)

    # `dbt build` runs seeds, snapshots, models and their tests in DAG order; a failing test
    # stops downstream models from being built on bad data.
    build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build", **common)

    gx = BashOperator(
        task_id="gx_checks",
        bash_command=f"{VENV}/python -m readmitiq.quality.gx_checks",
        outlets=[MARTS],
        **common,
    )

    docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"{DBT} docs generate",
        **common,
    )

    deps >> build >> gx >> docs


build_marts()
