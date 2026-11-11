.PHONY: help venv venv-analytics env up down logs synthea synthea-large ingest transform marts dbt gx test lint fmt psql clean

help:  ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-10s %s\n", $$1, $$2}'

venv:  ## Create local virtualenv for tests/lint
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt

venv-analytics:  ## Create local dbt + Great Expectations virtualenv
	python3 -m venv .venv-analytics && .venv-analytics/bin/pip install -q -r requirements-analytics.txt
	cd dbt && ../.venv-analytics/bin/dbt deps --profiles-dir .

env:  ## Create .env from template (with a generated Fernet key)
	@test -f .env || (cp .env.example .env && \
	  key=$$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())") && \
	  sed -i '' "s|^FERNET_KEY=.*|FERNET_KEY=$$key|" .env && echo "Created .env")

up: env  ## Start postgres, seaweedfs, airflow
	docker compose up -d --build

down:  ## Stop the stack (keeps volumes)
	docker compose down

logs:  ## Tail airflow scheduler logs
	docker compose logs -f airflow-scheduler

synthea:  ## Generate synthetic member population into ./data/synthea
	docker compose --profile generate run --rm synthea

synthea-large:  ## Generate ~50k members (3 states x 17,000) for realistic event volumes
	SYNTHEA_POPULATION_PER_STATE=17000 docker compose --profile generate run --rm synthea

ingest:  ## Trigger bronze ingestion (silver + marts follow automatically via assets)
	docker compose exec airflow-scheduler airflow dags unpause build_marts
	docker compose exec airflow-scheduler airflow dags unpause transform_silver
	docker compose exec airflow-scheduler airflow dags unpause ingest_bronze
	docker compose exec airflow-scheduler airflow dags trigger ingest_bronze

transform:  ## Run bronze->silver->warehouse without re-ingesting
	docker compose exec airflow-scheduler airflow dags unpause transform_silver
	docker compose exec airflow-scheduler airflow dags trigger transform_silver

marts:  ## Run dbt + Great Expectations in Airflow (build_marts DAG)
	docker compose exec airflow-scheduler airflow dags unpause build_marts
	docker compose exec airflow-scheduler airflow dags trigger build_marts

dbt:  ## Run dbt build locally against the warehouse (localhost:5432)
	cd dbt && ../.venv-analytics/bin/dbt build --profiles-dir .

gx:  ## Run Great Expectations checks locally
	PYTHONPATH=. .venv-analytics/bin/python -m readmitiq.quality.gx_checks

test:  ## Run unit tests
	.venv/bin/pytest -q

lint:  ## Lint with ruff
	.venv/bin/ruff check .

fmt:  ## Auto-fix lint + format
	.venv/bin/ruff check --fix . && .venv/bin/ruff format .

psql:  ## Open psql on the warehouse DB
	docker compose exec postgres psql -U readmitiq -d readmitiq

clean:  ## Stop stack AND delete volumes (destroys local data)
	docker compose down -v
