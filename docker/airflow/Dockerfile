ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG PYSPARK_VERSION=4.1.3
ARG DELTA_VERSION=4.4.1

# Java is needed because the Spark *driver* runs inside Airflow (client mode).
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java
USER airflow

# Airflow-side deps pinned by the official constraints file.
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Spark pinned separately (must match the Spark cluster image exactly, not Airflow's constraints).
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" "delta-spark==${DELTA_VERSION}"
COPY docker/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py

# dbt + Great Expectations in an isolated venv (their pins conflict with Airflow's constraints).
COPY requirements-analytics.txt /tmp/requirements-analytics.txt
RUN python -m venv /home/airflow/analytics-venv \
 && /home/airflow/analytics-venv/bin/pip install --no-cache-dir -r /tmp/requirements-analytics.txt

# Project package is bind-mounted at runtime for fast iteration; make it importable.
ENV PYTHONPATH=/opt/airflow/project
