ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Install project deps pinned against Airflow's official constraints file so
# provider/transitive versions stay compatible with this Airflow release.
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Project package is bind-mounted at runtime for fast iteration; make it importable.
ENV PYTHONPATH=/opt/airflow/project
