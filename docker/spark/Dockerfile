# Spark standalone master/worker built from pip pyspark on Python 3.11.
# The Airflow image installs the identical pyspark + jars, so driver and executors match.
FROM python:3.11-slim-bookworm

ARG PYSPARK_VERSION=4.1.3
ARG DELTA_VERSION=4.4.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" "delta-spark==${DELTA_VERSION}"
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

COPY docker/spark/fetch_jars.py /tmp/fetch_jars.py
RUN python /tmp/fetch_jars.py && rm /tmp/fetch_jars.py

ENV PYTHONPATH=/opt/project
WORKDIR /opt/spark-work
ENTRYPOINT ["/usr/bin/tini", "--"]
