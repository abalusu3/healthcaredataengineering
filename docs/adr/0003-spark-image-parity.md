# ADR 0003: One pip-based Spark build for the driver (Airflow) and the cluster

**Status:** Accepted · 2026-11-11

## Context
Airflow submits PySpark jobs in client mode, so the Spark driver runs inside the Airflow container and the executors run on the Spark worker. PySpark requires the same Python minor version and the same Spark version on both sides. Stock Spark images (`apache/spark`) ship their own Python, which usually differs from the Airflow image's. Delta Lake, `hadoop-aws`, and the JDBC driver must also be on both classpaths, with versions that match Spark's bundled Hadoop and Jackson.

## Decision
- Both images install the **same pip `pyspark==4.1.3` + `delta-spark==4.4.1` on Python 3.11 and OpenJDK 17**.
- A shared build script, `docker/spark/fetch_jars.py`, resolves `delta-spark_4.1_2.13`, `hadoop-aws` (pinned to the Hadoop version bundled in pyspark, 3.4.2), and the Postgres JDBC driver through Ivy **at build time**. It copies them into pyspark's `jars/` directory and re-pins any transitive Jackson modules to Spark's Jackson version.
- Transforms use only Spark built-in functions (no Python UDFs), so executors never start Python workers.

## Consequences
- Jobs don't download anything from Maven at runtime, and the driver and executors have identical classpaths.
- The Spark image and the Airflow image must be rebuilt together when versions change. Both pin versions through build args.
- The same transforms run unchanged on Databricks (DBR 17 = Spark 4.x), with only session setup differing.
