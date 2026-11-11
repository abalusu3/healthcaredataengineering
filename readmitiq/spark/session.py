"""SparkSession factory: Delta Lake + S3A (SeaweedFS/MinIO/AWS) configuration in one place."""

from __future__ import annotations

from pyspark.sql import SparkSession

from readmitiq.config import Settings


def build_spark(app_name: str, settings: Settings | None = None, master: str | None = None):
    settings = settings or Settings.from_env()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        # --- S3A -> object store ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )
    if settings.s3_endpoint_url:  # non-AWS endpoint: path-style, plain HTTP allowed
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_url)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(settings.s3_endpoint_url.startswith("https")).lower(),
            )
        )
    if master or settings.spark_master:
        builder = builder.master(master or settings.spark_master)
    return builder.getOrCreate()
