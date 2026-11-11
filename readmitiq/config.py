"""Runtime settings, read from environment variables (12-factor style)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    s3_endpoint_url: str | None
    s3_access_key: str
    s3_secret_key: str
    lake_bucket: str
    warehouse_host: str
    warehouse_port: int
    warehouse_db: str
    warehouse_user: str
    warehouse_password: str
    synthea_data_dir: Path
    spark_master: str | None = None
    spark_shuffle_partitions: int = 16

    @property
    def silver_root(self) -> str:
        return f"s3a://{self.lake_bucket}/silver"

    def silver_path(self, table: str) -> str:
        return f"{self.silver_root}/{table}"

    def quarantine_path(self, table: str) -> str:
        return f"{self.silver_root}/_quarantine/{table}"

    def warehouse_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.warehouse_host}:{self.warehouse_port}/{self.warehouse_db}"

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            # None -> real AWS S3; set to SeaweedFS/MinIO URL locally.
            s3_endpoint_url=env("S3_ENDPOINT_URL") or None,
            s3_access_key=env("S3_ACCESS_KEY", ""),
            s3_secret_key=env("S3_SECRET_KEY", ""),
            lake_bucket=env("LAKE_BUCKET", "readmitiq-lake"),
            warehouse_host=env("WAREHOUSE_HOST", "localhost"),
            warehouse_port=int(env("WAREHOUSE_PORT", "5432")),
            warehouse_db=env("WAREHOUSE_DB", "readmitiq"),
            warehouse_user=env("WAREHOUSE_USER", "readmitiq"),
            warehouse_password=env("WAREHOUSE_PASSWORD", ""),
            synthea_data_dir=Path(env("SYNTHEA_DATA_DIR", "data/synthea")),
            spark_master=env("SPARK_MASTER_URL") or None,
            spark_shuffle_partitions=int(env("SPARK_SHUFFLE_PARTITIONS", "16")),
        )

    def warehouse_dsn(self) -> str:
        return (
            f"host={self.warehouse_host} port={self.warehouse_port} "
            f"dbname={self.warehouse_db} user={self.warehouse_user} "
            f"password={self.warehouse_password}"
        )
