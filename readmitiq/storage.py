"""Thin S3 wrapper for the data lake (works with AWS S3, SeaweedFS, MinIO)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from readmitiq.config import Settings


class Lake:
    def __init__(self, client, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    @classmethod
    def from_settings(cls, settings: Settings) -> Lake:
        client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key or None,
            aws_secret_access_key=settings.s3_secret_key or None,
            region_name="us-east-1",
        )
        return cls(client, settings.lake_bucket)

    def ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
                raise
            self.client.create_bucket(Bucket=self.bucket)

    def put_file(self, key: str, path: Path, metadata: dict[str, str] | None = None) -> None:
        # upload_file transparently switches to multipart for large files.
        self.client.upload_file(str(path), self.bucket, key, ExtraArgs={"Metadata": metadata or {}})

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys
