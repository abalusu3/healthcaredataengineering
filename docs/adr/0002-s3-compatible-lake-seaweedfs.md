# ADR 0002: S3-compatible object storage via SeaweedFS

**Status:** Accepted · 2026-11-10

## Context
The lake should use the same object-storage APIs as production clouds (S3/ADLS, Databricks volumes), not a local filesystem. MinIO, the usual local choice, stopped publishing community container images in late 2025.

## Decision
Use SeaweedFS (Apache-2.0, actively maintained) with its S3 gateway. Application code accesses storage only through boto3 and an `S3_ENDPOINT_URL` setting.

## Consequences
- Pointing at AWS S3 only requires unsetting `S3_ENDPOINT_URL` and supplying IAM credentials. No code changes.
- Unit tests use `moto` to mock S3, so they need no running services.
