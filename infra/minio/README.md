# Pinned MinIO conformance fixture

Status: `[VERIFIED]` local-only S3 protocol fixture. It is not durability or cloud
evidence.

The server is MinIO `RELEASE.2025-09-07T16-13-09Z`, pinned by multi-platform
manifest digest. The client image is also digest-pinned. Ports bind to loopback
only, and the default credentials are intentionally development-only.

```bash
mkdir -p /tmp/objectkv-minio-data
docker compose -f infra/minio/compose.yaml up -d --wait minio
docker compose -f infra/minio/compose.yaml run --rm initialize

export OKV_S3_ENDPOINT=http://127.0.0.1:19110
export OKV_S3_BUCKET=okv-dev
export OKV_S3_ACCESS_KEY_ID=okvdev
export OKV_S3_SECRET_ACCESS_KEY=okv-dev-only-secret
export OKV_OBJECT_SERVER_VERSION=RELEASE.2025-09-07T16-13-09Z

cargo run -p okv-object -- --backend minio --profile authority
cargo run -p okv-eval -- run evals/suites/object-store.toml \
  --profile minio-authority \
  --workload named-object-authority-contract \
  --backend minio
```

Override `OKV_MINIO_DATA_DIR`, credentials, or ports when the defaults collide.
Do not reuse these credentials or this single-process fixture outside local
development.
