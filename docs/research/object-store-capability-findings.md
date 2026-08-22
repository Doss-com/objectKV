# Object-store capability findings

Status: `[EXISTS]` primary-source observations used by RFC-0004 and the first
conformance implementation.

## Observations

- Apache `object_store 0.14.1` defines atomic `put_opts`, conditional
  `PutMode::Create`, and conditional `PutMode::Update(UpdateVersion)`. It advises
  callers to preserve both ETag and version fields. Source:
  <https://github.com/apache/arrow-rs-object-store/blob/0.14.1/src/lib.rs>.
- The same revision's local filesystem implementation returns `NotImplemented`
  for `PutMode::Update`. Source:
  <https://github.com/apache/arrow-rs-object-store/blob/0.14.1/src/local.rs>.
- Its S3 adapter sends `If-None-Match: *` for create and `If-Match` for update
  only when conditional PUT is explicitly enabled. Source:
  <https://github.com/apache/arrow-rs-object-store/blob/0.14.1/src/aws/mod.rs>.
- Its GCS adapter maps create to generation match `0` and update to the stored
  object version, but its shared delete path carries no generation condition.
  Source:
  <https://github.com/apache/arrow-rs-object-store/blob/0.14.1/src/gcp/client.rs>.
- Google Cloud documents generation-match preconditions for uploads and deletes,
  including generation `0` for create-if-absent. It also warns that ranged reads
  without a generation can mix overwritten versions. Sources:
  <https://cloud.google.com/storage/docs/request-preconditions> and
  <https://cloud.google.com/storage/docs/consistency>.
- MinIO fixed a 2025 case where `If-Match` on a missing object could be ignored.
  The pinned fixture is the following release,
  `RELEASE.2025-09-07T16-13-09Z`. Source:
  <https://github.com/minio/minio/issues/21526>.

## Not observed

- No revision-guarded delete operation exists in the shared Apache
  `object_store 0.14.1` trait.
- No live GCS response has been captured for this repository because the local
  Google Cloud login is expired and the `objectKV-dev` project is not yet
  provisioned.
- No evidence yet maps actual provider throttling and ambiguous network failures
  into the full RFC retry taxonomy without inspecting backend-specific error
  sources.
