# RFC-0004: Object-store correctness contract

- Status: draft
- Created: 2026-08-22

## Questions to resolve

- Conditional create/update semantics required for publication.
- Read-after-write assumptions for named objects.
- Range GET, checksum, retry, timeout, and lost-response behavior.
- Provider conformance across memory, filesystem, MinIO/S3, GCS, and Azure.
- Why LIST is never logical authority and where it remains useful for leak audit.

## Failure gate

The conformance suite must detect duplicate publication, partial/corrupt reads,
lost successful responses, unsafe overwrite, and implementations that derive
logical state from LIST.
