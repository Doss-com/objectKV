# G4.1 indexed cold point-read diagnostic

> This single-object diagnostic is superseded as the current G4.1 readout by
> [the bounded row-object scale diagnostic](bounded-row-object-scale-g4.1.md).
> It remains the immutable record of the earlier physical hypothesis.

- Status: `[EVALUATING]`
- Date: 2026-08-26
- Source state: dirty, diagnostic only
- Backend: Apache `object_store` local filesystem
- Build: unoptimized diagnostic
- OTel export: disabled; schema-valid result measurements are retained

## Question

Can a public immutable row-object layout make point-read request work
independent of total object bytes after one small index is cached?

## Read path

```text
warm once
  index object GET -> validate index checksum -> cache 93,210 bytes

each point read
  key + version
       -> sparse block index
       -> one 65,048-byte data range GET
       -> verify block SHA-256
       -> select newest visible value or tombstone
```

The v1 pilot keeps every version of one key in the same block. Data blocks and
the index have independent checksums. The reader uses an exact object name and
revision and never calls LIST.

## Diagnostic result

The dataset contains 65,536 sorted keys with deterministic incompressible
1,024-byte values. The encoded data object is 68,757,844 bytes across 1,058
blocks. Candidate and control each ran 7,500 uniform point reads across three
fixed seeds.

| Path | Data requests/read | Bytes/read | p50 | p99 | Result |
|---|---:|---:|---:|---:|---|
| Serving-range candidate | 1.000 | 65,048 | 2.198 ms | 2.383 ms | all gates pass, dirty source |
| Direct indexed reader | 1.000 | 65,048 | 2.198 ms | 2.371 ms | all gates pass, dirty source |
| Complete-object scan poison | 1.000 full GET | 68,757,844 | 2.272 s | 2.272 s | discarded |

The ServingWorker boundary is effectively equal to the direct reader at p50
and adds 0.48 percent at p99 in this debug run. Both deltas are noise rather
than evidence of improvement. The scan poison returns the same exact values but
transfers 1,057 times more bytes and is about 1,034 times slower at p50. The
eval correctly records semantic
correctness separately from physical-strategy rejection.

## Receipts

| Workload | Run ID | Verdict | SHA-256 |
|---|---|---|---|
| Candidate | `dcbc4d57-707a-45bf-9c38-c0b08d81796c` | inconclusive | `b6636962826d723b3f47f79fc85c3caeb6594e14ae3cce868ce0adaed621b42e` |
| Direct control | `a5e98b01-5ec4-424b-bd6d-9c06073237c0` | inconclusive | `0ed1dfab72ffc93b7af9b323ac2dfb83a9b85e869e9934412ce3816d21d316df` |
| Scan poison | `0b5db912-2c11-4b9c-9a27-4231c4629763` | discard | `ce27766d73a62a7a5f65ebae8d7283c5aae4fb46a7169c100c2a8adcf578090b` |

The result files are under
`docs/artifacts/eval-receipts/cold-read-g4.1/`. The suite hash is
`969c65e980a1d72c0d8fd2b2adc31eb64383d776428ba3100cdef6bce52bd232`.

## What this establishes

- `[CODE-COMPLETE]` A format-versioned, checksummed row-object point reader can
  select one data block from a separately cacheable sparse index.
- `[CODE-COMPLETE]` The candidate, direct control, and complete-object scan
  poison run through the shared eval result schema.
- `[EVALUATING]` Request amplification is one data range GET for this 64 MiB
  local-filesystem profile.

## What remains unproven

- The current format covers point values and point tombstones. Range
  tombstones, transaction boundaries, compression, encryption, bloom filters,
  scans, and mixed-format compatibility remain open.
- One 64 MiB object does not prove flat metadata or request work as the database
  grows. The next curve must sweep object and manifest counts.
- Local filesystem latency is not S3, GCS, MinIO, or production network
  latency. Release builds, concurrency, CPU, RSS, and request cost remain open.
- The receipts are inconclusive because the source tree is dirty and telemetry
  export was disabled.

## Next admission run

Run the same format at 1 MiB, 8 MiB, 64 MiB, and 10 GiB without changing the
lookup algorithm. Require one data range GET, bounded cached index and manifest
work, and flat bytes per point at every size. Then repeat the admitted sizes on
MinIO and GCS with release binaries, concurrency, OTel, and a cost snapshot.
