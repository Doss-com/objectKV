# PostgreSQL replacement-worker readiness, 2026-08-24

Status: `[EXISTS]` local OS-warm release result. `[ACTIVE-WORK]` remote integrity
and cache-state proof.

## Answer

The object-native base does not inherently require a worker to scan the full
relation before its first OLTP read. Candidate `e2c9dd5` opened and read an
authority-selected 512 MiB PostgreSQL relation in 4.75 ms p50 with 61.9 MiB
median RSS. The prior 4.549-second restart result included a deliberate full
snapshot scan. The actual whole-relation operations remain size-linear and
took 1.046 seconds for closure audit and 4.493 seconds for the bounded semantic
oracle at 512 MiB.

This keeps the performance direction. It does not yet prove economical or safe
remote operation.

## Frozen question

Can a fresh process load one durable root, authenticate its complete selected
delta lineage, open the exact immutable base, and serve exact point and bounded
range reads without work proportional to untouched relation bytes?

Contract commit: `f73e20166dda4ee3b38b38a98025381c22d887c7`.

Candidate commit: `e2c9dd59bf266f6cc11cf0707ff2d94a7921f7c8`.

Suite: `postgres-replacement-worker-readiness-v0`.

Backend: local filesystem object root, fresh worker process, OS-warm cache.

Seeds: `724841`, `724842`, `724843`, `724844`, `724845`.

## Measured pipeline

```text
durable root load
  -> authenticate delta object and commit lineage
  -> open manifest-bound immutable view
  -> first delta-overlay point
  -> first immutable-base point
  -> first bounded ordered range
  -> bounded full-snapshot oracle
  -> complete physical-closure audit
```

The source heap did not exist in the worker fixture. Fixture construction was
outside the timed worker. Each reported point used five independent processes;
the runner also performed one deterministic semantic replay.

## Curve

| Relation pages | Logical bytes | Physical closure | View ready p50 | First delta p50 | First base p50 | First 8-page range p50 | Full oracle p50 | Closure audit p50 | RSS p50 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 1 MiB | 1,085,939 B | 2.326 ms | 4.083 us | 181.333 us | 570.417 us | 8.881 ms | 2.118 ms | 20.0 MiB |
| 4,096 | 32 MiB | 34,693,298 B | 2.508 ms | 4.167 us | 156.375 us | 577.958 us | 287.478 ms | 65.163 ms | 67.344 MiB |
| 65,536 | 512 MiB | 555,044,258 B | 4.749 ms | 4.458 us | 141.583 us | 621.250 us | 4.493 s | 1.046 s | 61.844 MiB |

From the smallest to largest fixture, physical bytes grew about 511x. View
readiness grew 2.04x. Full audit grew about 494x and full oracle grew about
506x. The first point and bounded range remained effectively flat.

## Frozen calibration

| Gate | Target | Observed | Result |
| --- | ---: | ---: | --- |
| 65,536-page / 128-page view-ready ratio | <= 4x | 2.04x | pass |
| 65,536-page view ready | <= 100 ms | 4.75 ms | pass |
| first immutable-base point | <= 5 ms | <= 0.181 ms | pass |
| first delta-overlay point | <= 1 ms | <= 0.0045 ms | pass |
| worker RSS | <= 1 GiB | <= 67.4 MiB p50 | pass |

All exact-read, dimension, source-absence, root-identity, delta-lineage,
bounded-oracle, closure-audit, RSS, and semantic-replay gates passed.

## Controls

| Subject | Run | Verdict | Detector |
| --- | --- | --- | --- |
| changed manifest | `04c43b92-97c2-46eb-8d5f-644b139de1d9` | discard | root identity, reads, and audit refused |
| changed delta | `c5002987-7a80-4bcc-9a77-c26ea58e5a96` | discard | delta lineage, reads, and audit refused |
| skipped audit | `f593ff30-f00a-46ea-8c93-41d2a21fee8a` | discard | mandatory closure gate failed |

Correct run IDs were `ddf560bb-0ef9-49d4-8e82-df8a15921f92`,
`b0c32414-718e-48ca-9a3b-24915b612074`, and
`bb7b8c57-b904-4045-ad0c-9977254c73cf`.

## Interpretation

The first-read architecture can work if immutable object identity is cheap to
establish and data blocks are fetched lazily. The Range Engine does not need a
relation-sized RAM image. It needs root and manifest metadata, a bounded delta
overlay, SlateDB's indexes and caches, and the blocks touched by the request.
Whole-relation verification and scans remain linear and belong off the request
path.

The strongest remaining risk is not local lookup cost. It is remote object
request latency and cost, especially with a cold cache or large SST touched by
one point. The current result used local files with OS-warm bytes. It did not
measure GCS GET count, transferred bytes, provider generation checks, CRC32C,
network p99, shared NVMe cache behavior, or dollar cost.

## Decision and next gate

Keep the readiness/audit split. Retain the eager production helper until one
of these contracts is proven:

1. The publication root binds immutable provider generation and checksum.
2. Every touched object or block is authenticated before its rows are returned.
3. The worker uses a previously authenticated local copy bound to the root.

Then run the identical five-seed curve on GCS in three states:

1. metadata warm, data cold;
2. shared persistent NVMe cache warm, decoded RAM cold;
3. empty cache after replacement-host loss.

Record readiness, first point and range p50/p95/p99, object GETs, bytes read,
cache hit rate, audit bandwidth, RSS, and estimated dollars per million reads.
Only that result can answer whether this path is both fast and economical.

## Verification

- release package tests: 63 passed;
- strict release Clippy for `okv-object`, `okv-postgres`, and `okv-eval`: passed;
- formatting and diff checks: passed;
- eval binary SHA-256:
  `6b3bb59509e617cbb74eaa538d39940b0e73c0cb2e71372929d6521ba51936f3`;
- OTel collector: not configured, `telemetry.enabled=false`.
