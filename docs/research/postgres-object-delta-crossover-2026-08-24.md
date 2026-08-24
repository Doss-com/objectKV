# PostgreSQL object-delta relation-size crossover

Status: `[EXISTS]` five-seed local release curve at candidate `efa9d54`.

## Verdict

Keep the immutable base-plus-delta architecture. It has a measured performance
crossover between a 1 MiB and 32 MiB relation when one 8 KiB page changes.

The delta object is byte-identical within each seed from 2 through 65,536
relation pages. It is slower and larger than rewriting a 16 KiB relation,
roughly tied on latency while writing 8.31 percent of a 1 MiB rewrite, 2.98x
faster at 32 MiB, and 4.00x faster at 512 MiB.

This result does not promote JSON delta v1. Its approximately 90,983-byte
segment remains 11.106x larger than the changed page. The architecture passes;
the payload encoding remains discarded.

## Measured curve

```text
same seed + same changed block + same certified mutation
                         |
                         v
       2       128       4,096       65,536 base pages
       |         |           |             |
       +---------+-----------+-------------+
                         |
                         v
             identical immutable delta
                         |
                         v
          isolated full-rewrite reference
```

All values are p50 over seeds `724841..724845` from the optimized arm64
binary on a local filesystem. Delta time includes materialization,
authentication, and activation. Full-rewrite time includes materialization
only, so the comparison is conservative for the delta path.

| Relation | Delta / rewrite bytes | Delta end to end | Full rewrite | Time ratio | Exact restart proof |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 pages, 16 KiB | 336.07% | 21.20 ms | 11.78 ms | 1.788x | 1.88 ms |
| 128 pages, 1 MiB | 8.308% | 23.16 ms | 20.42 ms | 1.134x | 10.11 ms |
| 4,096 pages, 32 MiB | 0.2622% | 88.36 ms | 275.42 ms | 0.3359x | 263.43 ms |
| 65,536 pages, 512 MiB | 0.01639% | 1.138 s | 4.578 s | 0.2502x | 4.549 s |

The 512 MiB point passed every correctness gate and used 3.26 GB maximum RSS
for the complete eval process. That process creates bases, rewrites, opens
views, and scans complete snapshots twice. It is not a serving-worker RSS
measurement.

## Calibration targets

| Target | Observed | Result |
| --- | ---: | --- |
| delta bytes vary by at most 5% | byte-identical within every seed | pass |
| latency crossover by 4,096 pages | ratio 0.3359x | pass |
| at most 1% rewrite bytes at 4,096 pages | 0.2622% | pass |
| at most 0.1% rewrite bytes at 65,536 pages | 0.01639% | pass |

The full-base-in-candidate-root control created 20 SSTs instead of 10, failed
the no-replacement gate for every seed, and discarded.

## Bounds exposed

`[BOUND]` Delta materialization itself is nearly flat through 4,096 pages,
11.10 ms, 10.24 ms, and 11.94 ms at the first three points. It rises to 31.55
ms in the 512 MiB subject.

`[BOUND]` Activation grows from 10.90 ms at 2 pages to 1.106 seconds at 65,536
pages. The current immutable-base open path is not relation-size independent.

`[BOUND]` The restart metric includes a complete ordered snapshot scan for the
correctness oracle. Its 4.549-second result is not worker-ready latency. The
eval must split root authentication, view readiness, first point read, and full
oracle scan before setting a restart service-level objective.

`[BOUND]` The local filesystem and warm operating-system cache do not model GCS
or S3 request latency, throughput, multipart behavior, or request price.

## Measurement correction

Candidate `f9bc4c5` used a generic counter conversion that clamped nanosecond
durations at 4.294967295 seconds. Its first 65,536-page latency result was
discarded. Candidate `efa9d54` converts nanoseconds through `Duration` and the
entire clean curve was rerun. Smaller points were below the old clamp and
remained consistent.

## Next gates

1. Split replacement-worker startup into root authentication, view ready,
   first point, first range, and optional full-snapshot verification. A worker
   must not scan the complete base before serving one point.
2. Run that readiness curve with warm RAM, persistent NVMe, empty cache, and
   GCS. Record object requests and bytes alongside latency.
3. Replace JSON v1 with compact binary v2 and retain byte-identical semantics,
   corruption refusal, and exact restart. Target at most 2x changed bytes.
4. Hold relation size constant while batching 1, 8, 64, and 512 changed pages.
   This locates the object flush knee and proof amortization.
5. Measure 1, 8, 32, and 128 selected delta layers before choosing compaction
   thresholds.

## Evidence identity

- candidate: `efa9d540d5aacdc84567c3912b7e8fb62dbc2093`
- contract: `1f42fb9d37796e58a93db89e262bab254c3fd4f6`
- suite SHA-256: `65361b89bc697d5f41f75bd4d21c7e4e431e56356791f6cd7312158eb603c25b`
- metric registry SHA-256: `496129c18636fa11b9a608a7a49fdb59a200b50e64c903c1c3a753c9c28ab0c2`
- release executable SHA-256: `229a4ea125a6f51ba3a614794cdd375c8d561cc7ca490e71697c04639928b3c6`
- telemetry: disabled because no collector was configured

The compact result hashes are recorded in `experiments/ledger.jsonl`.
