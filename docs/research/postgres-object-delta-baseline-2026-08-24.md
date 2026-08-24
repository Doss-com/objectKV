# PostgreSQL object-delta economics baseline

Status: `[EXISTS]` five-seed local release result at candidate `fc88122`.

## Verdict

The incremental architecture is worth continuing, but delta format v1 is not
the production encoding.

For one high-entropy 8 KiB page changed inside a 1 MiB relation, the immutable
delta writes only 8.31 percent as many bytes as a replacement full base. This
validates the main economic reason to separate flush from compaction. The same
delta is 11.11x larger than its logical changed page, which rejects nested JSON,
full commit envelopes, and per-record certificates as the long-lived format.

The release-build local path is close to the proposed latency targets. Delta
materialization takes 11.08 ms p50, activation takes 12.05 ms p50, and exact
source-free reopen takes 9.77 ms p50. Materialization plus activation takes
23.13 ms, versus 20.31 ms for the 1 MiB replacement-base reference. There is no
end-to-end latency win at this small relation size yet.

## Measured pipeline

```text
one changed PostgreSQL page, 8 KiB
        |
        v
certified Cell commit record
        |
        v
JSON delta v1 + full envelope + txLog certificate
        |
        |  11.08 ms p50 materialize
        v
content-addressed .segment, about 91 KiB
        |
        |  12.05 ms p50 authenticate + activate
        v
durable root: FullBase(F) + Delta(F, O]
        |
        |  9.77 ms p50 source-free reopen
        v
exact 128-page snapshot at O
```

## Results

All latency values are p50 across seeds `724841..724845` from the optimized
`okv-eval` binary on arm64 and a local filesystem. Pages contain deterministic
high-entropy bytes so compression cannot manufacture the result.

| Measure | Observed | Proposed target | Read |
| --- | ---: | ---: | --- |
| delta bytes / changed bytes | 11.106x | at most 2x | miss, format v1 discarded |
| delta bytes / replacement-base bytes | 8.31% | at most 10% | pass |
| delta materialization | 11.08 ms | at most 10 ms | near miss |
| delta activation | 12.05 ms | at most 25 ms | pass |
| source-free reopen | 9.77 ms | at most 50 ms | pass |
| delta materialize + activate | 23.13 ms | below full rewrite | miss at 1 MiB |
| replacement full-base materialization | 20.31 ms | reference | reference |

The median encoded delta is 90,983 bytes. Across all five seeds, delta output
averaged 91,051 bytes and the replacement full base averaged 1,095,175 bytes.
The candidate created one delta object per checkpoint capture and zero
replacement SSTs.

## Correctness controls

The correct append and restart subjects kept with exact deterministic replay.
Every unsafe subject discarded and its dedicated detector passed:

- selected delta object removed;
- selected delta object corrupted;
- prior commit-chain digest changed;
- delta omitted from the stable publication closure;
- txLog pop attempted beyond the selected object frontier;
- replacement full-base SST written.

## Decision

`[DECIDED]` Keep the base-plus-delta architecture and its immutable descriptor,
lineage, publication, restart, and pop boundaries.

`[DECIDED]` Do not promote delta format v1 as an economical storage format. Its
JSON byte arrays, nested envelope encoding, and repeated per-record proof are
baseline scaffolding.

`[ACTIVE-WORK]` The next format should use a compact binary page or row delta,
batch proof at the segment or publication-root level, and optional compression.
It must preserve the same corruption and exact-restart controls.

## Next curves

1. Hold one changed page constant across 1, 128, 4,096, and 65,536 relation
   pages. Delta bytes should remain flat while full-rewrite bytes scale.
2. Hold relation size constant and batch 1, 8, 64, and 512 changed pages. This
   locates proof amortization and the flush-size knee.
3. Reopen 1, 8, 32, and 128 delta layers. This sets the compaction trigger from
   measured restart and point-read cost.
4. Repeat warm RAM, warm NVMe, empty cache, and GCS profiles. The local result
   makes no remote latency or request-cost claim.
5. Compare binary v2 against v1 on identical certified records. Keep v2 only if
   it reaches at most 2x changed bytes without weakening any hard gate.

## Evidence limits

- This is a crate-level objectKV/PostgreSQL physical-page contract, not a
  literal PostgreSQL process checkpoint.
- The local filesystem and OS cache were warm. NVMe eviction and empty-cache
  behavior were not isolated.
- OTLP support exists in the runner, but this run had no collector configured,
  so telemetry export was disabled. Schema-valid JSON results were retained by
  content hash in the experiment ledger.
- Stable publication network latency, GCS/S3 PUT latency, object request cost,
  compaction, garbage collection, and many-layer reads remain unmeasured.
