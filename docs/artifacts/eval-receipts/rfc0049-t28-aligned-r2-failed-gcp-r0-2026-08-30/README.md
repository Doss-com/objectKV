# RFC-0049 admitted r2 failed execution

- Status: `[VERIFIED]` retained evaluator failure, diagnostic measurements only
- Date: 2026-08-30
- Controller run: `1af0e120-a6b0-4002-b877-feab81921a4d`
- Candidate commit: `c41b714adbac4e1b2f4c47310dc5e7ec5acc3100`
- Failed receipt SHA-256: `374e0f8c985cf5faee1174766a9db6dfc22aaa1e7f61f76f1cffad1ef2f90482`
- Failed archive SHA-256: `de30d4a0f05ef68c6a9c2ba0218f9560275a651e13269b5a527b01b0e6ef94a7`
- GCS generation: `1788107309663753`
- Archive size: 6,150,854 bytes
- Archive: `gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0049-t28-aligned-r2-c41b714/failed-run.tar.gz`

All 60 point positions and 30 projected-scan positions completed on the GCP R0
runner. The controller then rejected final evidence replay because its live
scan binding used `c0_indexed_row` and `c5v2_aligned_columnar`, while the
persisted validator expected names with a `_scan` suffix.

```text
workload execution: complete
performance aggregation: not sealed
OTel finalization: not independently admitted
curve verdict: none
```

This is an evaluator naming defect, not a workload result. The exact child
receipts are retained. The independently recomputed diagnostic measurements
are useful for architectural direction but cannot change RFC-0049 status.

## Diagnostic measurements

Each subject produced 30,720 point operations.

| Point percentile | C0 indexed row | C5v2 aligned columnar | C5v2 / C0 |
|---|---:|---:|---:|
| p50 | 25.964 ms | 27.627 ms | 1.064x |
| p95 | 41.997 ms | 45.442 ms | 1.082x |
| p99 | 58.772 ms | 69.313 ms | 1.179x |
| p99.9 | 140.168 ms | 184.923 ms | 1.319x |

Across the 15 paired point blocks, the p99 ratio ranged from 0.905x to 1.664x
with a 1.199x median. All 15 were below the frozen 2.00x limit.

The 15 projected-scan throughput ratios ranged from 21.523x to 33.031x with a
28.426x median. All were above the frozen 2.00x limit.

C5v2 stored media was 13,695,766 bytes versus 13,125,073 bytes for C0, a
1.043x ratio and below the 1.10x stored-amplification limit. Compaction,
complete-closure recovery, branch reference reuse, and independent OTel
finalization were not admitted by this failed run.

## Correction

The validator now uses the same stable subject identifiers as the live
controller. A focused regression asserts all three scan subject names. The 90
positions will not be rerun only to convert these diagnostics into a green
receipt. RFC-0049 advances to the remaining decision-bearing gates: recovery,
compaction write amplification, and branch reference reuse.
