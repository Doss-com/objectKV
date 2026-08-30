# RFC-0049 admitted r1 failed execution

- Status: `[VERIFIED]` retained evaluator failure, no performance verdict
- Date: 2026-08-30
- Controller run: `aae9ea84-089e-4599-8c4d-a54ebac09753`
- Candidate commit: `dfc29f5d5058af936040f01455e526d42016bc85`
- Admission plan SHA-256: `1faec4b6eabd37ae99f2ac3309edec659915705ab31ab5e2c2f59cf7e784f01a`
- Failed archive SHA-256: `90d2b6c29047edbe3d6b32dff071c69a8d7e1ca4f91ddb3e86fb0c71da49215d`
- GCS generation: `1788090732653022`
- Archive: `gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0049-t28-aligned-r1-dfc29f5/failed-run.tar.gz`

The one-execution controller started and completed the first C5v2 point
position. It stopped at the following C0 position before a performance run
receipt could be aggregated:

```text
trace seed 5701
block 0
position 1
subject c0_indexed_row
error: provider attempt differs from its fixture descriptor
```

The C0 closure contains two valid objects with role `data`. The r1 validator
selected the first same-role descriptor before comparing the observed key, so
an exact generation-pinned read from the second object failed validation. The
sealed `failed-run.json` records successful flush and shutdown for logs,
metrics, and traces. The result does not accept or reject C5v2 performance.

Admission r2 retains this archive digest and changes descriptor selection to
resolve by exact object key before checking generation, byte range, and
response length. Persisted evidence replay uses the same lookup.
