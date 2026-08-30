# C5v2 GCS media gates, r0

- Status: `[VERIFIED]`
- Date: 2026-08-30
- Source commit: `76d9920bfdee488edd01ab53087aef47291b0273`
- Executable SHA-256: `eb2c35ed9e1190a6fc1854aa86368b651f55275dd57fc79a60cff1799880d13f`
- `Cargo.lock` SHA-256: `2bea96cd06da295aa6be6c9a55925647044c5a8b70f83ebdee59753c4edc1341`
- Provider: Apache `object_store` against regional GCS
- Receipt: `gs://doss-objectkv-dev-okv-evals/eval-receipts/c5v2-media-gates-76d9920-r0/receipt.json`
- Receipt generation: `1788123803233451`
- Receipt file SHA-256: `7f19d09592ccded52784e8a8bef9981cff5f4d609280a0692d5df00e60744b38`
- Receipt self-digest: `cfb9019679c9c42ee85bd1672255a28a814915c7c6f460b9728de564be0bc05e`

## Result

| Gate | Result | Frozen bound |
|---|---:|---:|
| Branch root PUTs | 1 | exactly 1 |
| Branch child-object PUTs | 0 | exactly 0 |
| Branch incremental bytes | 4,344 | root only |
| Exact shared bytes referenced | 26,820,839 | unchanged children |
| Compaction runs | 6 | base, four deltas, final compacted run |
| Compaction object PUTs | 24 | four immutable objects per run |
| C5v2 bytes written | 27,304,907 | provider accounting equals independent encoding |
| C0 control bytes written | 26,253,246 | matched logical history |
| C5v2/C0 write ratio | 1.040058x | at most 1.10x |
| LIST requests | 0 | exactly 0 |
| Recovered records | 25,014 | exact oracle |
| Recovered live rows | 15,742 | exact oracle |

The branch root referenced every exact parent child generation without copying
one child object. The compaction sequence reconstructed canonical history
`d4be64434f6b69990a2787876f514c6036727b41dcf1c5e120f91b6ce968ecd4`
from the final four-object C5v2 closure. Both frozen gates passed.

## Execution boundary

The evaluator ran from the authenticated operator workstation against real
regional GCS in project `doss-objectkv-dev`. This receipt verifies provider
byte accounting, create-only object publication, exact branch reuse, no LIST,
and final logical reconstruction. It makes no host-performance, publication
latency, independent-media, or OTel claim. RFC-0049 and performance-matrix row
3 remain `[EVALUATING]` until their remaining independent telemetry and sealed
admission requirements close.
