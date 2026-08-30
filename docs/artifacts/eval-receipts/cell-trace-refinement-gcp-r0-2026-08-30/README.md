# Historical cell trace-conformance receipt, GCP R0

Status: `[EVALUATING]` as current-model conformance. This retained receipt is
historical mechanism evidence bound to model SHA-256 `cca6a66f...` and a
checker that did not independently replay its derived result. It does not
verify the current R2 model, a complete cell, or independent-machine media.

## Result

The exact Rust process events from the RFC-0045 L1 staged txLog mechanism were
mapped to the stable transition vocabulary in the earlier RFC-0050 model.

```text
three txLog processes
  -> CellTraceEventV1
  -> historical checker bound to exact ObjectKVCell.tla SHA-256
  -> healthy prefix accepted
  -> early-acknowledgement poison rejected
```

| Subject | Events | Post-restart assertions | Result |
|---|---:|---:|---|
| correct | 36 | 3 | accepted, zero anomalies |
| acknowledge before sync | 15 | 3 | rejected at assertion 0 |

The poison violation was
`StableQuorumAtAcknowledgement: acknowledgement lacks a restart-observed stable quorum`.

## Identity

| Boundary | Identity |
|---|---|
| source commit | `beba5ef44299397d0ebc5f1af5cd81c64f850148` |
| release binary SHA-256 | `263ce8d88a0ec1e3f656a593aaf7674875e8666ea83478a5de673b86131b6042` |
| `Cargo.lock` SHA-256 | `2bea96cd06da295aa6be6c9a55925647044c5a8b70f83ebdee59753c4edc1341` |
| TLA+ model SHA-256 | `cca6a66fb31c8d314f9347b6db285a231ca75324b6a0499c4e29755636470b4b` |
| healthy report SHA-256 | `47929fc04855b70208277468222d9dd0943066d7632497ba2fd3a4fb5d6a4219` |
| healthy implementation trace SHA-256 | `b6f8aa46e58aeca64b038a6d55c379327b81d9862b9926132177acc05c198d14` |
| healthy refinement trace SHA-256 | `a570ccb1a8df3680ed4861820a1512594eebea39a51d6e1f4f5c8663bb909ccb` |
| poison report SHA-256 | `a2785e868c6fe041c3c5443319caa602955a2fdbd09864d943998693464a1bfc` |
| poison implementation trace SHA-256 | `319188baadef4ed60ccc8dde4bf0ee577823ffa529d4ee71be2fe8391469458c` |

Runner: `objectkv-bench-t27a2-r0-runner`, project `doss-objectkv-dev`, zone
`us-central1-a`.

Retained object:
`gs://doss-objectkv-dev-okv-evals/eval-receipts/cell-trace-beba5ef-r0/trace-receipts.tar.gz`,
generation `1788118582554879`, 1,753 bytes, archive SHA-256
`c9d32475368f12f195cd5e5116381a622b172bdf50d27e306113718532cc0aa6`.

## Claim boundary

`[VERIFIED]` RFC-0045 L1 separately covers RAM staging, stable journal
persistence, process loss, restart observation, writer-generation replacement,
and the stable-quorum acknowledgement mechanism for this one-host scope.

`[EVALUATING]` A fresh trace must use the R2 model identity and replay-validating
checker. Transaction conflict resolution, replicated commit delivery, object
publication, txLog pop, complete serving recovery, and independent-machine
failure still need implementation traces. The fourth position exercises
generation advance and installation, but is not post-restart acknowledgement
evidence.
