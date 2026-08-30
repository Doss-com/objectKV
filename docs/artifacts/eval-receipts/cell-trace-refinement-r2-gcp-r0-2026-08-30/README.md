# Current-model staged txLog trace, GCP R0

- Status: `[VERIFIED]` scoped hand-written bounded trace conformance
- Date: 2026-08-30
- Source commit: `d351f3549fc6e38e397421cf36817bebe7fb5773`
- Release binary SHA-256: `8716b93a8bccd4b800ba29afa79169440372047d0f9ce696a7f8834e1836350d`
- `Cargo.lock` SHA-256: `2bea96cd06da295aa6be6c9a55925647044c5a8b70f83ebdee59753c4edc1341`
- TLA+ model SHA-256: `55d5bb137b9e3c37deace42f92b4602b022a7583b0a23a801ef707f40618a3ba`
- Runner: `objectkv-bench-t27a2-r0-runner`, `us-central1-a`
- Toolchain: Rust 1.88.0
- Raw archive: `gs://doss-objectkv-dev-okv-evals/eval-receipts/objectkv-cell-trace-r2-d351f35/objectkv-cell-trace-r2-d351f35.tar.gz`
- Archive generation: `1788125012353672`
- Archive SHA-256: `bf04f836915740d867f7254fb165666898ae5a7a9eab098fa516ea9e4d04f344`

## Result

Three healthy seeds, 17, 29, and 43, each started and killed six real TCP child
processes across three distinct local journals. Each emitted the same
deterministic 36-event semantic trace and three restart-observed stable-quorum
assertions. The current R2 checker independently replayed every trace and
accepted it with zero anomalies.

| Property per healthy seed | Result |
|---|---:|
| Process starts / kills | 6 / 6 |
| Acknowledged appends | 4 |
| Network append requests | 18 |
| Acknowledged record loss | 0 |
| Torn-tail repairs | 1 |
| Stale-writer rejections / mutations | 3 / 0 |
| Byte-identical segment previews | true |
| Object operations | 0 |
| Maximum journal bytes / bound | 5,992 / 65,536 |
| Trace events / assertions | 36 / 3 |
| Healthy semantic trace SHA-256 | `f897c76d2df0857b84d491848506b385554b67b36b675ce1dfd203fbccef0444` |

## Controls and proof boundary

| Control | Process oracle | Current trace checker |
|---|---|---|
| acknowledge before sync | detected | rejected at `StableQuorumAtAcknowledgement` after 15 events |
| accept stale epoch | detected | not represented in emitted trace vocabulary |
| node-specific segment bytes | detected | not represented in emitted trace vocabulary |

The result closes model-identity drift for the staged stable-media prefix. It
does not emit `CommitTxn` or `DeliverCommitted`, mechanically refine Rust to
TLA+, or prove consensus, liveness, independent-machine durability, complete
cell behavior, or performance. Generation-attempt and physical-segment poisons
remain process-oracle evidence until their semantics are added to the trace
contract.
