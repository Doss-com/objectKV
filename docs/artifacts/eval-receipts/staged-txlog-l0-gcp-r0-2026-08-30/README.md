# RFC-0045 staged txLog L0 protocol contract

Status: `[VERIFIED]` deterministic protocol semantics. This is not process,
network, media, latency, throughput, or transaction-commit evidence.

## Result

Clean source `efc723e2722a9ad49dedab598349230a49eae51f` ran the
`staged-txlog-v1` L0 protocol workload on one disposable GCP
`n2-standard-8` builder in `us-central1-a` with Rust 1.88.0.

The candidate returned `keep` with zero correctness anomalies across seeds
1103, 2207, and 3301. All nine emitted hard gates passed. The run exercised 21
deterministic checks in total:

```text
acknowledged appends             3
recovered unknown outcomes       3
writer takeovers                 9
repaired records                 6
stale-writer rejections          3
conflicting-retry rejections     3
published segments               3
ignored orphan objects           3
bounded-queue rejections         3
correctness anomalies            0
```

## Negative controls

Each poison returned `discard`, produced one targeted anomaly per seed, failed
contract agreement, and passed the negative-control-detection gate:

- acknowledge after one copy;
- accept a stale writer epoch;
- overwrite an acknowledged suffix;
- publish a segment beyond the transaction commit frontier;
- treat object-store LIST as read authority;
- accept an unbounded publication queue.

## Verification boundary

This verifies the L0 distinction between physical log acceptance and
transaction commit. It also verifies deterministic recovery of an ambiguous
append by immutable request identity, writer fencing, exact suffix repair,
manifest-only object visibility, and hard queue admission.

It does not start a `LogNode`, synchronize NVMe, send a network append, publish
a real GCS segment, measure a performance curve, or replace OpenRaft. The L1
process mechanism and L2 independent-machine comparison remain open.

The exact suite validated with 78 registered metrics, four lanes, 14 workloads,
and two profiles. The golden-path program validated with nine phases and 23
gates. Both focused `okv-sim` staged-txLog tests passed. The exact
`okv-eval` binary built and ran from the clean source above. Package-wide
Clippy remains blocked by pre-existing warnings in unchanged modules, so this
receipt does not claim a workspace Clippy pass.

Suite SHA-256:
`9444bd8fea826606769e31fe9b3bcfaf22d90ba7ae75412194f04eb945a1a0e2`.

Profile SHA-256:
`3a6397f53c1d87c87684de90ebdcc838f7c2e58b43bc9c118536a7f97bbef16e`.

Durable evidence:
`gs://doss-objectkv-dev-okv-evals/runs/staged-txlog-l0-20260830/efc723e/staged-txlog-l0-efc723e-final.tgz`.

Archive SHA-256:
`664300daf8f13d0a9b52b73ed9da540336fcdfa6cec6039a52c26f38eb89b1f7`.
