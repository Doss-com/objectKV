# RFC-0045 staged txLog L1 process contract

- Status: `[VERIFIED]`
- Source: `8a225cac10c51d65fbe08fe2933bbea9eac782c6`
- Builder: `objectkv-dev-build-l1-r0`, GCP `us-central1-a`, `e2-standard-8`
- Toolchain: Rust 1.88.0
- Seeds: 1103, 2207, 3301

The clean-source candidate passed every L1 gate with zero correctness anomalies.
Across the three seeds it started and killed 18 real child processes, completed
12 acknowledged appends through 54 TCP append requests, recovered every
acknowledged record, issued zero object operations, repaired each injected torn
tail, rejected each stale writer, and produced byte-identical immutable segment
previews. Exact retries caused zero journal growth and physical state stayed
inside the 64 KiB per-run bound.

The unchanged evaluator rejected every targeted process poison:

- `ack_before_sync` lost nine acknowledged records across three seeds and
  failed restart recovery;
- `accept_stale_epoch` failed writer fencing in every seed;
- `node_specific_segment_bytes` failed segment identity in every seed.

The measured 0.326 second median operation duration covers the full three-seed
process contract. It is not an append-latency result. This receipt verifies
one-host process, TCP, local journal, restart, fencing, and deterministic-format
mechanics. It does not verify independent machines or media, GCS publication,
latency, throughput, transaction commit, or an OpenRaft replacement.

Identities:

- suite hash: `b0e96a4b282bdfa243db6aa099d90c66a38f01e8585757fb98b799500f41fd5b`
- profile hash: `3a6397f53c1d87c87684de90ebdcc838f7c2e58b43bc9c118536a7f97bbef16e`
- durable archive: `gs://doss-objectkv-dev-okv-evals/runs/staged-txlog-l1-20260830/8a225ca/staged-txlog-l1-8a225ca-final.tgz`
- GCS generation: `1788053545817582`
- archive SHA-256: `60da2c3fdb3db59034e295db18b0638c75d8b8b0e201f004a4e8782ba4621544`

`SHA256SUMS` authenticates the files in this directory. `validation.txt`
contains the suite, golden-path, `okv-wal`, and focused `okv-eval` validation.
