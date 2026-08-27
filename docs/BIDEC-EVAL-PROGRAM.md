# objectKV BIDEC evaluation program

## Original intent

Turn the [detailed product and system specification](PRODUCT-SPEC-SHEET.md) into a falsification-oriented build and evaluation program. The program must show whether object-native durability creates enough branching, recovery, footprint, and HTAP leverage to justify owning a distributed transactional kernel.

## Workstreams (Level-1, pre-merge)

- W1. Kernel semantics: ordered keys, versions, MVCC, transactions, retention, and retries.
- W2. Fast durability: txLog frames, quorum acknowledgement, request outcomes, elections, and recovery.
- W3. Object authority: immutable row objects, manifests, publication, compaction, and GC.
- W4. Serving path: one contract with bounded SSD and RAM profiles, elastic object reads, admission, hydration, profile handoff, and demotion.
- W5. Distributed cell: range groups, routing, transaction coordination, splits, movement, and placement.
- W6. PostgreSQL bridge: page keying, WAL authority, checkpoints, crash recovery, compatibility, and amplification.
- W7. HTAP path: analytical changes, Parquet materialization, DataFusion overlay, leases, and certified writes.
- W8. Lifecycle leverage: snapshots, branching, backup, restore, retention, and reachability.
- W9. Operations and security: capacity envelopes, upgrades, credentials, health, integrity, and disaster recovery.
- W10. Research system: metrics, controls, poison subjects, OTel, immutable receipts, and experiment sequencing.

## Depth findings

### W1. Kernel semantics

Scope:

- Requirements: `PRD-*`, `MVCC-*`, and `TXN-*` in the [spec sheet](PRODUCT-SPEC-SHEET.md).
- Existing code: `crates/okv-model`, `crates/okv-sim`.
- Existing suites: `evals/suites/model-history.toml`, `evals/suites/commit-contract.toml`.
- Owning RFCs: RFC-0002 and RFC-0008.

Mechanism:

- Keep the kernel value-native.
- Freeze commit envelopes, conflict ranges, mutation ordering, request identity, exact read behavior, and retention errors before storage optimization.
- Use an independent normalized model as the differential oracle.

Failure mode: a physical implementation appears fast while returning an impossible snapshot, accepting a conflicting transaction, or applying a retry twice.

Open question: how should range and predicate conflicts be represented so the same transaction contract supports point KV, PostgreSQL pages, and future row-native indexes?

Scope estimate: medium. The narrow model exists; strict-serializable multi-range histories and explicit transaction bounds remain substantial.

### W2. Fast durability

Scope:

- Requirements: `LOG-*` and txLog-related `TXN-*`.
- Existing code: `crates/okv-wal`, `crates/okv-consensus`.
- Existing suites: persisted txLog, OpenRaft storage, cluster, process, generation takeover, and certificates.
- Owning RFCs: RFC-0005, RFC-0009, RFC-0011, and RFC-0015.

Mechanism:

- Acknowledge normal commits after the declared fast-media quorum.
- Rebuild durable retry outcomes from committed history.
- Fence stale generations before log, serving, or object effects.
- Track explicit durable watermarks before txLog truncation.

Failure mode: an acknowledged transaction disappears, applies twice, or is served by a stale generation after process or network failure.

Open question: how should a range-local MultiRaft design coordinate tenant-wide strict-serializable commits without making one cell service the throughput ceiling?

Scope estimate: high. The single-group recovery contracts exist; MultiRaft, automatic elections, independent failure domains, and transaction coordination remain a major systems program.

### W3. Object authority

Scope:

- Requirements: `OBJ-*` and `PUB-*`.
- Existing code: `crates/okv-object`, `crates/okv-publication`.
- Existing suites: object-store conformance, publication adapter, publication authority, publisher restart, ambiguous outcomes, worker recovery, and GC.
- Owning RFCs: RFC-0003, RFC-0004, RFC-0007, and RFC-0014 through RFC-0020.

Mechanism:

- Record durable intent before external effects.
- Write immutable digest-addressed objects.
- Resolve unknown outcomes with exact named reads.
- Publish a complete closure through a generation-fenced compare-and-swap root.
- Reclaim only objects absent from every reachable root and reservation.

Failure mode: a manifest names an incomplete closure, an ambiguous retry publishes conflicting bytes, or GC reclaims a reachable object.

Open question: how should row-object size, block size, index fanout, and compaction thresholds balance cold-point cost against scan and publication efficiency?

Scope estimate: medium to high. Narrow correctness contracts are unusually mature; the actual row format, cloud economics, compaction, and sustained throughput are not proven.

### W4. Serving path

Scope:

- Requirements: `SRV-*` and `CURVE-HOT-SSD`, `CURVE-HOT-RAM`,
  `CURVE-COLD-READ`, `CURVE-REOPEN`, `CURVE-FOOTPRINT`.
- Candidate code: a shared `ServingImage` eval seam with bounded RocksDB and
  in-memory implementations, plus `crates/okv-slate` as an object-native
  incumbent control.
- Existing suites: SlateDB filesystem Phase 0 and the executable RocksDB
  resident-wrapper gate.
- Required controls: RAM-backed RocksDB, NVMe RocksDB, a real TiKV durability
  control for later commit curves, and a direct indexed object reader.

Mechanism:

- Select `ssd_resident` or `ram_resident` per range behind one exact-read and
  coverage contract. Treat complete-range hydration as an explicit service
  class rather than the only architecture.
- Retain recent committed overlays above the row-base watermark in DRAM.
- Use bloom and sparse block indexes to select object blocks on elastic misses.
- Evict reconstructable blocks, demote, split, or move ranges before the
  selected profile's high-watermarks. Never use swap or OOM as RAM admission
  policy.
- Change serving profile through hydrate, tail catch-up, coverage proof, and a
  generation-fenced assignment flip.

Failure mode: stable p99 requires a complete durable local copy of the database, or a cache miss triggers unbounded object and metadata work.

Open question: on which workload, if any, does `ram_resident` improve an
end-to-end primary metric by at least 20% over admitted `ssd_resident` after
RPC, routing, concurrency, memory cost, and hydration are counted?

Scope estimate: high. This is the main economic and latency uncertainty and therefore the first new physical build after durability contracts.

### W5. Distributed cell

Scope:

- Requirements: `CELL-*`, `TXN-006`, and `CURVE-TX-SCALE`.
- Candidate code: `crates/okv-consensus`, a future range map and transaction service.
- Existing control: single OpenRaft group.
- Owning RFCs: RFC-0008 and RFC-0011, with a required MultiRaft RFC before implementation.

Mechanism:

- Partition write order and recovery by range consensus groups.
- Route transactions to participant ranges.
- Validate conflicts and reach one durable final decision across participants.
- Keep the cell as one version and recovery domain without requiring one global log for all data.

Failure mode: the system becomes a distributed key-value store that is slower than TiKV while providing no stronger semantics or object-native leverage.

Open question: how should version assignment and conflict resolution partition while preserving one exact snapshot and bounded recovery?

Scope estimate: very high. It should not start until the resident and object leverage gates pass.

### W6. PostgreSQL bridge

Scope:

- Requirements: `PG-*` and `CURVE-PG`.
- Candidate code: a PostgreSQL storage-manager extension or fork-side adapter, not the kernel API.
- Existing docs: `docs/POSTGRES-PATH.md`.
- Required controls: the same upstream PostgreSQL revision on local storage and the objectKV adapter.

Mechanism:

- Encode PostgreSQL page identity as an opaque ordered key.
- Keep PostgreSQL heap, index, MVCC, catalog, constraints, triggers, and planner behavior.
- Declare which system owns commit acknowledgement and which logs remain necessary.
- Route page reads through resident and elastic serving paths.

Failure mode: double WAL, double MVCC, page write amplification, or recovery ordering makes the bridge slower and more complex without enough branch or recovery benefit.

Open question: how can the first bridge preserve a literal upstream compatibility control while exposing the minimum hook needed to avoid two competing commit authorities?

Scope estimate: high. Start only after G3 proves a credible page-value storage path.

### W7. HTAP path

Scope:

- Requirements: `HTAP-*` and `CURVE-HTAP`.
- Existing code: `crates/okv-htap`.
- Existing suites: model, physical Parquet, and streaming DataFusion overlay.
- Owning RFCs: RFC-0010, RFC-0012, and RFC-0013.

Mechanism:

- Project schema-aware changes transactionally from one version history.
- Materialize Parquet bases at partition watermarks.
- Merge the base and complete analytical tail through target version `T`.
- Preserve invalidation keys before filter and projection pushdown.

Failure mode: exact freshness requires external CDC or tail processing dominates queries inside the admitted materialization policy.

Open question: how should PostgreSQL page changes become schema-aware analytical changes without decoding every page repeatedly or weakening upstream behavior?

Scope estimate: medium for operator correctness, high for a complete PostgreSQL change projection and sustained materialization pipeline.

### W8. Lifecycle leverage

Scope:

- Requirements: `LIFE-*`, `CURVE-BRANCH`, and G5.
- Candidate code: publication authority and a future lifecycle controller.
- Existing primitives: immutable closures, pins, marks, and GC contracts.

Mechanism:

- Create a branch by publishing a new root that references an existing immutable closure.
- Record divergent mutations in an independent history.
- Treat snapshots, branches, backups, CDC, and query leases as GC roots.

Failure mode: branch creation or recovery copies base data, or lifecycle metadata causes unbounded manifest-open and GC cost.

Open question: how should manifest structure bound open cost when branch count and historical roots grow?

Scope estimate: medium once row objects and manifests exist. The feature is strategically important because it is one of the clearest reasons to own the object-native base.

### W9. Operations and security

Scope:

- Requirements: `OPS-*` and the capacity-bound table.
- Existing paths: OTel, object-store adapters, generation authority, and experiment receipts.
- Candidate additions: health contract, capacity profiles, mixed-version fixtures, and disaster-recovery profiles.

Mechanism:

- Surface generation, quorum, assignment, txLog, publication, resident, and GC debt separately.
- Enforce safe limits before resource exhaustion.
- Scope credentials and verify content independently of transport success.

Failure mode: the data path is correct in a benchmark but cannot be upgraded, bounded, diagnosed, or recovered under real operational conditions.

Open question: how should the cell expose a small operational contract without coupling the kernel to one orchestrator or cloud?

Scope estimate: continuous. Only checks needed by the next admission gate should be built early.

### W10. Research system

Scope:

- Requirements: `EVAL-*` and every `CURVE-*`.
- Existing code: `crates/okv-eval`.
- Existing configuration: `evals/metrics.toml`, `evals/suites`, `evals/schema`, and `program.md`.
- Existing telemetry: `infra/otel`.

Mechanism:

- Map every product gate to requirement IDs, one suite lane, one primary metric, hard gates, controls, poison subjects, and a falsifier.
- Freeze one `GoldenPathScenario` whose checkpoint and artifact DAG is reused
  across kernel, storage, distribution, consumer, PostgreSQL, and HTAP gates.
- Keep correctness, latency, cost, and operability as separate decisions.
- Produce immutable, schema-checked receipts and append every result to the experiment ledger.

Failure mode: the team optimizes a blended number, changes the oracle with the implementation, or cites proposed suite configuration as measured evidence.

Open question: how should the runner attach scenario and artifact digests to
each receipt without forcing every checkpoint into one process or benchmark?

Scope estimate: low to medium. The runner exists; the missing layer is a validated product-program manifest and the metrics required by the target curves.

## Merged workstreams (post-synthesis)

- M1. Freeze the executable contract, covers W1 and the semantic portions of W6 and W7. Output: stable requirement IDs, oracles, and compatibility boundaries.
- M2. Prove the fast-tail and object-base loop, covers W2 and W3. Output: acknowledged durability, ambiguous-effect recovery, objectification, manifests, and log truncation.
- M3. Prove bounded serving leverage, covers W4, W8, and the physical portions of W9. Output: resident performance, cold-point bounds, empty-worker recovery, footprint, and branching.
- M4. Prove distributed and consumer semantics, covers W5, W6, and W7. Output: multi-range cell, PostgreSQL page bridge, and exact HTAP paths.
- M5. Operate the falsification program, covers W10 and the remaining W9 obligations. Output: validated product graph, one golden scenario DAG, paired controls, OTel receipts, capacity envelopes, and explicit go or stop decisions.

Hidden coupling:

- W2 and W3 share durable watermarks. txLog truncation cannot be designed independently of object publication and analytical-tail retention.
- W4 and W8 share the row-object and manifest format. Serving, recovery, and branching must use the same public closure rather than three storage layouts.
- W6 and W7 share change projection. A page-native adapter preserves PostgreSQL semantics but does not automatically expose logical rows to DataFusion.
- W5 must not start merely because consensus primitives exist. It depends on M3 proving that owning the storage path creates value.

Cut work:

- Metacluster and cross-cell coordination are future work.
- Redis and inverted search remain later golden-path consumer checkpoints, not
  early architecture drivers.
- Vortex remains behind a Parquet control.
- Row-native PostgreSQL remains a comparison lane, not the first implementation.

## Sequence

1. M1, because every later result needs a stable claim and correctness boundary.
2. M5 foundation, because the program manifest must reject missing controls and uncited requirements before new benchmarks are trusted.
3. M2, because durable commit and object authority are dependencies for any physical serving claim.
4. M3, because resident performance and object-native leverage are the highest-risk product assumptions and the cheapest place to stop.
5. M4 distributed cell, only after M3 passes G3 through G5.
6. M4 PostgreSQL bridge and HTAP integration, after one credible range and object format exist.
7. M5 product economics, because total cost is meaningful only after performance and recovery profiles are honest.

The first physical lane is `hot-ssd`, not MultiRaft and not PostgreSQL. It
compares the bounded SSD `ServingImage` with direct NVMe RocksDB under identical
cache and durability assumptions while asserting zero object requests after
warmup. `hot-ram` follows as a separate lane and must improve a predeclared
end-to-end metric by at least 20 percent over admitted SSD. The two results are
not blended.

## Recalibration check

The original intent still holds, but its scope is narrower and more testable:

- `[EVALUATING]` The repository can already test many semantic and recovery contracts.
- `[CODE-COMPLETE]` The golden scenario and program graph cover all twelve
  named architecture surfaces, but no end-to-end checkpoint is verified.
- `[PROPOSED]` The decisive missing evidence is the bounded resident hot path and public row-object format, not another consensus proof.
- `[PROPOSED]` MultiRaft and PostgreSQL work should remain gated until local serving plus object recovery demonstrates product leverage.

Stop or pivot to TiKV or RocksDB as the permanent kernel if one focused optimization cycle cannot satisfy G3, G4, and at least one of G5 or G8.
