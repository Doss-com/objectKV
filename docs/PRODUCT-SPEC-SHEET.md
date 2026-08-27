# objectKV detailed product and system specification

objectKV is a value-native, strict-serializable ordered KV kernel with a durable transaction tail and an immutable object-native base. Each range uses a bounded SSD or RAM serving profile while acknowledgement follows a separately selected durability profile. The first PostgreSQL integration preserves PostgreSQL pages and semantics; the product thesis survives only if local serving state stays bounded while recovery, branching, and exact HTAP reads gain material leverage from the object-native base.

## Abstract

This document converts the [objectKV product specification](PRODUCT-SPEC.md) into atomic requirements, bounds, failure contracts, target curves, and evaluation obligations. Each requirement has one stable ID, one status, and one named evidence path. The document does not claim that proposed behavior exists. It is the input contract for the eval program, RFC sequence, and implementation backlog.

## Context / Motivation

Existing systems prove most ingredients independently: replicated transactional logs, RocksDB-backed range replicas, page-server PostgreSQL, immutable object files, change streams, and columnar query engines. The open question is whether they can be composed into one reusable kernel whose permanent state is reconstructible from open object formats without putting object storage on the latency-sensitive commit path.

The product opportunity has three layers:

1. A distributed ordered KV kernel with a bounded fast tail and an object-native base.
2. A page-native PostgreSQL storage adapter that preserves PostgreSQL behavior while changing the durable storage boundary.
3. A version-aligned analytical path that serves exact DataFusion snapshots from a columnar base plus a transactional tail.

The project should proceed only while each layer produces measurable leverage over a simpler TiKV, RocksDB, or PostgreSQL plus object-tiering stack.

## Problem Framing

The project fails if it merely recreates TiKV while adding an asynchronous object uploader. The object-native base must create at least one decisive advantage:

- metadata-scale branching and snapshot publication;
- bounded local storage independent of total database size;
- fast empty-worker recovery without full database copying;
- independent analytical compute over the same committed history;
- portable, inspectable, S3-compatible durable bytes;
- compute and storage scaling that do not require permanent local replicas of the full database.

The project must also retain competitive hot-path behavior. Object storage is not a coordination service and is not expected to satisfy resident OLTP latency.

## Prior Work

- [Product specification](PRODUCT-SPEC.md) owns the product boundary, architecture narrative, and product decisions.
- [System shape](SYSTEM-SHAPE.md) owns the service topology and data flows.
- [Evaluation system](EVALS.md) owns suite semantics, lane rules, frozen controls, and receipts.
- [Telemetry contract](TELEMETRY.md) owns OTel signal and cardinality rules.
- [PostgreSQL path](POSTGRES-PATH.md) owns page-adapter staging and compatibility evaluation.
- [RFC-0001](../rfcs/0001-project-principles.md) owns project principles.
- [RFC-0005](../rfcs/0005-durability-and-wal.md) owns durability and txLog semantics.
- [RFC-0010](../rfcs/0010-oltp-olap-snapshots.md) owns exact OLTP and OLAP snapshot alignment.
- [RFC-0011](../rfcs/0011-cell-and-tenant-topology.md) owns the cell and tenant boundary.

## Definitions

| Term | Definition |
|---|---|
| Kernel | The ordered-key, opaque-value, version, transaction, recovery, and range contract. It does not implement SQL, Redis, or search semantics. |
| Cell | One bounded, complete transaction, durability, recovery, and storage cluster with an independent version space. |
| Tenant database | The normal transaction domain. One transaction may touch any admitted range inside its tenant database and cell. |
| txLog | The replicated, quorum-durable, ordered commit history retained on fast media until all required durable consumers cover it. |
| Object base | Immutable, checksummed object segments plus manifests that reconstruct committed state through an object-durable watermark. |
| ServingWorker | Disposable compute that owns range assignments, recent overlays, a selected bounded `ServingImage`, object-block caches, and materialization work. |
| ServingImage | The common exact-read, coverage, admission, eviction, and rebuild contract implemented by SSD or RAM hot state. It owns no unique permanent database bytes. |
| Serving profile | The range-local hot-state implementation: bounded RocksDB on disposable NVMe (`ssd_resident`) or a bounded in-memory index (`ram_resident`). |
| Resident range | A range whose current serving image is complete on the assigned worker and can answer admitted point reads without object access. |
| Elastic range | A range whose complete history remains on objects while the worker retains only overlays and admitted object blocks. |
| Page-native adapter | A PostgreSQL storage adapter that treats a PostgreSQL page as an opaque objectKV value and leaves heap, index, MVCC, catalog, and SQL semantics in PostgreSQL. |
| Row-native adapter | A future SQL compute architecture that maps logical tuples and index entries directly into objectKV keys and transactions. |
| Objectification | The asynchronous conversion of a committed txLog prefix into immutable objects followed by fenced manifest publication. |
| Row base | An ordered, point-readable object representation used to reconstruct transactional ranges. |
| Columnar base | A Parquet or other analytical representation derived from the same version history. |
| Analytical tail | Schema-aware row changes retained from a columnar watermark through a target version. It is not the recovery txLog suffix. |
| Control | A simpler reference system or mechanism run under the same declared machine, topology, durability, data, and workload profile. |
| Falsifier | A measured condition that rejects a product or architecture claim even when the implementation functions correctly. |

## What We Are Building

### Product boundary requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| PRD-001 | `[PROPOSED]` | The public kernel API exposes ordered binary keys, opaque values, point reads, ordered range reads, atomic multi-key transactions, snapshot versions, atomic mutations, versionstamps, retries, and watches or change notification. | Accepted API RFC plus differential client contract. |
| PRD-002 | `[PROPOSED]` | SQL schemas, PostgreSQL pages, Redis commands, inverted indexes, and DataFusion plans remain consumer-layer concepts. | Public API review contains no consumer-specific type. |
| PRD-003 | `[PROPOSED]` | Object storage contains every byte required to reconstruct object-durable versions without access to a retired ServingWorker. | Empty-worker recovery suite from a published manifest closure. |
| PRD-004 | `[PROPOSED]` | The txLog remains authoritative only for committed versions newer than required durable watermarks. | Recovery across objectification lag and log-pop boundaries. |
| PRD-005 | `[PROPOSED]` | No ordinary transaction spans cells. | Cross-cell request is rejected before partial execution. |
| PRD-006 | `[PROPOSED]` | A tenant database may transact across arbitrary admitted ranges inside one cell. | Multi-range strict-serializable histories. |
| PRD-007 | `[PROPOSED]` | S3 compatibility describes the object API, not the transaction, segment, or SQL format. | Object adapter conformance and independent reader tests. |
| PRD-008 | `[PROPOSED]` | Durable formats, manifests, and checksums are publicly specified and versioned. | Compatibility fixtures and independent decoder. |

### Value, version, and MVCC requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| MVCC-001 | `[VERIFIED]` narrow model | Every committed mutation receives one monotonically ordered cell commit version. | `mvcc-semantics-v1`. |
| MVCC-002 | `[VERIFIED]` narrow model | Reads at version `T` return the newest visible value at or before `T`. | Differential model histories. |
| MVCC-003 | `[VERIFIED]` narrow model | Range clears and point tombstones have unambiguous replay and visibility order. | Poison controls for ignored range clears and mutation reordering. |
| MVCC-004 | `[VERIFIED]` narrow model | A read newer than the available committed version does not silently fall back. | Future-read poison control. |
| MVCC-005 | `[VERIFIED]` narrow model | A read older than retained history returns `version_too_old`, never a newer value. | Retention-boundary and expired-read controls. |
| MVCC-006 | `[PROPOSED]` | Snapshot leases pin every manifest, segment, schema, and analytical-tail root needed through their declared expiry. | GC and snapshot-race suite. |
| MVCC-007 | `[PROPOSED]` | Version reuse is impossible across recovery generations. | Generation takeover with stale-owner poison. |
| MVCC-008 | `[PROPOSED]` | Version and mutation encoding remains stable across mixed-version rolling reads before a format is promoted. | Forward and backward compatibility fixtures. |

### Transaction requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| TXN-001 | `[VERIFIED]` narrow contract | A commit envelope has a stable request identity, read version, conflict ranges, mutation payload, required log tags, and generation. | `cell-commit-contract-v1`. |
| TXN-002 | `[CODE-COMPLETE]` single-group process contract | Acknowledged transactions are strict serializable within the tenant database. | Independent history oracle over point and range conflicts, lost reply, leader death, exact retry, and replica replay; clean and independent-machine receipts remain pending. |
| TXN-003 | `[VERIFIED]` narrow contract | Retrying one request identity returns the original durable outcome and does not apply mutations twice. | Lost-reply process contract. |
| TXN-004 | `[PROPOSED]` | A conflicting reuse of one request identity is rejected durably. | Conflicting-retry poison subject across failover. |
| TXN-005 | `[CODE-COMPLETE]` single-group process contract | Read and write conflict ranges can cover point keys and ordered ranges. | Point-conflict and range-phantom histories through three OpenRaft processes; range-clear and partitioned-resolver integration remain pending. |
| TXN-006 | `[PROPOSED]` | Every participant required by a multi-range commit records the same final decision or recovers it before serving conflicting work. | Coordinator death at each distributed-commit transition. |
| TXN-007 | `[PROPOSED]` | Locks, intents, or pending records have a bounded resolution and abandonment policy. | Stalled-coordinator fault suite and sweeper evidence. |
| TXN-008 | `[PROPOSED]` | Transaction byte, key, duration, and range-participant limits are explicit and enforced before resource exhaustion. | Boundary and rejection suite. |
| TXN-009 | `[PROPOSED]` | Read-only snapshot operations never create a durable write dependency unless explicitly requested. | Read-only load under commit and recovery faults. |
| TXN-010 | `[PROPOSED]` | Admission and backpressure reject or delay work before txLog, memory, or participant debt exceeds a recoverable bound. | Brownout curve with stable retained bytes. |

### txLog, consensus, and commit requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| LOG-001 | `[VERIFIED]` single-group prototype | An acknowledgement requires a matching quorum-durable replicated log record under the declared topology. | OpenRaft process suite. |
| LOG-002 | `[VERIFIED]` single-group prototype | Torn final frames may be ignored; complete corruption without a matching quorum halts recovery. | Persisted txLog and Raft storage suites. |
| LOG-003 | `[VERIFIED]` single-group prototype | Durable votes, membership, purge positions, and committed positions survive process restart. | OpenRaft storage conformance. |
| LOG-004 | `[PROPOSED]` | The normal commit path does not wait for object PUT, object LIST, manifest publication, or columnar materialization. | Trace assertion and object-store outage test. |
| LOG-005 | `[PROPOSED]` | One-range commits use one range-local consensus group after the MultiRaft transition. | Trace contains one participant group and one durability quorum. |
| LOG-006 | `[PROPOSED]` | Multi-range transactions coordinate only the participant groups plus cell transaction services required by the isolation contract. | Participant-count latency curve and trace topology. |
| LOG-007 | `[PROPOSED]` | Automatic leader election, lease expiry, and catch-up do not acknowledge stale-owner writes. | Real process and network partition suite. |
| LOG-008 | `[PROPOSED]` | Log truncation waits for every configured recovery and analytical durability dependency, each tracked by an explicit watermark. | Watermark divergence and premature-pop controls. |
| LOG-009 | `[PROPOSED]` | txLog retained bytes have warning, rate-limit, and commit-refusal thresholds tied to tested recovery time. | Object brownout sweep. |
| LOG-010 | `[PROPOSED]` | A durability profile is fixed for a tenant transaction domain during one generation and appears in the receipt and telemetry resource. | Cross-range mixed-profile rejection, generation transition, and receipt validation. |
| LOG-011 | `[PROPOSED]` | A transaction may emit application-log records atomically with its KV mutations at the same commit version. | KV state and consumer-log differential history. |
| LOG-012 | `[PROPOSED]` | Application-log cursors, retention, and sealed object segments remain independent of txLog recovery retention and safe-pop positions. | Slow-consumer, txLog-pop, and object-retention matrix. |

### Cell and range topology requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| CELL-001 | `[PROPOSED]` | Each cell owns an independent version space, recovery generation, membership, range map, and durable roots. | Two-cell isolation suite. |
| CELL-002 | `[PROPOSED]` | A range is a contiguous ordered-key interval with one generation-bound serving assignment. | Split and move state-machine suite. |
| CELL-003 | `[PROPOSED]` | Range boundaries may change without changing key or transaction semantics. | Concurrent split histories. |
| CELL-004 | `[PROPOSED]` | A range move transfers serving responsibility, not permanent database ownership. | Move with zero durable base copy when the destination can reference the same object closure. |
| CELL-005 | `[PROPOSED]` | Placement considers failure domains, resident budgets, hot-range load, and recovery debt. | Deterministic placement simulation and hotspot curve. |
| CELL-006 | `[PROPOSED]` | One stale assignment cannot publish objects, serve a current lease, or acknowledge mutations after fencing. | Generation and publication poison controls. |
| CELL-007 | `[FUTURE]` | Metacluster routing maps tenant databases to cells without entering ordinary commit coordination. | Tenant routing and move suite. |
| CELL-008 | `[FUTURE]` | Tenant movement uses snapshot plus tail, a bounded write freeze, and one routing epoch transition. | Tenant-move exactness and unavailable-time curve. |

### ServingWorker requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| SRV-001 | `[EVALUATING]` | A ServingWorker combines a transaction-local write overlay with either a native bounded resident engine or an explicit indexed-object cold path. The resident engine materializes the committed txLog suffix rather than consulting an external committed overlay on every read. | Layer-by-layer differential trace, native-engine snapshot oracle, and RFC-0040 failure subjects. |
| SRV-002 | `[VERIFIED]` R0 mechanism | A complete resident range answers admitted point reads without object requests after warmup. | Both GP3.1 AB and BA candidates recorded zero object operations across three million measured reads per order. |
| SRV-003 | `[CODE-COMPLETE]` local point-read pilot | An elastic range may issue object range GETs, but request count, bytes, and decode work are bounded by its checksummed row-object manifest and per-object indexes. | Cold-point curve across 1, 8, and 64 MiB range images. |
| SRV-004 | `[CODE-COMPLETE]` local point-recovery subset | A local miss is authoritative only when coverage metadata proves that the selected snapshot is complete locally. | Missing-key and tombstone poison controls. |
| SRV-005 | `[PROPOSED]` | `ssd_resident` enforces independent DRAM and NVMe high-watermarks; `ram_resident` charges all indexes, values, allocator overhead, and hydration debt to one memory limit and prohibits swap. Neither grows with total object-base size. | Footprint sweep from 1x to 100x local budget plus swap and OOM poisons. |
| SRV-006 | `[PROPOSED]` | Demotion removes disposable serving bytes without deleting durable data or invalidating snapshots. | Demote, reopen, and exact-read suite. |
| SRV-007 | `[CODE-COMPLETE]` local row-object pilot | A worker can serve its first correct read before full range hydration completes. | Time-to-first-read and hydration-byte metrics. |
| SRV-008 | `[PROPOSED]` | Hot ranges can split or receive additional read-serving replicas without changing write order. | Hotspot and read-scale curve. |
| SRV-009 | `[PROPOSED]` | Cache state is explicit in every performance profile: resident, warm elastic, cold elastic, or empty worker. | Suite validation rejects unspecified cache state. |
| SRV-010 | `[PROPOSED]` | Object unavailability does not break resident reads covered by local state, but unavailable cold data returns an honest availability error. | Object outage matrix. |
| SRV-011 | `[PROPOSED]` | Serving profile is selected per range and does not change transaction, object-format, durability, or read semantics. | One trace replayed against both profiles with identical logical results. |
| SRV-012 | `[PROPOSED]` | A serving-profile transition hydrates through a declared version, replays the durable tail, proves coverage, flips the assignment generation, and fences the old image. | Concurrent SSD-to-RAM and RAM-to-SSD handoff suite. |
| SRV-013 | `[PROPOSED]` | Every result receipt and serving trace declares both `ServingProfile` and `DurabilityProfile`. | Receipt validation rejects either missing axis. |
| SRV-014 | `[PROPOSED]` | Generation, closure, range coverage, and applied-version checks bind a resident engine snapshot at activation or frontier transition. Steady-state reads use that bound engine snapshot and do not repeat object-manifest work. | Native resident-engine AB and BA gate plus stale-generation, premature-frontier, incomplete-closure, and old-snapshot poisons. |

### Row-object and manifest requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| OBJ-001 | `[CODE-COMPLETE]` point-value subset | Row objects contain immutable sorted records or blocks with key bounds, version coverage, checksums, tombstones, and format version. | Independent decoder and corruption fixtures. |
| OBJ-002 | `[CODE-COMPLETE]` local point-read pilot | A point lookup selects one bounded row object and candidate block without scanning a complete object or manifest history. | GET and bytes per cold read curve. |
| OBJ-003 | `[PROPOSED]` | The target object size balances PUT economics, compaction debt, recovery parallelism, and range-read locality. | Object-size sweep with no blended score. |
| OBJ-004 | `[PROPOSED]` | Small commit batches are packed into larger immutable objects asynchronously rather than published as one object per transaction or page. | Objects created per logical MiB and median object size. |
| OBJ-005 | `[PROPOSED]` | One manifest root identifies a complete, checksummed closure for a tenant, range set, or snapshot. | Closure walk from an independent process. |
| OBJ-006 | `[PROPOSED]` | Manifest publication is compare-and-swap fenced by generation and expected prior root. | Publication adapter and authority suites. |
| OBJ-007 | `[PROPOSED]` | Readers do not depend on object LIST consistency for correctness. | LIST-disabled conformance profile. |
| OBJ-008 | `[PROPOSED]` | Duplicate immutable PUTs are idempotent only when identity and digest match exactly. | Lost-response and conflicting-content controls. |
| OBJ-009 | `[PROPOSED]` | Compaction publishes a new closure before old reachable objects become reclaimable. | GC race suite. |
| OBJ-010 | `[PROPOSED]` | Format evolution supports at least one previous readable version and rejects unknown incompatible versions. | Mixed-format fixture suite. |

### Objectification and recovery requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| PUB-001 | `[VERIFIED]` narrow contract | Durable publication intent precedes any external object effect. | Real publication adapter and process restart suites. |
| PUB-002 | `[VERIFIED]` narrow contract | Unknown object PUT outcomes are resolved by exact named reads and digest checks. | Ambiguous-PUT recovery suite. |
| PUB-003 | `[VERIFIED]` narrow contract | Unknown manifest PUT outcomes require complete closure verification. | Ambiguous-manifest recovery suite. |
| PUB-004 | `[VERIFIED]` narrow contract | Lost successful publication replies recover the original authority outcome without applying a second transition. | Lost-Publish-response suite. |
| PUB-005 | `[PROPOSED]` | Objectifiers can pipeline independent immutable uploads without publishing an incomplete root. | Concurrency and partial-failure suite. |
| PUB-006 | `[PROPOSED]` | Repeated unknown responses have bounded retry, quarantine, and operator-visible outcomes. | Retry-budget fault matrix. |
| PUB-007 | `[PROPOSED]` | Abandoned intents can be reassigned only through a fenced authority transition. | Publisher death and takeover suite. |
| PUB-008 | `[CODE-COMPLETE]` local point-recovery subset | A fresh worker reconstructs range state from the newest admissible manifest plus required txLog suffix. | Empty-worker exact recovery. |
| PUB-009 | `[CODE-COMPLETE]` local process subset | Recovery does not trust cache files, scratch state, or unverified local manifests. | Poisoned-cache and empty-scratch controls. |
| PUB-010 | `[PROPOSED]` | Backpressure preserves a bounded recovery suffix through object-store brownouts. | Brownout duration and ingest-rate matrix. |

### PostgreSQL page-adapter requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| PG-001 | `[PROPOSED]` | The first PostgreSQL path is page-native while the kernel remains value-native. | Accepted PostgreSQL adapter RFC. |
| PG-002 | `[PROPOSED]` | A page key includes tenant, database, tablespace, relation, fork, and block identity without embedding SQL semantics in the kernel. | Key codec compatibility fixtures. |
| PG-003 | `[PROPOSED]` | PostgreSQL retains heap, index, MVCC, catalog, constraint, trigger, view, planner, and executor behavior. | Selected `pg_regress`, isolation, extension, and crash suites. |
| PG-004 | `[PROPOSED]` | The adapter defines one authority for commit acknowledgement and one unambiguous crash-recovery order. | Kill at every fsync, commit, checkpoint, and reply boundary. |
| PG-005 | `[PROPOSED]` | PostgreSQL WAL and objectKV txLog roles are explicit; no benchmark hides unavoidable double logging or double write amplification. | Byte and fsync accounting by layer. |
| PG-006 | `[PROPOSED]` | A PostgreSQL page update becomes atomic with every other page and metadata mutation required by its storage transaction. | Multi-page commit and crash recovery. |
| PG-007 | `[PROPOSED]` | Checkpoint and buffer eviction do not make object storage part of foreground commit latency unless the selected profile explicitly says so. | Trace and outage assertions. |
| PG-008 | `[PROPOSED]` | Branch creation captures one consistent database version and does not copy the complete page base. | Branch open and divergent-write suite. |
| PG-009 | `[PROPOSED]` | Vacuum, freeze, visibility maps, free-space maps, relation extension, and truncation retain upstream semantics. | Focused PostgreSQL storage regression manifest. |
| PG-010 | `[PROPOSED]` | The page bridge is evaluated as a validation lane before it becomes a permanent ZebraDB storage contract. | Direct page-native versus row-native decision gate. |

### Composable consumer requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| CONSUMER-001 | `[PROPOSED]` | Redis, search, PostgreSQL, and DataFusion remain consumers of one ordered version history rather than kernel protocol modes. | Public kernel API review plus consumer adapter boundaries. |
| CONSUMER-002 | `[PROPOSED]` | The declared Redis strings, atomic update, and expiry subset matches an independent Redis oracle at the same logical times. | Differential command and expiry histories with poison subjects. |
| CONSUMER-003 | `[PROPOSED]` | Source-record and inverted-posting mutations commit atomically, including deletes and term changes. | Cross-range source and posting histories under coordinator failure. |
| CONSUMER-004 | `[PROPOSED]` | Search results declare an exact snapshot version and preserve result identity, deletes, recall, and freshness through index maintenance. | Exact-version update, delete, merge, and top-k query suite. |

### HTAP and analytical requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| HTAP-001 | `[VERIFIED]` narrow contract | Every exact query selects one target cell version `T`. | ZebraDB HTAP contract. |
| HTAP-002 | `[VERIFIED]` narrow contract | Every partition combines a columnar base at watermark `W` with all logical changes in `(W, T]`. | Model, physical, and streaming overlay suites. |
| HTAP-003 | `[VERIFIED]` narrow contract | Tail invalidation keys are preserved before predicate and projection pushdown. | Pushdown and projection poison controls. |
| HTAP-004 | `[VERIFIED]` narrow contract | Row movement across partitions resolves by logical row identity, not partition-local reduction. | Partition-move poison control. |
| HTAP-005 | `[PROPOSED]` | The analytical tail survives independently when its base watermark lags beyond recovery txLog retention. | Divergent-watermark and GC suite. |
| HTAP-006 | `[PROPOSED]` | A DataFusion operator streams sorted base and tail inputs with bounded memory where ordering permits. | Tail-ratio, batch-size, memory, and spill curves. |
| HTAP-007 | `[PROPOSED]` | Columnar materialization can lag without changing exact query freshness at `T`; lag changes query work and latency. | Exact query under controlled materialization lag. |
| HTAP-008 | `[PROPOSED]` | Invariant-critical aggregates remain transactional projections, not long analytical scans inside transactions. | Certified-write and transactional-projection tests. |
| HTAP-009 | `[PROPOSED]` | Long planning queries use snapshot leases and dependency validation before writes. | Concurrent mutation invalidates stale certificate. |
| HTAP-010 | `[PROPOSED]` | Parquet is the first analytical base; alternative formats remain replaceable behind the same snapshot contract. | Format comparison after Parquet control is admitted. |

### Branching, backup, and lifecycle requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| LIFE-001 | `[PROPOSED]` | A branch root references an immutable parent closure at one version plus an independent mutation history. | Metadata-only branch creation. |
| LIFE-002 | `[PROPOSED]` | Branch writes cannot mutate parent objects or roots. | Parent checksum and snapshot checks after divergent writes. |
| LIFE-003 | `[PROPOSED]` | Branch deletion updates reachability before asynchronous reclamation. | GC interleaving suite. |
| LIFE-004 | `[PROPOSED]` | Backup is a durable root and retention policy, not a ServingWorker disk copy. | Restore in an empty environment. |
| LIFE-005 | `[PROPOSED]` | Point-in-time restore resolves one consistent closure and required retained tails. | Restore across compaction and schema changes. |
| LIFE-006 | `[PROPOSED]` | Snapshot, branch, backup, CDC, and analytical leases are explicit GC roots. | Reachability oracle under concurrent lifecycle operations. |

### Operational and security requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| OPS-001 | `[PROPOSED]` | A cell publishes health for quorum, generation, range assignment, txLog debt, objectification lag, resident budget, and GC debt. | Fault-injected health-state transitions. |
| OPS-002 | `[PROPOSED]` | Every unsafe state prefers explicit unavailability over stale or partially durable success. | Failure matrix contains no silent downgrade. |
| OPS-003 | `[PROPOSED]` | Rolling upgrade compatibility is declared separately for API, txLog, object, manifest, snapshot, and telemetry formats. | Mixed-version matrix. |
| OPS-004 | `[PROPOSED]` | A cell has tested maximums for processes, ranges, resident bytes, txLog bytes, transaction bytes, participants, and recovery duration. | Capacity envelope receipt. |
| OPS-005 | `[PROPOSED]` | Operator actions are idempotent, generation-fenced, and auditable. | Repeated move, compact, recover, and retire operations. |
| OPS-006 | `[PROPOSED]` | Object credentials are short-lived and scoped to the cell or eval bucket; logs and receipts contain no secrets, keys, or values. | Credential and telemetry redaction checks. |
| OPS-007 | `[PROPOSED]` | Object integrity is verified with content digests and closure checks independent of transport success. | Corrupt-object and swapped-object fixtures. |
| OPS-008 | `[PROPOSED]` | Disaster recovery declares region loss assumptions, RPO, RTO, and which tails must be replicated or object-durable. | Region-loss tabletop plus executable recovery profile. |

### Evaluation and telemetry requirements

| ID | Status | Requirement | Acceptance evidence |
|---|---|---|---|
| EVAL-001 | `[VERIFIED]` | Each lane has one primary metric and independent hard gates. No blended product score is optimized. | Suite validator. |
| EVAL-002 | `[VERIFIED]` | Correctness oracles, seeds, budgets, result schemas, and aggregation remain frozen during an experiment. | Research program and review policy. |
| EVAL-003 | `[VERIFIED]` | Every run records suite hash, profile hash, revision, machine, toolchain, backend, seeds, budget, gates, and result. | Schema-validated result receipt. |
| EVAL-004 | `[VERIFIED]` | OTel logs, metrics, and traces use bounded-cardinality attributes and forbid keys, values, object paths, and request identities. | Metric-registry validation. |
| EVAL-005 | `[PROPOSED]` | Every admission gate names the product requirements and claim it tests. | Product eval-program validator. |
| EVAL-006 | `[PROPOSED]` | Every executable correctness gate has at least one poison subject that the oracle must discard. | Program validation and negative-control receipts. |
| EVAL-007 | `[PROPOSED]` | Every performance claim names a control with the same hardware, topology, durability, data, cache state, and operation mix. | Comparable profile validator and paired receipts. |
| EVAL-008 | `[PROPOSED]` | Every claimed curve records p50, p95, p99, p99.9 where sample size permits, throughput, CPU, memory, NVMe, network, object requests, object bytes, and estimated cost. | Product metric registry and curve artifact. |
| EVAL-009 | `[PROPOSED]` | A failed or discarded result remains in the experiment ledger. | Append-only ledger review. |
| EVAL-010 | `[PROPOSED]` | A proposed gate cannot be cited as evidence until its workload runner, control, poison subjects, and clean receipt exist. | Program readiness report. |
| EVAL-011 | `[CODE-COMPLETE]` | A `GoldenPathScenario` freezes one generator, seed set, architecture-surface registry, checkpoint DAG, and artifact handoff contract across the product program. | Scenario and program validation through `okv-eval`. |
| EVAL-012 | `[CODE-COMPLETE]` | Every golden-path checkpoint is covered by at least one requirement-linked gate and cannot consume an artifact absent from its dependency closure. | Missing-gate, forward-dependency, and missing-artifact validator tests. |
| EVAL-013 | `[PROPOSED]` | A complete golden-path claim requires one scenario identity across receipts; independent component receipts remain separately scoped evidence. | Cross-receipt scenario identity and artifact-digest verification. |

### Service inventory and ownership boundaries

| Service or structure | Durable state | Disposable state | Primary contract |
|---|---|---|---|
| Client and transaction library | Retry identity and optional session metadata | Routing and read-version caches | Ordered KV transaction API |
| Cell transaction service | Durable outcomes through replicated history | Batches, conflict caches, proxy state | Start version, validation, final commit outcome |
| Range consensus group | Raft log, vote, membership, commit and purge positions | Replication buffers and leader caches | Ordered range mutation durability |
| Range router and placement driver | Versioned range map and assignments | Load samples and planning state | Key-to-range routing, split, move, placement |
| ServingWorker | No unique permanent database bytes | RAM overlay plus selected bounded SSD or RAM serving image | Exact snapshot reads, profile handoff, and range materialization |
| Object publisher | Prepared and published outcomes through authority state | Upload buffers and retry state | Immutable object closure and root publication |
| Compactor | Published compaction intent and output root | Merge workspace | Read and storage amplification control |
| Change projector | Durable source position and analytical-tail objects | Decode and batch buffers | Complete schema-aware row changes |
| Columnar materializer | Published columnar roots and watermarks | Sort, encode, and upload workspace | Version-aligned Parquet base |
| DataFusion provider | Snapshot lease and selected root identities | Execution memory and spill files | Exact base-plus-tail result at `T` |
| PostgreSQL page adapter | Commit and page version state through the selected authority | PostgreSQL buffers and adapter caches | Upstream-compatible page reads and writes |
| Cell generation authority | Membership, generations, roots, pins, leases, and fences | Election and request caches | Recovery identity and external-effect fencing |

### Read pipeline

```text
OpenSnapshot(range, T)
  -> validate tenant, cell, generation, assignment, coverage, and retained T
  -> choose one explicit path
       resident
         -> require object base plus txLog materialized through T
         -> bind native engine snapshot and applied frontier
       cold
         -> bind checksummed row-object manifest and retained suffix

Read(snapshot, key)
  -> transaction-local write overlay
  -> resident: native engine snapshot point lookup
  -> cold: cached sparse index, one object range GET, verified block decode,
           and exact retained-suffix merge
  -> newest visible value or tombstone at T
```

The pipeline must expose whether the result was resident, warm elastic, cold
elastic, or unavailable. A complete resident handle does not silently fall back
to object storage. A cold-path local cache miss is never logical absence without
complete coverage metadata.

### Commit pipeline

```text
Commit(request_id, read_version, conflicts, mutations, durability)
  -> route participant ranges
  -> validate generation and transaction bounds
  -> resolve read and write conflicts
  -> append the replayable decision to required range txLogs
  -> wait for the declared quorum durability
  -> apply to recent overlays and durable retry outcomes
  -> acknowledge exactly once

Asynchronous after acknowledgement
  -> objectify covered txLog prefixes
  -> publish fenced row-base manifests
  -> project schema-aware analytical changes
  -> materialize columnar bases
  -> advance independent watermarks
  -> pop txLog only after every required durable dependency permits it
```

### Durability profiles

| Profile | Commit acknowledgement | Object dependency | Intended use | Explicit sacrifice |
|---|---|---|---|---|
| `regional_quorum` | Quorum fsync across declared failure domains | Asynchronous | Default HA OLTP | Retains a replicated fast tail and pays regional consensus latency |
| `single_zone_quorum` | Quorum fsync inside one zone | Asynchronous | Development or lower-cost regional workloads | Does not survive zone loss |
| `single_node_sync` | One local durable append | Asynchronous | Local development and embedded evaluation | No process or disk HA |
| `external_journal` | Declared durable journal acknowledgement | Asynchronous | RAM-only objectKV compute nodes | Adds a separately operated durable dependency |
| `object_ack` | Required object closure and root are durable before acknowledgement | Synchronous | Bulk ingest or explicitly high-latency durability experiments | Object latency and availability enter the foreground path |
| `volatile_buffered` | Returns `BUFFERED`, not `COMMITTED`, after a live DRAM quorum | Asynchronous | Explicit ephemeral and cache-like experiments | Loses `(O,C]` after volatile quorum destruction or restart |

No profile silently degrades into another. The normal product thesis is `regional_quorum` with asynchronous objectification.

### Target performance curves

Absolute numbers become binding only after machine and topology profiles are frozen. Until then, relative controls are binding.

| Curve ID | Independent variables | Primary metric | Required control | Initial target | Falsifier |
|---|---|---|---|---|---|
| CURVE-HOT-SSD | concurrency, key distribution, value bytes, resident hit ratio | operations/s | Direct NVMe RocksDB under the same RPC and durability path | At 99.9% resident coverage, p99 and throughput within 20% of control | The wrapper or object-base bookkeeping makes admitted SSD structurally noncompetitive |
| CURVE-HOT-RAM | concurrency, key distribution, value bytes, resident hit ratio | p99, operations/s, or CPU/op, declared before the run | Admitted `ssd_resident`, with RAM-backed RocksDB as a mechanism control | At least one named end-to-end primary metric improves by 20% without breaking memory, recovery, or cost gates | The engine lookup advantage disappears after RPC and concurrency, or RAM cost and rebuild risk erase the benefit |
| CURVE-COLD-READ | object size, block size, bloom quality, miss ratio | GETs per successful read | Direct indexed object reader | At most one data range GET after manifest and index cache warmup | Point reads scan objects or require unbounded metadata opens |
| CURVE-COMMIT | transaction bytes, keys, participant ranges, contention | durable commit p99 | Same-durability Raft or MultiRaft KV | One-range p99 within 25% of control | Object operations appear in normal commit traces or coordinator cost dominates one-range commits |
| CURVE-TX-SCALE | range groups, clients, hotspots | committed transactions/s | Single-group control | Throughput increases as independent ranges are added until another named resource saturates | One global service caps throughput before range groups scale |
| CURVE-APP-LOG | partitions, consumers, retention lag, record bytes | consumer freshness p99 | Direct partitioned append-log control | Transactional records remain complete while lag and retained bytes stay bounded under the declared retention policy | Consumer retention blocks txLog reclamation or a committed KV mutation lacks its application-log record |
| CURVE-REDIS | clients, command mix, key distribution, expiry density | command p99 | Same declared command subset on a pinned Redis revision | Zero semantic divergence and a named competitive latency envelope after the distributed cell is admitted | Correctness requires Redis-specific kernel behavior or latency is structurally noncompetitive |
| CURVE-SEARCH | corpus size, update rate, delete rate, query mix, target version | exact top-k queries/s | Same frozen corpus and query mix on a pinned inverted-index control | Exact identity and recall at the requested version with bounded freshness lag | Posting maintenance diverges from source state or exact-version queries require a second truth |
| CURVE-OBJECTIFY | ingest rate, object outage duration, object target size | objectification lag p99 | Direct batch uploader | Stable lag below warning threshold at admitted ingest; bounded txLog under brownout policy | Debt grows without bound or acknowledged data becomes unrecoverable |
| CURVE-REOPEN | total database bytes, assigned range bytes, cache state | first correct read duration | Full local replica restore | First read depends on metadata and requested blocks, not full database bytes | Full-cell download is required before serving |
| CURVE-FOOTPRINT | database bytes, resident budget, active ranges | local bytes per worker | Full local replica | Local bytes remain within configured budget plus bounded transient debt | Worker disk grows with total database size |
| CURVE-BRANCH | base bytes, branch count, divergent bytes | create duration | Physical database copy | Create time and initial bytes are metadata-scale | Branch creation copies the base or blocks foreground commits for data-scale time |
| CURVE-HTAP | tail ratio, selectivity, partitions, batch size | exact query p99 | Base-only DataFusion query | Tail at or below 1% adds at most 20%; policy intervenes before 10% | Tail processing dominates inside admitted policy or exactness requires external ETL |
| CURVE-PG | working set, buffer hit ratio, checkpoint rate, transaction shape | TPS and p99 | Same PostgreSQL revision on local storage | Within 2x for first prototype, then within 25% for resident steady state | Double WAL, page write amplification, or recovery makes the adapter structurally noncompetitive |
| CURVE-COST | logical bytes, operation mix, retention, branch count | estimated total cost per workload unit | TiKV or PostgreSQL plus object tier | Object-native advantages cover the added control and log cost in at least one target workload | The system costs more without material branch, recovery, footprint, or HTAP benefit |

### Capacity and safety bounds

Every limit must be profile-specific and measured before a production claim. These are required dimensions, not fabricated final numbers.

| Bound | Enforcement point | Failure behavior | Required curve |
|---|---|---|---|
| Transaction bytes and mutations | Client and transaction service | Reject before durable partial work | Commit size sweep |
| Transaction duration | Read-version and coordinator services | Retry with explicit age error | Long-transaction and retention sweep |
| Participant ranges | Router and coordinator | Reject before prepare or apply a declared slower path | Participant-count curve |
| Value bytes | Client and range API | Reject before txLog append | Value-size curve |
| Ranges per cell | Placement and control plane | Stop split admission and surface capacity state | Range-count control-plane curve |
| Resident bytes per worker | ServingWorker admission | Demote or move ranges before high-watermark | Local-footprint curve |
| Object cache bytes per worker | ServingWorker cache manager | Evict verified blocks by policy | Cache churn curve |
| txLog retained bytes | Cell ratekeeper | Warn, rate-limit, then refuse commits before unrecoverable exhaustion | Brownout curve |
| Objectification jobs | Publisher scheduler | Bound concurrency and queue debt | Objectification throughput curve |
| Snapshot and branch roots | Lifecycle authority | Enforce quota or retention policy | Manifest-open and GC curve |
| Analytical-tail bytes | Materialization ratekeeper | Trigger materialization or reject new long lease | HTAP tail curve |
| Telemetry series per run | Eval recorder | Reject the measurement and fail telemetry gate | Cardinality poison suite |

### Failure matrix

| Failure | Acknowledged write rule | Read rule | Recovery rule |
|---|---|---|---|
| ServingWorker death | No acknowledged loss | Route to another valid assignment or return unavailable | Rebuild from object base plus txLog suffix |
| Range leader death | Quorum-committed writes survive | Do not serve through an expired leader lease | Elect successor and replay committed suffix |
| Minority disk loss | Quorum-committed writes survive | Continue only with valid quorum and assignment | Repair from surviving log or snapshot |
| Majority loss | No new acknowledgement | Explicitly unavailable | Restore only under declared disaster-recovery procedure |
| Object-store timeout | Resident commit remains independent | Resident covered reads may continue; cold reads may be unavailable | Retain txLog and retry objectification under a bound |
| Unknown object PUT | No publication acknowledgement from response alone | Continue from prior published root | Exact named read and digest reconciliation |
| Unknown manifest PUT | No root advance from response alone | Serve last authoritative root | Verify complete closure before outcome recovery |
| Lost commit reply | Retry returns original outcome | Reads follow committed history | Durable request outcome deduplicates effect |
| Stale generation process | No acknowledgement | No current lease read | Fence every durable and external effect |
| Corrupt local cache | No effect on durable truth | Reject bad checksum and refetch | Delete disposable cache entry |
| Corrupt object | No silent acceptance | Return integrity failure or use another verified copy | Halt affected closure and invoke repair policy |
| Premature GC attempt | No reachable data removed | Existing snapshots remain exact | Reachability and reservation block deletion |
| Columnar materializer death | OLTP unaffected | Exact query uses retained tail or returns explicit resource failure | Resume from durable source position |
| PostgreSQL compute death | Acknowledged commit survives selected authority | Reconnect and recover exact page state | Replay in one declared recovery order |

### Product admission gates

| Gate | Claim | Must pass | Product decision on failure |
|---|---|---|---|
| G0 Semantic kernel | Ordered MVCC and retry semantics are executable and poison-sensitive | MVCC, commit-envelope, and retained-outcome suites | Stop physical optimization until corrected |
| G1 Durable fast tail | Quorum txLog survives process, reply, partition, and restart faults | Real-process consensus and recovery suites | Do not add MultiRaft or PostgreSQL |
| G2 Object authority | Immutable objects and fenced roots recover every ambiguous publication boundary | Publication adapter, authority, publisher, GC, and cloud conformance suites | Stop claiming object-native durability |
| G3 Resident hot profiles | Bounded SSD approaches direct RocksDB and RAM earns product status only through a material workload-specific advantage, both without object I/O after admission | SSD/RAM hot read, write, commit, resource, and profile-handoff curves | Keep SSD only, or prefer TiKV/RocksDB as the permanent serving base if SSD also fails |
| G4 Elastic recovery | Empty workers serve correctly without full database hydration | Reopen, cold-read, and footprint curves | Object base does not provide sufficient compute-storage separation |
| G5 Branch leverage | Branches and snapshots are metadata-scale and GC-safe | Branch create, divergent write, restore, and GC races | Remove branching from the product case |
| G6 Multi-range cell | Strict serializability survives range distribution and coordinator faults | Multi-range history and recovery suite | Retain single-group cell or build above TiKV |
| G7 PostgreSQL bridge | Upstream semantics survive the page adapter with tolerable amplification | PostgreSQL compatibility, crash, and performance controls | Stop the adapter or keep it only as a research control |
| G8 Exact HTAP | DataFusion reads exact version-aligned snapshots with bounded tail cost | Physical and streaming overlay plus scale curves | Treat analytics as external CDC and ETL |
| G9 Product economics | At least one target workload gains a material cost, branch, recovery, footprint, or HTAP advantage | Paired total-cost and operational receipts | Stop owning the kernel |

## Convictions

1. Object storage is the permanent capacity and recovery substrate, but the normal regional commit path is a quorum-durable txLog on fast local media.
2. SSD and RAM are alternative bounded disposable serving profiles; predictable OLTP latency requires an explicit resident-range contract.
3. The page-native PostgreSQL bridge is a validation lane until evidence justifies making it a permanent ZebraDB contract.

## Open Questions

1. How should multi-range strict serializability coordinate range-local Raft groups without introducing a global throughput ceiling?
2. How should PostgreSQL WAL and objectKV txLog responsibilities be split so commit and crash recovery have one authority without hiding double-write cost?
3. How should row-object block indexes and manifest fanout bound both cold point reads and large ordered scans?
4. How should resident-range placement expose predictable service classes while local capacity remains independent of total database size?
5. How much branching, recovery, footprint, or HTAP leverage is required to justify owning the kernel instead of building on TiKV or RocksDB?

## Milestones

Pending: human-generated. The PRD owner must assign owners and dates after G0 through G3 have executable profiles and frozen controls.

The dependency order is fixed even without dates: G0 -> G1 -> G2 -> G3 -> G4/G5 -> G6 -> G7/G8 -> G9.

## Decisions Log

| ID | Status | Decision | Reason and tradeoff |
|---|---|---|---|
| D1 | audited | Keep the kernel value-native and consumer semantics above it. | Preserves a reusable ordered KV contract; gives up consumer-specific shortcuts in the kernel. |
| D2 | audited | Keep object storage off the normal commit path. | Protects OLTP latency and availability; requires a replicated fast tail and bounded objectification debt. |
| D3 | unaudited | Expose `ssd_resident` and `ram_resident` behind one serving contract, and select durability separately per tenant generation. | Lets workloads buy capacity-efficient SSD or lower-latency RAM without silently changing commit semantics; adds placement, resource, and profile-handoff complexity. |
| D4 | unaudited | Expose resident and elastic range classes explicitly. | Makes latency and local-footprint tradeoffs honest; gives up one undifferentiated read SLA. |
| D5 | audited | Pack many mutations or pages into immutable objects asynchronously. | Avoids small-file explosion; introduces objectification and compaction policy. |
| D6 | audited | Keep row and columnar bases physically separate but version-aligned. | Fits OLTP and OLAP access paths; requires a complete analytical tail and independent watermarks. |
| D7 | audited | Use the page-native PostgreSQL path as the first compatibility proof. | Preserves upstream semantics; may incur double logging and is not automatically the final HTAP shape. |
| D8 | unaudited | Reject the project if object-native branching, recovery, footprint, or HTAP leverage does not outweigh kernel ownership. | Prevents a technically functional but strategically redundant TiKV clone. |
