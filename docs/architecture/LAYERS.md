# objectKV layer contracts

Status: `[EVALUATING]` architecture map. Proposed layers are not presented as
implemented services.

## End-to-end flows

The stack is easier to reason about as three paths over the same versioned
state.

```text
┌─[ READ ]───────────────────────────────────────────────────────────┐
│ okv-fabric → range route → RangeEngine → local tier → object miss │
└───────────────────────────────────────────────────────────────────┘

┌─[ COMMIT ]─────────────────────────────────────────────────────────┐
│ okv-fabric → commit proxy → conflict check → txLog quorum → reply │
└───────────────────────────────────────────────────────────────────┘

┌─[ OBJECTIFY ]──────────────────────────────────────────────────────┐
│ txLog prefix → materialize runs → verify closure → publish O      │
└───────────────────────────────────────────────────────────────────┘
```

Object PUT is absent from the default low-latency commit path. RangeEngine
local state is absent from the permanent recovery equation.

## L1. `okv-fabric`

```text
┌─[ FABRIC CONTRACT ]────────────────────────────────────────────────┐
│ input   application operations and explicit snapshot intent       │
│ owns    transaction, KV, log, snapshot, branch, projection APIs   │
│ output  kernel requests plus versioned application results        │
│ excludes PostgreSQL, Redis, search, or filesystem semantics       │
└───────────────────────────────────────────────────────────────────┘
```

- Function: provide one application-facing version and lifecycle fabric rather
  than separate products for current KV, history, logs, branches, and
  analytical projections.
- Lock: application semantics remain above the value-native kernel.
- Evidence: `[PROPOSED]` as one unified API. `okv::SingleRange` and the pure
  `okv-log` algebra are `[CODE-COMPLETE]` contributing primitives. Tetris and
  Chess exercise local application boundaries, not distributed durability.
- Review: stable client sessions, multi-range transaction API, watches,
  retention leases, and consumer-specific capability negotiation.
- Performance rows: [9 through 11](EVIDENCE.md#matrix-by-layer).

## L2. objectKV public kernel

```text
┌─[ PUBLIC WAIST ]───────────────────────────────────────────────────┐
│ ordered binary keys · opaque values · exact versions              │
│ bounded transactions · point/range reads · lifecycle identities   │
└───────────────────────────────────────────────────────────────────┘
```

- Function: expose the smallest contract from which transaction, log, branch,
  PostgreSQL, Redis, search, filesystem, and analytical adapters can be built.
- Lock: value-native semantics, strict serializability target, bounded
  transactions, explicit versions, and no cross-cell transaction.
- Evidence: `[CODE-COMPLETE]` one experimental public `SingleRange` path.
  `[VERIFIED]` model semantics and bounded single-range read measurements do
  not verify a complete multi-range kernel.
- Review: final stable API, compatibility policy, range sessions, historical
  retention errors, and transaction-size limits.
- Performance rows: [0, 1, and 7](EVIDENCE.md#matrix-by-layer).

## L3. bounded cell and transaction plane

```text
┌─[ CELL ]───────────────────────────────────────────────────────────┐
│ generation authority                                              │
│ read-version service → commit proxy → resolver → txLog quorum     │
│ range map → assignments → RangeEngine processes                   │
│ object frontier O → retention and recovery bounds                 │
└───────────────────────────────────────────────────────────────────┘
```

- Function: own one tenant database's transaction history, commit versions,
  conflict decisions, durable outcomes, range ownership, and recovery
  generation.
- Lock: the cell is the transaction and failure boundary. Cells have
  independent version spaces and never synchronize ordinary commits.
- Evidence: `[VERIFIED]` deterministic and local-process authority mechanisms.
  `[EVALUATING]` single-range native transaction plane. `[PROPOSED]`
  independent-host, multi-range cell.
- Review: Raft-group topology, version authority, cross-range validation,
  resolver partitioning, retry retention, ratekeeping, and cell capacity.
- Performance rows: [4, 5, and 7](EVIDENCE.md#matrix-by-layer).

### Commit sequence

```text
┌─[ COMMIT AT C ]────────────────────────────────────────────────────┐
│ request identity                                                   │
│   ↓                                                               │
│ read and write conflict ranges                                    │
│   ↓                                                               │
│ strict-serializable validation                                    │
│   ↓                                                               │
│ ordered commit version C                                          │
│   ↓                                                               │
│ required txLog voters synchronize                                 │
│   ↓                                                               │
│ COMMITTED outcome retained → caller                               │
└───────────────────────────────────────────────────────────────────┘
```

The current implementation verifies narrower components of this sequence. It
does not yet support arbitrary multi-range tenant transactions on independent
machines.

## L4. RangeEngine

```text
┌─[ RANGEENGINE ]────────────────────────────────────────────────────┐
│ assignment + generation + read version T                          │
│   ↓                                                               │
│ bounded recent MVCC overlay                                       │
│   ↓                                                               │
│ RAM image | NVMe object-block cache | RocksDB resident image      │
│   ↓                                                               │
│ indexed immutable-object fallback                                 │
└───────────────────────────────────────────────────────────────────┘
```

- Function: run disposable compute for assigned key ranges, catch up the txLog,
  serve exact versioned reads, manage local admission, and expose bounded
  storage and cache metrics.
- Lock: serving bytes are bounded and reconstructable. Serving profile does not
  define `COMMITTED`.
- Evidence: `[VERIFIED]` the RocksDB resident single-range point-read boundary
  through 32 clients. `[EVALUATING]` cache pressure and physical-media curve.
  `[PROPOSED]` RAM image, raw NVMe object-block cache, multi-provider
  composition, eviction, and live handoff.
- Review: see [RangeEngine profiles](RANGE-ENGINE.md).
- Performance rows: [0, 1, 2, 3, and 8](EVIDENCE.md#matrix-by-layer).

### Point read at `T`

```text
┌─[ EXACT POINT READ ]───────────────────────────────────────────────┐
│ validate generation, assignment, and T                            │
│   ↓                                                               │
│ transaction-local write                                           │
│   ↓                                                               │
│ recent MVCC value, tombstone, or range clear                      │
│   ↓                                                               │
│ selected local base-serving mechanism                             │
│   ↓ miss or incomplete coverage                                   │
│ named manifest → sparse index → selected object range GET         │
│   ↓                                                               │
│ Value | Tombstone | Absent | VersionTooOld | Unavailable          │
└───────────────────────────────────────────────────────────────────┘
```

A local miss is authoritative absence only when coverage metadata proves the
requested key and version are complete.

## L5. objectification and publication

```text
┌─[ ADVANCE OBJECT FRONTIER ]────────────────────────────────────────┐
│ choose complete txLog prefix through O                            │
│   ↓                                                               │
│ build immutable data, index, and manifest objects                 │
│   ↓                                                               │
│ verify every exact named identity and complete closure            │
│   ↓                                                               │
│ fence and publish the new root through O                          │
│   ↓                                                               │
│ retain or reclaim txLog below the authenticated safe floor        │
└───────────────────────────────────────────────────────────────────┘
```

- Function: turn many small committed mutations into indexed immutable runs,
  advance the permanent frontier, and bound the recovery suffix.
- Lock: block-before-manifest publication, immutable named objects, complete
  closure verification, fenced root transition, and no LIST authority.
- Evidence: `[VERIFIED]` scoped local publication and lost-response recovery
  mechanisms plus exact GCS fixture reconstruction. `[EVALUATING]` continuous
  materialization, compaction debt, brownout, and safe reclamation as one live
  service.
- Review: object target sizes, level policy, scheduling, range movement,
  abandoned publications, and `C - O` backpressure thresholds.
- Performance rows: [5 and 6](EVIDENCE.md#matrix-by-layer).

## L6. manifested object state

```text
┌─[ IMMUTABLE CLOSURE THROUGH O ]────────────────────────────────────┐
│ root manifest                                                      │
│ ├─ row-oriented delta and compacted runs                          │
│ ├─ sparse indexes and checksummed data blocks                     │
│ ├─ optional typed columnar projections                            │
│ ├─ snapshot and branch roots                                      │
│ └─ retention and GC identities                                    │
└───────────────────────────────────────────────────────────────────┘
```

- Function: hold portable permanent capacity state, cold-read indexes,
  snapshots, branches, and optional analytical layouts.
- Lock: one manifest authenticates one complete closure; physical formats may
  differ without creating another logical history.
- Evidence: `[VERIFIED]` bounded row-object closure identity and GCS reuse.
  `[EVALUATING]` cold-read geometry, typed split runs, format compaction, and
  branch-size independence.
- Review: row versus typed-run placement, Parquet and Vortex capabilities,
  bloom/index layout, block and object sizes, historical version encoding, and
  GC roots.
- Performance rows: [2, 3, 6, and 11](EVIDENCE.md#matrix-by-layer).

## L7. provider-neutral object API

```text
┌─[ OBJECT CAPABILITIES ]────────────────────────────────────────────┐
│ immutable segment path: PUT · exact GET · range GET · checksum    │
│ authority path: conditional root update · one-winner race         │
│ audit path: LIST, never used to decide database truth             │
└───────────────────────────────────────────────────────────────────┘
```

- Function: normalize the exact provider operations used by immutable data and
  separately classify whether a provider can host mutable authority.
- Lock: support is capability-profiled by exact implementation and version,
  not inferred from an `S3-compatible` label.
- Evidence: `[VERIFIED]` memory and pinned MinIO authority profiles and local
  filesystem segment profile. `[VERIFIED]` GCS for named fixture placement,
  ranged recovery, and read-only consumer use. GCS authority conformance
  remains `[EVALUATING]`.
- Review: provider-specific conditional semantics, throttling, multipart
  residue, delete horizons, cross-region placement, and request economics.
- Performance rows: [2, 3, 5, 6, and 12](EVIDENCE.md#matrix-by-layer).

## Cross-cutting telemetry and evaluation

```text
┌─[ PROOF LOOP ]─────────────────────────────────────────────────────┐
│ frozen suite → exact run → OTel logs/metrics/traces → receipt     │
│      → named control → matrix row → next first-unverified gate    │
└───────────────────────────────────────────────────────────────────┘
```

`okv-eval` owns identities, metrics, hard gates, and receipts across the
layers. It cannot turn a single-process mechanism into an independent-machine
claim. The proof ladder and current limits are summarized in
[EVIDENCE.md](EVIDENCE.md).
