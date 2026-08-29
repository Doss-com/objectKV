# objectKV architecture

Status: `[EVALUATING]` canonical architecture review index.

This document set answers four questions:

1. What exists between `okv-fabric` and object storage?
2. What contract does each layer own?
3. Which decisions are stable enough to lock, and which still need review?
4. Which claims have measurements in the master performance matrix?

The graph grammar follows the compact framed `Flow`, `Tree`, `Spec`,
`Compare`, and `Bullet` patterns from
[`markdown-graphs`](https://github.com/keshav-exe/markdown-graphs/tree/bf287ee267353ca476fcb413ccac036d2e3378ef).
The components are not vendored. These remain plain Markdown diagrams that are
diffable in architecture review.

## Read this set

```text
┌─[ ARCHITECTURE DOCS ]──────────────────────────────────────────────┐
│ README.md       → system boundary, navigation, decision posture   │
│ LAYERS.md       → each layer's inputs, mechanism, outputs, limits │
│ RANGE-ENGINE.md → RAM, NVMe, and RocksDB serving composition      │
│ EVIDENCE.md     → layer claims mapped to performance-matrix rows  │
└───────────────────────────────────────────────────────────────────┘
```

The detailed RFCs remain the design authority. The
[master performance matrix](../BOOTSTRAP-PLAN.md#master-performance-matrix)
remains the measurement authority. These documents are the map between them.

## System from fabric to objects

```text
┌─[ OBJECTKV STACK ]─────────────────────────────────────────────────┐
│ okv-fabric                                                        │
│ transactions · KV · log/WAL · snapshots · branches · projections │
├───────────────────────────────────────────────────────────────────┤
│ objectKV public kernel                                            │
│ ordered bytes · exact versions · bounded transactions · lifecycle │
├───────────────────────────────────────────────────────────────────┤
│ bounded cell                                                      │
│ generation · commit ordering · conflict resolution · txLog        │
├───────────────────────────────────────────────────────────────────┤
│ RangeEngine                                                       │
│ recent MVCC overlay · RAM | NVMe | RocksDB · indexed cold reads   │
├───────────────────────────────────────────────────────────────────┤
│ objectification                                                   │
│ pack · verify · publish manifest · advance O · reclaim txLog      │
├───────────────────────────────────────────────────────────────────┤
│ manifested object state                                           │
│ row runs · typed projections · snapshots · branch roots           │
├───────────────────────────────────────────────────────────────────┤
│ object API                                                        │
│ exact name · GET/range GET · immutable PUT · conditional root     │
├───────────────────────────────────────────────────────────────────┤
│ GCS · S3/MinIO · compatible blob providers                         │
└───────────────────────────────────────────────────────────────────┘
```

The stack has one reconstruction invariant:

```text
┌─[ STATE AT C ]─────────────────────────────────────────────────────┐
│ ManifestedObjectState(O) + txLog(O, C] = Database(C)              │
└───────────────────────────────────────────────────────────────────┘
```

`C` is the latest committed cell version. `O` is the highest version covered
by an authenticated immutable object closure. Local RangeEngine bytes are
reconstructable projections of that equation, never a second authority.

## Two independent axes

```text
┌─[ SERVING ]───────────────────┐  ┌─[ DURABILITY ]─────────────────┐
│ RAM image                     │  │ regional quorum txLog          │
│ NVMe object-block cache       │  │ external durable journal      │
│ RocksDB resident image        │  │ synchronous object ack        │
│                               │  │ explicit volatile buffering   │
│ answers: where reads resolve  │  │ answers: what COMMITTED means │
└───────────────────────────────┘  └────────────────────────────────┘
```

These axes must not be conflated. A RAM-serving range can still have durable
commits through a regional txLog. A RocksDB image on local NVMe does not make a
commit durable unless the selected durability profile says so.

## Decision posture

Decision posture and proof status are intentionally separate:

| Posture | Meaning |
| --- | --- |
| **Lock** | Keep the contract stable while implementations and metrics advance. |
| **Review** | The boundary is useful, but the mechanism or policy is not settled. |

| Architecture question | Posture | Proof status |
| --- | --- | --- |
| Applications use one `okv-fabric`; protocol semantics stay above the kernel | **Lock** | Fabric `[PROPOSED]`; contributing `okv` and `okv-log` primitives are `[CODE-COMPLETE]` |
| One tenant transaction domain lives inside one bounded cell; no cross-cell transaction | **Lock** | Multi-range cell `[PROPOSED]` |
| `ObjectState(O) + txLog(O,C]` is the sole recovery equation | **Lock** | Components `[VERIFIED]` in bounded local and GCS scopes; complete cell `[EVALUATING]` |
| Object storage holds immutable permanent state and is not normal-path commit coordination | **Lock** | Named-object and publication mechanisms have `[VERIFIED]` scoped receipts |
| RangeEngine serving state is bounded, disposable, and independent of durability | **Lock** | RocksDB path `[VERIFIED]` for the single-range read boundary; other profiles are open |
| RAM, NVMe block cache, and RocksDB are independently configurable RangeEngine mechanisms | **Review** | Target composer `[PROPOSED]`; current code admits one resident provider |
| Every routable production range enables at least one local base-serving mechanism | **Lock** for target architecture | Enforcement `[PROPOSED]`; object-direct remains a recovery and diagnostic path |
| Cell transaction protocol and cross-range serializability mechanism | **Review** | `[EVALUATING]` single-range authority; multi-range `[PROPOSED]` |
| Row, typed split-run, Parquet, and Vortex placement inside the object LSM | **Review** | Row and split-run mechanisms `[EVALUATING]` |
| Cache admission, spill, eviction, and profile handoff policy | **Review** | RocksDB cache pressure `[EVALUATING]`; RAM and raw NVMe profiles `[PROPOSED]` |

Locking an architecture contract does not upgrade its proof status. Only an
immutable receipt meeting the [status contract](../STATUS-TAXONOMY.md) can move
a scoped claim to `[VERIFIED]`.

## Runtime and permanent-state boundary

```text
┌─[ DISPOSABLE RUNTIME ]─────────────────────────────────────────────┐
│ cell services                                                     │
│ ├─ generation and membership authority                           │
│ ├─ read-version, commit, resolver, and txLog roles                │
│ ├─ RangeEngine processes                                         │
│ └─ materializer and publication workers                          │
└──────────────────────────────┬────────────────────────────────────┘
                               ↓
┌─[ PERMANENT RECOVERY STATE ]──────────────────────────────────────┐
│ authenticated object closure through O                           │
│ + quorum-durable txLog suffix (O, C]                              │
└───────────────────────────────────────────────────────────────────┘
```

Process disposal is safe only while this permanent recovery state is complete,
authenticated, and within its declared retention bounds.

## Review sequence

```text
┌─[ ACTIVE REVIEW ORDER ]────────────────────────────────────────────┐
│ 1  T27 RangeEngine cache, skew, eviction, and direct-I/O curve    │
│ 2  T28 cold object read and object-layout geometry                │
│ 3  T29 independent-media replicated commit                       │
│ 4  T30 objectification debt, brownout, and recovery bounds        │
│ 5  T31 branch and lazy empty-worker reopen                        │
│ 6  multi-range cell, RAM profile, fabric surfaces, Postgres, HTAP │
└───────────────────────────────────────────────────────────────────┘
```

The order is controlled by the first unverified row in the
[performance matrix](../BOOTSTRAP-PLAN.md#master-performance-matrix), not by
which upper-layer demo is easiest to show.
