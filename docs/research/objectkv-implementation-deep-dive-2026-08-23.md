# objectKV implementation architecture deep-dive

Status: `[ACTIVE-WORK]` evidence readout, 2026-08-23. Implementation is paused
at candidate commit `99df834b7bb62451ce6717f0cc657fd6b65e40e1`. The read-only
strategy audit is pinned to `22e4728e2a80e29399b45010f53c69ca3fc7de26`.

## Answer

objectKV has crossed from an architecture concept into a broad set of
executable local contracts. The emerging design is coherent enough for a real
architecture review, but not yet composed enough for a product claim.

The decisive risks are now physical stability and economics, bounded recovery,
and end-to-end composition:

```text
bounded FoundationDB-like cell
        +
replicated authority over the recent committed suffix
        +
immutable object storage for permanent bulk state
        +
disposable serving and materialization workers
        +
one exact version history feeding row and column layouts
```

The strategy should continue if objectKV can preserve that shape under a real
S3-compatible backend, GCS latency and cost, independent-host failure, a real
PostgreSQL page path, and a durable HTAP source. It should narrow to an
object-native storage layer if those proofs require a second commit authority,
unbounded retained tails, or recovery work proportional to the entire database.

## Evidence labels

- `[EXISTS]`: implemented and backed by a bounded receipt.
- `[ACTIVE-WORK]`: implemented in part or carrying an unresolved evidence gap.
- `[PROPOSED]`: a recommended next decision or target, not current behavior.
- `[FUTURE]`: intentionally outside the current implementation boundary.

## Strategy clarity

**Question:** Is objectKV becoming a coherent architecture, and what must the
next proof cycle establish?

**Punchline:** Yes, as a research kernel. The strongest current evidence is in
semantic contracts and local recovery. The weakest evidence is in remote object
behavior, composed cell performance, and compatibility integration.

**Counter:** The thesis fails if repeated object-store authority is unstable,
PostgreSQL needs dual commit authority, recovery work cannot be bounded by the
recent suffix, or exact HTAP requires an unbounded analytical tail.

**Next:** Resolve the MinIO authority discontinuity, complete the clean resolver
curve, then run GCS physical and economics, PostgreSQL crash, and durable HTAP
source gates.

**Confidence:** Medium. The local semantic surface is broad and increasingly
specific. The remote and composed evidence remains narrow.

## The current implementation architecture

### System boundary

```text
                         clients and adapters
              PostgreSQL | Redis | search | DataFusion
                                    |
                         ordered transactional KV
                                    |
      +---------------- transaction system ----------------+
      | read versions | commit proxies | resolver ranges   |
      | generation authority | tagged replicated tLogs     |
      +------------------------+-----------------------------+
                               |
                         committed suffix
                               |
                  fenced object publication root
                               |
       +---------------- immutable object tier -------------+
       | sorted MVCC segments | manifests | change objects  |
       | Parquet or Vortex-derived layouts | snapshots      |
       +------------------------+-----------------------------+
                               |
                 disposable serving/materialization
```

The cell is the complete transaction, durability, recovery, and storage system.
It is not a page, block, shard, or single-writer KV. A tenant database is the
normal transaction domain inside the cell. Ordered key ranges sit below that
boundary and can be split or reassigned without changing the transaction API.

### What exists by layer

| Layer | Status | Current implementation | Claim boundary |
|---|---|---|---|
| Semantic model | `[EXISTS]` | Canonical versioned mutation batches, point and range MVCC, retention, differential oracle, and exact HTAP oracle | Model and bounded fixtures, not a production storage engine |
| SlateDB adapter | `[EXISTS]` | Pinned adapter with externally versioned point mutations, durable logical version, measured serving profile, and separate compaction work | Historical range behavior and full range clear remain incomplete |
| Object authority | `[ACTIVE-WORK]` | Immutable creation, identity tokens, exact ranges, guarded publication roots, lost-reply recovery, publication and GC contracts | Repeated MinIO authority became unstable in cycles 2 and 3 of the latest audit |
| Transaction durability | `[EXISTS]` | Tagged replicated tLogs, commit visibility, retained outcomes, authenticated inventory, generation fencing, and staged takeover | Independent hosts, WAN-like delay, and a production quorum service remain open |
| Transaction ordering | `[EXISTS]` | Causal read versions, actual-read witnesses, range phantom checks, partitioned resolvers, globally ordered multi-proxy commits, and online range split | No end-to-end cell throughput envelope yet |
| Recovery | `[EXISTS]` | Full transaction-system generation replacement, absence-quorum abort, authenticated inventories, successor recruitment, and monotonic version admission | Full-generation replacement is still the fallback and inventory dominates the largest curve |
| Serving recovery | `[EXISTS]` | Fresh worker reconstructs an exact target from published object base plus retained tLog suffix | Production RangeMap, ServingWorker, and DataDistributor routes remain proposed |
| Exact HTAP | `[EXISTS]` bounded operator | DataFusion merges Parquet base plus ordered Arrow tail at one target version with bounded memory | Durable manifests, leases, interval reads, schema evolution, and lag curve remain open |
| PostgreSQL bridge | `[ACTIVE-WORK]` | PostgreSQL 18.6 second `smgr` slot compiles, boots, and restarts | Real objectKV page callbacks and crash semantics are not implemented |
| Fleet | `[FUTURE]` | Architecture defines bounded cells and tenant placement | Metacluster routing, tenant movement, cell evacuation, and upgrades are not implemented |

### Four current end-to-end contracts

#### Commit

```text
tenant session
  -> causal read version
  -> commit proxy batch
  -> resolver partitions
  -> globally ordered version
  -> tagged tLog quorums
  -> visible commit
```

The authoritative commit point is durable replicated log state under the active
generation. Object publication is not synchronous with every commit.

#### Recovery

```text
failure observation
  -> durable generation fence
  -> authenticated tLog inventory
  -> successor role recruitment
  -> staged visibility
  -> admission above the old high watermark
```

This favors an auditable full-generation fallback. The cost is a larger
coordinated recovery event when transaction-system roles fail.

#### Fresh serving

```text
published object base at O
  + retained committed suffix (O, T]
  -> fresh empty-cache worker
  -> exact state at T
```

Serving workers own assignment, cache, recent overlay, and materialization work.
They do not own the permanent bytes.

#### Exact HTAP

```text
columnar base at Wp
  + transactional table tail (Wp, T]
  -> ordered primary-key overlay
  -> exact rows at T
```

The base watermark controls query cost, not freshness. The system must either
return an exact snapshot at `T` or return `snapshot_unavailable`.

## System diagrams

The HTML artifact contains the full diagram set with legends and state labels.
These text equivalents preserve the architecture in the canonical source.

### Diagram 1 of 6: transaction and fleet boundaries

```text
[FUTURE] global fabric / metacluster
  |
  +-> Cell A: complete transaction, durability, recovery, storage system
  |     |
  |     +-> tenant database A: one serializable transaction domain
  |            |
  |            +-> ordered ranges -> immutable segments -> blocks -> KV
  |
  +-> Cell B: independent version space and recovery generation

No cross-cell transaction.
```

### Diagram 2 of 6: commit and objectification

```text
tenant transaction
  -> causal read version
  -> commit proxy
  -> resolver partitions
  -> global cell version
  -> tagged tLog quorums
  -> commit visible
          |
          +-- async --> materializer
                         -> immutable sorted segment
                         -> fenced root publication
                         -> durable tLog pop
```

Commit visibility is decided by replicated tLog durability under the active
generation. Object storage is not in the commit coordination path.

### Diagram 3 of 6: generation and serving recovery

```text
transaction system:
failure -> durable generation fence -> authenticated inventory
        -> successor recruitment -> admit above old high watermark

durable state:
published base at O + retained suffix (O,T] -> exact logical state at T

serving worker:
empty cache -> range assignment -> base blocks -> recent MVCC overlay
            -> exact point and range reads at T
```

### Diagram 4 of 6: exact HTAP snapshot

```text
Parquet or Vortex base at Wp ----+
                                  +-> SnapshotOverlayExec -> exact Arrow rows at T
table-change tail (Wp,T] --------+
```

The ordered merge suppresses invalidated base keys, preserves deletes and row
moves, and emits the latest tail upsert. Tail keys required for invalidation
cannot be removed by unsafe predicate pushdown.

### Diagram 5 of 6: PostgreSQL authority boundary

```text
PostgreSQL WAL / LSN / tuple MVCC
        | sole initial commit authority
        v
buffer manager + second smgr slot
        | WAL-before-page barrier
        v
objectKV page materialization
        -> replicated recent suffix
        -> immutable object segments
```

`[FUTURE]` objectKV may own page durability only after WAL, checkpoint,
truncate, restart, and repeated crash matrices pass. A second commit decision
is a stop condition.

### Diagram 6 of 6: how the codebase is being built

```text
okv-model
  versions, canonical batches, MVCC, retention, exactness oracles
        |
        v
okv-object + okv-slate + okv-wal
  immutable state, roots, compaction, local quorum frames
        |
        v
okv-consensus + okv-sim
  generations, proxies, resolvers, tagged logs, recovery, online split
        |
        v
okv-eval
  frozen TOML suites, hard gates, OTel, budgets, negative controls, ledger
```

The build sequence keeps one client contract while internal roles become more
distributed:

```text
Cell v0 centralized throughput
  -> v1 distributed read and storage
  -> v2 partitioned conflict resolution
  -> v3 multiple read-version and commit proxies
  -> v4 partitioned durable logs
  -> [FUTURE] v5 composed cell and metacluster
```

## What the measured performance curves say

### 1. Untuned SlateDB reopen has a 64 MiB metadata cliff

The repeated untuned filesystem scale curve is stable across three audit cycles.
That repeatability makes the cliff credible, not acceptable.

| Logical data | First correct reopen, median | Fresh-open requests | Fresh-open bytes | Total read bytes | Total written bytes |
|---:|---:|---:|---:|---:|---:|
| 1 MiB | 0.0043 to 0.0046 s | 33 | 452 B | 830,024 B | 2,212,263 B |
| 8 MiB | 0.0062 to 0.0068 s | 33 | 452 B | 5,706,936 B | 17,676,009 B |
| 64 MiB | 0.414 to 0.429 s | 142 | 210,773,938 B | 255,477,624 B | 141,386,507 B |

At 64 MiB, fresh open reads about 210.8 MB, more than three times the logical
dataset. Total written bytes stay near 2.1 times logical bytes across the scale
points. This is an incumbent profile and fails the desired scale shape.

`[EXISTS]` The configured 64 MiB candidate removes most of the byte cliff:

| Metric | Untuned | Configured | Delta |
|---|---:|---:|---:|
| Fresh-open read bytes | 210,773,938 | 402 | down 99.9998% |
| First-point requests | 3 | 5 | up 2 requests |
| First-point read bytes | 1,395,893 | 210,439 | down 84.9% |
| Total read bytes | 255,477,624 | 27,390,034 | down 89.3% |
| Total written bytes | 141,386,507 | 68,873,267 | down 51.3% |
| Total requests | 345 | 455 | up 31.9% |

The tradeoff is explicit: spend more small requests to avoid reading and writing
far more bytes. The frozen configured gates are fresh open at or below 1 MiB,
and first point read at or below 8 requests and 512 KiB.

**Desired curve:** fresh-open bytes independent of total database size, point
read work bounded by local index depth, total byte amplification declared and
stable, and request count priced against the target backend rather than treated
as free.

### 2. Separate compaction is near 1x at 8 MiB, locally and through MinIO

The admitted 8 MiB compaction path measured 8.61 MB read, 8.62 MB written,
1.027x maintenance write amplification, 538 B fresh open, and five requests for
an 83.3 KB first point read. The MinIO serving and compaction result was nearly
identical.

That proves a bounded local S3 protocol path. It does not prove cloud latency,
throttling behavior, regional failure, or request economics.

**Desired curve:** compaction bytes close to the amount rewritten, bounded
request fanout per object, resumable work after interruption, and a declared
cost per logical GiB ingested and retained.

### 3. Recovery is independent of permanent database size but linear in retained authority work

The admitted recovery candidate used 21 samples per point and read zero permanent
database bytes.

| Curve | Point | Recovery p50 | Dominant work |
|---|---:|---:|---|
| Retained records per tLog | 256 | 0.292 s | 0.014 s inventory |
| Retained records per tLog | 4,096 | 0.465 s | 0.183 s inventory |
| Retained records per tLog | 65,536 | 3.158 s | 2.870 s inventory |
| Pending transactions | 8 | 0.468 s | flat against 512 |
| Pending transactions | 512 | 0.459 s | flat against 8 |
| Topology | 2x3 tLogs, 3 resolvers | 0.390 s | bounded recruitment |
| Topology | 2x5 tLogs, 9 resolvers | 0.627 s | more roles |
| Topology | 4x5 tLogs, 33 resolvers | 1.313 s | 0.616 s inventory, 0.607 s sequential recruitment |
| Permanent database | 1 GiB | 0.460 s | zero DB bytes |
| Permanent database | 1 PiB | 0.474 s | zero DB bytes |

The audit repeated the large-tail point at 3.131 and 3.140 seconds, and the
largest topology at 1.357 and 1.336 seconds. The curve is stable enough to name
the bottlenecks: authenticated inventory at large retained tails, and sequential
role recruitment at larger topologies.

**Frozen current gate:** recovery work must depend on declared authority state
and the unobjectified suffix, not total permanent database bytes. Unsafe
controls must fail.

**Desired curve:** constant against permanent database size, near-linear against
authenticated suffix bytes, sublinear against role count through parallel
recruitment, and bounded by checkpoint and objectification policy.

`[PROPOSED]` Do not freeze a product SLO from local-process timings. First require
the next candidate to improve the owning median by at least 10%, then freeze a
recovery SLO after independent-host and backend measurements. A useful research
target is below one second for the current 65,536-record local calibration
without weakening authentication or full-generation safety.

### 4. Resolver splitting shows a strong best case, not yet an admitted curve

The dirty-tree diagnostic for the balanced hotspot used 21 paired samples and
equal logical work. It measured a 3.814 median split/source throughput ratio.
The source examined 1,391,853,330 history entries; the split examined
694,177,554. Both made 172,032 resolver decisions.

This is a useful early signal and an invalid product claim. The source tree was
dirty, the full skew and crossing curve did not run, and the result has not been
admitted.

**Frozen threshold:** the conservative split/source ratio, computed as
`(split median - split MAD) / (source median + source MAD)`, must be at least
1.10 on the clean balanced curve.

**Desired curve:** clear gain when independent hot ranges split, collapsing gain
when one hot key remains pinned to a child, and increasing coordination cost as
transactions cross resolver boundaries. A later cell curve must include
proxies, tLogs, read-version authority, and publication pressure.

### 5. HTAP exactness is stable, but the meaningful cost curve is still missing

The current streaming suite repeats at 0.229, 0.236, and 0.263 seconds with zero
anomalies. The operator has measured four peak buffered rows, 5,518 peak bytes,
and zero spill in its bounded fixture.

The missing curve is not another microbenchmark of the same fixture. It is cost
against analytical lag, `T - Wp`, with a durable tail source, manifests, leases,
and interval reads.

**Desired curve:** work linear in the uncovered tail, not the base or database;
memory bounded by streaming batches and key groups; zero mixed snapshots; and
explicit failure when the retained tail cannot cover the requested snapshot.

### 6. The latest audit found an object-store repeatability incident

The first MinIO authority cycle passed all 12 contracts in 0.121 seconds. Cycles
2 and 3 failed all 12 with `not_found: object_store operation failed`, taking
37.5 and 45.6 seconds. The MinIO health endpoint still returns HTTP 200, the
container reports healthy, and the `okv-dev` bucket remains present.

| Cycle | Verdict | Authority failures | Requests | Response bytes | Duration |
|---:|---|---:|---:|---:|---:|
| 1 | keep | 0 | 44 | 441 | 0.121 s |
| 2 | discard | 12 | 24 | 0 | 37.547 s |
| 3 | discard | 12 | 24 | 0 | 45.576 s |

`[ACTIVE-WORK]` This is unresolved. The evidence currently supports an authority
or fixture availability discontinuity, not a specific root cause and not an
architectural correctness conclusion. Gate zero is to reproduce and classify
it without changing the frozen suite.

This is also evidence that the eval loop is doing useful work. It preserved
the failure instead of averaging it into a latency number.

## The bounds and tradeoffs

| Decision | Optimize for | Give up or pay for |
|---|---|---|
| D1. Tenant database is the normal transaction domain inside one bounded cell | Useful multi-key, multi-range serializable transactions with bounded recovery and blast radius | No cross-cell atomic transaction; tenant migration needs snapshot plus tail and a routing cutover |
| D2. Replicated tLogs own the recent committed suffix; objects own permanent bulk state | Fast commits without synchronous object publication, disposable serving workers | Retained-WAL pressure, objectification lag, and recovery inventory must be actively bounded |
| D3. Object storage is publication, never coordination | Survive high latency and limited conditional primitives without weakening commit semantics | A separate generation, log, and publication-root authority remains necessary |
| D4. Full generation replacement remains the recovery fallback | Small, auditable recovery semantics and hard fencing of stale roles | Larger coordinated recovery events; inventory and recruitment become visible SLO work |
| D5. Short, bounded transactions and values | Makes global order, conflict checking, retry, and durable queues implementable | Long computation and broad analytics live above the kernel, not inside transactions |
| D6. Exact HTAP is base plus tail at one version | One logical truth and fresh analytical reads without external CDC | Materialization lag becomes tail scan cost and retention pressure |
| D7. Invariant-critical aggregates are transactional projections | Correct short decisions for credit, inventory, counts, and balances | More write amplification and schema/index maintenance |
| D8. PostgreSQL WAL remains sole authority in the first bridge | Preserve PostgreSQL crash and compatibility semantics | The first bridge is subordinate page materialization, not native Postgres transactions on objectKV |
| D9. File formats are storage policies below logical history | Parquet, Vortex, and future layouts can be chosen per workload | Format swaps still require measured readers, writers, compaction, and compatibility rules |
| D10. Cells bound fleets, ranges scale within cells | Limit operational and recovery radius without turning every shard into a transaction silo | Metacluster placement and migration become explicit future systems |

## What comes next to verify

### Gate 0. Classify the MinIO authority discontinuity

`[ACTIVE-WORK]` Reproduce cycles 2 and 3 from the pinned candidate and unchanged
suite. Capture request-level endpoint, bucket, prefix, credentials identity,
backend responses, container events, and object lifecycle. Separate these
outcomes:

- backend unavailable or wrong endpoint;
- fixture or credential lifecycle failure;
- bucket or prefix lifecycle failure;
- adapter error classification failure;
- actual authority semantic failure.

Exit only when the failure is deterministically classified and a same-suite
repeat proves the narrow correction.

### Gate 1. Admit or reject the clean resolver hotspot curve

Run the frozen 50/50 independent, missed-hot-key 100/0, 25% crossing, and 100%
crossing profiles plus all negative controls from a clean candidate.

- Continue if the balanced conservative ratio is at least 1.10 and controls
  preserve equivalent work and semantics.
- Narrow if benefit exists only in independent-range fixtures.
- Stop the scaling claim if the clean balanced gate misses or a control passes.

### Gate 2. Prove the physical object tier on GCS

Use the `objectKV-dev` project to freeze request counts, bytes, latency,
throttling, retry, brownout, range-read, conditional-root, compaction, and cost
curves. Run one bounded layout-tuning pass after the incumbent.

- Continue if fresh open remains independent of database size, byte and request
  amplification fit declared ceilings, and lost replies are safely recoverable.
- Narrow to cold or bulk state if latency and request economics are acceptable
  only off the commit and point-read paths.
- Stop object-native authority if correctness depends on list consistency,
  synchronous publication per commit, or unbounded retry.

### Gate 3. Put real PostgreSQL pages through the bridge

Route heap and index reads, writes, extends, truncates, checkpoints, and restarts
through objectKV while PostgreSQL WAL, LSN, and tuple MVCC remain authoritative.
Run repeated kill points around WAL flush, page publication, checkpoint, root
update, and truncate incarnation.

`[PROPOSED]` Initial triage bands, to freeze before running:

- Continue: zero acknowledged loss or impossible states across at least 30
  controlled kills, combined durable bytes at or below 3x vanilla PostgreSQL,
  and p95 commit latency at or below 2x.
- Narrow: 3x to 8x durable bytes or 2x to 5x latency with intact semantics.
- Stop: any second commit decision, page visible ahead of durable PostgreSQL
  WAL, acknowledged loss, more than 8x bytes, or more than 5x latency.

### Gate 4. Turn the HTAP operator into a durable source

Add versioned manifests, snapshot leases, schema history, durable table-change
objects, interval-bounded reads, and tail retention. Measure the same query over
increasing `T - Wp`, partition count, update density, deletes, row moves, and
predicate invalidations.

- Continue if results remain exact and cost tracks the uncovered tail.
- Narrow certified analytical writes to explicit dependency domains if
  validation certificates become too broad.
- Stop the zero-ETL claim if exactness requires a second source of truth.

### Gate 5. Re-run recovery across independent hosts

Keep full-generation recovery as the safety baseline. Compare compact
authenticated summaries, checkpoint cadence, and parallel recruitment against
the frozen inventory and topology curves. Inject partial inventory, stale
generation, missing signer, corrupt certificate, and slow-role controls.

- Continue if work stays independent of permanent DB bytes and the owning
  median improves by at least 10% without weakening controls.
- Narrow cell size if tail or topology envelopes remain operationally large.
- Stop the recovery design if permanent-state scans become necessary.

### Gate 6. Compose one bounded cell receipt

Only after the owning gates pass, measure a cell path that includes read-version
authority, commit proxies, resolver partitions, tagged tLogs, objectification,
fresh serving, failure, recovery, and exact reads. This is the first point where
an end-to-end throughput, latency, availability, and cost claim becomes valid.

## The emerging vision of objectKV

### One sentence

objectKV is an open-source, object-native, FoundationDB-like transactional
kernel built as a fleet of bounded cells, where replicated logs own the recent
committed suffix, immutable object storage owns permanent bulk state, and one
exact version history feeds transactional and analytical layouts.

### Five principles

1. **Cells, not atomic shards.** A cell is a complete distributed database
   cluster. Transactions span arbitrary ranges within a tenant database, while
   cells bound recovery, failure, and operations.
2. **Objects, not permanent storage servers.** Permanent bytes live in immutable
   segments and manifests. Serving and materialization workers can be rebuilt
   from objects plus the retained committed suffix.
3. **Coordination stays small and explicit.** Read versions, conflict checking,
   commit order, generation fencing, and retained log durability remain in a
   constrained transaction system. Object stores do not become consensus.
4. **One history, multiple layouts.** Ordered MVCC segments, Parquet, Vortex,
   indexes, and projections are physical representations of one commit and
   schema history.
5. **Evidence is part of the implementation.** Every architecture claim owns a
   frozen metric, negative controls, a budget, OTel telemetry, and an immutable
   receipt. Optimization is accepted only when semantics hold and the owning
   curve moves.

### Product path

```text
objectKV kernel
    -> ordered transactional KV and exact version history
    -> Redis and inverted-search adapters as semantic pressure tests
    -> PostgreSQL page bridge with PostgreSQL as initial authority
    -> objectKV-owned page and object durability after crash proofs
    -> DataFusion exact snapshot provider over base plus live tail
    -> ZebraDB, PostgreSQL compatibility plus hybrid OLTP and OLAP
```

`[FUTURE]` The fleet layer routes tenant databases to cells, gives large tenants
dedicated cells, and moves tenants by snapshot, tail catch-up, short write
freeze, and routing-epoch change. There is no synchronous cross-cell
transaction.

### What objectKV is not yet

- It is not a production FoundationDB replacement.
- It is not distributed PostgreSQL.
- It is not ZebraDB.
- It is not a proof that GCS or S3 economics fit the target workload.
- It is not a metacluster or fleet control plane.
- It is not an endorsement of one file format for every access path.
- It is not allowed to hide correctness failures inside average latency.

## Architecture review decisions to calibrate

1. **D1, cell boundary:** accept tenant database as the normal transaction
   domain and no cross-cell transaction.
2. **D2, authority split:** accept tLog and generation authority for the recent
   suffix, fenced immutable object publication for permanent state.
3. **D3, recovery:** retain full-generation recovery until smaller recovery can
   be proven simpler and equally safe.
4. **D4, PostgreSQL:** keep PostgreSQL WAL as the sole initial commit authority.
5. **D5, HTAP:** require exact base plus tail at one version or explicit
   unavailability.
6. **D6, public claim:** describe objectKV as a research kernel until GCS,
   independent-host, PostgreSQL crash, durable HTAP, and composed-cell receipts
   exist.

## Program operating system

`[EXISTS]` The repository currently contains 65 TOML eval suites and 41 ledger
records, with JSON schema validation, hard gates, budgets, negative subjects,
OTel traces, metrics, logs, and artifact references. The research agent may
change candidates, not references, frozen evals, runners, or budgets. Every run
must produce a keep or discard receipt.

The next improvement is not more suite count. It is better coverage of external
state and composition:

- backend and fixture identity in every physical receipt;
- request and response class telemetry, not only aggregate bytes;
- tail, topology, and checkpoint policy recorded as first-class dimensions;
- comparable reference and candidate traces;
- a composed cell receipt after the owning lanes pass.

## Evidence checkpoint

| Item | Value |
|---|---|
| Implementation candidate | `99df834b7bb62451ce6717f0cc657fd6b65e40e1` |
| Read-only audit candidate | `22e4728e2a80e29399b45010f53c69ca3fc7de26` |
| Audit snapshot used here | 48 records, 46 expected, 2 unexpected |
| Unexpected results | MinIO authority cycles 2 and 3 |
| Eval suites | 65 |
| Ledger records | 41 |
| Current implementation state | paused for architecture calibration |

## Primary sources

- [System shape](../SYSTEM-SHAPE.md)
- [Bootstrap plan](../BOOTSTRAP-PLAN.md)
- [Evaluation system](../EVALS.md)
- [Telemetry contract](../TELEMETRY.md)
- [Program contract](../../program.md)
- [Cell and tenancy topology](../../rfcs/0011-cell-and-tenant-topology.md)
- [Exact OLTP and OLAP snapshots](../../rfcs/0010-oltp-olap-snapshots.md)
- [Exact DataFusion overlay](../../rfcs/0012-datafusion-overlay-implementation.md)
- [Streaming DataFusion overlay](../../rfcs/0013-streaming-datafusion-overlay.md)
- [Replicated publication authority](../../rfcs/0015-replicated-publication-authority.md)
- [SlateDB scale curve](../../rfcs/0022-slatedb-filesystem-scale-curve.md)
- [SlateDB serving configuration](../../rfcs/0024-slatedb-bounded-configuration-pass.md)
- [Compaction work](../../rfcs/0025-slatedb-separate-compaction-contract.md)
- [MinIO serving and compaction](../../rfcs/0027-slatedb-minio-serving-compaction-contract.md)
- [Fresh serving from base plus WAL](../../rfcs/0036-serving-worker-base-plus-wal-recovery.md)
- [tLog lag and ratekeeping](../../rfcs/0044-sustained-tagged-log-lag-and-ratekeeping.md)
- [Chunked repair](../../rfcs/0047-resumable-chunked-tlog-repair-with-live-tail.md)
- [Full generation recovery](../../rfcs/0054-transaction-system-recovery-curve.md)
- [Resolver hotspot curve](../../rfcs/0055-resolver-hotspot-throughput-curve.md)
- [PostgreSQL path](../POSTGRES-PATH.md)
- [PostgreSQL 18.6 storage bridge research](postgres-18-6-storage-bridge.md)
- [Prior architecture review](architecture-review-readout-2026-08-23.md)

The measured audit records are local run artifacts under
`/tmp/okv-overnight-strategy-rfc0054-20260823T200000Z`. They are not committed
source, so the exact candidate SHA, run IDs, suite hash, and aggregate snapshot
are recorded above.
