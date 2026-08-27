# RFC-0041: Incumbent transaction-plane adapter

- Status: `[CODE-COMPLETE]` adapter types, `[VERIFIED]` semantic selection,
  logical lifecycle, and provider-media-loss reconstruction; `[EVALUATING]`
  final provider admission
- Authors: DOSS
- Created: 2026-08-27
- Supersedes: the production direction of RFC-0040, not its prototype or evidence

## Decision

Stop extending objectKV's custom RocksDB and OpenRaft transaction plane. Put a
small objectKV lifecycle adapter over one incumbent distributed transaction
system. FoundationDB 7.4.6 and TiKV 8.5.7 receive the same semantic preflight.
Only a provider that satisfies the required transaction semantics without an
objectKV-owned resolver proceeds to the R0 lifecycle run.

The first provider candidate is FoundationDB. TiKV remains a required negative
subject because its documented transaction isolation is snapshot isolation,
which permits write skew. A layer that adds predicate locking, read-conflict
ranges, or serializable validation above TiKV would reopen the transaction-plane
work stopped by D52.

This is not yet final provider admission. FoundationDB passed the live semantic,
logical-lifecycle, and provider-media-loss gates. External incarnation
authority and matched retained-write overhead remain open.

## Context and invariant

RFC-0040 preserved exact recovery and bounded local state, but its native
candidate failed the frozen p99 ceiling in both process orders. D52 therefore
stopped custom transaction-plane expansion.

The retained product invariant is:

```text
one incumbent transaction plane
  -> strict-serializable ordered transactions
  -> atomic user mutation + retained change + request outcome
  -> objectifier publishes immutable closure through O
  -> retained changes above O remain recoverable
  -> historical reads, branches, and DataFusion use objectKV objects
```

For one active generation `G`, latest committed provider stamp `C`, and
object-durable stamp `O`:

```text
O <= C

Database(G, C)
  = ObjectState(G, O)
  + RetainedChanges(G, O, C]
```

The provider may retain a complete current copy. The objectKV layer must still
prove that its immutable closure and retained suffix can reconstruct the exact
logical state after loss of the provider's local data. A provider backup alone
does not satisfy this contract.

## Terminology

- **transaction plane**: the incumbent system that owns current MVCC,
  transaction conflict resolution, distributed commit, replication, range
  placement, and hot serving.
- **lifecycle adapter**: the objectKV code that emits retained changes, advances
  the object frontier, reconstructs a new provider generation, and maps provider
  versions to objectKV versions.
- **provider stamp**: the provider's total-order commit identity. FoundationDB's
  ten-byte versionstamp maps to `(commit_version, batch_order)`.
- **logical version**: `(generation, provider_stamp)`. Provider stamps are never
  compared across generations.
- **retained change**: one immutable, transactionally emitted objectKV command
  sufficient to reproduce the committed mutation.
- **object frontier**: the highest logical version covered by a verified,
  published object closure.

## Architecture boundary

```text
PostgreSQL | Redis | search | filesystem | DataFusion
                         |
                objectKV public contract
                         |
          +--------------+---------------+
          | lifecycle adapter            |
          | retained changes             |
          | object frontier              |
          | restore and generation map   |
          +--------------+---------------+
                         |
              incumbent transaction plane
              FoundationDB or TiKV preflight
                         |
                replicated RAM and SSD

immutable objects <----- objectifier
  row history
  columnar projections
  branch roots
```

The adapter must stay off ordinary point reads and range reads for the active
main branch. Its mandatory hot-path cost is limited to retained-change and
request-outcome writes in the same provider transaction.

Historical reads and lazy branches may compose a provider overlay with an
object base. Those paths receive separate latency and request-amplification
gates and cannot be presented as the main-branch hot path.

## Required provider capabilities

Every item is a hard gate. There is no blended provider score.

| ID | Required capability | Falsifier |
|---|---|---|
| P1 | Strict serializability across arbitrary point and ordered-range read conflicts | Both disjoint writes in the frozen write-skew history commit |
| P2 | Ordered binary point and half-open range reads at one explicit read version | A page mixes versions or loses key order |
| P3 | Atomic multi-key mutation with point clear and range clear | Any partial logical transaction becomes visible |
| P4 | Total-ordered commit identity returned or materialized by commit | Two committed changes cannot be ordered exactly |
| P5 | Atomic user mutation, retained change, and durable request outcome | Any one of the three becomes visible alone |
| P6 | Exact retry after an unknown commit result | Retry duplicates a logical effect or returns a conflicting outcome |
| P7 | Bounded retained-change scan after a cursor | Change capture depends on physical engine files or an unbounded snapshot |
| P8 | Compare-and-advance object frontier | Frontier advances before the named closure is verified |
| P9 | Empty-generation reconstruction from objects plus retained changes | Restore requires the lost provider media or returns another state |
| P10 | Explicit limits and typed errors | Limit failure is silent, partial, or indistinguishable from commit success |

P1 is a knockout gate. objectKV will not add a global predicate-lock service,
resolver, or serializable certifier to make a provider pass it.

## Provider mappings to test

### FoundationDB 7.4.6

One FoundationDB transaction performs:

```text
read exact request-outcome key
add declared read conflict ranges
apply ordered user mutations
SET_VERSIONSTAMPED_KEY retained change
SET_VERSIONSTAMPED_VALUE request outcome
commit
```

The complete ten-byte FoundationDB versionstamp becomes the provider stamp. The
eight-byte commit version and two-byte transaction batch order map directly to
objectKV's current stream ordering. The adapter must not infer order from wall
clock time.

FoundationDB defaults to strict serializability and exposes read versions,
explicit read versions, ordered range reads, conflict ranges, commit versions,
and versionstamps. Its normal MVCC window is short, so retained changes are
ordinary durable keys, not a dependency on reading old storage-server versions.

The objectifier scans versionstamped retained-change keys, writes immutable
objects, verifies their complete closure, then advances the object frontier in
another strict-serializable transaction. It clears retained changes only after
that frontier is durable.

### TiKV 8.5.7

The TiKV client exposes point and range operations, transactions, explicit
timestamps, commit timestamps, and snapshots. TiKV and TiDB document snapshot
isolation. The frozen write-skew history therefore runs before any lifecycle or
performance work.

Pessimistic point locks do not satisfy arbitrary ordered-range read conflicts.
TiKV CDC or log backup may provide an ordered post-commit stream, but neither
changes the isolation gate. If the real preflight admits both disjoint writes,
TiKV is rejected for this objectKV contract and no broader adapter is built.

## Public adapter contract

The first Rust boundary exposes these operations:

```text
read_version(tenant) -> LogicalVersion
get(tenant, version, key) -> value | tombstone | absent
scan(tenant, version, [start,end), limit) -> ordered page
commit(tenant, request_id, fingerprint, TransactionCommand)
  -> committed(version) | conflict | prior_exact_outcome | unknown
changes(tenant, after_cursor, through, limit) -> ordered page
object_frontier(tenant) -> version + root
advance_object_frontier(expected, replacement) -> applied | conflict
restore_chunk(new_generation, chunk_id, records) -> exact idempotent outcome
finish_restore(new_generation, through) -> active generation
```

The boundary uses `TransactionCommand` for the first spike. It does not expose
FoundationDB system keys, TiKV Regions, RocksDB column families, provider file
formats, or provider backup manifests.

## Version and generation rules

1. A logical version is valid only inside one objectKV generation.
2. A provider stamp is compared as unsigned big-endian bytes.
3. FoundationDB's versionstamp maps losslessly to the current scalar commit
   version and batch order.
4. Restore creates a new generation. It does not claim that the destination
   provider reused source commit versions.
5. Historical source-generation reads resolve through immutable object history.
6. A new generation cannot acknowledge writes until restore completion and the
   active-generation pointer commit atomically.
7. Change pages never split or reorder one provider transaction.

## Provider-incarnation fencing

Provider-local generation state cannot choose between two FoundationDB cluster
identities. GP2.5.4 therefore composes two fences:

1. The external cell authority owns the active incarnation, authoritative
   routing, and reader-visible object frontier.
2. The source provider receives a transactionally visible generation-fence
   write before the destination can activate. Every objectKV commit in that
   provider reads the same key, so a commit ordered after the fence conflicts or
   observes that its generation is stale.

`Prepare(G + 1)` first stops authoritative routing for `G`. Destination
activation then requires a certified source fence, complete reconstruction, and
an exact destination-ready digest. After activation, the old provider identity
cannot receive a route or publish a root even if its source process restarts.
The provider-local fence separately prevents a correctly implemented adapter
from committing against the retained source media.

This R0 contract assumes the resurrected source uses media containing the fence.
A provider image rolled back before the fence is a different failure case. It
requires a current route lease or per-commit external authorization; the latter
must be included in the GP3.1 overhead pair before provider admission.

## Request outcome rule

`commit_unknown` is not converted to conflict or success. Every request has a
stable identity and content fingerprint. The same provider transaction that
applies the user mutation writes an outcome record at the request identity.

After an unknown result:

```text
read outcome(request_id)
  missing                -> retry same identity and fingerprint
  matching fingerprint   -> return retained exact outcome
  different fingerprint  -> reject identity reuse
```

The R0 preflight kills the client after the provider applies the transaction but
before the success reply is consumed.

## Objectification protocol

1. Read current object frontier `O` and freeze target `T` at a complete retained
   change boundary.
2. Read immutable retained changes in `(O,T]` using bounded pages.
3. Build row-history objects and any declared typed projections outside a
   provider transaction.
4. Upload digest-addressed objects and the manifest.
5. Verify every named child and manifest digest.
6. Compare-and-advance the object frontier from `O` to `(T, root)`.
7. Reclaim retained-change keys through `T` only after the frontier read proves
   that exact root.

New commits after `T` append later versionstamped changes and do not block the
publication.

## Restore protocol

Restore is a lifecycle operation, not one long provider transaction.

```text
create fenced destination generation G2
  -> verify source closure through O
  -> apply deterministic idempotent chunks
  -> replay retained changes (O,C]
  -> compare exact digest at C
  -> atomically mark G2 active
  -> fence G1
```

Each chunk has a content-derived identity. Replaying a matching chunk is a
no-op. Reusing an identity with different bytes fails closed.

## Failure model

- client death before commit, during commit, and after an unknown result;
- provider node, leader, coordinator, or process loss;
- stale read version and transaction-age rejection;
- objectifier death before upload, after data upload, after manifest upload,
  and after frontier commit with the reply lost;
- retained-change pagination at a shared commit version;
- provider media loss followed by empty-generation restore;
- restore worker death between chunks and before generation activation;
- stale source generation attempting to commit or advance a frontier;
- object corruption, missing child, conflicting immutable bytes, and delayed
  object response;
- retention debt exceeding the configured byte or version bound.

## Eval plan

### Stage A, semantic preflight

The frozen history starts with `left=1` and `right=1`. Two concurrent
transactions read both keys. One writes `left=0`; the other writes `right=0`.
Strict serializability permits at most one commit because each transaction read
the key written by the other.

Primary metric: `correctness.anomalies`, lower is better and must equal zero.

Hard gates:

- write skew rejected;
- point and range reads ordered at one version;
- point clear and range clear atomic;
- retained change and request outcome atomic with user state;
- unknown-result exact retry;
- commit stamp and retained cursor strictly ordered;
- no object request on the active main-branch read path;
- suite and provider image pinned by immutable identity.

Negative controls:

- snapshot-isolation write skew;
- retained change written after commit;
- outcome stored only in client memory;
- retained page ordered by request start rather than commit stamp;
- frontier advanced before closure verification.

### Stage B, R0 lifecycle

Run the surviving provider on sequential private R0 source and restore runners,
regional versioned GCS, and required OTel. GP2.5.2 first proves logical
generation reconstruction while source media remains present. GP2.5.3 then
replaces the source VM and provider disk, observes the source instance, boot
disk, and data disk absent from an external controller, and restores into a
fresh FoundationDB cluster identity. This stage proves mechanism and
single-machine lifecycle only.

Provider-media loss and provider-incarnation fencing are separate claims:

```text
GP2.5.3 source cluster and disks absent -> exact fresh-cluster restore
GP2.5.4 old cluster resurrected         -> cannot commit, route, or publish
```

The logical generation key inside FoundationDB cannot fence another
FoundationDB cluster after the first cluster is lost. GP2.5.4 therefore
requires external cell-incarnation authority consistent with RFC-0009. A
deleted source process is not fencing evidence.

The frozen GP2.5.4 contract and GCP topology are in
`docs/research/provider-incarnation-gp2.5.4.md`.

Measure:

- transaction p50, p99, throughput, conflicts, and unknown outcomes;
- extra bytes and keys written for retained history and request outcomes;
- objectification throughput, lag, request count, and write amplification;
- empty-generation restore time and bytes;
- exact post-restore digest;
- active hot reads with and without lifecycle emission;
- branch-create metadata bytes and first-read cost.

The frozen GP2.5.3 topology, receipt fields, positive gates, and executed
hidden-source-media poison are in
`docs/research/provider-media-loss-gp2.5.3.md`. Candidate
`50c72159781e14d3db06d792beac34838572fc91` passed the positive lane with
zero anomalies after all source media was absent; the poison was discarded.

### Stage C, R1 durability comparison

Only after R0 passes, run three provider data machines in separate zones and a
controller outside the data identities. Compare the adapter configuration with
the unmodified provider at the same durability. This is the earliest valid
solution-level latency or throughput claim.

## Provider decision rule

1. Reject any provider that fails P1 through P10.
2. Do not repair a P1 failure with objectKV-owned distributed coordination.
3. If one provider remains, run its R0 lifecycle, provider-media-loss, external
   incarnation-authority, and hot-path overhead gates.
4. If FoundationDB's retained-change overhead exceeds 25 percent at p99 or
   throughput after one bounded optimization, evaluate its backup-worker or
   Blob Granule path as one orthogonal mechanism.
5. If no provider passes, stop the strict-serializable kernel product. Retain
   `okv-log`, object publication, branch/history formats, and DataFusion as
   independent libraries.

## Alternatives

### Continue the native plane

Optimizes for full control and a truly disposable resident tier. Gives up the
measured p99 stop rule and reopens consensus, resolver, MVCC, placement, repair,
and operational work that D52 rejected.

### FoundationDB plus objectKV lifecycle

Optimizes for exact semantic fit, ordered versionstamps, and proven recovery.
Gives up a Rust-only stack, externally assigned commit versions, and direct
control over hot storage. The C client and cluster version remain operational
dependencies.

### TiKV plus objectKV lifecycle

Optimizes for a Rust and RocksDB range architecture, existing CDC, backup, and
SST import surfaces. Gives up strict serializability unless objectKV adds the
coordination layer that this RFC prohibits.

### PostgreSQL or Neon as the transaction plane

Optimizes for the first relational consumer. Gives up the minimal ordered-KV
waist and makes Redis, search, and filesystem consumers depend on PostgreSQL
semantics.

## Compatibility and migration

- The adapter is internal and unpublished.
- Provider stamps are encoded behind a versioned objectKV logical-version type.
- Existing `OKVT2`, `okv-log`, row-object, and publication bytes do not change.
- RFC-0040 code and receipts remain for correctness comparison and rollback of
  the research decision, not as a production fallback.
- The losing provider spike is removed after its receipt and rejection reason
  are durable.

## Unresolved questions

1. Can FoundationDB transactionally emit one retained command and request
   outcome with less than 25 percent hot-path cost at the frozen transaction
   mix?
2. Does the ten-byte FoundationDB versionstamp map across every API binding
   without losing transaction batch order?
3. Should R0 use explicit retained keys first, or the experimental partitioned
   backup worker as the first change-capture mechanism?
4. Can a lazy branch overlay avoid hydrating the parent while keeping first-read
   request and p99 bounds acceptable?
5. What exact objectification-debt ceiling must throttle new commits?

## Primary sources

- [FoundationDB developer guide](https://apple.github.io/foundationdb/developer-guide.html)
- [FoundationDB C API at 7.4.6](https://github.com/apple/foundationdb/blob/e77b64d4c5d01d240931c08c5384a834cae27337/bindings/c/foundationdb/fdb_c.h)
- [FoundationDB backup and restore](https://apple.github.io/foundationdb/backups.html)
- [FoundationDB partitioned backup-log design](https://github.com/apple/foundationdb/blob/main/design/backup_v2_partitioned_logs.md)
- [FoundationDB experimental feature status](https://apple.github.io/foundationdb/experimental-features.html)
- [TiKV 8.5.7](https://github.com/tikv/tikv/tree/3f446cfa9eb1d5c653031d261e185911495d0359)
- [TiKV Rust client](https://github.com/tikv/client-rust/tree/88688d6eb3a55a864885d7bccc8abf428dce076c)
- [TiDB transaction isolation](https://docs.pingcap.com/tidb/stable/transaction-isolation-levels/)
- [TiKV storage maintenance guide](https://github.com/tikv/tikv/blob/3f446cfa9eb1d5c653031d261e185911495d0359/doc/maintenance-guides/src/storage.md)
