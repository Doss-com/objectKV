# RFC-0007: Manifest publication and garbage collection

- Status: proposed
- Created: 2026-08-22
- Revised: 2026-08-23

## Decision

objectKV makes immutable bytes visible only through a transactionally published
root. A publisher registers an intent, writes and verifies every content-
addressed object, then atomically installs the new manifest reference and
retires the intent. Readers never discover live state through object-store
`LIST`.

Garbage collection derives liveness by walking every authoritative root and
retained intent. Counters and `LIST` are audit inputs only. An incomplete walk,
an unresolved root generation, or a concurrent publication causes the affected
deletion plan to fail closed.

This decision applies to transactional segments, analytical artifacts,
snapshots, branches, backups, CDC retention, and query leases. Each root type
may have a different retention policy, but none may bypass the shared
reachability proof.

## Authority model

The transactional control plane owns:

- current range and table manifest references;
- publication intents and their owner, epoch, object closure, and expiry;
- snapshot, branch, backup, CDC, and active-lease roots;
- schema and format compatibility roots;
- the last completed GC mark epoch and delete-plan identity.

The object store owns immutable bytes. A manifest object can describe a large
closure, but a small transactional root selects the exact manifest identity.
Object-store names, timestamps, ETags, versions, and listing order do not select
the current database state.

Every immutable object key includes the SHA-256 digest of its exact stored
bytes. Finding different bytes at the same key is corruption, not a retryable
conflict.

## Publication protocol

One publication has a stable `PublicationId`, source interval, destination root,
expected prior root, complete object set, and intended manifest digest.

```text
Prepared
  -> Uploading
  -> BytesVerified
  -> Published
  -> Retired

Prepared | Uploading | BytesVerified
  -> Abandoned
```

The required sequence is:

1. Create or resume `PublicationIntent` in a serializable transaction. The
   intent names every object digest before any of those objects become eligible
   for GC deletion.
2. Write each immutable object with `put_if_absent`.
3. Resolve every unknown PUT outcome by named identity read. Verify exact
   length and digest. Retry only when the named identity is absent.
4. Write and verify the immutable manifest object that closes over the data
   objects.
5. In one serializable transaction, validate the expected prior root and
   publication epoch, install the new root, record provenance and covered
   version, and retire the intent.
6. Resolve an unknown transaction outcome through the retained request identity
   or exact root transition. Never infer success from object existence.

Readers use only `Published` roots. `Prepared`, `Uploading`, and
`BytesVerified` intents are GC roots but are not reader-visible database roots.

An expired intent is not immediately deletable. The reaper first proves that no
live owner can still publish its epoch, marks the intent `Abandoned`, waits the
configured quarantine horizon, and lets the next complete mark determine
liveness. This optimizes for safe retry and gives up immediate orphan recovery.

## Garbage collection protocol

One GC cycle pins a transactional `MarkSnapshot` containing:

- root-set version and membership epoch;
- every published root;
- every non-abandoned publication intent;
- every active snapshot, branch, backup, CDC, and query-lease root;
- every retained schema and reader-compatibility root;
- the object quarantine horizon and backend capability profile.

The marker walks the exact manifest graph from that snapshot. It records the
visited manifest identity, referenced object digest, and traversal checksum.
The mark is complete only when every root and referenced manifest was readable,
checksum-valid, format-supported, and closed without a missing child.

Candidate discovery may use an inventory export or `LIST`, but subtraction is
valid only against a complete mark. A candidate must also:

- be older than the quarantine horizon;
- be absent from the pinned reachable set;
- be absent from a newer publication intent or root at delete revalidation;
- carry the exact object identity required by the backend's guarded-delete
  capability, when available.

Immediately before deletion, the sweeper re-reads the candidate's root and
intent domains in one serializable transaction. If either domain changed after
the mark snapshot, the candidate is deferred to a later complete cycle. A
backend with guarded delete uses the pinned generation or version. A backend
without guarded delete requires immutable digest keys plus the conservative
quarantine and revalidation proof from RFC-0004.

Deletion with an unknown outcome is resolved by named identity read. A later
root may reference the same content digest only after publication has again
verified that the object exists.

## Ground truth and observability

Reachability is ground truth. Refcounts, live-byte counters, segment counters,
and compaction accounting are derived telemetry. Drift opens an incident and
may pause reclamation; it can never authorize deletion.

The first implementation records:

- objects and bytes reachable by root class;
- publication-intent age and unresolved outcomes;
- orphan and quarantined bytes;
- complete and incomplete mark counts;
- candidates, deferred deletions, guarded deletes, and reclaimed bytes;
- root-set changes that invalidate a delete plan;
- object-store requests, bytes, latency, and estimated cost.

Object keys and root identities remain forbidden metric attributes.

## Failure cases

1. A publisher installs a root before one block is readable. The reader can
   observe an absent object, so the publication is rejected.
2. A publisher uploads bytes without a retained intent while GC runs. The
   sweeper can delete the block before root installation, so the publication is
   rejected.
3. A refcount drifts to zero for a reachable block. A counter-authoritative GC
   deletes live data, so counters cannot authorize reclamation.
4. `LIST` omits a new object or returns a deleted object. A listing-authoritative
   mark produces either data loss or leakage, so listing is audit-only.
5. One manifest read fails during mark. Sweeping the partial complement can
   delete its entire unknown closure, so the cycle reclaims nothing from the
   affected domain.
6. A new snapshot pins a candidate after mark but before sweep. Delete-time
   root-domain revalidation detects the change and defers the object.
7. A block PUT succeeds but its response is lost. Named digest verification
   converts the unknown outcome into success without publishing early.
8. Root publication commits but its response is lost. Transaction request
   replay or exact transition comparison resolves the outcome without relying
   on objects or listing.

## Evaluation contract

`object-publication-gc-v1` executes deterministic publication, unknown-outcome,
mark, concurrent-root, and sweep histories. The correct subject must prove:

- no root references absent or corrupt bytes;
- every in-flight publication is rooted before upload;
- unknown block and root outcomes resolve by identity;
- only a complete reachability walk can produce deletions;
- counters and `LIST` cannot change liveness;
- delete-time revalidation protects new roots;
- same-seed replay is byte-exact.

Unsafe subjects publish a pointer before its bytes, omit the publication intent,
trust a drifted counter, trust stale `LIST`, sweep after an incomplete mark, or
skip delete revalidation. Each must fail at a bounded event with a `discard`
verdict.

The correctness adapter may use an in-memory transactional authority and object
model. Performance and cost curves are admitted only after the same contract
runs against the real object client and a durable authority implementation.

## Tradeoffs

D1: transactional intents instead of age-only orphan quarantine. This optimizes
for publication racing GC and exact ownership. It adds one small control-plane
transaction before upload.

D2: complete graph walk instead of refcount-authorized deletion. This optimizes
for recoverable correctness under accounting drift. It gives up constant-time
reclamation and requires scalable partitioned walks later.

D3: delete-plan revalidation instead of a long global GC lock. This optimizes
for continued publication and bounded coordination. It gives up reclaiming
objects whose root domains changed during the cycle.

## Unresolved questions

- Partitioning the mark graph without losing one globally complete receipt.
- Exact publication-intent expiry and owner-fencing protocol.
- Provider-specific guarded-delete adapters for GCS, S3, and Azure.
- Inventory export consistency and cost for candidate discovery.
- Encryption-key retirement as an additional liveness root.
- Proving analytical-tail replacement and query-lease release before reclaiming
  old ZebraDB objects.
