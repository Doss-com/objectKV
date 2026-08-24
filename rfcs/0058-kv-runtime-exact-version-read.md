# RFC-0058: KV Runtime exact-version read path

- Status: accepted for the next serving prototype, 2026-08-24
- Authors: DOSS
- Created: 2026-08-24
- Depends on: RFC-0002, RFC-0003, RFC-0034, RFC-0057

## Decision

`[DECIDED]` Store objectKV-owned MVCC entries inside the one-database KV
Runtime layout selected by RFC-0057. Use SlateDB as the ordered object LSM, not
as the owner of objectKV snapshot semantics.

One physical entry is ordered by:

```text
user namespace
  + escaped user key
  + key terminator
  + complemented 128-bit commit version
  -> tagged value or point tombstone
```

Escaping must preserve arbitrary binary user-key order. Complemented versions
sort newest first inside one user key. A point read at `T` seeks to the first
physical version less than or equal to `T`. A range read walks user keys in
ascending order and selects the first visible version in each key group.

The KV Runtime must refuse a read at `T` until its applied frontier is at least
`T`. The transaction system remains responsible for assigning `T`; the txLog
remains the recent durability authority. SlateDB sequence numbers continue to
receive the externally assigned generation-zero version, but readers do not
depend on a public `snapshot_at(sequence)` API.

## Why this gate exists

The pinned SlateDB revision exposes external write sequence numbers and already
has an internal maximum-sequence visibility filter. Its public snapshot API can
only capture the latest local sequence. Depending on a new upstream method
would still leave objectKV coupled to SlateDB retention and compaction semantics
for the stable transactional segment contract.

An objectKV-owned encoding preserves the RFC-0003 narrow waist and makes exact
version reads testable now. It also creates real costs:

- a point read is a bounded prefix seek instead of a direct latest-key lookup;
- retained versions increase read and compaction work;
- objectKV must implement range tombstones and history GC explicitly;
- logical range movement scans key intervals instead of moving one database.

## Range Engine relationship

A Range Engine assignment is a half-open interval in the tenant's ordered
logical keyspace plus routing epoch and applied frontier. Its ID is not embedded
in the durable user key. Splitting or moving a Range Engine therefore does not
rewrite key identity.

The synthetic `range/{id}` prefixes in RFC-0057 were density-fixture data, not a
production key-layout decision. Tenant and system namespaces remain part of the
global ordered key contract; transient serving ownership does not.

## Frozen semantic cases

The adapter must prove:

1. point reads return the newest set or clear at or below `T`;
2. reads between commit versions return the preceding visible value;
3. ordered scans return each key at most once in user-key order;
4. keys containing zero bytes and keys that prefix other keys retain exact
   lexical order;
5. a point clear hides older values without deleting their history;
6. a requested version above the applied frontier returns
   `snapshot_unavailable`, never a partial answer;
7. exact commit replay remains idempotent after later versions;
8. close and empty-cache reopen preserve the same answers.

Atomic range clear remains outside this slice and must stay visibly unsupported.

## Frozen curve

The first performance lane uses one fresh child process, one local filesystem
object store, one 64 MiB decoded cache, one 256 MiB filesystem cache, 64 KiB SST
blocks, no SlateDB object WAL, no automatic flush, and no embedded compactor or
garbage collector.

Measure three version depths per hot key: `1`, `16`, and `256`. At every depth,
measure:

- warm and empty-cache point p50 and p99 at latest, 8 versions behind when
  present, and oldest retained;
- warm and empty-cache ordered scan rows per second over 1,024 logical keys;
- object requests and bytes per returned row;
- physical bytes per live logical byte;
- flush and empty-cache reopen time;
- exact agreement with `okv-model` at every requested version.

Use seeds `1103`, `2207`, and `3301`. Preserve the raw child receipt with the
exact executable SHA-256.

## Hard gates

- all frozen semantic cases pass;
- requested versions never exceed the reported applied frontier;
- point and scan results match the reference model exactly;
- the physical key decoder rejects malformed escape, terminator, version, and
  value-tag encodings;
- every correct subject completes under 1 GiB RSS and 120 seconds;
- object requests and bytes are measured, not estimated;
- empty RAM and NVMe cache state is proven before cold reads;
- identical seed and configuration reproduce one semantic receipt;
- every negative subject violates its owning gate and discards.

No machine-independent latency ceiling is admitted in this local lane. The
first result establishes shape, amplification, and scaling with retained
history. MinIO and GCS receive separate comparable profiles after the local
shape is known.

## Negative subjects

1. store only the latest physical value and answer an old `T` with it;
2. skip a point tombstone and reveal the preceding value;
3. claim an applied frontier beyond the actually applied batch;
4. length-prefix raw user keys so lexical scan order changes.

## Stop and keep rules

Keep this encoding for the next KV Runtime prototype only if every semantic
case passes, every control discards, and point-read object amplification stays
bounded as retained history grows. A slower oldest-retained read is expected;
latest and near-latest reads must not scan the complete history of the key.

Reject the encoding if exact reads require loading every retained version, if
binary key order is not stable, or if a frontier gap can return a partial
snapshot. A rejection reopens the small SlateDB `snapshot_at` seam or an
objectKV-owned transactional segment reader.

## Tradeoff

This gate optimizes for owning the stable MVCC contract while reusing SlateDB's
object LSM and cache machinery. It gives up using SlateDB's direct latest-key
lookup as the objectKV read contract. It does not yet prove range tombstones,
history collection, remote-object latency, concurrent commit application,
prefix-aware compaction, or online range movement.

## Result

Candidate `fe2906d` kept all three correct depth points under suite hash
`e3bc8644`; all four semantic controls discarded. Near-latest empty-cache
point p99 was 1.85 ms at depth `1`, 2.05 ms at depth `16`, and 6.69 ms at
depth `256`. The depth-256 point batch used about four 64 KiB range reads per
sample rather than scanning all 256 versions.

The retained-history cost was linear where expected. Physical bytes per live
byte were `1.20x`, `17.77x`, and `283.47x`. At depth `256`, a cold 1,024-row
scan read 74,032,200 bytes through 1,128 range GETs and completed in about
0.92 seconds at the near-latest snapshot. Keep the encoding for the next
prototype. Do not keep every version indefinitely. The next gate must bind
snapshot leases to a monotonic minimum-readable version and filter older MVCC
history during object compaction.
