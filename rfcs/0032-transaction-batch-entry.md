# RFC-0032: Transaction batch entry

- Status: `[CODE-COMPLETE]`, retained by the `[EVALUATING]` G4.9 local receipt
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 commit proxy, transaction versioning, and recovery stream

## Decision

Test an explicit transaction batch as one quorum-durable Raft application
entry. The batch contains independently encoded client transaction commands.
The state machine resolves them in their encoded order and returns one durable
outcome per request identity.

```text
independent transaction requests
             |
             v
commit proxy closes a bounded batch
             |
             v
one Raft application entry, OKVB1
             |
             v
one leader stable append and quorum replication
             |
             v
ordered per-transaction results
```

The batch is a durability and ordering unit. It is not one atomic application
transaction. Each item commits or conflicts independently.

## Why this follows G4.8

G4.8 submitted up to 32 one-entry transactions concurrently. Followers grouped
roughly 11 to 15 entries per stable append, but the leader still performed 512
appends for 512 transactions. The candidate reached 153.708 median durable
transactions per second, 264.887 ms maximum p99, and a 3.964x gain over the
sequential control. It missed every frozen admission threshold except follower
append grouping.

RFC-0032 changes the seam that G4.8 proved limiting. One leader application
entry now carries several transaction requests.

## Transaction versionstamp

All accepted transactions in one batch share the Raft entry's commit version.
Each receives a distinct 16-bit batch order. Their durable transaction
versionstamp is:

```text
(commit_version: u64, batch_order: u16)
```

The pair is unique and ordered. `commit_version` alone identifies the database
snapshot. A read at that version observes every accepted transaction in the
batch; no read can address a partially applied batch.

This matches the FoundationDB commit-proxy shape: a write version is assigned
to a transaction batch, while the transaction versionstamp contains a batch
component that orders transactions sharing that version. The reference is the
FoundationDB Developer Guide transaction-processing section and its public
versionstamp contract:

- <https://apple.github.io/foundationdb/developer-guide.html#transaction-processing>
- <https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/MutationType.html>

This supersedes RFC-0031's G4.8-only requirement that every transaction have a
distinct scalar commit version. Existing one-transaction entries use batch
order zero, so their versionstamp remains unambiguous.

## Resolution order

The batch vector is canonical serialization order. For each item:

1. validate its request identity and exact command fingerprint;
2. recover an existing durable outcome if this is an exact retry;
3. reject a reused identity with different bytes;
4. apply the active generation fence;
5. check conflicts against prior committed versions and earlier accepted items
   in this batch;
6. apply all mutations atomically for that transaction or apply none;
7. retain its exact outcome and, if committed, one recovery record carrying
   the shared commit version and distinct batch order.

An earlier accepted transaction may cause a later item to conflict. A later
item cannot affect an earlier result. Items with no overlapping conflicts can
commit at the same snapshot version.

## Wire and bounds

The first command format is:

```text
OKVB1 {
  commands: [ClientCommand]
}
```

Every inner command must decode as a transaction command. The batch is rejected
before mutation if it is empty, has more than 32 items, contains duplicate
request identities, contains a control-plane payload, or exceeds the existing
Raft application-entry byte limit.

There is no separate outer retry identity. Replaying the exact batch recovers
every inner outcome. Replaying one inner command through the ordinary
single-transaction API recovers the same outcome and versionstamp.

## Recovery stream cursor

Recovery records are ordered by `(commit_version, batch_order)`. The retained
stream cursor must carry both fields so a bounded page may end inside one batch
without skipping its remaining records. Old scalar cursors retain their old
meaning: they resume strictly after the entire named commit version.

Object frontiers remain scalar commit versions. Objectification and physical
pop cover complete commit versions only; they cannot split a batch.

## G4.9 candidate and controls

Frozen local profile:

```text
transactions per seed:          512
candidate transactions/batch:    16
candidate batches in flight:       1
control transactions in flight:   32
live keys:                       256
value bytes:                     128
seeds:                     4901, 4902, 4903
```

### Candidate

Submit 32 explicit batches of 16 transactions through the normal quorum-
durable path. Verify exact per-item outcomes, unique ordered versionstamps,
complete retained-stream pagination, final values, exact individual retry,
exact whole-batch replay, leader failover, and killed-voter restart.

### Same-durability control

Run the same 512 transactions as distinct Raft entries with the G4.8 bounded
window of 32. Use the same executable, stable journals, processes, recovery
checks, and failure sequence.

### Duplicate-identity control

Submit one batch containing the same request identity twice and require the
whole batch to fail before mutation or durable retry-state insertion.

### Early-ack poison

Acknowledge one batch without quorum durability, kill the isolated leader, and
require the recovered quorum to expose the missing acknowledged outcomes. The
suite must discard the subject regardless of throughput.

## Frozen gates

Correctness gates:

1. every input identity has exactly one durable outcome;
2. accepted items in one batch share a commit version and have contiguous batch
   orders in encoded order;
3. versionstamps are globally unique and ordered;
4. a later transaction that conflicts with an earlier accepted item in the
   same batch is rejected;
5. paginated recovery emits every committed item exactly once;
6. exact individual and whole-batch retries do not apply twice;
7. final values, leader failover, and killed-voter restart are exact;
8. duplicate identities are rejected before mutation;
9. the early-ack poison is detected;
10. fresh-controller replay is exact and the executable is a release build.

Candidate performance gates:

1. at least 400 durable transactions per second on the frozen local profile;
2. at least eight logical transactions per leader stable append;
3. commit p99 no greater than 100 ms per transaction;
4. total runner time no greater than 120 seconds;
5. at least 2.5x median throughput over the paired one-entry control.

These are local mechanism gates, not production SLOs.

## Keep or discard

Keep the batch entry if every correctness gate passes and the candidate clears
all absolute and paired performance gates. Then retain it as the Cell v0 commit
proxy primitive and move to delay, byte, overload, and independent-machine
curves.

Discard it if it weakens individual retry semantics, makes recovery pagination
ambiguous, produces partial-batch visibility, or misses the performance gates.
That result would force a choice between deeper OpenRaft integration, a
different replicated-log implementation, or pivoting native transaction
authority to TiKV or FoundationDB while retaining objectKV's upper layers.

## Tradeoff

This optimizes for amortizing consensus and stable-log synchronization while
retaining short independent transactions. It gives up the simple equivalence
between one Raft entry and one transaction, and it makes the versionstamp pair
part of the public contract.

## Not claimed

- adaptive batch delay or byte sizing;
- multiple commit proxies;
- partitioned resolvers or txLogs;
- independent-machine throughput or latency;
- production overload behavior;
- a production cell admission.

## G4.9 outcome

The candidate reached 559.511 median durable transactions per second, 34.016
ms maximum p99, and 16 logical transactions per leader stable append. The
one-entry same-durability control reached 151.944 transactions per second, so
the paired gain was 3.682x. Every frozen correctness, absolute performance,
paired performance, replay, and budget gate passed. Duplicate-identity and
early-ack controls were rejected.

D40 retains this mechanism for the next sustained-load and independent-media
gates. The receipt remains evaluating because it uses dirty source and three
processes on one host.
