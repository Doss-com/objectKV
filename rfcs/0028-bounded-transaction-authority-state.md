# RFC-0028: Bound transaction-authority state before safe pop

- Status: `[PROPOSED]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 transaction-authority state economics

## Decision

Measure the complete serialized transaction-authority state before implementing
the object-frontier safe-pop protocol. The G4.4 prototype retains four distinct
classes of state:

| State | Required frontier | Intended long-term owner |
| --- | --- | --- |
| Current values | Serving-storage coverage | Disposable range serving image plus objects |
| OCC conflict history | Oldest admitted read version | Resolver partitions |
| Request outcomes and fingerprints | Exact retry window | Transaction proxy outcome cache |
| Recovery commands | Object-durable version `O` | Quorum-durable txLog |

Advancing `O` can reclaim only the fourth class. A stream-only pop is not a
bounded-state solution if the other classes continue growing with lifetime
commits.

G4.5 therefore measures the exact JSON bytes that the current OpenRaft snapshot
builder would serialize. It also computes a non-mutating lower-bound projection
that removes every retained recovery command through `C` and sets the retention
floor to `C`. This projection assumes ideal objectification with `O = C`. It is
not a product safe-pop command.

## Frozen accounting response

`transaction-log-storage-stats-v1` returns:

```text
high_watermark
retention_floor
projected_retention_floor
live_keys
retained_conflict_versions
durable_outcomes
request_fingerprints
retained_records
projected_retained_records
snapshot_bytes
projected_snapshot_bytes
transaction_authority_bytes
retained_transactions_bytes
projected_retained_transactions_bytes
durable_outcomes_bytes
request_fingerprints_bytes
```

The server performs a Raft linearizability barrier before reading the state.
`snapshot_bytes` is the byte length of the same `StateMachineData` JSON encoding
used by `build_snapshot`. The projection clones the state and cannot change the
readable stream or transaction state.

## G4.5 boundedness gate

The fixed workload keeps 256 live keys and 128-byte values while increasing
lifetime commits through checkpoints 256, 1,024, and 4,096. Each transaction
overwrites one key, has no read conflict range, and carries one complete point
write-conflict range. Seeds are 1103, 2207, and 3301.

The candidate reports complete-state growth after the ideal stream-pop
projection. The same-correctness control reports the current no-pop snapshot.
The poison reports only the projected retained-stream field and omits the rest
of the state. The poison must be rejected by the complete-accounting oracle.

The primary metric is `authority.snapshot_growth_ratio`, defined as bytes at
4,096 commits divided by bytes at 256 commits. At fixed live key cardinality,
`O = C`, no active old read version, and no required retry history, the admission
ceiling is 2.0. A larger ratio discards the current authority state shape. It
does not discard objectKV as a product thesis.

## Required invariants

1. Every checkpoint is read through a linearizable data-authority RPC.
2. The actual snapshot encoding contains every accepted recovery record.
3. The ideal projection contains no recovery record at or below its projected
   floor.
4. Computing the projection does not mutate the actual retained stream.
5. Live key cardinality remains fixed after the first 256 commits.
6. Request outcomes, fingerprints, and OCC conflict history are included in
   complete-state accounting.
7. The retained-only poison is detected even if it reports a favorable growth
   ratio.
8. Three fresh-process seeds emit the same structural counts at each checkpoint.

## Consequence

If the candidate fails, the next implementation must give each state class its
own owner and reclamation frontier before adding safe pop:

```text
object frontier O       -> recovery command pop
minimum read version    -> OCC conflict pop
retry retention floor   -> outcome and fingerprint pop
serving coverage        -> remove user values from transaction authority
```

Only then should object publication issue a generation-fenced frontier proof
that the data authority can use to advance the recovery retention floor.

## Not claimed

- an implemented safe-pop command;
- a production state-machine snapshot codec;
- OpenRaft physical-log purge or snapshot installation under load;
- bounded resolver or retry-cache state;
- independent-machine, GCS, OTel, or release-build performance.
