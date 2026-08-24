# RFC-0045: Tagged-log learner repair under retained lag

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0009, RFC-0023, RFC-0040, RFC-0044

## Decision

`[DECIDED]` A failed tagged-log process is replaced first as a non-voting
learner with a new process identity and empty storage. The learner may install
one exact retained-log snapshot only after a write quorum of the active log-set
policy signs the same snapshot identity. It may then receive an ordered tail,
but it reports repair readiness only after an active write quorum signs the
same installed frontier and retained-root identity. It does not contribute
capacity, durability, pop, or serving quorum evidence until a later replicated
policy transition promotes it.

## Context and invariant

RFC-0044 bounds retained bytes while objectification lags and authenticates
pop. It assumes the required tLog sets retain their configured membership. A
single process can fail without losing quorum, but operating indefinitely at
minimum quorum removes failure margin. Copying one survivor or immediately
counting a new process would let one corrupt or stale source become durable
truth.

For active member set `M`, write quorum `Q`, failed member `F`, learner `L`,
object frontier `O`, and commit frontier `C`:

```text
O < C
F is unavailable
L is not in M

install(L, snapshot) only if:
    at least Q distinct members of M sign the same snapshot identity

ready(L, position, root) only if:
    L has synchronized through position
    at least Q distinct members of M sign the same position and root

quorum(capacity | append | pop | serve) counts only members of M
```

The snapshot binds every retained record in `(popped_through, C]`, not only the
latest value or last position.

## Proposed repair certificate

```text
TaggedLogRepairSnapshot {
  format_version
  cell_id
  tenant_id
  generation
  log_set_id
  policy_epoch
  repair_id
  failed_node_id
  learner_node_id
  learner_incarnation
  learner_public_key
  last_position
  popped_through
  source_sample_epoch
  snapshot_length
  snapshot_sha256
}
```

Each attestation binds one active signer identity and Ed25519 signature. The
canonical snapshot carries the ordered retained records and their complete
wire bytes. The learner validates:

1. distinct signers are current active members and reach write quorum;
2. every signer signed the same statement;
3. the target learner identity, incarnation, and public key match its local
   configuration, and the learner proves possession of the private key;
4. snapshot length and digest match the supplied bytes;
5. records are contiguous, byte-exact, domain-matched, and strictly above the
   durable `popped_through` marker;
6. the learner root is empty and no process with its identity is active in the
   current policy;
7. snapshot and retention state synchronize before readiness is reported.

Exact replay of the same repair is idempotent. A conflicting replay fails
closed. If active members append after the base snapshot, the learner installs
the ordered tail before requesting a readiness certificate. Restart preserves
the installed snapshot, tail, readiness receipt, and learner identity.

This is a two-step contract:

```text
quorum-certified base snapshot
    -> ordered tail catch-up
    -> quorum-certified learner readiness
```

Snapshot transfer does not freeze writes. A snapshot that falls behind is a
valid repair base while the learner remains excluded. A stale learner cannot
claim readiness at a later frontier or enter any active quorum.

## Frozen scenario

Use one tenant, two three-process tLog sets, write quorum two, and the accepted
RFC-0044 capacity policy.

1. Start from `C=O=10`, freeze objectification, and commit transactions 11
   through 14 into both required sets.
2. Stop node `1` in log set `10` and treat its private root as lost. Nodes `2`
   and `3` remain the only active write quorum.
3. Start node `4` as an empty learner with a new storage incarnation. It does
   not appear in active policy.
4. Nodes `2` and `3` sign one exact repair snapshot covering transactions 11
   through 14 and `popped_through=10`.
5. Node `4` installs and synchronizes that snapshot, restarts from its private
   root, and returns the same retained bytes and ordered records. Nodes `2` and
   `3` then sign one readiness identity for node `4` at position `14` and the
   exact retained root.
6. Capacity collection still counts only nodes `2` and `3`. A fresh serving
   worker also ignores node `4` and reconstructs exact `Database(14)` from the
   active survivor quorum.
7. Record node `4` as repair-ready for continued tailing and a later log-set
   policy transition. This RFC does not promote it.

## Negative subjects

The frozen suite independently attempts to:

1. install from one active source signature;
2. alter one retained record after the quorum signs the snapshot;
3. claim repair readiness at a later survivor frontier without tail catch-up;
4. install a certificate for a different learner identity or incarnation;
5. count the learner toward serving or capacity quorum before promotion;
6. install into a process claiming the target learner identity and storage
   incarnation without the certified learner private key.

Every subject must replay exactly, produce a correctness or membership anomaly,
export OTel, and discard.

## Eval plan

Freeze `cell-tagged-log-learner-repair-v0` with seeds `1103`, `2207`, and
`3301`. Each seed starts three transaction-authority processes, two
three-process tLog sets, one fresh learner process, and one fresh serving
worker. The event budget is 210 checks.

The primary metric is correctness anomalies. The receipt counts active tLog
starts, failed processes, learner starts and restarts, repair attestations,
snapshot bytes, installed records, readiness attestations, capacity
attestations, serving responses, active-policy members counted, and final
frontiers. OTel carries
`wal.retained_bytes`, `availability.success_ratio`, `operation.duration`, and
correctness anomalies with exact candidate, suite, profile, run, and workload
labels.

## Evaluation evidence

Candidate `670ef0a` kept OTel run `a3c3356a` at 69 of 210 allowed events with
zero anomalies and exact replay across seeds `1103`, `2207`, and `3301`. Each
seed used three unique transaction-authority nodes, two three-process active
tLog sets, one failed member, one new learner process, one learner restart,
and one fresh serving worker. The suite verified six active repair
attestations, six readiness attestations, 12 installed retained records, and a
maximum certified snapshot size of 3,977 bytes. Every worker counted only
active nodes `2` and `3` and reconstructed exact transaction `14` from object
frontier `10`.

| Control | Run | Anomalies per seed |
|---|---|---:|
| one source signature | `6d99de75` | 2 |
| tampered snapshot after signing | `b97e5c23` | 2 |
| stale readiness frontier | `c90d7af6` | 2 |
| wrong learner incarnation | `2deb8392` | 1 |
| count unpromoted learner | `4fe75506` | 1 |
| duplicate live learner identity | `5c00a9ae` | 2 |

All six controls replayed exactly and discarded. The frozen source suite hash
is `b85fbfdb`; the evaluated suite hash is `5b2db17a`; the profile hash is
`6cd2ae37`. Candidate `8ef5c87` and run `38ac862c` were discarded before
scoring because `operation.duration` omitted its required `result` attribute.
Candidate `670ef0a` corrects that telemetry contract without changing the
repair subject.

The gate admits one complete retained-suffix snapshot with no concurrent
append during the correct transfer. The stale-readiness control proves that a
learner cannot enter readiness after survivors advance, but ordered live-tail
catch-up, chunking and resume, remote transfer, promotion, independent-host
failure, and production key custody remain open. Commit versions and tLog
positions are separate domains. The certificate binds object frontier and
retained position without ordering them numerically.

## Alternatives

### Copy one survivor

One source is available but not authoritative. It can contain a stale suffix,
corrupt bytes, or a local pop that never reached quorum.

### Replace the member in one step

Combining transfer and promotion makes it difficult to prove whether a quorum
included the new process before its state was complete. Learner repair and
policy movement remain separate gates.

### Rebuild only from object storage

Object storage is authoritative only through `O`. During `O < C`, it cannot
reconstruct the retained suffix without a tLog quorum.

## Unresolved questions

1. Which authority allocates repair identities and storage incarnations?
2. How are large repair snapshots chunked and resumed without digest ambiguity?
3. How much capacity is reserved for repair traffic during object-store stalls?
4. How should snapshot chunks and live tail appends share learner bandwidth?
5. How does the next policy epoch prevent the failed process from rejoining?

The protocol does not attempt to prove that the failed process is dead. A dead
process and a partitioned process are indistinguishable. Safety instead comes
from distinct learner identity, storage incarnation, and the later replicated
policy transition that removes the failed member before promoting the learner.
The bounded contract assumes a crash-fault model where a stable root and its
private key are not cloned. Preventing two live copies of the same private key
requires an external machine-identity or lease authority and is not claimed by
this RFC.

## Tradeoff

This contract optimizes for repair correctness and keeps unverified state out
of quorum decisions. It gives up immediate redundancy restoration and performs
one full retained-suffix transfer before promotion.
