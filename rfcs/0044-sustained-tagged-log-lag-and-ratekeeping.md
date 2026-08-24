# RFC-0044: Sustained tagged-log lag and commit ratekeeping

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0007, RFC-0009, RFC-0011, RFC-0039, RFC-0040, RFC-0043

## Decision

`[DECIDED]` A cell must stop admitting new transactions before any required
tagged-log set loses quorum append capacity. Admission uses fresh,
policy-authenticated capacity attestations from a write quorum in every
required log set and the exact projected frame bytes for the next transaction.
The transaction identity, commit sequence, staged envelope, and any tLog append
must not exist before the ratekeeper grants capacity.

Objectification advances an authenticated cell watermark `O`. A tagged-log
process may pop records only through a watermark certified by the replicated
publication authority. Pop must be durable on a write quorum before that
capacity can authorize more commits. A stale capacity sample, unreplicated pop,
timeout, or best-node-only sample is not capacity.

## Context and invariant

RFC-0043 proves deterministic recovery of one bounded unresolved window. It
does not bound the committed suffix while object publication is delayed. An
object-store brownout can otherwise turn the fast durability tier into an
unbounded database copy or make the system fail only after partial tLog
append.

For latest visible commit `C`, object-durable watermark `O`, process retained
bytes `R`, soft admission limit `S`, and hard process limit `H`:

```text
O <= C
R <= H

admit(tx) only if, for every required log set:
    a fresh write quorum proves R + exact_frame_bytes(tx) <= S

pop_through(P) only if:
    P <= authenticated O
```

The soft limit preserves hard-limit headroom. The local process remains the
final safety boundary and rejects any append that would cross `H`, even if the
ratekeeper is wrong.

## Proposed contract

### Capacity attestation

Each tLog process signs one versioned statement after reading its synchronized
local state:

```text
TaggedLogCapacity {
  cell_id
  tenant_id
  generation
  log_set_id
  policy_epoch
  node_id
  process_incarnation
  last_position
  popped_through
  retained_bytes
  soft_limit_bytes
  hard_limit_bytes
  sample_epoch
}
```

The authority validates membership, signature, policy epoch, generation,
monotonic sample epoch, and exact log-set identity. Capacity for one set is the
largest projected frame admitted by at least its write quorum. The cell uses
the minimum capacity across every set required by the transaction.

An attestation is single-use for one replicated reservation epoch. A later
append, pop, restart, or policy change invalidates it. This bounded gate uses
one centralized ratekeeper but freezes the state needed for later proxy
partitioning.

### Pre-admission reservation

The commit proxy encodes the bounded transaction and computes its maximum exact
tLog frame bytes before allocating a commit sequence. One replicated
reservation binds transaction identity, mutation digest, required log sets,
projected bytes, capacity sample epochs, and a short retry result.

If capacity is unavailable, the authority returns `rate_limited` with the same
deterministic retry token. It does not allocate a sequence, stage an envelope,
append to any tLog, or advance visible state. Exact retry after capacity
returns commits the original transaction identity once at the next available
sequence.

### Authenticated pop

The publication authority certifies one exact object closure and watermark.
Every tLog process validates that certificate, synchronizes a local
`popped_through` marker, removes only complete records at or below the
watermark, and reports its new retained bytes and sample epoch.

Admission resumes only after a write quorum in every required set proves the
same or later durable pop. Records above the pop watermark remain byte-exact and
readable. Restart must preserve both the pop watermark and remaining suffix.

## Frozen scenario

The local gate uses one tenant, two required three-process tLog sets with write
quorum two, a soft retained-byte limit of 8 KiB, and a hard limit of 16 KiB.
The exact padded frame size is fixed before the run.

1. Start from visible and object-durable frontier `C=O=10`.
2. Freeze objectification and commit transactions 11 through 14. Every commit
   reaches quorum in both log sets and remains recoverable.
3. Attempt transaction 15 three times. The ratekeeper returns the same
   `rate_limited` outcome before sequence allocation or any tLog append. The
   visible frontier remains 14 and retained bytes stay below the hard limit.
4. Publish and certify object state through `O=12`.
5. Pop through 12 on a quorum in both log sets, restart one tLog process, and
   prove only transactions 13 and 14 remain.
6. Retry the original transaction identity. It commits exactly once at 15.
   Commit transaction 16 under the same bounded capacity policy.
7. A fresh worker reconstructs exact `Database(16)` from object state through
   12 plus the quorum-retained suffix `(12,16]`.

## Negative subjects

The frozen suite independently attempts to:

1. admit transaction 15 only after a partial tLog append reaches the hard
   limit;
2. derive capacity from the least-retained single process instead of a quorum;
3. reuse a stale capacity sample after intervening appends;
4. pop through 14 while authenticated object durability remains at 12;
5. resume after one process pops but before any required set has a pop quorum;
6. allocate a commit sequence and staged envelope before returning
   `rate_limited`.

Every subject must replay exactly, produce a correctness or bounded-availability
anomaly, export OTel, and discard.

## Eval plan

Freeze `cell-tagged-log-lag-ratekeeping-v0` before implementation. Reuse seeds
`1103`, `2207`, and `3301`. Each seed starts three transaction-authority
processes, three publication-authority processes, two three-process signed tLog
sets, and one fresh serving worker. Every process owns a private synchronized
root.

The primary metric is correctness anomalies. The event budget is 180 checks
across the three seeds. The receipt separately counts admitted commits,
rate-limited attempts, sequence allocations, staged records, tLog appends,
capacity attestations, retained-byte high watermarks, object publications, pop
attestations, process restarts, suffix records, recovery reads, and final
frontiers. Existing `wal.retained_bytes`, `objectification.lag`,
`availability.success_ratio`, and `operation.duration` measurements carry the
OTel curve.

## Evaluation evidence

Candidate `868c3de` kept OTel-enabled run `d510af28` at the exact 180-event
budget with zero anomalies and exact replay. Across three seeds, it started 21
transaction-authority processes, nine publication-authority processes, 18
tagged-log processes, and three fresh workers. It admitted 18 commits, denied
nine attempts before sequence allocation, collected 180 signed capacity
attestations, durably popped on both required log-set quorums, restarted one
tLog per seed, and reconstructed exact state through transaction 16 from the
object base at 12 plus suffix 13 through 16. Retained bytes reached 7,820 and
fell to 3,910 after pop without crossing the 16 KiB hard limit.

All six frozen subjects replayed exactly, exported OTel, and discarded:

| Subject | Run | Anomalies per seed |
|---|---|---:|
| ratekeep after partial append | `e8e5595e` | 4 |
| trust the best single node | `181de920` | 5 |
| reuse a stale capacity sample | `af7f4cc4` | 5 |
| pop beyond the object frontier | `a9b0376d` | 7 |
| resume without a pop quorum | `9668e699` | 5 |
| allocate before ratekeeping | `f7d0114c` | 3 |

This admits replicated pre-allocation capacity reservation, durable local pop
state, restart, and base-plus-suffix recovery. The publication authority now
signs one exact replicated root through a process quorum. Every tLog pins that
signer membership, verifies distinct signatures, hashes the referenced
manifest bytes, decodes the embedded cell snapshot, and requires its cell,
tenant, generation, and frontier to match before deletion. A missing quorum or
tampered capability is rejected at the tLog process boundary.

The admission remains bounded to deterministic evaluation keys, fixed
three-process authorities, two fixed three-process tLog sets, centralized
capacity reservation, one local host, and a 16 KiB hard limit. Production key
custody and rotation, failed-log repair, moving log sets, partitioned proxies,
independent failure domains, and public-cloud retention curves remain open.

## Alternatives

### Block only at the tLog hard limit

This keeps ratekeeping out of the authority but discovers exhaustion after a
sequence or partial append may already exist. It converts an expected object
stall into staged-prefix recovery pressure and makes admission depend on
which process receives the first append.

### Ratekeep from `C - O` versions only

Version lag is useful but does not bound bytes. One large transaction can
consume more retained capacity than many small transactions. The kernel uses
exact projected bytes and reports version lag as a secondary curve.

### Pop from object-store LIST or file presence

Object existence does not prove a complete publication root or authorized
watermark. Only the replicated publication certificate may advance pop.

## Unresolved questions

1. Which ratekeeper policy owns per-tenant fairness when one tenant fills a
   shared log set?
2. How much emergency headroom must remain for recovery and control records?
3. Should publication certificates authorize one global pop or independent
   range-tagged pops?
4. How do multiple commit proxies reserve capacity without serializing on one
   authority key?
5. Which retry delay curve avoids synchronized client retry storms?

## Tradeoff

This contract optimizes for bounded durability memory and predictable
degradation during object-store stalls. It gives up unlimited write
availability, adds a pre-admission capacity round, and blocks on the least
available required log set rather than risking partial durability.
