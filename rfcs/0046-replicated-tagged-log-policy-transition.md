# RFC-0046: Replicated tagged-log policy transition

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0009, RFC-0023, RFC-0040, RFC-0044, RFC-0045

## Decision

`[DECIDED]` Move one tagged-log set through a replicated, three-phase policy
transition. The active transaction authority first prepares the exact change
from old policy `P` to successor policy `P+1` using the RFC-0045 learner
readiness certificate. A quorum of the successor policy then stages the exact
policy while the old policy remains active. The authority commits the successor
policy atomically, after which an authority quorum certifies the committed
transition and successor nodes activate it durably. The removed process cannot
participate in later capacity, durability, pop, or serving quorums.

The first bounded transition replaces failed node `1` with repair-ready node
`4` in log set `10`:

```text
old policy E1:  members {1,2,3}, quorum 2
next policy E2: members {2,3,4}, quorum 2
unchanged set:  log set 20 remains at E1
```

This is not a joint transaction quorum across both policies. Every transaction
certificate names exactly one active policy epoch per log set.

## Context and invariant

RFC-0045 proves that node `4` has the exact retained suffix and can be certified
ready without entering any active quorum. It does not restore redundancy. A
controller-selected policy update would let one process count an unready
learner, revive the failed identity, skip policy epochs, or combine signatures
that were never one valid quorum.

For active policy `P`, successor policy `N`, repair certificate `R`, successor
stage certificate `S`, and committed activation certificate `A`:

```text
prepare(P -> N) only if:
    N.epoch = P.epoch + 1
    N.members = P.members - failed + learner
    unchanged member keys are byte-exact
    learner key and incarnation match R
    R is valid under P and names the current retained frontier
    no unresolved staged transaction references P

commit(P -> N) only if:
    prepare is replicated and still current
    S has one distinct quorum from N over the same transition

activate(node, N) only if:
    A has one distinct transaction-authority quorum
    every authority signer observed N as committed

transaction certificate for log set 10 after activation:
    uses only N.epoch and members from N
```

Policy preparation temporarily rejects new transaction staging for the moving
log set. Reads and recovery remain available. This first contract accepts a
short write pause rather than carrying old-epoch transactions across the
activation boundary.

## Canonical statements

The authority stores:

```text
PendingTaggedLogPolicyTransition {
  format_version
  cell_id
  tenant_id
  generation
  transition_id
  log_set_id
  old_policy
  next_policy
  failed_node_id
  learner_node_id
  learner_incarnation
  repair_readiness_sha256
  retained_root_sha256
  retained_last_position
  state: prepared | successor_staged
}
```

Successor tLogs sign:

```text
TaggedLogPolicyStageStatement {
  format_version
  transition identity and policy digests
  repair_readiness_sha256
  retained_root_sha256
  retained_last_position
}
```

An unchanged survivor signs only after its local retained root and position
match. The learner additionally checks its storage incarnation, public key,
and durable RFC-0045 readiness receipt. Signatures are verified against the
successor member map, but staging does not make the successor policy active.

After the replicated commit, transaction-authority nodes sign:

```text
TaggedLogPolicyActivationStatement {
  format_version
  transition identity and policy digests
  authority_commit_position
  repair_readiness_sha256
  successor_stage_sha256
}
```

Each authority process signs only after its local applied state contains the
exact committed successor policy and completed transition identity. A tLog
activates only after verifying a distinct authority quorum against a pinned
authority signer policy. Activation writes one synchronized receipt before the
process may append, attest, report capacity, pop, or serve under the new epoch.

## State transition

1. Objectification remains at `O=10`; both log sets retain transactions 11
   through 14. Node `1` in log set `10` is unavailable.
2. Node `4` installs the RFC-0045 snapshot, restarts, and receives a readiness
   certificate from active nodes `2` and `3` at retained position `4`.
3. The transaction authority prepares transition `T` from log-set-10 policy E1
   to E2. Preparation validates readiness and freezes new staging for that set.
4. Nodes `2`, `3`, and `4` stage E2. A distinct E2 quorum certifies the same
   transition, retained root, and position.
5. The authority records the stage certificate and commits E2 in one replicated
   state transition. A lost response is resolved by exact transition identity.
6. An authority quorum signs the committed E2 activation statement. Nodes `2`,
   `3`, and `4` verify it, persist activation, and restart under E2. Node `4` is
   no longer a learner.
7. Stop node `2`. Nodes `3` and `4` append and attest transaction 17 for log set
   `10` under E2. Log set `20` remains under E1. The transaction authority
   accepts both exact per-set certificates and publishes transaction 17.
8. Restart removed node `1` from its old root. It remains on E1, cannot activate
   E2, and contributes no signature to transaction 17, capacity, pop, or a
   fresh serving worker. A worker uses nodes `3` and `4` for set `10` and
   reconstructs exact `Database(17)` from `O=10` plus retained logs.

Authority entries 15 and 16 prepare and commit the policy transition. The next
transaction is therefore version 17. Tagged-log positions remain 1 through 5;
they are not compared numerically with commit versions.

The cell generation does not change. The log-set policy epoch does.

## Negative subjects

The frozen suite independently attempts to:

1. prepare or commit E2 without the RFC-0045 readiness certificate;
2. finalize E2 while an old-epoch staged transaction remains unresolved;
3. skip from policy epoch 1 to 3 or change an unrelated member key;
4. count one old-policy signer and one successor-policy signer as a quorum;
5. activate successor nodes from a controller request without an authority
   quorum certificate;
6. let removed node `1` rejoin or attest after restart;
7. apply an exact lost-response retry as a second transition.

Every subject must replay exactly, produce a correctness, membership, or
durability anomaly, export OTel, and discard.

## Eval plan

Freeze `cell-tagged-log-policy-transition-v0` with seeds `1103`, `2207`, and
`3301`. Each seed starts three transaction-authority processes, two
three-process tLog sets, one repair learner, one fresh serving worker, and the
minimum restarts needed to prove durable activation and removed-node fencing.
The event budget is 300 checks.

The primary metric is correctness anomalies. The receipt counts authority and
tLog processes, repaired learners, repair and readiness attestations, successor
stage attestations, authority activation attestations, policy prepares and
commits, idempotent retries, old-epoch rejections, post-transition appends,
capacity members, serving members, and final frontiers. OTel carries
`recovery.membership_epoch`, `wal.retained_bytes`,
`availability.success_ratio`, `operation.duration`, and correctness anomalies
with exact candidate, suite, profile, run, workload, and backend labels.

## Evaluation evidence

Candidate `b69714c` kept OTel run `8b8d9705` at 90 of 300 allowed events with
zero anomalies and exact replay across seeds `1103`, `2207`, and `3301`. Each
seed failed old member `1`, repaired learner `4`, collected one old-policy
repair quorum and one readiness quorum, staged the successor policy on nodes
`2`, `3`, and `4`, committed it once through the three-process transaction
authority, collected one authority activation quorum, and persisted activation
before learner restart. After node `2` failed, nodes `3` and `4` certified
transaction `17` under E2 while log set `20` remained under E1. Fresh workers
counted only nodes `3` and `4` and reconstructed exact transaction `17`.

| Control | Run | Anomalies per seed |
|---|---|---:|
| missing repair readiness | `c45557f7` | 1 |
| unresolved old-policy stage | `aa1166c8` | 1 |
| skipped policy epoch | `6fad03b0` | 3 |
| mixed-policy quorum | `9363aad0` | 1 |
| missing authority activation quorum | `b89ce548` | 1 |
| removed member rejoins | `1fbd0e47` | 5 |
| transition applies twice | `d92439f4` | 1 |

All seven controls replayed exactly and discarded. The unresolved-stage control
fails closed before final transaction visibility. The frozen source suite hash
is `6bb75c49`; the evaluated suite hash is `8b287d4e`; the profile hash is
`c13048bc`. Prometheus observed availability `1`, correctness anomalies `0`,
and membership epoch `2` for the OTel-labeled correct run.

This gate admits one member replacement in one log set with a bounded write
pause. It does not admit concurrent live-tail catch-up, chunked or remote
repair, joint-policy writes, multi-member or zone replacement, production key
custody, removed-root destruction, independent hosts, or concurrent policy
movement.

## Alternatives

### Change the authority policy and restart every tLog from configuration

This is operationally simple but trusts the controller to deliver the committed
policy. A stale or compromised controller can activate different member maps on
different processes.

### Count an old quorum and a new quorum for one transaction

This keeps writes flowing during movement but creates an ambiguous receipt
space and makes retry behavior depend on transition timing. The first bounded
contract freezes new staging and uses one epoch per log set.

### Promote as soon as the learner has the bytes

Local byte equality does not prove authority approval, successor membership,
or durable activation. Repair readiness and policy activation remain separate
proofs.

### Allocate a new cell generation

That is safe but treats routine redundancy repair as transaction-system
recovery. It increases recovery scope and discards continuity that a surviving
quorum still provides.

## Unresolved questions

1. Can production movement avoid the write pause with a bounded joint-policy
   barrier without admitting mixed-epoch certificates?
2. Which authority owns policy-signing keys, and how are those keys rotated?
3. How is a removed root quarantined and eventually destroyed?
4. What policy limits concurrent log-set movement inside one cell?
5. How do zone evacuation and whole-log-set replacement compose with this
   one-member transition?
6. How does live-tail learner catch-up interact with the prepare barrier?

## Tradeoff

This contract optimizes for one unambiguous durable policy at every transaction
boundary and removes controller trust from tLog activation. It gives up write
availability during the bounded prepare-to-activate interval and requires two
quorum certificates in addition to the learner readiness proof.
