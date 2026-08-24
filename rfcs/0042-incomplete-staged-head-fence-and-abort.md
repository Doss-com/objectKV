# RFC-0042: Incomplete staged-head fence and abort

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0008, RFC-0009, RFC-0011, RFC-0039, RFC-0040, RFC-0041

## Decision

A successor transaction-system generation may abort one old-generation staged
head only after it verifies two independent facts:

1. every required old-generation tagged-log set has a quorum of members that
   durably fenced that generation under the same recovery identity; and
2. at least one required log set has a quorum of those fenced members attesting
   that the exact staged record was absent when each local fence became durable.

The first fact prevents a future old-generation append from becoming quorum
durable. The second proves that the staged record did not already have a durable
quorum in at least one required log set. Together they make the missing commit
obligation permanent rather than merely delayed.

Timeout, process unreachability, or a single absence observation is not an abort
proof.

## Why quorum absence is sufficient

For one three-member log set with write quorum two and fence quorum two, every
possible write quorum intersects every possible fence quorum. A member may sign
an absence observation only after it has durably fenced the old generation and
verified that the exact record is absent from its local durable log. It must
reject every later append from that generation.

If the record had already reached a write quorum, at least one member in any
absence quorum would have the record and could not sign absent. A valid absence
quorum and a valid prior write quorum therefore cannot both exist under the
declared failure model.

This argument depends on authenticated member identity, durable per-process
fences, quorum intersection, and non-equivocating signers. It does not hold for
timeout-based inference or volatile process state.

## Contract

### Tagged-log fence observation

Each tagged-log process accepts one idempotent fence request for an exact old
generation and recovery identity. The common statement binds:

```text
TaggedLogFenceStatement {
  format_version
  cell_id
  tenant_id
  generation
  recovery_id
  transaction_identity
  commit_sequence
  envelope_sha256
  log_set_id
  policy_epoch
}
```

After serializing the request against local appends, the process:

1. determines whether the exact envelope is present in its durable log;
2. persists and synchronizes the generation fence;
3. signs the common statement plus its `record_present` observation; and
4. rejects every later append whose envelope names the fenced generation.

The process returns the same signed observation on exact retry. It rejects a
different recovery identity or staged envelope for the already fenced
generation.

### Fence certificate

One `TaggedLogFenceCertificate` contains the common statement and distinct
policy-member attestations. Every attestation signs its own `record_present`
bit. The transaction authority verifies member identity, signature, policy
epoch, generation, recovery identity, exact transaction, commit sequence, and
envelope digest.

For every required log set, at least the configured fence quorum must attest.
For at least one required set that lacks a recorded durability certificate, at
least the configured write quorum must attest `record_present = false`.

### Replicated abort action

The staged transaction protocol adds one action:

```text
TakeoverAbort {
  previous_generation
  recovery_id
  expected_commit_sequence
  expected_envelope_sha256
  log_set_fences[]
}
```

The command uses the active successor `GenerationCredential`. The deterministic
state machine verifies:

- the successor generation is active for the same completed recovery identity;
- the domain is still owned by `previous_generation`;
- exactly one unresolved staged transaction exists and is the named head;
- its immutable old-generation envelope matches the expected sequence and
  digest;
- it lacks at least one required durability certificate;
- every required log set has one valid quorum fence certificate; and
- at least one missing-certificate log set has a valid quorum absence proof.

On success, the state machine marks the staged outcome terminally aborted,
retains that outcome for exact retry, and changes the domain generation to the
successor atomically. It does not apply mutations, advance the visible frontier,
or add the aborted envelope to committed history.

### Version and chain rule

The aborted sequence remains consumed. If transaction 11 is aborted, the next
successor transaction is transaction 12. Its envelope chains from the last
committed envelope at transaction 10, not from the aborted envelope. A retry of
the old transaction returns the retained aborted outcome and cannot restage or
publish transaction 11.

This deliberately permits gaps in the visible logical version sequence. Version
ordering remains monotonic; contiguity is not a user-visible invariant.

### Recovery order

The bounded gate uses this order:

1. stage transaction 11 and make it durable on two members of log set 10 but
   only one member of log set 20;
2. reserve the successor recovery identity;
3. durably fence every old-generation tagged-log set and collect signed local
   presence observations;
4. fence the old transaction data log after the staged head;
5. recover and activate the successor voter set under the same recovery;
6. replicate `TakeoverAbort` through the active successor; and
7. commit successor transaction 12.

## Negative subjects

The frozen gate must independently attempt:

1. abort while the successor generation is still recovering;
2. abort with only one absence signer in the incomplete log set;
3. abort without a fence certificate for every required log set;
4. forge an absent observation from a process that durably holds the record;
5. acknowledge a volatile fence, restart that process, and accept a late
   old-generation append; and
6. reuse aborted sequence 11 or chain transaction 12 from the aborted envelope.

Every subject must replay exactly, produce a correctness anomaly, export OTel,
and discard. The correct subject must restart a fenced tagged-log process,
reject a late old-generation append on the restarted quorum, retain the abort
outcome across a lost reply, leave rows and visible frontier at transaction 10,
and commit successor transaction 12 from the last committed chain.

## Eval plan

Freeze `cell-incomplete-staged-head-abort-v0` before implementation. Reuse seeds
`1103`, `2207`, and `3301` with three external generation-authority processes,
three old transaction voters, three successor voters, and two three-process
authenticated tagged-log sets. Every process owns a private synchronized root.

The primary metric is correctness anomalies. The event budget is 132 checks
across the three seeds. The process receipt must separately count tagged-log
appends, fence attestations, absence attestations, process restarts, rejected
late appends, abort attempts, abort commits, abort retries, and successor
commits.

## Eval evidence

Candidate `341beb9` kept run `338ef8b4` at the exact 132-event budget with
zero anomalies and exact replay across seeds `1103`, `2207`, and `3301`. The
correct path started 45 external processes, collected 18 fence attestations
and 9 absence attestations, restarted three fenced tLog processes, rejected
six late old-generation appends, committed three aborts, replayed three lost
abort replies, and committed successor transaction 12 in every seed.

Six controls replayed exactly and discarded:

| Subject | Run | Anomalies |
|---|---|---:|
| abort before successor activation | `6a9f4002` | 3 |
| one absence signer | `86eda531` | 9 |
| missing log-set fence | `6b7f30a8` | 6 |
| forged absence over a present record | `af6cc5a5` | 12 |
| volatile fence after process restart | `10988118` | 6 |
| reused aborted sequence or chain | `125b71cc` | 6 |

OTel exported availability `1` and correctness anomalies `0` for the admitted
path. Every control exported availability `0` and its nonzero anomaly count.
The frozen suite hash is `1db99836`; the profile hash is `e528f8cc`.

## Deliberately unresolved

1. How does a multi-record staged prefix classify its longest certified prefix
   and first provably incomplete transaction?
2. When may fenced old-generation tLog data and signing policy be garbage
   collected?
3. How are signer keys held, rotated, and revoked without losing old proof
   verifiability?
4. What staged-byte and age ceilings trigger generation recovery before the
   prefix becomes operationally unbounded?
5. How do partitioned commit proxies and moving log sets agree on the exact
   fence-policy epoch?
6. Which independently authorized recovery capability may request a durable
   tLog generation fence without turning this safety mechanism into a denial
   of service primitive?

## Tradeoff

This contract optimizes for one irreversible safety proof before liveness. It
gives up timeout-based failover and consumes an aborted logical version. A cell
can make progress only when it can gather a fence quorum from every required log
set and an absence quorum from at least one incomplete set.
