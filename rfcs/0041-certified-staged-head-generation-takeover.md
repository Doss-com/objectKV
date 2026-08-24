# RFC-0041: Certified staged-head generation takeover

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0008, RFC-0009, RFC-0011, RFC-0039, RFC-0040

## Decision

A successor transaction-system generation may publish one old-generation
staged head only when all of the following are true:

1. the old data-log generation is durably fenced after the staged transaction
   and both verified tagged-log certificates;
2. the successor voter set recovers through the certified membership position;
3. the external generation authority activates the successor with that recovery
   proof;
4. the takeover command is bound to the active successor credential and the
   same recovery identity;
5. the command names the exact old generation, transaction identity, commit
   sequence, and envelope digest; and
6. the staged head already contains one verified certificate for every required
   tagged-log set.

The takeover transition publishes the original immutable envelope exactly once,
applies its mutations, advances the visible frontier, and changes the domain's
active transaction generation atomically. It does not rewrite the envelope as a
successor-generation transaction.

This first contract admits exactly one unresolved staged transaction. It does
not admit aborting an incomplete head or recovering a multi-record staged
prefix.

## Why this is the next boundary

RFC-0040 proves that a proxy cannot forge tagged-log participation. It does not
prove liveness after every old-generation proxy dies before publication. The
existing generation-recovery protocol can fence an old data log and move voter
membership, but a normal successor write cannot safely skip an ordered staged
head.

Without an explicit takeover rule, either:

- the old head blocks the cell forever;
- a successor silently drops an acknowledged durability obligation;
- a successor rewrites the transaction into a second history; or
- old and new generations can both publish.

The contract must select one history before multi-record streaming or
partitioned log sets increase the recovery state space.

## Contract

### Replicated takeover action

The staged transaction protocol adds one action:

```text
TakeoverPublish {
  previous_generation
  recovery_id
  expected_commit_sequence
  expected_envelope_sha256
}
```

The command envelope carries the active successor `GenerationCredential`. The
existing generation fence rejects the action before activation and rejects an
old-generation `Publish` after the fence.

The deterministic state-machine transition verifies:

- the mirrored generation authority is active for the command credential;
- its generation is exactly the command generation and its recovery identity
  matches;
- `previous_generation + 1` is the successor generation;
- the domain is still owned by `previous_generation`;
- exactly one unresolved staged transaction exists and it is the named head;
- the old transaction and envelope still bind `previous_generation`;
- commit sequence and envelope digest match the takeover expectation;
- every required log set has one previously verified certificate; and
- no earlier unresolved transaction exists.

On success, the state machine applies the old envelope once, retains its exact
bytes in committed history, marks the staged outcome visible, advances the
frontier, and changes the domain generation to the successor. A retry returns
the retained committed outcome. A later successor-generation transaction must
observe the published head and continue from its frontier.

### Version rule

The recovered head retains its original generation and logical commit sequence
inside the immutable envelope. The domain then changes to the successor
generation. Later transactions bind the successor generation and a strictly
higher logical sequence. Control-plane Raft positions do not consume database
commit versions.

### Failure and retry rule

The transition is one replicated state-machine action. A lost reply or leader
death may require retry, but cannot apply the mutations twice or create a new
envelope. The successor may not accept ordinary writes until the certified head
is visible.

## Negative subjects

The frozen gate must independently attempt:

1. takeover while the successor generation is still recovering;
2. takeover when one required tagged-log certificate is absent;
3. takeover with a changed envelope digest or commit sequence;
4. an ordinary successor write that skips the staged head; and
5. replacement of the old envelope with a successor-generation rewrite.

Every subject must replay exactly, produce a correctness anomaly, export OTel,
and discard. The correct subject must also prove that an old-generation publish
is rejected after the fence, a lost takeover reply returns the retained result,
and a later successor transaction commits only after the head.

## Eval evidence

The `cell-staged-head-generation-takeover-v0` contract was frozen before
implementation. It reuses
seeds `1103`, `2207`, and `3301` with three external generation-authority
processes, three old-generation voters, three successor voters, one staged
transaction, two authenticated log-set certificates, voter-set handoff, leader
loss, and exact replay.

The tagged-log certificate state uses the RFC-0040 wire contract and pinned
signer keys. RFC-0040 separately proves process-local append-before-signature.
This gate tests whether the verified certificate state survives and controls a
real transaction-system generation handoff.

Candidate `f350a12` kept run `959a2211` with zero anomalies at the exact
105-event budget. Across three seeds, the reported path started nine external
generation-authority processes and eighteen data-voter processes, killed three
authority leaders, admitted nine learners, changed membership three times, and
collected nine fence plus nine recovery signers. Each seed replayed exactly.

The successor retained the completed recovery identity, rejected the old
generation's publish attempt, published the original transaction-11 envelope
once after activation, recovered a deliberately lost takeover reply, and then
committed successor transaction 12. OTel exported availability `1` and
correctness `0`.

Five exact-replay controls discarded:

- takeover during recovery, run `81bef774`, six anomalies;
- missing log certificate, run `e086ad66`, three anomalies;
- tampered envelope expectation, run `fd11f355`, three anomalies;
- skipped staged head, run `e6061870`, 30 anomalies; and
- successor-generation rewrite, run `59dffe26`, 27 anomalies.

Every control exported availability `0`. The frozen suite hash is `79cdd0c1`;
the profile hash is `567d8a98`.

## Deliberately unresolved

1. What proof safely aborts a staged head missing one required log-set
   certificate?
2. Does recovery publish a contiguous certified prefix in one action or one
   transaction at a time?
3. How are retained tLog generations fenced and garbage-collected after every
   staged transaction is resolved?
4. What backpressure ceiling prevents an operationally unbounded staged prefix?
5. How does the rule compose with partitioned commit proxies, resolver sets,
   and log-set movement?

## Tradeoff

This contract optimizes for one recoverable history and forward progress after
a fully durable head. It gives up automatic abort and broad pipeline recovery.
An incomplete head remains safely blocked until a separate fence-and-abort proof
is admitted.
