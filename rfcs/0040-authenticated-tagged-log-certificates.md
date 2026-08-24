# RFC-0040: Authenticated tagged-log durability certificates

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0008, RFC-0011, RFC-0038, RFC-0039

## Decision

Every tagged-log durability proof accepted by the transaction authority must be
a quorum certificate over one canonical statement. Each tLog node signs only
after the exact envelope is locally durable. The transaction authority verifies
the statement against the staged transaction and a separately installed log-set
policy before recording the certificate. A commit proxy may transport the
certificate but cannot declare its signers, membership, or quorum.

## Context and invariant

RFC-0039 proves the ordering-to-durability-to-visibility state transition, but
its receipt contains an unauthenticated list of node identities. A client can
forge the current payload without contacting a tLog process.

The invariant is:

```text
Visible(tx)
  -> for every required log set L
       authority verifies quorum signatures from Policy(L, epoch)
       over the exact staged envelope, generation, version, and durable position
```

Transport identity, a proxy assertion, or a list of node IDs is not durable
evidence.

## Contract

### Authority-installed log-set policy

The replicated cell authority stores a monotonic policy per log set:

```text
CellLogSetPolicy {
  format_version
  cell_id
  generation
  policy_epoch
  log_set_id
  quorum_size
  members: node_id -> Ed25519 public key
}
```

Policy installation and rotation are control-plane transitions, not fields a
transaction or certificate may choose. A policy update is rejected while an
unresolved staged head references the old epoch.

### Canonical durability statement

One tLog attestation signs domain-separated canonical bytes for:

```text
CellTaggedLogStatement {
  format_version
  cell_id
  tenant_id
  generation
  transaction_identity
  commit_sequence
  log_set_id
  policy_epoch
  envelope_sha256
  durable_position
}
```

The certificate contains the statement and a set of
`{ signer_id, signature }` attestations. Signer identities must be distinct,
belong to the named policy, and meet that policy's quorum. Every signature must
verify over identical canonical bytes.

### tLog behavior

A tLog node returns an attestation only after its local synchronized append
completes. Reading an existing exact record may reproduce the same statement
and signature. A node rejects a statement whose cell, generation, log set,
policy epoch, envelope digest, or durable position differs from its local
record and active configuration.

### Transaction-authority behavior

`RecordLogCertificate` validates all of the following before durable state
changes:

1. the policy exists and matches the staged transaction's generation;
2. the statement names the exact staged transaction identity and version;
3. the envelope digest matches the immutable staged envelope;
4. the log set is required by that envelope;
5. the policy epoch and durable position are nonzero and exact;
6. all signer identities are distinct policy members;
7. every Ed25519 signature verifies; and
8. the distinct valid signer count reaches the configured quorum.

An exact certificate retry is idempotent. A different certificate for the same
log set and transaction is rejected as conflicting. `Publish` retains the
RFC-0039 rule that every required log set must have one verified certificate.

## Failure model

- commit proxy death before or after collecting one attestation;
- tLog death after stable append but before returning its attestation;
- response replay, duplication, omission, and reordering;
- forged node IDs, signatures, membership, or quorum size;
- a statement changed after signature collection;
- a valid signer from another log set or policy epoch;
- old-generation certificate replay after a generation change; and
- authority leader death before or after certificate recording.

Key theft, online signer compromise, policy rotation during staged-head
recovery, and cross-host custody are not admitted by the first local process
gate.

## Alternatives

### Authority queries every tLog directly

This removes certificate transport from the proxy but puts external network I/O
inside or around a deterministic replicated state transition. It also couples
transaction-authority availability to every tLog endpoint. Keep it as a
diagnostic and recovery path, not the ordinary proof.

### Authenticate only the RPC connection

Mutual TLS can identify the current peer but does not create replayable durable
evidence for authority recovery. It remains transport hardening, not the commit
certificate.

### Aggregate signatures

BLS or threshold signatures reduce certificate bytes but add new cryptographic
and key-management complexity before certificate size is measured as a
bottleneck. Use distinct Ed25519 attestations first.

## Eval evidence

The `cell-tagged-log-certificate-v0` contract was frozen before implementation.
It reuses seeds
`1103`, `2207`, and `3301`, two three-process log sets, quorum two, the exact
RFC-0039 staged transaction, two proxy deaths, and fresh-worker recovery.

The correct subject must install policies independently of the transaction,
collect process-generated attestations after stable appends, record both
certificates, publish exactly once, replay the retained outcome, and recover
the exact visible state. Every hard gate must pass with zero correctness
anomalies.

Negative subjects must independently attempt:

- the old unsigned node-list receipt;
- one valid signature duplicated to claim quorum;
- a valid signature from the wrong log set;
- a statement whose envelope digest or generation changes after signing; and
- a certificate from an obsolete policy epoch.

Every negative subject must discard, replay exactly, and emit OTel traces,
metrics, and structured logs under the frozen candidate, suite, profile, and
run identities.

Candidate `6a81821` kept run `f5e3720a` with zero anomalies at the exact
96-event budget. Across three seeds, 21 authority processes, 18 tLog processes,
nine proxy processes, and three fresh workers participated. The tLogs completed
18 synchronized appends and produced 45 process attestations. Six proxy deaths
left visible frontier `10`; the verified certificates then allowed exactly one
publication per seed at `T=11`. Every retry returned the retained outcome and
every fresh worker reconstructed exact state.

Five exact-replay controls each produced 51 anomalies and discarded:

- unsigned node list: `f4425295`;
- duplicate attestation: `83fbcf79`;
- wrong log-set attestation: `26433766`;
- tampered statement: `1235b238`; and
- obsolete policy epoch: `52044094`.

OTel exported availability `1` and correctness `0` for the admitted subject,
then availability `0` and correctness `51` for every control. The frozen suite
hash is `ffbd31cb`; the profile hash is `30ae0d7c`.

Installing a log-set policy consumes an authority Raft position but must not
consume a database commit version. The admitted implementation therefore makes
the staged commit sequence a logical transaction sequence while retaining the
actual Raft log position in the immutable commit envelope. This separation is
part of the accepted contract.

## Compatibility and migration

The certificate uses a new wire magic and format version. The existing
unauthenticated receipt remains decodable only for historical evidence and is
rejected by an authority configured to require certificates. No mixed mode may
publish a new transaction. Rollback is safe only before the first
certificate-required transaction is staged.

## Unresolved questions

1. Which external control authority installs and rotates log-set membership?
2. How are signer keys provisioned, rotated, and bound to process incarnation?
3. What certificate proves a log generation is fenced before an incomplete
   staged head may be aborted during transaction-system takeover?
4. Does the production protocol retain individual signatures or introduce an
   aggregate only after measured certificate overhead requires it?
