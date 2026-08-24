# RFC-0038: Range-tagged transaction-log serving path

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0007, RFC-0011, RFC-0037

## Decision

`[PROPOSED]` A dedicated transaction-log role retains exact committed
`CommitEnvelope` bytes plus their required range tags. The initial topology is
three independent processes with a two-process durable quorum. A serving worker
may reconstruct a range suffix only from matching checksum-valid records
returned by the required quorum for that range tag.

Each log process owns private synchronized storage and a hard retained-byte
limit. It rejects an append when the next durable frame would cross the limit.
Cell commit admission must eventually wait for every required tagged log set,
but this first gate isolates the transaction-log to serving-worker boundary.

## Question

Can three dedicated tLog processes durably retain one committed envelope with
range tags, lose any one process, and let a fresh serving worker reconstruct
exact `Database(T)` from object state through `O<T` plus a quorum-matched
range-tagged suffix, while a missing required tag and retained-byte overflow
fail closed?

## Frozen history

1. Run the admitted Cell v0 history to `C=10` and publish object state through
   `O=8`.
2. Fetch the committed envelope in `(8,10]` from the linearizable transaction
   authority.
3. Start three tLog processes, each with private storage and one fixed hard
   retained-byte limit.
4. Append the same envelope and its required tags to every tLog. Acknowledge
   the bridge only after two processes have synchronized matching bytes.
5. Send an over-limit probe and require every process to reject it without
   changing the retained prefix.
6. Kill one tLog process.
7. Start one serving worker with empty private state. It resolves the object
   base, requests tag `10` from the two survivors, requires two matching record
   digests, validates the envelope chain, and applies the suffix through `T`.

The negative control removes tag `10` from the retained record before append.
The worker must find no quorum suffix for its assigned range, stop at `O`,
return stale rows, and discard.

## Hard gates

- the source transaction history has zero anomalies;
- `0 < O < C=T`;
- three dedicated tLog processes start with distinct private roots;
- at least two processes synchronize the same tagged record before bridge
  acknowledgement;
- the retained record decodes to the exact committed envelope and carries all
  required log tags;
- every over-limit probe is rejected and leaves the retained prefix unchanged;
- one tLog process dies after durable append and before the worker read;
- the two survivors return matching records for assigned tag `10`;
- no transaction-authority feed is available to the worker;
- the base chain connects to the quorum-reconstructed suffix;
- the worker reaches `T` and reconstructs the transaction oracle exactly;
- two fresh executions produce the same canonical report;
- the missing-tag control exposes the incomplete range feed.

## Interpretation

A pass admits a bounded independent-process tLog to serving-worker path, one
range tag, one-record quorum reconstruction, hard retained-byte rejection, and
one log-process failure. It does not admit integration into transaction commit
acknowledgement, multi-record streaming, lag-based ratekeeping, log repair,
partitioned log sets, range movement, concurrent serving, independent hosts,
or production latency and throughput.

## Evidence

Candidate `beec908` kept run `851d0654-5d9a-4661-804f-1dc182f9e3be`
across seeds `1103`, `2207`, and `3301`. It passed 69 of 69 checks, started nine
tLog processes with distinct private roots, received nine durable append
acknowledgements, rejected nine retained-byte overflow probes, killed three
tLog processes, and reconstructed three exact suffixes from six survivor
responses. Every worker reached `T=10` from `O=8` with exact rows.

Missing-required-range-tag control
`136b2523-8912-4f99-a21d-abf8c49f335b` retained the same envelope without tag
`10`. Its workers contacted both survivors but reconstructed no suffix, stopped
at `8`, returned stale rows, produced 12 anomalies, and discarded. Both
subjects replayed exactly. OTel exported availability `1` for the admitted
subject and `0` for the control under suite hash `1afec3bd`.

## Tradeoff

This optimizes for falsifying tag and durability assumptions before building
log partitioning. It gives up claiming that a controller-mediated append is the
final commit proxy to tLog protocol.
