# RFC-0039: Commit visibility after tagged-log durability

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0008, RFC-0011, RFC-0038

## Decision

A transaction may be ordered and resolved before every required tagged log set
is durable, but it is not yet client-visible or acknowledged. The Cell v0
transaction path must represent two distinct states:

```text
ordered and resolved
  -> exact CommitEnvelope staged under request identity
  -> every required tagged log set reaches its declared quorum
  -> durable receipts validated
  -> commit version becomes visible
  -> client acknowledgement may return
```

The transaction authority must not advance the readable commit frontier, apply
user rows to a visible snapshot, or return `committed` before the complete log
receipt set exists. A proxy or authority restart may resume the same staged
request identity. It cannot allocate a second version or publish the outcome
twice.

## Why this gate is next

RFC-0038 proves that a dedicated tagged log can serve one exact suffix after a
process failure. Its controller appends after the transaction authority has
already committed and exposed the row state. That is a valid serving-boundary
probe but not a valid final commit protocol.

The next proof must close this ordering gap before work expands into
multi-record streaming, log partitioning, or throughput tuning.

## Frozen history

1. One transaction touches tags `10` and `20`, which map to two independently
   replicated log sets. Each set has three processes and quorum two.
2. The transaction authority orders and resolves the request, fixes one version
   and one exact envelope, and records a staged outcome under the request
   identity. The readable frontier remains unchanged.
3. Log set `10` reaches quorum. Kill the active proxy before log set `20`
   reaches quorum. No client acknowledgement or visible row change is allowed.
4. A replacement proxy retries the same request identity, recovers the staged
   envelope and existing set-`10` receipt, and durably appends the exact bytes to
   set `20`.
5. Kill the replacement after both log sets are durable but before visibility
   publication. A second retry validates both receipt sets and publishes the
   staged outcome once.
6. A fresh serving worker reconstructs the exact visible version from the two
   tagged log sets. Repeating the request identity returns the same outcome and
   creates no new log record.

The negative control publishes and acknowledges after only set `10` is durable.
It must fail the visibility, acknowledged-durability, and recovery witnesses.

## Required state

The staged transaction record must bind:

- cell, tenant, generation, request identity, and exact commit version;
- the complete encoded `CommitEnvelope` and its digest;
- required resolver set and required tag-to-log-set mapping;
- durable receipts received from each required log set;
- visibility state and final retained client outcome;
- the recovery rule for a proxy or authority generation change.

A receipt must bind log-set identity, generation, envelope digest, durable
position, quorum membership, and receipt format version. A self-declared count
is not sufficient evidence.

## Hard gates

- the staged envelope and version remain stable across every retry;
- one durable log set cannot advance the readable or acknowledged frontier;
- every required log set contains the same exact envelope bytes;
- acknowledgement waits for a quorum receipt from every required log set;
- proxy death before the final log set produces no visible partial commit;
- proxy death after log durability but before visibility is recoverable;
- final visibility occurs once and only once;
- repeated client identity returns the retained outcome without another append;
- a fresh worker reconstructs the visible version from tagged logs;
- the acknowledge-after-one-set control is detected and discarded;
- two fresh executions produce the same canonical report and OTel evidence.

## Interpretation

A pass admits the Cell v0 atomic boundary among ordering, conflict resolution,
tagged-log durability, visibility, and client acknowledgement. It does not
admit partitioned resolvers, dynamic tag placement, log-set repair, generation
takeover during staging, multi-record lag curves, or production throughput.

The durable receipt in this bounded proof is derived from exact responses from
separate log-set processes and binds their configured identities. It is not yet
an authenticated production certificate. A malicious client could forge the
current receipt payload, so signed log-set receipts or authority-side receipt
verification remain a hard follow-on gate.

## Evidence

Candidate `c549587` kept run `5a2e5a7f-50c5-4fca-8ae8-141870df6039`
across seeds `1103`, `2207`, and `3301`. It passed 84 of 84 checks. Nine commit
proxy processes staged one stable envelope per seed. Six proxy deaths left the
visible frontier at `10`. Eighteen tagged-log processes retained eighteen exact
records across two required log sets. The third proxy published and
acknowledged once at `T=11`, a retry returned `already_committed` without
another append, and three fresh workers reconstructed the exact visible rows.

Premature-acknowledgement control
`0da1a0c1-d241-4981-b074-46c563e9dca1` acknowledged after only log set `10`,
left all three nodes in set `20` empty, remained at visible frontier `10`,
produced 51 anomalies, and discarded. Both subjects replayed exactly. OTel
exported availability `1` and `0` for exact candidate `c549587` under suite
hash `22fb3497`.

## Tradeoff

This optimizes for preventing acknowledged or visible commits whose required
serving log is incomplete. It introduces a recoverable staged state and another
publication boundary in the centralized Cell v0 path.

The alternative is to keep the transaction authority's OpenRaft log as the
only Cell v0 durable log and defer independent tLogs until their acknowledgement
can replace that role atomically. That is simpler, but it postpones the central
scaling proof instead of resolving it.
