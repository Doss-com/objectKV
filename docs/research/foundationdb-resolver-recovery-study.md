# FoundationDB resolver recovery study

- Status: `[EXISTS]` primary-source review and bounded local falsifier
- Reviewed: 2026-08-23
- Question: Must objectKV persist and finalize every partitioned resolver
  decision, or can it use FoundationDB's stateless resolver and generation
  recovery pattern?

## Answer

`[DECIDED]` For the bounded local architecture, the intended objectKV
transaction system treats resolver
state as generation-scoped memory, not as a per-transaction durable journal.
A resolver or commit-proxy failure should stop the active transaction-system
generation. The recovery authority should fence the old generation, determine
the last durable committed version from transaction logs, and activate empty
successor resolvers with a read-version floor at or above that boundary.

RFC-0048 remains a useful safety oracle. Its durable signed prepare and
finalize protocol proves that ordered resolver partitioning can preserve the
centralized result. It is not evidence that those synchronous writes belong in
the production commit path.

## Primary evidence

The published FoundationDB architecture separates the in-memory transaction
system from the durable log system. It describes sequencers, proxies, and
resolvers as stateless processes, while LogServers retain the committed WAL.
It also states that a transaction-system or log-system role failure triggers
reconfiguration into a new epoch rather than local role repair.

Source: [FoundationDB SIGMOD 2021 paper, sections 2.3 and 2.4](https://www.foundationdb.org/files/fdb-paper.pdf).

The conflict algorithm gives every resolver the candidate commit version
before conflict checking. A transaction commits only if every range resolver
admits it. An aborted transaction may already have been admitted by a subset
of resolvers and may therefore create false conflicts until the short MVCC
window expires. The paper treats this as safe conservatism, not as a condition
requiring rollback or per-resolver finalization.

Source: [FoundationDB SIGMOD 2021 paper, section 2.4.2](https://www.foundationdb.org/files/fdb-paper.pdf).

The current FoundationDB recovery guide says a resolver failure triggers
cluster recovery. Recovery locks the old coordinated state and old tLogs,
computes `knownCommittedVersion` and `recoveryVersion` from durable tLog state,
recruits a new transaction system, and exposes it only after the successor
configuration is installed. Resolver recovery consists of receiving the prior
epoch boundary. It does not describe replaying a resolver journal.

Source: [FoundationDB recovery internals](https://github.com/apple/foundationdb/blob/main/design/recovery-internals.md).

The current write-path guide describes dynamic commit-proxy batching, one
range-clipped request per resolver, all-resolver agreement, and persistence in
the log system after resolution. The durability synchronization point is the
log push, not the resolver reply.

Source: [FoundationDB read and write path](https://apple.github.io/foundationdb/read-write-path.html).

## Consequences for objectKV

### D1. Remove resolver disk synchronization from the intended normal path

`[PROPOSED]` Resolver state is an acceleration structure over the recent
committed-write window. The authoritative durable facts are generation state,
the committed or recoverable tLog prefix, and transaction outcomes. The normal
path should not require one resolver filesystem synchronization for prepare
and another for finalize.

Optimizes for: batching, throughput, and a smaller durable protocol.

Gives up: restarting one resolver inside the same generation. Resolver loss
becomes a cell transaction-system recovery event.

### D2. Permit safe false positives from partial resolver admission

`[PROPOSED]` A resolver may retain an accepted transaction that another
resolver rejected. This can reject later non-conflicting work conservatively,
but cannot admit a transaction that should conflict. The entry expires with
the bounded read-version window or disappears at generation recovery.

Optimizes for: no distributed rollback or finalize round across resolvers.

Gives up: the exact centralized commit rate under cross-partition contention.
The eval must track false conflicts separately from safety anomalies.

### D3. Make the recovery floor the resolver state boundary

`[PROPOSED]` Successor generation read versions must not precede the recovered
old-generation commit boundary. Empty successor resolvers are safe only when
every newly admitted transaction starts at or after that floor. Old-generation
requests, replies, and read versions must fail closed.

Optimizes for: constant-work resolver recovery independent of database size.

Gives up: keeping old read transactions alive across transaction-system
recovery. Clients retry them in the successor generation.

### D4. Test batches before multiple proxies

`[PROPOSED]` One proxy should first process ordered transaction batches across
several resolvers. Multiple proxies add cross-proxy ordering, metadata
propagation, and known-committed-version coordination. Those concerns should
remain outside the first falsifier.

Optimizes for: isolating the resolver recovery thesis.

Gives up: a direct multi-proxy throughput claim from the next gate.

## Next falsifier

RFC-0049 freezes a bounded real-process evaluation with three range resolvers,
one replicated transaction authority, ordered batches, one resolver loss, and
a successor generation with empty resolver state. It requires the partitioned
commit set to be a subset of the centralized oracle, classifies conservative
false conflicts separately, and requires zero resolver durable synchronizations
and zero finalization RPCs. Tagged-log composition remains a later gate.

The unsafe subjects continue the generation after resolver loss, activate a
successor before fencing the old generation, accept an old-generation reply,
admit a read below the recovery floor, publish unresolved old-generation work,
or omit a durably committed old-generation head from recovery.

## Remaining unknowns

1. How does one objectKV commit proxy communicate its known committed version
   to every tLog without adding another serialized authority write?
2. What exact tLog inventory and fence certificate defines the recovery floor
   under partial replication?
3. What batch ordering contract allows several commit proxies to send work to
   each resolver without arrival-order disagreement?
4. How large can the resolver MVCC window become before memory, hot ranges, or
   conservative false conflicts dominate?
5. Can online resolver-map split and merge preserve order without forcing a
   full transaction-system generation change?

## Strategy implication

`[EXISTS]` RFC-0049 supports this correction. OTel run `e334c857` keeps 1,800
attempts with zero anomalies, exact replay, three resolver-loss recoveries,
three safe false conflicts, and zero resolver sync or finalization operations.
Six unsafe controls discard. The expensive part
of RFC-0048 is not required by the closest proven architecture. The cost moves
to fast whole-generation recovery and a rigorous tLog recovery boundary, both
of which are already central objectKV workstreams. This is a bounded
simplification, not a production proof. Multiple proxies, the composed tLog
fence, recovery-time curves, and online resolver-map movement remain open.
