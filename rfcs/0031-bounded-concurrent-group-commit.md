# RFC-0031: Bounded concurrent group commit

- Status: `[CODE-COMPLETE]`, discarded as the final group-commit mechanism by
  the `[EVALUATING]` G4.8 local receipt
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 transaction commit throughput

## Decision

Test the least invasive group-commit mechanism before introducing a new
multi-transaction command format. A commit proxy admits a bounded window of
independent transaction requests and submits them concurrently to OpenRaft.
Each transaction remains one Raft application entry with its own request
identity, log index, commit version, conflict result, and retry outcome.

The existing `OpenRaftLogStore::append` and `NodeJournal::append` boundaries
already accept consecutive entry batches. One adapter append writes every
frame in that batch and calls `sync_all` once. G4.8 measures whether concurrent
submission causes OpenRaft to use that seam in practice.

```text
independent client requests
      |  bounded window, maximum 32
      v
commit proxy submits concurrently
      |
      v
OpenRaft orders distinct entries
      |
      v
NodeJournal append(entries[]) -> one sync_all
      |
      v
individual commit or conflict results
```

This is durability batching, not transaction batching. Transactions in one
storage append do not become one atomic application transaction.

## Why this precedes a batch command

A new batch command would require a second version-allocation rule, per-item
retry recovery inside one outer request, partial semantic rejection rules, and
a compatibility promise for a new wire format. Concurrent submission can
reuse the current strict-serializable state machine and durable retry contract.

If the current Raft implementation does not coalesce concurrent entries, or if
its resulting curve misses the frozen gate, the next experiment may introduce
an explicit commit-proxy batch entry. That later design must be justified by
this result rather than assumed.

## Commit semantics

For every request in the window:

1. request identity and transaction bytes remain independently fingerprinted;
2. consensus assigns one distinct Raft log index;
3. the transaction authority checks conflicts in Raft apply order;
4. a committed transaction uses its applied log index as its commit version;
5. a conflict or semantic rejection does not apply mutations;
6. an exact retry returns the original result and does not apply twice;
7. loss of the proxy or one voter cannot convert an acknowledgement into a
   non-quorum result.

Concurrent clients may receive responses in a different order from commit
versions. The commit version, not response arrival, defines the cell order.

## Bounded admission

The first proxy contract has one explicit `max_in_flight` setting. It does not
queue without limit. When the window is full, the caller waits for one result
before admitting another request.

Cell v0 freezes these bounds for G4.8:

```text
candidate max_in_flight: 32
control max_in_flight:    1
transactions per seed:   512
live keys:                256
value bytes:              128
seeds:                    4801, 4802, 4803
```

Production queue capacity, delay-based flush, adaptive windows, overload
rejection, fairness, and multi-proxy coordination remain later decisions.

## Instrumentation

Every voter exposes cumulative local stable-log observations through a
read-only diagnostic RPC:

```text
append calls
entries passed to append
append durable-I/O duration
committed-marker writes
committed-marker durable-I/O duration
vote writes and durable-I/O duration
physical journal bytes
```

The counters are diagnostic and do not participate in correctness or recovery.
One successful adapter append corresponds to one `NodeJournal` append batch and
one journal synchronization. The result reports entries per append and
transactions per durable append for each node.

## G4.8 candidate and controls

### Candidate

Submit at most 32 transactions concurrently through the normal quorum-durable
client path. Use a release build. After all acknowledgements:

1. read every retained transaction through the linearizable recovery API;
2. verify unique increasing commit versions and exact final values;
3. retry an accepted request exactly and require the same outcome;
4. kill the leader, elect a successor, restart the killed voter, and require
   exact replicated state;
5. collect stable-log observations from every reachable voter.

### Same-durability control

Run the identical workload, executable, processes, journal, object-free commit
path, recovery checks, and failure sequence with `max_in_flight = 1`.

### Early-ack poison

Enable the existing acknowledgement-before-quorum fault, isolate or kill the
accepting node before quorum apply, and require the oracle to detect that an
acknowledged request is absent from recovered quorum state. A high throughput
number cannot admit this subject.

## Frozen gates

Correctness gates:

1. all 512 requests have one durable final outcome;
2. every committed version is unique and strictly increasing in retained-stream
   order;
3. final values equal the deterministic oracle;
4. exact retry returns the original outcome;
5. leader failover and killed-voter restart preserve the same state digest;
6. the early-ack poison is rejected;
7. fresh-controller replay is exact across all seeds;
8. the executable is a release build.

Candidate performance gates:

1. at least 200 durable transactions per second on the frozen local profile;
2. at least four transactions per stable append on the median voter;
3. commit p99 no greater than 250 ms;
4. total runner time no greater than 120 seconds.

The candidate must also improve median throughput by at least 4x over the
same-durability control when the paired receipts are reviewed. The runner does
not weaken an individual hard gate to make the ratio pass.

These are local mechanism gates, not production SLOs. Independent-machine
latency, throughput, cost, and failure-domain claims require a later frozen
profile.

## Interpretation

Keep the mechanism if all correctness gates pass, the candidate clears its
absolute gates, the paired curve improves by at least 4x, and voter observations
show real multi-entry stable appends.

Discard it if concurrency only pipelines one-sync-per-entry work, if p99 grows
beyond the ceiling, or if recovery differs from the sequential control. A
discarded result opens an explicit commit-proxy batch-entry experiment.

## Tradeoff

This optimizes for throughput without changing transaction bytes, version
meaning, or retry semantics. It gives up deterministic per-request response
order and adds bounded client/proxy concurrency. It may expose OpenRaft
scheduling behavior as the next bottleneck rather than solving group commit.

## Not claimed

- a production commit proxy or backpressure controller;
- an explicit multi-transaction Raft command;
- range-partitioned logs or resolvers;
- multiple commit proxies;
- independent-machine or cloud performance;
- an admitted production latency or throughput SLO.

## G4.8 outcome

The candidate preserved the full correctness and recovery contract but reached
153.708 median transactions per second, 264.887 ms maximum p99, and a 3.964x
gain over the 38.772 transaction per second sequential control. It therefore
missed the frozen 200 transaction per second, 250 ms, and 4x paired gates.

Followers performed multi-entry stable appends, while the leader retained one
append per transaction. D39 discards this mechanism as the final answer and
opens an explicit commit-proxy batch-entry experiment. The implementation and
I/O observations remain useful controls.
