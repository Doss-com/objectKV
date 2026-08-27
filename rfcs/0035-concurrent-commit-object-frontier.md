# RFC-0035: Concurrent commit-proxy and object-frontier composition

- Status: `[CODE-COMPLETE]`, local release receipts `[EVALUATING]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 commit proxy, conflicts, objectification, and recovery suffix

## Decision to test

Compose RFC-0033's bounded independent-request commit proxy with RFC-0030's
authenticated object frontier. Freeze object coverage at one exact version
`O`, continue admitting transactions while the object-frontier handshake runs,
and prove that object state through `O` plus the retained suffix `(O, C]`
reconstructs the final authority state.

```text
prefix transactions
        ↓
immutable closure through frozen O
        ↓ prepare pending(O)
        ├──────────────────────────────┐
        ↓                              ↓
validate → pop through O → activate   independent requests → 32-item batches
        │                              │
        └──────────────┬───────────────┘
                       ↓
          ObjectState(O) + txLog(O, C]
                       ↓
               exact Database(C)
```

The foreground commit path performs no object request. Objectification may
compete for CPU, network, and stable-media capacity, but it does not become part
of transaction acknowledgement.

## Why this gate is next

G4.10a.1 proves a useful local batch shape only while the object frontier is
idle and writes do not conflict. G4.7 proves physical recovery-stream pop only
after a small sequential setup workload. Neither receipt proves the load-
bearing equation while writes continue:

```text
Database(C) = ObjectState(O) + txLog(O, C]
```

This experiment joins those two implemented mechanisms before adding range
groups, PostgreSQL, or independent-machine deployment.

## Frozen G4.10b profile

```text
prefix transactions through O:         512
concurrent suffix attempts:           1,024
value bytes:                             128
concurrent clients:                       64
maximum batch items:                      32
maximum application entry bytes:     262,144
maximum batch delay:                       2 ms
admission queue capacity:              2,048
candidate declared conflict share:        25%
high-conflict control share:               75%
authority processes:                  3 data + 3 publication
seeds:                         5101, 5102, 5103
wall budget per subject:                 180 s
```

The candidate's conflict share is generated deterministically. Unique-key
requests do not conflict. Conflict-designated requests reuse a bounded hot-key
set at a read version frozen before the suffix, so only the first post-snapshot
writer for each hot key may commit. Admission order may vary, but the accepted
history must match an independent ordered conflict oracle exactly.

## Execution protocol

1. Start three publication-authority and three data-authority processes.
2. Commit the 512-transaction prefix through the retained 32-item batcher.
3. Materialize and publish one exact immutable row closure through frozen `O`.
4. Prepare the pending frontier and hold it as a garbage-collection root.
5. Release the suffix request tasks and object-frontier controller from one
   barrier.
6. Validate the closure, quorum-commit physical pop through `O`, collect the
   data-voter certificate, and activate the pending frontier.
7. Freeze final `C`, page the retained suffix, and reconstruct exact state from
   the object closure plus `(O, C]`.
8. Retry committed and conflicted request identities, fail over both leaders,
   restart one data voter, and repeat reconstruction from a fresh controller.

The run records how many suffix requests resolve before frontier activation and
requires at least one resolution during the frontier protocol. A barrier alone
is not evidence of overlap.

## Subjects and controls

### Candidate

Twenty-five percent of suffix attempts target the deterministic hot-key set.
The candidate retains 32-item batching, concurrent objectification, and exact
recovery.

### No-conflict control

Every suffix request targets a unique key. This isolates objectification
contention from resolver work.

### High-conflict control

Seventy-five percent of attempts target the hot-key set. This is an explanatory
curve, not a throughput champion. It must remain correct and bounded.

### Same-durability one-entry control

The candidate history and object-frontier protocol run with one transaction per
Raft application entry and the same synchronized journals, process topology,
callers, and conflict generator.

### Moving-frontier poison

The controller incorrectly substitutes latest `C` for frozen `O` after suffix
commits start. Complete closure validation or authority coverage checks must
reject the transition before unsafe pop.

### Premature-pop poison

The controller skips pending publication protection. The data authority must
retain every prefix recovery record and leave the floor unchanged.

## Frozen gates

Correctness gates:

1. all six real OpenRaft processes start and use synchronized stable journals;
2. every admitted suffix identity resolves exactly once as committed or
   conflicted;
3. accepted order and conflict outcomes match the independent oracle;
4. versionstamps are unique, ordered, and contiguous inside each batch;
5. foreground commits issue zero object-store operations;
6. pending protection exists before physical pop;
7. persisted recovery floor equals frozen `O`, never moving `C`;
8. every retained transaction after pop has versionstamp greater than `O`;
9. object state through `O` plus the retained suffix reconstructs exact state at
   final `C`;
10. exact retries, both leader failovers, one data-voter restart, and a fresh
    controller preserve outcomes and reconstruction;
11. moving-frontier and premature-pop poisons fail before unsafe mutation;
12. at least one suffix request resolves during the object-frontier protocol.

Performance gates for the 25 percent conflict candidate:

1. at least 700 resolved durable outcomes per second on every seed;
2. no more than 150 ms client-observed p99;
3. at least 16 resolved transactions per leader stable append;
4. at least 4x median throughput over the same-durability one-entry control;
5. zero backpressure rejections;
6. no-conflict control reaches at least 850 outcomes per second;
7. object-frontier activation completes within two seconds;
8. each subject stays within its 180-second budget.

Conflict rejection is a correct durable outcome, not a committed mutation. The
runner reports attempted, admitted, committed, conflicted, retried, and
backpressured counts separately.

## Keep or discard

Keep the native transaction-authority composition only if every correctness
gate passes and the candidate clears both absolute and paired performance
gates. Then run the same exact revision on three independent stable-media hosts
with a remote object backend.

Discard or redesign if batch ordering changes conflict meaning, objectification
pops a required suffix outcome, the foreground commit path touches object
storage, conflicts collapse the same-durability advantage, or `C - O` cannot
converge under the frozen write rate.

## Tradeoff

This optimizes for proving the novel separation of fast quorum commits and
asynchronous permanent objects under actual concurrent work. It gives up a
smaller isolated benchmark and may expose contention that requires explicit
publication ratekeeping before any production claim.

## Not claimed

- adaptive batching or fairness across tenants;
- multiple commit proxies, resolver partitions, or range groups;
- independent-machine durability or cloud latency;
- OpenRaft internal-log compaction;
- an admitted PostgreSQL, Redis, search, or HTAP consumer;
- a production cell SLO.
