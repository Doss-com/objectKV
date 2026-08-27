# RFC-0033: Commit-proxy batch closure and admission

- Status: `[CODE-COMPLETE]`, G4.10a.1 local receipts `[EVALUATING]`
- Authors: DOSS
- Created: 2026-08-26
- Scope: Cell v0 commit-proxy queue, batch closure, byte bounds, and overload

## Decision

Place one bounded FIFO batcher in front of RFC-0032's transaction-batch entry.
Clients continue to submit independent transaction requests and receive one
independent result. The commit proxy may combine accepted requests into one
quorum-durable entry, but the batch is not exposed as the normal client API.

```text
independent client transactions
              |
              v
bounded FIFO admission queue
              |
              v
close on item count, encoded bytes, or maximum delay
              |
              v
one RFC-0032 transaction-batch entry
              |
              v
ordered result demultiplexing by request identity
```

G4.9 proved that an already-formed 16-item entry can amortize the leader's
stable append. It did not prove a usable commit-proxy policy. G4.10a owns that
missing boundary.

## Client contract

The batcher accepts one `TransactionBatchItem` and returns that item's
`TransactionApplyResponse`. It preserves the existing request identity,
fingerprint, generation credential, conflict result, versionstamp, and exact
retry path.

Concurrent callers have no order before admission. Once the queue accepts a
request, FIFO admission order defines its order relative to other accepted
requests at this commit proxy. The RFC does not claim ordering across multiple
future commit proxies.

If the queue is full, the request fails before replication with an explicit
backpressure result. The client may retry the same identity. The batcher cannot
silently wait behind an unbounded queue.

## Closure policy

The first G4.10a experiment had four frozen bounds:

```text
maximum items per entry:       16, protocol ceiling 32
maximum encoded entry bytes:   256 KiB
maximum delay after first item: 2 ms
admission queue capacity:      2,048 requests in the saturated profile
```

The deadline starts when the first request enters an empty batch. A batch
closes when the first of these conditions occurs:

1. adding another item would exceed the item bound;
2. adding another item would exceed the encoded RFC-0032 entry-byte bound;
3. the maximum delay expires;
4. every sender closes.

An individual request whose one-item encoding exceeds the byte bound is
rejected before admission. A request that would overflow the current batch is
carried to the next batch without reordering.

The byte bound covers the exact versioned application bytes submitted to Raft.
G4.10a initially exposed `OKVB1` integer-array amplification, so all retained
candidate writes use the backward-readable `OKVB2` encoding from RFC-0034.
Transport framing and consensus metadata are measured separately.

## Result demultiplexing

The authority returns results in encoded item order. Before completing client
requests, the commit proxy checks response count and identity at every offset.
An outer transport or consensus failure is returned to every item in that
attempt. Retrying each original identity is safe because the authority retains
the per-item outcome.

## G4.10a frozen experiment

The first experiment compares the independent-request batcher with the same
three-process, synchronized stable-journal, one-entry-per-transaction control.

Saturated candidate:

```text
transactions:                 1,024
concurrent clients:              64
live keys:                    1,024
value bytes:                    128
maximum items:                   16
maximum entry bytes:        262,144
maximum delay:                    2 ms
queue capacity:               2,048
seeds:                  5001, 5002, 5003
```

Additional subjects isolate the policy:

- a 32-caller admission-knee control measures the point before queueing breaks
  the 100 ms latency ceiling;
- sparse arrivals must close on delay without hanging or exceeding the latency
  ceiling;
- large values must close on bytes and never exceed the encoded-entry limit;
- a deliberately undersized queue must return explicit backpressure while
  every admitted identity resolves exactly once;
- one oversized transaction must fail before admission and mutation.

## Frozen gates

Correctness gates:

1. every admitted identity has exactly one authority outcome;
2. every result identity matches the request at the same accepted FIFO offset;
3. versionstamps are unique and ordered after sorting by authority order;
4. every shared commit version has contiguous batch orders from zero;
5. retained-stream replay exactly reconstructs the authority's serving state;
6. individual retry returns the retained outcome without another mutation;
7. leader failover and killed-voter restart retain the exact state;
8. observed application-entry bytes never exceed the frozen bound;
9. overload rejection happens before replication and admitted work completes;
10. the oversized-item poison is rejected before admission and mutation.

Saturated performance gates:

1. at least 500 durable transactions per second on every seed;
2. no more than 100 ms client-observed p99;
3. at least eight logical transactions per leader stable append;
4. at least 2.5x median throughput over the same-durability one-entry control;
5. zero backpressure rejections in the saturated candidate;
6. total subject time no greater than 180 seconds.

Sparse and bounded-overload results are hard gates, not throughput candidates.
No blended score is produced.

## Keep or discard

Keep the commit-proxy batcher only if the client API remains independent,
every correctness gate passes, and the saturated candidate clears both the
absolute and paired performance gates. Then G4.10b may test conflict curves
and concurrent object-frontier advancement through the same admission path.

Discard the batcher if useful fill requires a delay that breaks sparse latency,
if the byte cap can be crossed, if overload loses admitted identities, or if
the same-durability gain disappears once batching starts from independent
requests.

## Tradeoff

This optimizes for amortized quorum synchronization behind an ordinary
transaction API. It gives up zero queueing delay and introduces a bounded
commit-proxy availability dependency. A future multi-proxy design must solve
cross-proxy ordering and version assignment separately.

## Pre-suite diagnostic

Release traces before the frozen suite produced this local concurrency curve:

```text
callers   throughput   p99       mean batch
16        602.982/s    31.104 ms 16
32        602.238/s    66.605 ms 16
64        572.786/s   121.846 ms 16
```

All three traces had zero correctness anomalies. The 64-caller candidate misses
the frozen 100 ms p99 gate; 32 callers are the current local admission knee.
These are diagnostic traces, not the final G4.10a receipt.

The frozen G4.10a suite subsequently discarded the 16-item, 64-caller subject:
581.791 median transactions per second and zero anomalies passed, but 131.488
ms maximum p99 missed the 100 ms ceiling. The 32-caller control reached 595.440
transactions per second and 63.398 ms maximum p99.

G4.10a.1 therefore freezes a distinct 32-item candidate at the same 64 callers,
byte bound, delay, durability, and workload. Its gates are at least 900
transactions per second, at most 100 ms p99, at least 16 logical transactions
per leader append, and at least 4x the same-durability one-entry control. Seeds
5051, 5052, and 5053 are held out from the pre-suite traces.

## G4.10a.1 result

The 32-item local candidate is retained for G4.10b. Across seeds 5051, 5052,
and 5053 it reached 1,157.369 median durable transactions per second, 76.101 ms
maximum p99, and exactly 32 logical transactions per leader append. The same
executable and synchronized stable journals with one transaction per entry
reached 182.093 transactions per second, for a 6.356x paired gain. Every frozen
absolute and paired candidate gate passed.

The policy controls also passed their scoped gates:

- sparse traffic closed one-item batches on delay at 30.961 ms maximum p99;
- the 128 KiB byte cap closed at eight 8 KiB-value transactions and a 119,731
  byte maximum application entry;
- overload admitted and resolved 32 requests, rejected 480 before replication,
  and produced no accounting anomaly;
- one oversized request was rejected before admission or mutation;
- all admitted identities replayed exactly after failover and voter restart.

These receipts remain `[EVALUATING]`. The source tree was dirty, OTel was
disabled, and all three processes and stable journals shared one host and
filesystem. Thirty-two items is the retained local stress envelope, not a
public production constant. G4.10b must now measure conflicts and concurrent
object-frontier advancement through this exact admission path.

## Not claimed

- adaptive delay or workload learning;
- fairness across tenants or multiple commit proxies;
- partitioned resolvers or txLogs;
- concurrent object-frontier safety, which belongs to G4.10b;
- independent-machine performance or a production cell admission.
