# RFC-0055: Resolver hotspot throughput curve

- Status: proposed, eval frozen before implementation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0008, RFC-0011, RFC-0048, RFC-0051, RFC-0052

## Decision under test

`[PROPOSED]` Retain online resolver-map splitting as the local hotspot mechanism
only if two long-lived child resolver processes produce a repeatable throughput
gain for a balanced, splittable hot range under the same machine, executable,
logical workload, client concurrency, and ordered transaction batches as the
one-source incumbent. Preserve arbitrary transactions across the split and
measure the loss of benefit as work becomes one-sided or crosses both children.

This is a resolver-service calibration, not a cell throughput claim. The timed
region excludes process startup, history installation, workload serialization,
warmup, outcome validation, commit-proxy work, sequencer work, tLog durability,
and object storage. Every excluded phase remains an untimed receipt. A later
end-to-end cell curve must include those shared roles.

## Context and invariant

RFC-0052 proves that one source range can move to two children without changing
the tenant transaction domain or copying durable database bytes. It does not
show that the extra processes relieve a hotspot. A correct split that leaves the
same hot key on one child cannot scale that key, and a transaction that crosses
the boundary still needs both decisions.

The invariant is:

```text
one logical workload digest
        |
        +-- map epoch 1: source owns the full range
        |
        `-- map epoch 2: left and right children own exact disjoint halves

source outcome(tx) = aggregate(child outcomes(tx)) = frozen oracle outcome(tx)
```

For a crossing transaction, aggregation includes both child decisions. Logical
throughput counts that transaction once even though it creates two resolver
decisions.

## Frozen bounded model

Use seeds `1103`, `2207`, and `3301`, seven paired repetitions per seed, one
source worker for `[0x50, 0xa0)`, and two children for `[0x50, 0x78)` and
`[0x78, 0xa0)`. All three are long-lived, memory-only OS processes from the same
binary. The controller requires at least two available logical CPUs and uses two
fixed dispatch threads.

Each sample has 8,192 logical transactions in 128 globally ordered batches of
64. Each splittable point installs 2,048 conflict-history entries per child and
their exact 4,096-entry union in the source. The missed-boundary point installs
all 4,096 entries in the left range and none in the right. A separate 512-
transaction warmup is executed and discarded before timing. Prepared workload
bytes and cloned initial history are acknowledged before the timer starts.

Topology order alternates by repetition. Both topologies therefore run from the
same immutable initial history without allowing one result to warm or mutate the
other.

The curve contains:

| Point | Left only | Right only | Crossing | Purpose |
|---|---:|---:|---:|---|
| balanced independent | 50% | 50% | 0% | best case for a correct split |
| missed hot-key boundary | 100% | 0% | 0% | unsplittable-key limit |
| crossing 25 | 37.5% | 37.5% | 25% | coordination sensitivity |
| crossing 100 | 0% | 0% | 100% | every transaction needs both children |

One percent of transactions have a deterministic conflict against installed
history. The remainder are deterministic accepts whose writes do not change a
later expected read. This makes outcomes independent of dispatch timing while
still requiring real conflict checks and exact global aggregation.

## Performance interpretation

For each point, record source and split operations per second, duration, logical
operations, resolver decisions, history entries examined, child load, and a
machine and executable fingerprint. Report median, median absolute deviation,
minimum, and maximum across 21 paired samples.

Define the conservative ratio:

```text
(split median - split MAD) / (source median + source MAD)
```

The balanced point is a positive directional signal only if this ratio is at
least `1.10`. A valid result below that threshold is evidence against prioritizing
more resolver-split machinery. The other three points describe the semantic and
operational envelope. They do not receive a required speedup.

## Negative subjects

The frozen suite independently attempts to:

1. route a crossing transaction to only one child;
2. change the logical workload between the source and split measurements;
3. report throughput without validating every resolver outcome;
4. include worker startup and history installation in only one timed topology;
5. serialize the two child executions while claiming parallel split service.

Every subject must export the same telemetry surface, violate its owning hard
gate, and discard.

## Eval plan

Freeze `cell-resolver-hotspot-throughput-curve-v0`. The primary metric is
the paired `resolver.throughput_ratio`, with raw `operation.throughput`,
`operation.duration`,
`serializability.constraints_checked`, and `range.hotspot_ratio` as secondary
evidence. A comparable sample requires the same suite hash, profile hash,
candidate commit, executable digest, machine fingerprint, backend, and paired
topology order.

Passing semantic and benchmark-integrity gates require:

- one exact logical workload digest for source and split;
- source and aggregated split outcomes equal the frozen oracle;
- every crossing transaction reaches both children;
- each transaction uses one map epoch in one topology;
- source history equals the exact union of child histories;
- process startup, history preparation, and warmup are outside timing;
- every expected outcome is validated after timing;
- split child executions overlap when both own work;
- operation count, batch order, concurrency, executable, and machine identity
  remain fixed within the paired sample;
- seven samples per seed and alternating topology order are complete;
- exact untimed receipts replay for duplicate executions.

## Failure model

- a split boundary that misses the hot key;
- one child receiving most or all non-crossing work;
- transactions touching both children;
- one child omitted from a crossing decision;
- different source and child workload bytes;
- benchmark setup contaminating the timed region;
- children executed sequentially;
- local scheduling, thermal, and TCP timing noise.

Independent hosts, network partitions, resolver loss, split-controller loss,
several simultaneous splits, production conflict indexes, proxy batching, tLog
durability, and end-to-end commit throughput are outside this calibration.

## Alternatives

### Infer throughput from the semantic split fixture

RFC-0052 starts one-shot workers and measures the complete correctness history.
Process construction and unrelated roles dominate that duration, so it cannot
answer whether two active child resolvers relieve the hot range.

### Benchmark only the balanced best case

That would hide the load-shape constraint. A range split does not split one hot
key, and cross-boundary work amplifies resolver decisions. The missed-boundary
and crossing points are part of the architecture decision.

### Add a production conflict index first

The admitted resolver currently uses a bounded linear conflict-history scan.
Measure that incumbent before changing both topology and data structure. A
production interval index receives its own frozen candidate and comparison.

## Tradeoff

This gate optimizes for an early falsifiable signal about resolver partitioning
on one fixed machine. It gives up an end-to-end cell throughput claim and remote
failure evidence. A positive balanced result justifies continuing the split
path; it does not establish a production capacity envelope.

## Compatibility and migration

The candidate adds evaluation-only worker and receipt formats. It changes no
public client API, resolver-map format, transaction envelope, or object format.
Unknown receipt versions fail closed. RFC-0049 full generation recovery remains
the fallback for resolver loss.

## Unresolved questions

1. Does a production interval or range-conflict index preserve the measured
   split benefit?
2. At what skew does a split stop clearing the practical threshold?
3. How much throughput remains after sequencer, proxy, tLog, and authority work
   are restored to the timed region?
4. Can several disjoint resolver splits execute without one metadata bottleneck?
5. What independent-host topology and network latency should define the first
   capacity envelope?

## Evaluation outcome

`[ACTIVE-WORK]` The frozen suite precedes implementation and measurement.
