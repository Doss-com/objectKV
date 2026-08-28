# RFC-0043: Native resident cache-pressure curve

- Status: `[EVALUATING]`, experiment contract frozen
- Authors: DOSS
- Created: 2026-08-27
- Scope: T27 cache sizing, eviction, and physical-read attribution

## Decision to test

Keep the admitted GP3.1.1 single-range recovery topology and eight-client read
boundary. Add one explicit RocksDB block-cache budget to both the native
RangeEngine and matched direct RocksDB control. Reuse one content-addressed
fixture across every sample, sweep fixture-to-cache ratio and access skew, and
measure the physical work below logical point reads.

```text
content-addressed fixture on GCS
  -> empty replacement worker
  -> complete resident image on local NVMe
  -> fixed RocksDB block cache
  -> deterministic Zipf read trace
  -> cache, CPU, physical-byte, latency, and throughput receipt
```

This gate answers whether the current direct-RocksDB proximity survives when
the working set exceeds the declared cache. It does not test remote RPC, lazy
object refill, range routing, replicated commit, or multi-range scaling.

## Context and invariant

The admitted GP3.1.1 receipt used about 4 MiB of logical values. Its frozen
revision optimized RocksDB for point lookup but did not report a block-cache
capacity. The operating-system page cache may also have satisfied reads. The
receipt therefore verifies software composition around a resident working set,
not the RAM-to-NVMe curve. The first T27 source slice now exposes one explicit
shared cache and cumulative RocksDB counters. The paired runner now applies the
same cache to the direct control, captures measured-window counter deltas,
generates deterministic Zipf traces with a trace digest, and exports the cache
and RocksDB counters through OTel. Reusable fixture execution now builds one
recovered fixture per seed and runs five independently warmed samples after
explicit block-cache eviction. Each sample reports process user/system CPU,
RSS, Linux logical and physical I/O, and host network-namespace deltas.
Mismatched-cache and counter-reset poison workloads both discard. The cloud
receipt remains open.

The invariant is:

```text
same keys + same values + same NVMe image + same block-cache bytes
+ same trace + same process topology
= one attributable native-versus-control comparison
```

The measured window must issue zero object operations. Object reads belong to
T28. A native miss in T27 must terminate at the same local NVMe boundary as the
control.

## Frozen experiment contract

### Fixture identity

One fixture descriptor names:

- generator revision and schema version;
- key count, value-size distribution, logical byte count, and digest;
- ordered key and value seed;
- object manifest identity and complete closure digest;
- RocksDB options fingerprint and engine format version;
- local image digest after activation.

The first calibration fixture is 64 MiB logical. It exists to prove that the
runner can build once, clone or reopen without reseeding through the transaction
authority, and produce identical image digests. The first admission fixture is
1 GiB logical. Larger 10 GiB and 100 GiB points remain later capacity curves,
not requirements for freezing T27.

Fixture construction is outside every warmup and measurement window. The
runner must not rebuild the base through 32-key replicated transactions for
each seed or repeat.

### Cache and media contract

Both subjects declare:

- RocksDB block-cache capacity in bytes;
- high-priority pool ratio and whether index and filter blocks are pinned;
- direct-I/O setting, compression, block size, checksum, compaction style, and
  background-thread settings;
- local device identity, mount, filesystem, and free bytes;
- process resident bytes and operating-system page-cache treatment.

The first curve uses one block-cache capacity and three fixture-to-cache
coverage points:

| Declared cache coverage | Fixture bytes / block-cache bytes | Purpose |
|---:|---:|---|
| 50% | 2x | mixed resident and NVMe reads |
| 20% | 5x | sustained cache pressure |
| 5% | 20x | eviction-heavy bound |

If direct I/O is disabled, the receipt must report process physical read bytes
and identify the run as a combined RocksDB plus operating-system-cache curve.
It must not call every block-cache miss an NVMe read. A later direct-I/O lane
may isolate the device curve after the portable default is admitted.

### Access traces

Use deterministic Zipf traces at alpha 0.8, 1.4, and 2.0. Each subject receives
the exact same serialized trace digest. Eight clients partition one total
operation budget, as in RFC-0042. Each client receives at least one operation,
and all clients enter the measured window through one barrier.

The three trace shapes answer different questions:

- 0.8 exposes broad working-set and physical-read cost;
- 1.4 represents a useful skewed database workload;
- 2.0 checks whether a small hot set preserves the admitted resident envelope
  while cold keys continue to churn.

### Measurement windows

Every sample contains distinct phases:

```text
fixture verify
  -> worker open and image verify
  -> cache reset or fresh process
  -> declared warmup
  -> synchronized measured window
  -> metric snapshot and receipt
```

Setup, GCS download, complete image activation, compaction, and process startup
must not enter hot-read latency or throughput. Their duration and bytes remain
separate recovery metrics.

## Required metrics

The candidate and control must report the same metric names and units:

| Layer | Required measurements |
|---|---|
| Logical | attempted reads, completed reads, wrong values, not-found results |
| Latency | p50, p95, p99, p99.9, maximum, synchronized wall duration |
| Throughput | completed reads per wall second and per client |
| CPU | process user, system, and total CPU time; CPU nanoseconds per read |
| RocksDB cache | capacity, usage, pinned usage, hit, miss, data hit, data miss |
| RocksDB I/O | bytes read, block-cache bytes read, useful bytes, total read-amplification bytes |
| Process I/O | physical read bytes and logical read bytes from the operating system |
| Memory | RSS before warmup, after warmup, and after measurement; peak RSS |
| Object store | GET, ranged GET, LIST, PUT, response bytes, and request latency |
| Identity | source, binary, lockfile, suite, fixture, trace, options, machine, and device digests |

All counters are captured immediately before and after the measured window.
Delta values, not process-lifetime totals, enter the comparison.

## Admission gates

The first GCP R0 admission uses five repeats for each seed, both process orders,
three seeds, eight clients, and the 1 GiB fixture. A cache and skew point is
admitted only when:

1. every value and operation count is exact;
2. the trace, fixture, options, cache bytes, and process topology match;
3. measured-window object operations equal zero;
4. reported cache usage stays within the declared budget plus a 5 percent
   instrumentation allowance;
5. native throughput is at least 0.80x matched control;
6. native p99 is at most 1.20x matched control;
7. native CPU nanoseconds per read are at most 1.25x control;
8. native physical bytes per read and RocksDB read-amplification bytes are each
   at most 1.25x control;
9. every run ID occurs in OTel logs, metrics, and traces;
10. scratch data and leased compute are removed after evidence capture.

The throughput and p99 limits retain the admitted resident boundary. CPU and
physical-byte limits detect an implementation that hides extra work behind
device or scheduler slack.

The runner becomes `[CODE-COMPLETE]` when its local contracts and negative
controls pass. A 64 MiB R0 calibration can verify fixture reuse and telemetry,
but it is not a performance claim. Only the clean 1 GiB paired GCP receipt can
move T27 to `[VERIFIED]`.

## Negative controls

The suite must discard subjects that:

- silently use an unbounded or shared global block cache;
- change one cache or RocksDB option only on the control;
- report process-lifetime counters as measured-window deltas;
- change the trace or fixture digest between subjects;
- rebuild or compact fixture state inside the measured window;
- issue an object request during the local read window;
- relabel operating-system-cache hits as block-cache or NVMe hits;
- omit CPU, physical-byte, amplification, identity, or OTel fields;
- retain scratch objects or benchmark compute after the lease ends.

At least one deliberately mismatched-cache subject and one counter-reset poison
must receive `discard` before the clean GCP run.

## Infrastructure sequence

1. `[CODE-COMPLETE]` Expose the resident engine's explicit shared cache budget
   and cumulative RocksDB cache and amplification counters.
2. `[CODE-COMPLETE]` Wire the same cache into the direct control, report counter
   deltas, bind deterministic Zipf traces to a digest, and reject counter
   resets.
3. `[CODE-COMPLETE]` Report process CPU and I/O, reuse fixtures, and add poison
   subjects.
4. `[PROPOSED]` Run the 64 MiB calibration fixture once on the R0 machine shape.
5. `[PROPOSED]` Inspect attribution and adjust operation counts so each sample
   reaches steady behavior without exceeding the lease budget.
6. `[PROPOSED]` Freeze the suite hash, source revision, and 1 GiB fixture.
7. `[PROPOSED]` Execute both process orders on clean GCP R0 with required OTel.
8. `[PROPOSED]` Preserve receipts, remove scratch state, and destroy compute.

No three-machine topology is needed for T27. T27 isolates one local serving
engine and its cache hierarchy. Independent media first becomes load-bearing
for T29 replicated commit.

## Failure model

- Fixture creation stops partway through object upload or local image build.
- A process dies between cache reset and the measured barrier.
- Metric snapshots fail, wrap, reset, or expose unsupported counters.
- The local device fills or background compaction overlaps measurement.
- GCS is unavailable during activation.
- OTel is unavailable or omits one signal.
- The worker reopens a fixture with an incompatible format or options digest.

Each failure must either occur before measurement and produce no comparison, or
invalidate the sample. Partial samples never enter percentile aggregation.

A local diagnostic reproduced activation-time `ENOENT` while accounting for a
live RocksDB directory. A deterministic regression proved that RocksDB can
remove an obsolete entry between directory enumeration and metadata lookup.
Accounting now ignores only `NotFound` for that vanished entry while preserving
all other I/O errors and symlink rejection. The original reused-fixture CLI
preflight passes after the correction; the GCP calibration must still record
any recurrence.

## Alternatives

### Use the default RocksDB cache

This minimizes code. It gives up knowing the working-set boundary and cannot
support a RAM, NVMe, or amplification claim.

### Drop the operating-system cache before every sample

This can expose cold-device behavior. It requires elevated host controls and
changes the benchmark into a storage-device test. The portable first lane
measures and labels the combined cache hierarchy, then a direct-I/O lane can
isolate NVMe.

### Begin at 10 GiB or 100 GiB

This improves scale confidence. It makes fixture-construction defects expensive
and slows iteration before reuse and counter attribution are verified. T27
starts at 64 MiB for calibration and 1 GiB for admission, then T28 and later
capacity work reuse the same descriptor at larger sizes.

### Combine cache pressure with object refill

This better resembles an elastic worker. It confounds local eviction with
object request geometry. T27 ends at local NVMe. T28 adds bounded object refill
using the admitted fixture and cache contract.

## Compatibility and migration

Cache configuration and metric fields are evaluator and serving-profile
metadata. They do not change ordered KV, txLog, or object-segment semantics.
Old result schemas remain readable, but missing T27 counters make them
ineligible for this comparison. RocksDB format or options changes require a new
fixture and options digest.

## Unresolved questions

- Whether the portable admission lane should enable direct I/O after the first
  calibration exposes operating-system-cache attribution.
- Whether 1 GiB reaches a stable physical-read signal on `n2-standard-8` with
  the current local NVMe and available RAM.
- Whether index and filter blocks should share the declared cache budget or get
  a separately declared metadata budget.
