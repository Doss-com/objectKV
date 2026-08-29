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
recovered calibration fixture and runs 15 independently warmed samples after
explicit block-cache eviction. Each sample reports process user/system CPU,
RSS, Linux logical and physical I/O, and host network-namespace deltas.
Mismatched-cache and counter-reset poison workloads both discard. The first
cloud calibration completed on GCP R0 in both process orders and exposed a
forced tail SST. After keeping the disposable tail mutable, the corrected
60-million-read rerun retained 0.9432x and 0.9735x control throughput; p99 was
1.0441x and 0.9949x; CPU/read was 1.0586x and 1.0298x. All 84 workload gates
and eight explicit comparison constraints passed, every run ID appeared in
OTel logs, metrics, and traces, and cleanup completed. The 64 MiB calibration
is `[VERIFIED]`. T27 remains `[EVALUATING]` because the 1 GiB coverage and skew
curve has not run. A subsequent matched direct-read preflight produced about
2.96 KiB of Linux physical reads per logical read for both native and control,
verifying that the evaluator can isolate the NVMe path. That one-sample smoke
is not a performance admission.

The comparison invariant is:

```text
same logical fixture ID + same tail ID + matched resident options
+ same block-cache bytes + same trace + same process topology
= one attributable native-versus-control comparison
```

Native and control do not share a mutable RocksDB directory. Their physical key
codecs differ, so RFC-0044 binds each subject-local image to the same logical
fixture and verifies exact outcomes independently.

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
- subject-local semantic resident-image identity after activation.

The first calibration fixture is 64 MiB logical. It exists to prove that the
runner can build once, clone or reopen without reseeding through the transaction
authority, and bind native and control images to one logical fixture and one
exact tail. RFC-0044 first proves one empty anchor record, zero base-value txLog
records, complete closure verification, and separate subject-image identities.
The first admission fixture is 1 GiB logical. Larger 10 GiB and 100 GiB points
remain later capacity curves, not requirements for freezing T27.

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
It must not call every block-cache miss an NVMe read. Direct table reads are an
explicit measurement treatment, not the portable product default. They bypass
the operating-system page cache for RocksDB table files while preserving the
declared RocksDB block cache. Candidate and control must both report the mode,
and any profile mismatch invalidates the result.

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

The 64 MiB calibration uses one seed, 15 independently warmed samples, both
process orders, and two independent fixture reconstructions per subject. The
first GCP R0 admission uses five repeats for each seed, both process orders,
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
4. `[VERIFIED]` Execute the 64 MiB calibration once per subject and process
   order on R0. The four receipts contain 60 million measured reads. Native
   missed throughput, p99, and the diagnostic CPU bound in both orders.
5. `[VERIFIED]` Preserve receipts and all three OTel signals, remove 152 current
   scratch objects, and destroy all nine leased resources.
6. `[VERIFIED]` Remove the forced tail SST, make CPU and physical-byte
   cross-result constraints executable, and rerun the unchanged 64 MiB A/B and
   B/A gate. All explicit constraints passed.
7. `[VERIFIED]` Apply a matched RocksDB direct-table-read mode to candidate and
   control, report the mode in every measured sample, and verify on Linux that
   the receipt distinguishes OS page-cache behavior from NVMe reads. Both
   subjects passed 22 of 22 hard gates and reported about 2.96 KiB of physical
   reads per logical read. This is mechanism evidence from one smoke sample.
8. `[VERIFIED]` Implement RFC-0044 phases 0 through 4: establish one empty
   transaction anchor, keep base values out of txLog, bind separate native and
   control images to one complete logical digest, and reopen one persisted
   64 MiB GCS descriptor three times across fresh ABBA subjects. The phase-4
   candidate and reuse-bypass poison passed all 19 gates.
9. `[EVALUATING]` Prepare and commit one exact 1 GiB fixture placement locator,
   separate fixture seed from three trace seeds, remove the direct control's
   hidden native database, then freeze and execute the fresh-process ABBA plan
   on clean GCP R0 with required OTel.

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
- Which native point-read layer owns the observed 1.67x to 1.75x CPU cost when
  cache hit ratio, peak RSS, and physical bytes do not explain the gap.
- Whether Zipf 0.8, direct I/O, explicit operating-system page-cache treatment,
  or a larger fixture should own the separate physical NVMe curve. Zipf 1.4 at
  2x cache produced more than 99.4 percent block-cache hits and zero reported
  physical read bytes.
