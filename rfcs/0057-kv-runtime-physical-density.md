# RFC-0057: KV Runtime physical density

- Status: accepted for the next serving prototype, 2026-08-24
- Authors: DOSS
- Created: 2026-08-24
- Depends on: RFC-0024, RFC-0056

## Decision

`[DECIDED]` Use one pinned SlateDB database with many logical range prefixes as
the default physical layout inside one KV Runtime. The frozen gate measured
three real process topologies:

1. one SlateDB database containing many logical range prefixes;
2. one SlateDB database per Range Engine with one caller-owned shared decoded
   block cache;
3. one SlateDB database and one private decoded block cache per Range Engine.

The one-database topology reached 1,000 resident Range Engines under the fixed
process envelope while database instances, decoded-cache instances,
background tasks, and object families remained constant. Multi-database
topologies remain possible isolation modes, but are not the default because
their physical costs grew with assignment count.

This gate uses the accepted `objectkv-serving-v1` SlateDB settings: no SlateDB
object WAL, no automatic flush, no embedded compactor, no embedded garbage
collector, 64 KiB SST blocks, and Bloom filters for non-empty SSTs. The objectKV
txLog remains the recent durability authority.

## Why this gate exists

RFC-0056 proves only accounted resource semantics. It does not prove that the
selected embedded engine follows those semantics. SlateDB normally creates a
decoded block and metadata cache per `DbBuilder`, but it also exposes
`with_db_cache` specifically so one caller-owned cache may be shared across
multiple databases. Each database still owns writer state, manifests, and
background tasks.

The open architectural question is therefore not "RocksDB or no RocksDB." It
is:

```text
Does one Range Engine require one embedded database?

or

Can one embedded object LSM host many logical range assignments?
```

The second topology preserves process-wide caching and avoids per-range
manifest and task families, but gives up hard local-engine isolation between
ranges. Range movement and per-range objectification then require prefix-aware
metadata and compaction rather than closing one independent database.

## Frozen process harness

Every sample runs in a fresh child process using one current-thread Tokio
runtime. The worker creates:

- one temporary local filesystem object-store root;
- one request and byte counting wrapper around that object store;
- one shared 256 MiB filesystem cache representing disposable NVMe;
- one 64 MiB decoded RAM cache for shared-cache subjects;
- one deterministic 256-byte value per target Range Engine;
- one explicit flush before point reads;
- one close of every database and decoded cache;
- one reopen with empty RAM and empty NVMe cache state;
- one exact point read per reopened Range Engine.

The worker samples its own current RSS, OS thread count, open file descriptors,
Tokio live tasks, object requests and bytes, object files and bytes, NVMe cache
files and bytes, database and cache instance counts, open time, flush time,
point-read p50 and p99, and empty-cache rebuild time.

RSS uses the safe `sysinfo` current-process probe with one probe instance held
for the complete sample, avoiding per-range instrumentation allocation. On
macOS, the underlying implementation uses the same process APIs studied in
celld. File descriptors use `/proc/self/fd` on Linux and `/dev/fd` on macOS.
Unsupported probes fail visibly rather than substituting accounted values.

## Frozen points and safety bounds

Run target densities 1, 100, and 1,000 for every topology under seeds `1103`,
`2207`, and `3301`.

| Bound | Value | Effect |
|---|---:|---|
| worker RSS | 1 GiB | stop opening more databases or writing more ranges |
| worker elapsed time | 120 seconds | stop the subject and preserve a partial receipt |
| decoded RAM cache | 64 MiB | one shared instance or one private instance per database |
| NVMe cache | 256 MiB | one shared filesystem cache per worker |
| NVMe part | 64 KiB | align with the accepted SST block size |
| NVMe open handles | 64 | keep cache file descriptors process-bounded |
| keys per range | 1 | isolate residency overhead before throughput load |
| value bytes | 256 | force a real write, flush, object, reopen, and read |

Crossing a worker bound is a valid topology result, not a correctness failure,
provided the worker stops, closes opened databases, reports the exact completed
count, and never claims the target was reached. The single-database subject
must reach all 1,000 logical ranges for the proposed default to survive.

## Frozen hard gates

Every correct receipt must prove:

- the exact pinned SlateDB revision and `objectkv-serving-v1` settings;
- a supported non-zero physical RSS sample;
- truthful topology, database-instance, and decoded-cache-instance counts;
- a single process-wide NVMe cache;
- exact writes and reads for every completed range;
- object requests and bytes are accounted;
- object and NVMe file inventories are measured;
- the empty-RAM and empty-NVMe reopen path executed;
- background task, thread, and file-descriptor samples are physical;
- the RSS and time guards were checked after every bounded batch;
- a stopped subject reports its completed density and reason;
- identical configuration and seed reproduce the semantic receipt.

The single-database subject additionally requires target completion at 1, 100,
and 1,000. Multi-database subjects may stop at a safety bound and remain valid
measurements.

## Negative subjects

The frozen suite independently attempts to:

1. substitute RFC-0056 accounted RAM for a physical RSS probe;
2. claim one shared decoded cache while constructing private caches;
3. report a warm handle as an empty-cache reopen;
4. omit the process safety-bound receipt.

Each subject must violate its owning gate and discard.

## Metrics and interpretation

The primary metric is incremental physical RSS per completed Range Engine.
Completion ratio, absolute RSS, live tasks, threads, descriptors, database and
cache instances, object operations, object files, NVMe files, open duration,
empty-cache rebuild duration, and point-read p50 and p99 are required secondary
evidence.

Do not compare RSS values across machines. A comparable topology sample
requires the same suite hash, profile hash, executable, host, SlateDB revision,
resource bounds, and seed. The three topologies run as separate child processes
so allocator residue from one subject cannot bias another.

## Stop and keep rules

Keep the one-database topology as the default only if:

- it completes 1,000 logical ranges under both safety bounds;
- its database and decoded-cache instance counts remain one;
- live-task, thread, and descriptor growth is bounded rather than per range;
- empty-cache exact reads complete without one object or file family per range.

Reject one-database-per-range as the default if either multi-database topology
hits a safety bound before 1,000 or shows per-range database, task, manifest, or
file growth that the one-database topology avoids. This does not remove the
multi-database topology as a deliberate isolation tier.

## Tradeoff

This gate optimizes for an early physical answer about range-to-engine
cardinality. It gives up production-sized values, concurrent traffic, remote
S3 latency, compaction load, and long-running cache churn. Passing the
single-database subject only selects the local topology for the next serving
prototype. It does not establish the final KV Runtime capacity envelope.

## Follow-up

The selected topology next receives mixed point-read, ordered-range-read,
write, objectification, and range-movement curves under MinIO and GCS. That
gate must measure cache hit rate, request amplification, retained txLog debt,
and failover reconstruction while the worker is under load.

## Evaluation outcome

`[EXISTS]` The exact committed executable passed all nine correct workloads and
all four controls discarded. The 1,000-assignment medians were:

| Topology | RSS per range | Peak RSS | Live tasks | DBs | decoded caches | object files | empty-cache reopen | cold point p50 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| one DB, logical ranges | 18.7 KiB | 30.6 MiB | 9 | 1 | 1 | 9 | 113 ms | 51 us |
| DB per range, shared cache | 138.4 KiB | 147.9 MiB | 8,001 | 1,000 | 1 | 9,000 | 4.03 s | 879 us |
| DB per range, private cache | 190.5 KiB | 198.8 MiB | 8,001 | 1,000 | 1,000 | 9,000 | 4.04 s | 886 us |

The exact executable SHA-256 was
`29f7aa8c394dc739bb500396efd5c290f2de03398a84f254bef50f12e3e67e81`.
The one-database layout used 7.6 times less RSS per range than the shared-cache
layout and 10.4 times less than the private-cache layout at 1,000 assignments.
This accepts engine cardinality only. It does not admit production throughput,
capacity, remote object latency, concurrent load, compaction, or range
movement.
