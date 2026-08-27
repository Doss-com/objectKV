# G3.1 resident hot-path evaluation

- Status: `[EVALUATING]`
- Latest date: 2026-08-27

## GCP R0 public-kernel admission

A clean frozen source snapshot ran on one private `n2-standard-8` with a named
375 GiB GCP local NVMe SSD, regional GCS, and a separate OTel collector. Both
subjects used the same machine receipt, source, lockfile, seeds, 4 MiB working
set, batch ID, and 15 samples.

| Path | Median reads/s | Median p99 | Local bytes | Object reads during measurement |
|---|---:|---:|---:|---:|
| Public `SingleRange` plus RocksDB image | 516,973 | 2,482 ns | 4,351,739 | 0 |
| Direct RocksDB control | 702,142 | 1,749 ns | 4,347,486 | 0 |

The public path retained 73.63 percent of direct throughput and had 1.419x p99.
The comparison verdict is `worse`, so GP3.1 remains `[EVALUATING]`. Exact
recovery, retained-stream pagination, generation fencing, image bounds, and
zero-object hot reads all passed. The result isolates a public-path software
tax; it is not an SSD cache-pressure curve because the working set fits in RAM.

Evidence is recorded in
`docs/artifacts/eval-receipts/single-range-ssd-gcp-r0-2026-08-27/README.md`.

## GCP R0 focused optimization

The next frozen source moved complete-image lookup ahead of manifest location.
This removes a binary search plus cloning a `RowObjectReference` and its owned
strings and vectors from every admitted resident read. The evaluator also made
the declared cross-result p99 limit executable and bound candidate and control
suite hashes to the current program plan.

| Order | Public reads/s | Direct reads/s | Throughput gap | Public p99 | Direct p99 | P99 gap |
|---|---:|---:|---:|---:|---:|---:|
| candidate, control | 575,498 | 713,304 | 19.32% worse | 2,490 ns | 1,841 ns | 35.25% worse |
| control, candidate | 573,999 | 717,362 | 19.98% worse | 2,427 ns | 1,867 ns | 29.99% worse |

All runs used the same frozen revision, lockfile, machine receipt, suite hash,
three seeds, five repeats, 4 MiB working set, and three million measured reads
per subject. All correctness, byte, process-replacement, and zero-object-read
gates passed. All four run IDs occurred in OTel logs, metrics, and traces.

The average candidate throughput improved 11.18 percent over the prior clean
run. Average candidate p99 improved 0.95 percent. H1, repeated manifest routing,
was a material throughput cost but was not the dominant p99 cost. The remaining
tail-latency candidates are owned value transfer, overlay conflict checks, and
the composed async and dynamic-dispatch boundary. The experiment does not
separate those costs.

Evidence is recorded in
`docs/artifacts/eval-receipts/single-range-ssd-gcp-r1-2026-08-27/README.md`.

## GCP R0 native resident-engine decision run

Before implementing a second read boundary, the control was corrected to
return an owned 1 KiB value, matching the public `ReadOutcome::Value(Vec<u8>)`
contract. Against that control, the existing wrapper retained 88.19 percent of
throughput and measured 1.092x p99 in a diagnostic order. The prior 1.30x to
1.35x p99 result therefore overstated wrapper cost because its control used a
pinned value slice.

The native prototype then materialized object base plus txLog suffix into
RocksDB `head`, `history`, and `metadata` column families. Activation and
advancement bind generation, assigned range, immutable object closure, and the
applied frontier. Point reads use one bound engine snapshot and return an owned
value.

The first native run exposed thirty correctness anomalies, two per sample. The
engine had treated immutable object `first_key` and `last_key` values as serving
range bounds, hiding two valid tail inserts beyond the base object's key span.
Separating authority-owned assigned range bounds from object closure span
removed all anomalies and added a regression test for a tail insert beyond the
object maximum.

| Order | Native reads/s | Control reads/s | Throughput ratio | Native p99 | Control p99 | P99 ratio |
|---|---:|---:|---:|---:|---:|---:|
| candidate, control | 589,717 | 701,119 | 0.8411x | 2,226 ns | 1,839 ns | 1.2104x |
| control, candidate | 587,199 | 710,184 | 0.8268x | 2,261 ns | 1,778 ns | 1.2717x |

Both throughput ratios passed the 0.80x floor. Both p99 ratios failed the 1.20x
ceiling. Each subject and order used fifteen samples and three million measured
reads. Candidate correctness anomalies and measured object operations were
zero, and maximum native local bytes were 13,152,909, below the 128 MiB cap.
All four final run IDs occur in OTel logs, metrics, and traces.

Evidence is recorded in
`docs/artifacts/eval-receipts/single-range-native-resident-gcp-r2-2026-08-27/README.md`.

## Earlier local diagnostics

## Optimized ABBA diagnostic

The optimized runner executed control, candidate, candidate, control as separate
processes and averaged the two run medians for each path.

| Path | Reads/s aggregate | p99 aggregate | Object fallbacks | Max local bytes | Verdicts |
|---|---:|---:|---:|---:|---|
| Direct RocksDB control | 1,288,503 | 1,281 ns | 0 | 68,704,219 | inconclusive, dirty source |
| Resident ServingWorker candidate | 1,310,783 | 1,292 ns | 0 | 68,704,219 | inconclusive, dirty source |
| Post-warmup fallback poison | not compared | not compared | 2,400,000 | 68,704,219 | discard |

- Candidate/control throughput ratio: `1.0173`
- Candidate/control p99 ratio: `1.0082`
- Candidate read-validation failures: `0`
- Candidate local-byte ceiling: `134,217,728`
- Poison zero-object-request gate: `fail`, as required

The apparent 1.7 percent candidate throughput advantage is treated as noise,
not improvement. The supported finding is that the wrapper's current overhead
is below this harness's resolution for one resident, read-only range.

## Rejected dev-profile diagnostic

The corrected deterministic-incompressible dev-profile run made a complete resident
ServingWorker range physically occupy 68,704,219 bytes. The candidate stayed
within 0.8 percent of direct RocksDB throughput and within 1.4 percent of its
p99 latency while recording zero object-base fallback attempts.

| Path | Median reads/s | Median p99 | Object fallbacks | Max local bytes | Verdict |
|---|---:|---:|---:|---:|---|
| Direct RocksDB control | 1,048,476 | 1,500 ns | 0 | 68,704,219 | inconclusive, dirty source |
| Resident ServingWorker candidate | 1,040,681 | 1,521 ns | 0 | 68,704,219 | inconclusive, dirty source |
| Post-warmup fallback poison | not compared | not compared | 2,400,000 | 68,704,219 | discard |

- Candidate/control throughput ratio: `0.9926`
- Candidate/control p99 ratio: `1.0140`
- Candidate read-validation failures: `0`
- Candidate local-byte ceiling: `134,217,728`
- Poison zero-object-request gate: `fail`, as required

## Harness correction

The first formal attempt used zero-filled value bodies. RocksDB compressed the
64 MiB logical dataset to 2.5 MiB, so that attempt was rejected before use. The
runner now fills values with deterministic SplitMix64 bytes and retains the key
identity plus checksum sentinels used by the read oracle. The corrected physical
size is 68.7 MB.

These numbers verified the harness gates but are not an admissible performance
baseline because the binary was unoptimized. The one-command runner now uses
the release profile.

The first release-profile attempt used fixed control-then-candidate process
order and produced a `1.164` throughput ratio plus a `0.907` p99 ratio. That
apparent candidate win is rejected as order-biased. The runner now uses ABBA
process order and aggregates both run medians.

## Decision

Stop building a custom resident transaction plane. The corrected ownership
diagnostic shows that the original wrapper tax was partly a control mismatch.
The separately implemented native engine then failed the corrected p99 ceiling
in both process orders. The predeclared pivot rule fired.

Preserve `okv-log`, `okv-wal`, immutable publication, reconstruction, branching,
historical views, and DataFusion projection. Put TiKV or FoundationDB below
those contracts for resident MVCC and transaction processing. Keep the native
engine as a correctness prototype, especially its assigned-range versus object-
span distinction and atomic data-frontier advancement.

This optimizes for testing objectKV's actual product leverage without first
rebuilding a production distributed database. It gives up direct control of
the incumbent's point path and internal MVCC layout. GP3.2 RAM, multi-range,
PostgreSQL, and HTAP performance remain blocked until the provider adapter is
selected and GP3.1 is reframed around that plane.

Do not infer production point-read latency. The current gate excludes RPC,
range routing, Raft read policy, concurrent clients, overlay hits and
tombstones, compaction, writes, hydration, demotion, and PostgreSQL execution.
Those costs could dominate the roughly 1.3 microsecond local-engine baseline.

## Next curve

Freeze the smallest objectKV-to-plane contract, then implement the same
single-range objectification, branch, and empty-worker rebuild experiment over
TiKV and FoundationDB. Select by semantic fit, integration surface, recovery
ownership, operational burden, and measured lifecycle leverage. Only the
selected plane proceeds to concurrency, working sets beyond RAM, PostgreSQL,
and exact DataFusion base-plus-tail evaluation.
