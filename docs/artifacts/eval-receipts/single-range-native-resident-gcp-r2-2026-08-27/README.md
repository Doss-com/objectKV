# GP3.1 native resident-engine decision run, 2026-08-27

Status: `[VERIFIED]` for the native correctness prototype, frozen AB and BA
comparison, OTel evidence, and the D52 pivot trigger. `[EVALUATING]` for the
TiKV versus FoundationDB provider selection. The native candidate did not earn
GP3.1 admission.

## Decision result

```text
machine:                    private n2-standard-8, Intel Cascade Lake
stable volume:              200 GiB pd-ssd at /var/lib/objectkv
serving scratch:            375 GiB GCP local SSD, NVMe, ext4
object base:                regional versioned GCS
source revision:            3c4cae96fe77104830a641b8429ace82391e0533
source bundle sha256:       2b92692d4e6c141e7db00ed1ab91289b110140899f0a5c41183cb6f7a9b2f356
binary sha256:              a439facf7b28aceb453857aa5bd7937bc006b26ce5e748e18931ccba76b7b0fb
machine receipt sha256:     ff5d009d8ecc2f5fe13f743b6ac8dbbfd837b7a4b2780821b59aeae640921024
suite hash:                 fbeaf139305aa234e54168c8b3eb2ddf468168651744a39b4120012a60f5ca00
samples per subject/order:  15
measured reads per subject: 3,000,000

order AB candidate:         589,717 reads/s, 2,226 ns p99
order AB control:           701,119 reads/s, 1,839 ns p99
order AB ratios:            0.8411 throughput, 1.2104 p99
order AB verdict:           worse, p99 constraint failed

order BA candidate:         587,199 reads/s, 2,261 ns p99
order BA control:           710,184 reads/s, 1,778 ns p99
order BA ratios:            0.8268 throughput, 1.2717 p99
order BA verdict:           worse, p99 constraint failed

candidate local image max:  13,152,909 bytes
post-activation object ops: 0
correctness anomalies:      0
scratch objects removed:    288 current objects across six exact run prefixes
lease teardown:             9 resources destroyed, 0 benchmark resources remain
```

Both throughput comparisons passed the frozen 0.80x floor. Both p99
comparisons failed the 1.20x ceiling. The predeclared RFC-0040 stop rule fired:
objectKV stops expanding a custom resident transaction plane. TiKV or
FoundationDB becomes that plane; objectKV retains `okv-log`, `okv-wal`, object
publication, reconstruction, branching, historical views, and DataFusion
projection.

## What was constructed

- `ResidentRangeEngine` separates activation, retained-tail advancement, and
  version-bound snapshots from the public range API.
- RocksDB stores latest values in `head`, historical versions in `history`, and
  generation, assigned range, immutable object span, object root, and frontiers
  in `metadata`.
- Engine transition state prevents reads from opening while activation or
  advancement is incomplete.
- One RocksDB batch applies complete txLog pages and advances the applied
  frontier atomically.
- Latest point reads bind one engine snapshot and return owned values, matching
  the public API and control.
- The local image remains disposable and rebuilds from verified object closure
  plus the retained transaction suffix.

## Correctness finding

The first native run returned thirty deterministic anomalies, two per sample.
The engine had interpreted the immutable object's `first_key` and `last_key` as
the assigned serving range. Two valid txLog inserts beyond the base object's
maximum key were therefore hidden.

The correction gives `ResidentRangeBounds` its own authority-owned half-open
range and treats object first and last keys only as closure span at object
version `O`. The single-range pilot passes unbounded start and end. A regression
test now verifies a tail insert beyond the object maximum and verifies that an
older snapshot does not expose it.

## Benchmark ownership correction

The prior direct control returned a RocksDB pinned slice while the candidate
returned `ReadOutcome::Value(Vec<u8>)`. A diagnostic rerun changed the control
to return an owned 1 KiB value. The existing wrapper then measured 630,611
reads/s and 1,968 ns p99 against 715,057 reads/s and 1,802 ns, ratios of 0.8819x
and 1.0921x. This shows that value ownership was a material confounder in the
earlier 1.30x to 1.35x p99 result.

The final native decision does not depend on that earlier comparison. Both
final orders used the corrected owned-value control, the same source, lockfile,
machine, profile, suite, seeds, batch, and sample count.

## What was verified

- Exact base plus suffix reconstruction, same-version cursor preservation,
  point set, point clear, range clear, and tail inserts.
- Generation and assigned-range bound snapshots, stable older snapshots across
  advancement, and atomic data plus applied-frontier writes.
- One killed serving worker and one empty replacement process.
- Fifteen samples for each subject in each process order.
- Zero incorrect final reads and zero object operations in the measured native
  windows.
- Candidate local bytes below the 128 MiB ceiling.
- All four final run IDs occur in OTel logs, metrics, and traces. Each run has
  two log records, one metric record, and one trace record.
- The frozen suite and golden-path program validate, and selected clippy and
  regression tests pass on the frozen source.

The RFC-0040 process-death poisons during an in-progress engine activation and
advancement are not complete. They no longer block a native-plane admission
because the performance candidate is rejected. Their transition invariants
remain requirements for whichever incumbent adapter is selected.

## Architectural consequence

The result rejects one plan, not the objectKV product thesis. Building a direct
RocksDB MVCC engine did not produce enough p99 headroom to justify continuing
toward a custom distributed transaction system. The retained product question
is narrower: can one incumbent transaction plane support open object history,
cheap branches, bounded empty-worker reconstruction, independent compute, and
exact OLTP plus OLAP snapshots with a material lifecycle or economics win?

The next golden-path task is T25. Freeze the minimal plane contract, implement
the same bounded objectification and rebuild adapter against TiKV and
FoundationDB, select one, then reframe GP3.1 around the winner. GP3.2 RAM,
MultiRaft, PostgreSQL, and HTAP performance stay blocked.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/results/gp31native1-r2/receipts/
gs://doss-objectkv-dev-okv-evals/results/gp31native1-r2/diagnostics/
gs://doss-objectkv-dev-okv-evals/results/gp31native1-r2/objectkv-otel-evidence/
gs://doss-objectkv-dev-okv-evals/results/gp31native1-r2/verification/
gs://doss-objectkv-dev-okv-evals/bundles/sha256/2b92692d4e6c141e7db00ed1ab91289b110140899f0a5c41183cb6f7a9b2f356.bundle
```

The six exact scratch prefixes held 288 current objects before deletion and
zero afterward. Bucket versioning keeps those deletions recoverable. The
durable source bundle, receipts, diagnostics, validation logs, and OTel records
remain in the evidence prefixes above.
