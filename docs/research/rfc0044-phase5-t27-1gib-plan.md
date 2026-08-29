# RFC-0044 phase 5: T27 1 GiB admission plan

Status: `[EVALUATING]`, with the cross-invocation fixture, read-only consumer,
standalone control, independent-seed boundaries, four-position 64 MiB
preflight, 1 GiB fixture, and immutable 540-position plan `[VERIFIED]`

## Decision

Do not run the 1 GiB admission on the current cache-pressure runner. First
close five attribution gaps:

1. persist the exact fixture locator across evaluator invocations;
2. remove the hidden native database from the direct RocksDB control;
3. make every admission position a fresh process and fresh mutable directory;
4. use matched direct table reads for the admitting cache-to-NVMe curve;
5. separate immutable fixture randomness from read-trace randomness.

The corrected pipeline is:

```text
one fixture preparation invocation
  -> one exact 1 GiB fixture placement locator
  -> committed locator envelope and suite hash
  -> immutable 540-position ABBA execution plan
  -> one fresh process and one database/cache per position
  -> 27 independently gated cache/skew/trace strata
```

This optimizes for attributable performance. It gives up a short benchmark and
requires a separate preparation phase, explicit plan artifact, and substantial
fresh-process execution cost.

## D1. One fixture placement locator crosses every invocation

Freeze one logical fixture with `fixture_seed = 4244`, 1,048,576 keys, and
1,024 value bytes. `FixturePlacementLocatorV1` contains:

```text
semantic identity
  fixture_id
  descriptor_length
  descriptor_sha256
  base_version

placement identity
  provider
  bucket
  prefix
  descriptor_key
  descriptor_generation

frozen fixture profile
  fixture_seed
  key_count
  value_bytes
  logical_bytes
  generator_version
  row_object_format_version
  target_object_bytes
  target_block_bytes

build envelope
  source_sha256
  suite_sha256
  binary_sha256
  cargo_lock_sha256
```

The locator has one canonical `envelope_sha256`. The locator file enters the
performance suite's `contract_files`, so the suite hash binds both semantic
identity and placement. Consumers receive the locator path plus expected
envelope SHA. Missing or mismatched locators fail before any PUT, txLog tail
application, or resident-image construction. Generation fallback and LIST as
an authority input are forbidden.

The preparation result is setup evidence, not a T27 performance result. It
must exact-open the descriptor and full closure after publication using
read-only credentials before the locator can be committed.

## D1.1. A persisted workload plan needs a live execution incarnation

The first 1 GiB plan correctly bound the runner instance, boot, NVMe
filesystem, executable, machine receipt, and six-hour infrastructure lease.
The runner was then destroyed after setup evidence capture. That full plan is
therefore an authenticated historical setup artifact, but it cannot execute on
a replacement runner. Reusing it would fail the controller's execution-envelope
check, as intended.

Do not weaken that check and do not mutate the persisted plan in place. Add one
authenticated execution-incarnation step:

```text
historical authenticated plan
  -> derive workload digest excluding the execution envelope
  -> exact-open the same generation-pinned fixture
  -> preserve profile, fixture, oracle, positions, treatments, and thresholds
  -> preserve source, executable, and Cargo.lock digests
  -> bind a live machine receipt, instance, boot, NVMe filesystem, and lease
  -> emit a new immutable execution plan plus incarnation receipt
  -> execute that plan before its lease expires
```

The incarnation receipt must bind both full plan digests, both execution
envelope digests, and one equal workload digest. It passes only if the runtime
source, executable, and lockfile digests are unchanged and every non-execution
field is byte-for-byte equivalent. A no-op incarnation, changed fixture,
changed oracle, changed position, changed treatment, changed threshold, or
changed runtime digest fails closed.

The first replacement-runner attempt exposed one additional artifact rule.
The original executable had been hashed into the plan but had not been retained
as a content-addressed object. A rebuild on the same Debian image and CPU class,
with the exact source archive, Cargo.lock, Rust 1.88.0 toolchain, and release
feature set produced executable SHA-256 `28addf68...`, not the frozen
`f3471d07...`. Absolute source paths are present in the unstripped ELF, and the
original build path and linker manifest were not captured. Source and lockfile
identity are therefore insufficient to reconstruct byte-identical executable
identity.

Two paths are valid:

```text
exact executable retained
  -> execution incarnation
  -> runtime digest remains identical

exact executable absent
  -> do not call it an incarnation
  -> rebuild and seal a new full plan on the live runner
  -> require equal portable workload digest and equal non-execution fields
  -> execute only the new plan with its newly bound executable
```

Future admitted plans must publish the executable by content digest beside the
source archive, Cargo.lock, machine receipt, and plan. A plan without that
executable remains valid historical evidence but is not restartable as an exact
runtime incarnation.

This separation is required by the teardown gate. Immutable workload intent
must survive ephemeral infrastructure, while every measured receipt still
binds one exact live machine and device. It optimizes for resumable, auditable
experiments. It gives up treating one full plan digest as portable across
machines.

## D1.2. Large-fixture process barriers and repeated setup must be bounded

The first live 1 GiB setup probe failed before measurement. The serving parent
waited for a fixed 1,000 polls at 10 ms each, so a child had only 10 seconds to
exact-open the object fixture and create its initial catch-up barrier. The child
was still active when the parent rejected it. The failed invocation ended after
54.68 seconds with 2.39 GiB maximum RSS and no resident-image file written.

This is a workload-size bug, not a performance result. The 64 MiB preflight
completed inside the fixed deadline; the 1 GiB admission cannot.

Replace the fixed barrier with a deterministic bounded deadline derived from
the frozen fixture logical bytes. The first implementation uses a 60-second
minimum, 30 seconds of fixed overhead, a conservative 2 MiB/s reconstruction
floor, and a 30-minute maximum. The same deadline is serialized into both the
parent and child process configuration. A child exit still fails immediately.
A deadline expiration still fails closed.

The barrier correction is necessary but not sufficient. The current position
path exact-opens the same fixture three times:

```text
position parent
  -> exact-open full generation-pinned closure

first disposable worker
  -> exact-open full closure
  -> reach catch-up barrier
  -> killed intentionally

measured replacement worker
  -> exact-open full closure
  -> construct fresh resident database
  -> measure hot reads
```

The replacement runner exact-opened and derived the plan in 3m17s with 4.63 GiB
maximum RSS. At that observed rate, 540 positions would spend about 88 hours on
three repeated opens per position before hot-read time. Do not launch that run
under the current 24-hour lease.

T27 statistical positions will not repeat the deliberate kill. Restart and
empty-scratch recovery remain separate required correctness probes under the
same retained runtime, fixture generation, and storage profile. Each T27
position still creates one fresh measured child, one empty mutable directory,
one database, and one measured cache. The parent still exact-opens the fixture
before the child exact-opens it and constructs its own resident state.

```text
same-runtime recovery probe
  -> first child reconstructs and reaches barrier
  -> first child is killed
  -> replacement child reconstructs from empty scratch
  -> exact replay required

each T27 statistical position
  -> parent exact-opens generation-pinned fixture
  -> one fresh measured child reconstructs from empty scratch
  -> hot-read window
```

This separates a correctness axis from repeated performance sampling. It gives
up proving a deliberate restart in every one of 540 positions. It retains the
same fixture ID, descriptor generation, semantic oracle, trace, subject order,
cache budget, and hot-read measurement boundaries, while removing one complete
non-measured reconstruction per position.

Run one full 20-position stratum after this correction. If its measured wall
time still projects beyond one 24-hour lease, add authenticated per-stratum
resumption before expanding the run. Parallel generation-pinned object reads
and a content-verified local closure remain later setup-cost optimizations; they
must not be used to relabel setup time as hot-read performance.

## D2. Candidate and control each own exactly one database and cache

The current direct-control branch is not admission-worthy. It opens and
populates the native resident database, then creates a second RocksDB from a
native snapshot. That leaves an unreported database, cache, and page-cache
footprint in the control process.

The corrected subject paths are:

```text
native
  exact object fixture + exact txLog tail
    -> one RocksDB resident engine
    -> objectKV RangeEngine snapshot API
    -> measured point reads

direct control
  exact object fixture + exact txLog tail
    -> one directly owned RocksDB
    -> direct RocksDB point reads
```

The control must construct its logical image directly from verified object
records plus retained txLog mutations. It may not read through, snapshot, or
open a native resident engine. A process inventory gate requires exactly one
database and one measured block cache for both subjects.

This compares the objectKV RangeEngine abstraction cost against a matched
RocksDB control. Setup time remains reported and excluded from hot-read
latency, throughput, CPU, and physical-byte deltas.

## D3. Fixture and trace randomness are independent

The fixture seed is always 4244. Read-trace seeds are 1103, 2207, and 3301.
Each sample receipt binds the fixture ID, tail ID, trace seed, and serialized
trace digest.

One fixture removes data-layout variance from the cache and skew question.
Fixture-byte sensitivity becomes a later robustness experiment. This also
avoids publishing three 1 GiB bases that do not strengthen T27's primary
claim.

## D4. The immutable plan contains 540 fresh subject positions

The cache and skew axes are:

| Point | Block cache bytes | Coverage | Access trace |
|---|---:|---:|---|
| C50-Z08 | 536,870,912 | 50% | Zipf 0.8 |
| C50-Z14 | 536,870,912 | 50% | Zipf 1.4 |
| C50-Z20 | 536,870,912 | 50% | Zipf 2.0 |
| C20-Z08 | 214,748,364 | 20% | Zipf 0.8 |
| C20-Z14 | 214,748,364 | 20% | Zipf 1.4 |
| C20-Z20 | 214,748,364 | 20% | Zipf 2.0 |
| C05-Z08 | 53,687,091 | 5% | Zipf 0.8 |
| C05-Z14 | 53,687,091 | 5% | Zipf 1.4 |
| C05-Z20 | 53,687,091 | 5% | Zipf 2.0 |

For each of 27 cache/skew/trace strata, execute five ABBA blocks:

```text
A native -> B direct -> B direct -> A native
```

Every position uses a fresh process, fresh empty mutable directory, 200,000
warmup reads, 1,000,000 measured reads, and eight clients. The full plan is:

```text
3 cache levels x 3 skews x 3 trace seeds x 5 blocks x 4 positions
= 540 fresh subject invocations
= 540 million measured reads
```

The controller writes and hashes the complete plan before execution. Every
receipt carries plan ID, ordinal, subject, AB or BA position, fixture and tail
identity, trace digest, option digest, machine identity, and timestamps. AABB,
missing, duplicated, relabeled, or overlapping positions invalidate the
affected stratum.

This is intentionally more expensive than 15 samples inside one process.
Only resetting the RocksDB block cache leaves process, allocator, file
descriptor, database, and page-cache state shared across samples.

## D5. Direct reads admit the cache-to-NVMe mechanism

Both subjects use effective RocksDB `direct_reads=true` for the 27 admitting
strata. The result is cache-to-local-NVMe mechanism evidence, not a claim about
the default buffered production profile.

Two buffered product sentinels run separately after close and fresh-process
reopen:

- 20% cache with Zipf 1.4;
- 5% cache with Zipf 0.8.

Each sentinel must prove low SST page residency before warmup using `mincore`
or equivalent evidence. It then permits the normal warmup to establish a
steady-state buffered profile. Buffered results never enter the direct-I/O
admission ratios.

## D6. Infrastructure and required gates

Use the private GCP R0 serving topology:

```text
private n2-standard-8 runner
  -> 375 GiB local NVMe, ext4, subject scratch
  -> 200 GiB pd-ssd, stable authority media
  -> regional versioned GCS, immutable fixture
  -> private OTLP path to a separate collector
```

T27 isolates one serving engine and its cache hierarchy, so it does not require
three machines. Every stratum gates independently in AB and BA position:

1. exact fixture, tail, logical image, trace, option, machine, and device
   identity;
2. exact descriptor and complete closure reopened with zero generation path;
3. one empty anchor and zero base values in txLog;
4. exactly one subject database and block cache;
5. fresh process and empty mutable directory;
6. no build, flush, or compaction work in warmup or measurement;
7. measured-window object operations equal zero;
8. block-cache usage at most 105 percent of the declared budget;
9. native throughput at least 0.80x matched control;
10. native p99 at most 1.20x matched control;
11. native CPU nanoseconds per read at most 1.25x matched control;
12. native physical bytes per read and RocksDB read amplification at most
    1.25x matched control;
13. all required OTel signals and exact machine identity;
14. bounded scratch and complete lease teardown after evidence capture.

One failed stratum keeps T27 `[EVALUATING]`. It does not erase passing strata
or stop the objectKV program. It selects the next resident-engine, cache, or
measurement correction.

## D7. Required poisons

Before the full plan, the controller must discard:

- missing or corrupted locator envelope;
- wrong fixture ID, descriptor length, SHA, bucket, prefix, or generation;
- consumer credentials that permit PUT, LIST, or DELETE;
- local generation presented as fixture reuse;
- direct control that opens a hidden native database or second cache;
- fixture and trace seeds coupled again;
- candidate and control with different locator, tail, options, cache, or I/O
  treatment;
- reused mutable subject directory;
- AABB, missing, duplicated, or relabeled plan receipts;
- skipped block-cache eviction;
- counter reset or wrap;
- build, flush, compaction, or object I/O in the measured window;
- buffered execution labeled direct or NVMe;
- buffered sentinel with resident SST pages before warmup;
- concurrent benchmark processes or lost machine lease;
- missing process, physical-I/O, CPU, fixture, or OTel evidence.

Each poison must flip only its targeted gate. Phase 4's combined poison
treatment is not reused.

## Minimum implementation sequence

1. `[VERIFIED]` Add canonical `FixturePlacementLocatorV1` encoding,
   validation, envelope hashing, and corrupt-field unit tests. Commit `d5018bc`
   passed the focused remote suite with the RocksDB feature enabled.
2. `[VERIFIED]` Add separate fixture preparation and required-existing
   consumer commands. Commit `28a732f` prepared one fixture under writer
   credentials, then native and direct consumers exact-opened its generation
   under object-viewer credentials in separate invocations.
3. `[VERIFIED]` Build a standalone direct subject that never opens the native
   resident engine. Commit `19b4e11` passed seven focused remote tests and one
   actual two-worker kill/replacement controller trace. The trace reported no
   native resident provider, exact base-plus-tail identity, zero hot-window
   object requests, and one directly owned RocksDB image.
4. `[VERIFIED]` Split fixture seed from trace seed at the resident validator.
   Commit `1cfad27` reduced the reproduced native aggregate failure from 4,080
   to zero without changing the native/control trace digest. The plan runner
   derives the canonical tail from fixture seed 4244 rather than from any of
   the three independent read-trace seeds. The cross-seed GCS replay is now
   `[VERIFIED]`.
5. `[VERIFIED]` Build and validate the immutable ABBA plan controller at the
   64 MiB preflight profile. The 1 GiB profile is `[CODE-COMPLETE]`.
   The runner freezes either four 64 MiB preflight positions or 540 1 GiB
   admission positions, starts one sequential evaluator process per position,
   and seals one receipt with the plan, locator, tail, trace, options, process,
   executable, machine, boot, NVMe device, cache, CPU, I/O, and timing
   identities. Twenty-seven focused plan and controller tests reject AABB, missing,
   altered-option, execution-drift, wrapper-substitution, reused-process,
   overlapping, cross-lease, systematic tail or trace, hidden-provider,
   implicit-cache, malformed-raw-evidence, zero-pressure, telemetry-drift,
   exporter-completion, host-lock-contention, and performance-regression
   poisons. Failed comparisons or exporter completion seal a `passed=false`
   run receipt before the command exits nonzero. The controller derives its
   single lock path from machine and NVMe identity, requires an OTLP endpoint,
   then flushes and shuts down logs, metrics, and traces before binding all six
   outcomes into the receipt.
6. `[VERIFIED]` A valid fixture preparation under object-viewer credentials
   failed with permission denied and created zero objects. `[CODE-COMPLETE]`
   `t27-plan-poison-check` now authenticates one valid source plan, creates an
   exact AABB, missing-position, or option-mismatch artifact, recomputes its
   internal digest, and passes only when the production decoder returns the
   intended frozen-contract rejection. Its schema-bound receipt also rejects
   artifact tampering. `[CODE-COMPLETE]` `t27-position-poison-check` also
   authenticates a real direct-position receipt, adds one hidden native
   provider without changing its measurements, recomputes the position digest,
   and passes only on the intended runtime-inventory rejection. Twenty-five
   focused library tests pass locally. The commands then rejected the exact
   AABB, missing-position, option-mismatch, and hidden-provider artifacts built
   from the frozen GCP plan and direct-position receipt. The missing-locator
   process exited before plan creation, and the versioned 20-object fixture
   manifest was unchanged. Eight structured artifacts are immutable in GCS.
7. `[VERIFIED]` Source `9cf5014` prepared one 1,073,741,824-byte logical
   fixture as 266 objects totaling 1,101,701,925 bytes. It revoked writer
   authority, exact-opened descriptor generation `1788020925446068` under
   object-viewer credentials, and froze plan
   `b76be02aa012ce3646104e56c1b9c2c6118ee046b33a419103ae7bfdba433de2`.
   The independent inventory found exactly 540 positions, 27 strata, 270
   native subjects, and 270 direct subjects. Versioned GCS retains the source,
   machine receipt, locator, and plan. The viewer binding and all nine leased
   resources were removed after capture.
8. `[PROPOSED]` Add and poison an authenticated execution-incarnation command.
   Preserve the exact runtime source, executable, lockfile, fixture, semantic
   oracle, and 540 positions while rebinding only the live machine, boot, NVMe,
   receipt, scratch, and lease fields. Rebuild the source archive at the same
   path and require the target executable to retain digest `f3471d07`.
9. `[PROPOSED]` Execute the 27 admitting strata and two buffered sentinels,
   preserve every partial failure, update the master matrix, and remove the
   leased infrastructure.

## Review disposition

The adversarial review's six blocking findings are closed. The corrected
phase-5 fresh-process runner and its 64 MiB preflight are `[VERIFIED]`, while
the full T27 curve remains `[EVALUATING]`. It binds the measured nested worker, executable, lockfile,
machine receipt, boot, NVMe device, host-global lease, independently derived
oracle, subject-specific RocksDB topology, raw report, cache pressure, OTLP
emission and exporter completion, and AB/BA gates. Locator
serialization, the clean direct-control construction boundary, separate
preparation and read-only consumption, and the base-seed boundary are
`[VERIFIED]`. The preflight retained 0.8652x and 0.9739x direct RocksDB
throughput while clearing p99, CPU/read, physical-read, amplification,
pressure, and telemetry gates in both process orders. The negative controls
and the 1 GiB preparation boundary are now `[VERIFIED]`. The machine-bound plan
became historical when its runner was destroyed. The next experiment adds an
authenticated execution incarnation, proves that the workload digest is
unchanged, and executes the newly bound plan before teardown. Prior
cross-invocation evidence is in
`docs/artifacts/eval-receipts/t27-gcs-placement-boundary-gcp-r0-2026-08-28/README.md`.
The preflight evidence is in
`docs/artifacts/eval-receipts/t27-fresh-process-preflight-gcp-r0-2026-08-29/README.md`.
The five negative-control results are in
`docs/artifacts/eval-receipts/t27-preflight-poisons-r0-2026-08-29/README.md`.
The 1 GiB fixture and plan receipt is in
`docs/artifacts/eval-receipts/t27-1gib-fixture-plan-gcp-r0-2026-08-29/README.md`.
