# RFC-0044 phase 5: T27 1 GiB admission plan

Status: `[PROPOSED]`, revised after adversarial preimplementation review

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

1. `[PROPOSED]` Add canonical `FixturePlacementLocatorV1` encoding,
   validation, envelope hashing, and corrupt-field unit tests.
2. `[PROPOSED]` Add separate fixture preparation and required-existing
   consumer commands. Split durable fixture placement from run scratch.
3. `[PROPOSED]` Build a standalone direct subject that never opens the native
   resident engine.
4. `[PROPOSED]` Split fixture seed from trace seed and emit one receipt per
   fresh process.
5. `[PROPOSED]` Build and validate the immutable ABBA plan controller.
6. `[PROPOSED]` Pass 64 MiB versions of the missing-locator, read-only-GCS,
   hidden-cache, and AABB poisons.
7. `[PROPOSED]` Prepare the 1 GiB fixture, commit its locator, and freeze the
   suite hash.
8. `[PROPOSED]` Execute the 27 admitting strata and two buffered sentinels,
   preserve every partial failure, update the master matrix, and remove the
   leased infrastructure.

## Review disposition

The adversarial review's five blocking findings are accepted. Phase 5 is not
`[CODE-COMPLETE]` on the current runner. The next code slice is locator
serialization plus a clean direct-control construction boundary. No 1 GiB
performance claim will be made before both pass their negative controls.
