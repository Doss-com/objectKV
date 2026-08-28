# RFC-0044: Content-addressed object-frontier fixtures

- Status: `[EVALUATING]`, with phases 0 and 1 `[VERIFIED]`
- Authors: DOSS
- Created: 2026-08-28
- Scope: T27 fixture construction, reuse, and transaction-plane isolation
- Review: Claude Fable 5, no blocker for the local-first slice after the
  documented corrections

## Decision

Build each T27 logical base once as a content-addressed immutable row-object
closure. Start every fresh evaluation transaction authority at that closure's
covered-through version with one canonical empty anchor transaction, then put
only mutations in `(O, C]` through the transaction plane. Native and control
derive separate, immutable resident-image identities from the same fixture.
They never share one mutable RocksDB directory.

This is an evaluator bootstrap, not a production restore or import API. It
uses existing transaction, publication, object, and recovery semantics to
remove base generation from the measured system. A production object-frontier
bootstrap requires a separately accepted authenticated restore contract.

## Context and invariant

The first 64 MiB T27 calibration committed all 65,536 base values through
three replicated transaction-authority processes before publishing the same
values as immutable row objects. Aggregate authority scratch reached about
1.2 GiB, roughly 19 times the logical value bytes, and each subject spent 35 to
40 minutes constructing the base. Native, control, A/B, and B/A repeated that
work independently.

That path measures fixture construction, not the resident read hierarchy. It
also contradicts the intended ownership boundary. Once a complete object
closure covers version `O`, the transaction recovery stream should contain
only changes newer than `O`.

```text
logical fixture F
  │
  ├─ immutable row-object closure covered through O
  │
  ├─ native resident image N(F, options, tail)
  │
  └─ control resident image R(F, options, tail)

fresh transaction authority
  ├─ physical retained stream contains one empty anchor at O
  └─ recovery cursor after O exposes only (O, C]
```

The invariant is:

```text
same fixture ID + same tail ID + same trace ID + matched options
  = comparable logical workload

resident image IDs may differ by subject codec
  = expected physical representation difference
```

No admitted receipt may claim fixture reuse merely because several samples
read one resident process. The same logical fixture ID must appear in native,
control, A/B, and B/A receipts.

## Proposed contract

### Logical fixture descriptor

`ObjectFixtureDescriptorV1` has these semantic fields:

```text
schema_version
generator_version
seed
key_count
value_bytes
logical_bytes
logical_key_value_sha256
base_version
row_object_format_version
target_object_bytes
target_block_bytes
manifest = { key, length, sha256 }
closure_sha256
object_count
object_bytes
```

The JSON representation is for receipts and inspection. `fixture_id` is the
SHA-256 digest of a versioned fixed-field `OKVF1` binary encoding. Unsigned
integers use big-endian bytes, byte strings are length-prefixed, optional
fields use an explicit presence byte, and collections are sorted before
encoding. Object keys in the descriptor are global content keys derived from
object bytes. They never contain `fixture_id`. Provider URI, object revision
tokens, local paths, timestamps, and run IDs are excluded from the encoding.
They are placement evidence, not fixture identity.

The closure digest covers the ordered tuple of every exact object key, length,
and content digest reachable from the manifest. A consumer verifies the
manifest and complete named closure before accepting the descriptor.

### Base version anchor

A fresh evaluation authority establishes `O` by committing one canonical empty
transaction:

```text
read_version:     0
read_conflicts:   []
write_conflicts:  []
mutations:        []
```

The committed log position becomes `O`. The fixture generator creates every
base `RowRecord` at `O`, publishes the content-addressed closure, and asserts:

```text
authority current version       = O
authority serving values        = 0
anchor txLog records             = 1
anchor txLog mutations           = 0
base-value txLog records         = 0
base-value txLog mutation bytes  = 0
fixture manifest covered through = O
every decoded base record version = O
every segment min/max version     = O/O
```

The normal suffix workload then commits against read version `O`. The serving
worker reconstructs the base from the object closure and applies only records
with commit version greater than `O`.

The anchor is safe for this evaluator because the generated base is external
fixture input and no user transaction can observe the authority before the
closure is published. It does not prove that an arbitrary production authority
may trust an externally supplied object frontier.

### Canonical retained tail

The fixture also binds one exact suffix schedule. Candidate and control receive
the same ordered `RetainedTransactionRecord` values, including
`commit_version`, `batch_order`, conflicts, and mutations. Request identity is
not part of a retained transaction record and does not enter `tail_sha256`.
A subject may not change batch formation or issue the same logical mutations at
different log positions.

`tail_sha256` covers the canonical encoding of that exact retained-record
stream. The current T27 native and owned-value control both use the integrated
kernel recovery mode. The similarly named full-hydration recovery control is
not an eligible T27 performance control because it commits its setup history
differently.

### Immutable object reuse

Canonical GCS placement separates global content bytes from fixture metadata:

```text
fixtures/single-range/v1/blobs/sha256/{content_sha256}
fixtures/single-range/v1/descriptors/{fixture_id}.json
```

Every upload uses create-if-absent. An already-present object is reused only
after an exact named GET verifies length and SHA-256. A mismatched existing
object fails closed. LIST is never an authority input.

The first writer stores the descriptor only after the complete closure
verifies. Later subjects open the descriptor by exact key, verify the closure,
and report `fixture_reused=true`. A partial upload has no valid descriptor and
can be retried without overwriting immutable bytes.

Local filesystem diagnostics may rebuild the same fixture in a temporary
directory. They can verify descriptor determinism but cannot claim persisted
cross-subject reuse.

### Resident image identity

Each subject emits a separate `ResidentImageDescriptorV1`:

```text
fixture_id
tail_sha256
subject
engine_provider
engine_format_version
options_sha256
applied_through
record_count
resident_logical_sha256
```

`resident_image_id` is the SHA-256 digest of this semantic descriptor. The
native image uses objectKV's version-bound key encoding. The owned-value
control uses direct user-key to owned-value encoding, so their semantic image
IDs differ by design. Each image must independently reproduce the same exact
logical values at `C`.

`resident_logical_sha256` covers tagged read outcomes in canonical key order
after the exact tail applies. The domain contains every generated base key,
every inserted key, and one declared never-written key. Each entry encodes
`Value(bytes)`, `Tombstone`, or `Absent` distinctly. Native and control must
report the same digest. The owned-value control stores and decodes the same
outcome tags rather than collapsing tombstones into absence. Sentinel reads and
measured hot keys remain useful checks, but they do not substitute for this
full logical image digest.

Physical RocksDB directory bytes and file names are not semantic identity.
Implementations may report `resident_checkpoint_sha256` and
`resident_image_local_bytes` as setup observations, but neither enters
`resident_image_id`. Compaction scheduling and file creation order may change
those physical observations without changing the logical image.

A/B and B/A use fresh subject-local RocksDB directories. They may not
concurrently open or mutate the same directory. Repeated samples may reuse one
read-only subject image only when table reads follow the declared operating-
system page-cache treatment and every sample begins with a fresh declared
RocksDB block-cache state.

### Receipt fields

Every subject reports:

```text
fixture_id
fixture_descriptor_sha256
fixture_reused
fixture_verification_seconds
fixture_object_requests
fixture_object_bytes
base_anchor_version
anchor_txlog_records
anchor_txlog_mutations
base_value_txlog_records
base_value_txlog_mutation_bytes
tail_sha256
resident_image_id
resident_logical_sha256
resident_image_build_seconds
resident_image_local_bytes
resident_checkpoint_sha256
```

Fixture verification and resident-image build stay outside warmup and
measurement windows. They remain visible recovery and setup metrics.

## Failure model

- Process death before the anchor commit produces no fixture.
- Process death after anchor commit but before descriptor publication leaves
  unreferenced immutable objects that a later retry can verify and reuse.
- Missing, short, corrupt, or swapped closure objects reject the descriptor.
- An existing object with the expected key but different bytes fails closed.
- A descriptor whose `base_version` differs from the authority anchor fails
  before suffix commits.
- A decoded base record or segment version range that differs from `O` fails
  before publication.
- A transaction authority containing base values, more than one anchor record,
  or any anchor/base mutation fails the fixture gate.
- A candidate and control with different retained-record streams or tail
  digests fail before measurement.
- A resident image with the wrong fixture, tail, options, subject, or applied
  version fails before measurement.
- Native and control with different complete logical-image digests fail before
  measurement.
- Concurrent mutable use of one resident directory invalidates both samples.
- GCS or local-media failure during setup produces no comparison sample.
- Object access during the measured resident-read window remains a hard
  failure under RFC-0043.

## Alternatives

### Copy one completed transaction-authority snapshot

This reduces repeated consensus work. It preserves the roughly 19 times base
duplication and treats transaction-authority serving values as permanent
storage. It does not test the intended object-base plus suffix boundary.

### Share one RocksDB directory between native and control

This minimizes setup bytes. It is invalid because native and control use
different physical key and value encodings. It also introduces cross-subject
mutation and cache contamination.

### Restore a production authority directly from the object descriptor

This matches the long-term rebuild path. It requires authenticated generation,
tenant, key-range, conflict, retry, and version-space semantics that T27 does
not need. That is a separate one-way protocol decision.

### Keep committing the generated base through txLog

This exercises the complete write path. It optimizes for ingest coverage and
gives up a practical 1 GiB cache experiment. Ingest and objectification debt
belong to T30, where their time and space costs are primary measurements.

## Eval plan

The first implementation slice changes only fixture construction and identity
reporting. It does not change the RFC-0043 trace, cache, hot-read path, metric
aggregator, or admission thresholds.

The owned-value control must be derived from the verified object closure plus
the exact retained tail. Its current independent `base_value` generator path is
removed from T27. Both subjects hash and verify the complete visible image,
including updated, inserted, cleared, and range-cleared keys, before warmup.

The local contract uses a 4 MiB fixture and proves:

1. one empty anchor establishes `O`;
2. the authority contains one empty anchor record, zero base values, zero base
   mutation records, and zero base mutation bytes;
3. the object closure reconstructs every generated value at `O`;
4. all seven existing suffix commands and retained records recover exactly;
5. rebuilding from the same inputs yields the same fixture ID;
6. native and control consume one exact tail digest, produce different
   resident image IDs, and return one identical complete logical-image digest;
7. corrupt-descriptor, mutated-anchor, tail-mismatch, and shared-mutable-image
   poisons discard;
8. several fresh authorities place the anchor at the same `O`;
9. a lost anchor response retried with the same request identity returns the
   original `O` and retains one record, while a different anchor identity is
   rejected before it can establish a second anchor.

The GCP R0 preflight uses the 64 MiB fixture and both subjects. Its primary
metric is fixture setup wall time. It must reduce aggregate transaction-
authority scratch from about 19 times logical values to at most 0.25 times and
finish base setup in at most 300 seconds. Those are mechanism gates, not T27
performance admission.

The 1 GiB admission proceeds only when four subject receipts share one fixture
ID and one tail ID, at least three report persisted reuse, every receipt reports
one empty anchor plus zero base-value txLog records, and the unchanged RFC-0043
correctness, telemetry, throughput, p99, CPU, and physical-I/O gates pass.

## Compatibility and migration

The descriptor is evaluator schema version 1. Unknown schema versions fail
closed. Changing the generator, row-object format, logical data, object sizing,
or base version creates a new fixture ID. Changing only provider placement does
not.

No public kernel API, transaction command, txLog frame, object-segment format,
or production snapshot format changes in the first slice. The empty anchor is
submitted through the existing transaction API.

The minimum implementation sequence is:

1. fresh-authority anchor distribution and exact-retry falsifier;
2. local filesystem descriptor, empty-anchor, canonical-tail, complete-closure,
   identity, and poison contracts;
3. 4 MiB local process recovery with fresh subject directories;
4. 64 MiB persisted GCS reuse and setup-bound preflight;
5. frozen 1 GiB T27 suite.

The frozen `native-resident-cache-pressure-rerun-v2` suite and its result
schema remain byte-identical. RFC-0044 receives a new versioned contract suite
before any new receipt field becomes a hard gate. A fixture implementation may
reuse existing runner code, but it may not rewrite prior evaluation evidence.

A future production restore contract must receive its own RFC, command version,
compatibility fixtures, and generation-fenced authorization.

## Unresolved questions

- Whether A/B and B/A should reopen one subject image or use RocksDB checkpoints
  with reflinked files on filesystems that support them.
- Whether the 1 GiB fixture descriptor should live in GCS only or also be
  cached on the persistent runner volume.
- Whether object closure verification should read every byte on every subject
  or use one full verification plus immutable provider revision evidence.
- Which production restore authority validates an object frontier without a
  preceding local commit history.
