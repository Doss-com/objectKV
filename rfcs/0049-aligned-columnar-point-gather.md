# RFC-0049: Aligned columnar point gather

- Status: `[EVALUATING]`, immutable GCS publication and preflight `[VERIFIED]`
- Authors: DOSS
- Created: 2026-08-30
- Corrects: RFC-0048 C5 point path
- Scope: eval-only C5v2 media, generation-pinned GCS point and projected-scan curve

## Decision to review

Keep C5's projection-only columnar scan path, replace its sequential point
gather with an aligned stripe pair, and evaluate the new format against the
unchanged RFC-0048 C0 object closure.

```text
resident compact group directory
  key -> group ordinal
           |-> projection frame range -+
           |-> payload frame range -----+-> concurrent generation-pinned GETs
                                           -> verify both Merkle proofs
                                           -> choose visible MVCC record
                                           -> reconstruct exact value
```

C5v2 stores the projection records and opaque payloads for the same bounded
MVCC record group in two independently range-readable frames. The compact
resident directory identifies both ranges before either data object is read.
Point lookup starts both reads together and waits for the slower call. A
projection-only scan reads only projection frames and does not touch payload
media.

This is a new format, fixture, and execution plan. RFC-0048 remains a retained
rejection. Its plan, thresholds, receipts, and C5v1 bytes are not rewritten.

## Evidence that opens this work

The RFC-0048 read-only GCS preflight measured:

| Metric | C0 indexed row | C5v1 columnar | Ratio |
|---|---:|---:|---:|
| point p99 | 90.791 ms | 230.636 ms | 2.540x |
| provider-call p99 | 90.450 ms | 95.907 ms | 1.060x |
| point response bytes | 16,585,027 | 5,819,521 | 0.351x |
| projected scan rows/s | 2,127.8 | 67,227.3 | 31.595x |
| projected scan bytes | 13,105,844 | 1,527,824 | 0.117x |

C5v1 missed the frozen point preflight guard of 2.50x by 1.61 percent. Its
individual object request was not materially slower than C0. The remaining
exposure is structural: C5v1 must decode the projection stripe before it knows
which payload page to request, so two provider tails compose serially.

The 31.595x projected-scan result establishes that replacing the columnar
media with a row sidecar would discard demonstrated leverage. The correction
must preserve projection-only scan I/O while removing sequential point I/O.

## Implementation and preflight result

Source `8ff14a2` implements the exact C5v2 encoder, decoder, compact aligned
index, Merkle range proofs, concurrent point gather, immutable root closure,
and projection-only DataFusion source. The Rust encoder byte-matches the
independent JavaScript oracle for all 13,695,766 media bytes.

The real GCS viewer-only preflight measured:

| Metric | C0 indexed row | C5v2 aligned columnar | Ratio |
|---|---:|---:|---:|
| point p50 | 36.737 ms | 38.113 ms | 1.037x |
| point p95 | 82.928 ms | 65.777 ms | 0.793x |
| point p99 | 130.172 ms | 113.082 ms | 0.869x |
| point p99.9 | 251.138 ms | 135.772 ms | 0.541x |
| point response bytes | 16,585,027 | 4,422,951 | 0.267x |
| projected scan rows/s | 1,885.6 | 59,758.5 | 31.692x |
| projected scan bytes | 13,105,844 | 1,701,414 | 0.130x |
| projected scan GETs | 203 | 7 | 0.034x |

All 256 projection and payload request pairs overlapped. Both subjects returned
the exact independent point outcomes and projected snapshot with zero
anomalies. C5v2 performed zero opaque-payload reads during the scan. The
preflight passes its 2.50x point and 1.25x scan guards. Admission remains
`[EVALUATING]` until the frozen 15-block curve and OTel requirements pass.

Evidence:
`docs/artifacts/eval-receipts/rfc0049-t28-aligned-preflight-gcp-r0-2026-08-30/README.md`.

Admission r1 started against the immutable GCS fixture and stopped at position
2 of 90. The new fixture-exact correlation code selected the first descriptor
with role `data`, but C0 correctly contains two immutable data objects. A read
against the second object was rejected before aggregation. This is an
evaluator failure, not a kernel or curve result. The sealed failed run is
retained under SHA-256
`90d2b6c29047edbe3d6b32dff071c69a8d7e1ca4f91ddb3e86fb0c71da49215d`.

Admission r2 changes only descriptor selection: it finds the exact observed
object key among all descriptors with the expected role, then checks the
generation, returned range, length, and response bytes against that object.
Both live correlation and persisted replay use the same selection function.
The original 1,024-read C0 position now passes with reads distributed across
both data objects, 1,024 bounded generation-pinned GETs, and zero correctness
anomalies. The full remote evaluator library suite passes 157 of 157 tests
before the two replay-path review regressions were added; both new focused
regressions also pass.

## Admission controller

`[CODE-COMPLETE]` The Rust admission controller now owns the complete 60-point
position and 30-scan position execution. Every position runs in a fresh child
process and binds the controller, physical plan, admission plan, position plan,
binary, process, subject, seed, block, and receipt digest.

The finalizer replays the persisted evidence graph instead of trusting stored
aggregates. It authenticates both plans, the postpublication operation plan,
oracle, candidate and source locators, candidate build edge, runtime
`Cargo.lock`, benchmark machine, reader-only IAM, telemetry endpoint, media
inventory, every provider key, generation and range, every child binding, and
the raw logs, metrics and traces exports. Collector counts are derived from
actual `logRecords`, metric `dataPoints`, and spans whose complete run resource
matches the controller run. A resealed locator with a changed object generation
is a required negative control.

Every failure after telemetry starts attempts to seal `failed-run.json`. A
valid run that misses any performance or telemetry gate emits a typed
`verified = false` verdict. The frozen admission-plan SHA-256 is
`1faec4b6eabd37ae99f2ac3309edec659915705ab31ab5e2c2f59cf7e784f01a`.
The GCP runner passed 155 of 155 evaluator tests and the changed-surface strict
Clippy gate. Fable's final adversarial review returned `SHIP`.

The controller is not a performance result. The RFC remains `[EVALUATING]`
until the one frozen GCS execution and independent collector finalization seal
either a passing or failing receipt.

## Format boundary

The format identity is `okv.columnar-overlay.v2`. It is an evaluation format,
not a stable public objectKV media contract. C5v1 remains readable by its
current decoder. C5v2 receives new magic values, decoders, compatibility
fixtures, capabilities, and object names. A reader never guesses a version
from object contents after manifest selection.

One C5v2 child contains:

```text
active manifest
  -> compact group index
       group count
       projection and payload object lengths
       projection and payload Merkle roots
       ordered entries
         first key | projection offset | payload offset
  -> projection object
       projection frame 0
       projection frame 1
       ...
  -> payload object
       payload frame 0
       payload frame 1
       ...
```

The manifest authenticates the complete index and both complete data objects.
The index authenticates the two Merkle roots and closes both offset spaces.
Each fetched frame carries the proof needed to authenticate its exact content
against the corresponding resident root.

## Aligned record groups

The encoder walks canonical key chains ordered by key ascending. Each chain is
ordered by version descending. Starting with an empty group, it applies this
exact greedy rule:

1. Reject an empty chain or a chain longer than 32 records.
2. If the current group is nonempty and adding the complete next chain would
   make the group longer than 32 records, close the current group first.
3. Append the complete chain to the current group.
4. Close the final nonempty group after the last chain.

The algorithm is `ordered-key-chain-greedy-v1`. It produces groups of at most
32 MVCC records and never splits one key's version chain. The first record key
is the group's lookup fence. The next group's first key is the exclusive upper
lookup fence. The final group covers all greater lookup keys and returns true
absence when the decoded projection has no matching record.

For this candidate, one key with more than 32 retained versions is rejected at
publication. This is an explicit eval-format bound. A later production format
may use overflow groups or retention-aware version directories, but neither is
introduced before this curve proves the basic point geometry.

The paired frames have the same group ordinal and record count:

```text
projection frame N                    payload frame N
  frame header                          frame header
  key/version/MVCC fields               concatenated opaque values
  typed projected columns               frame-local value bytes
  payload offset + length               Merkle proof
  Merkle proof
```

Payload offsets are relative to the decoded payload frame, not the complete
payload object. Tombstones have zero offset and length. Values may not cross a
payload frame. The encoder rejects a single value or complete group whose
framed bytes exceed the declared maximum point-fetch budget.

## Compact resident index

Adding a version directory entry for every one of the fixture's 25,014 MVCC
records would violate the unchanged resident-metadata gate. C5v2 instead keeps
one 24-byte logical entry per group:

```text
u64 first_key
u64 projection_offset
u64 payload_offset
```

The next entry supplies both range ends. Object lengths close the final entry.
Offsets are strictly increasing and start at zero. Group keys are strictly
increasing because key histories cannot split. Decoding rejects duplicate
keys, empty ranges, non-closing offsets, arithmetic overflow, excessive group
counts, unknown flags, and roots of the wrong length.

Per-frame digests do not live in the index. Two SHA-256 Merkle roots remain
resident. Each data frame carries its ordinal, framed-content length, record
count, and Merkle proof. The proof is verified before projection decoding or
payload slicing. The group ordinal, index fence, frame count, and both frame
headers must agree.

This preserves cryptographic range authentication without placing one digest
per projection and payload frame in resident memory. Generation pinning still
rejects replacement media before byte validation.

## Frozen wire and Merkle construction

Every integer is unsigned big-endian except signed `quantity`. Index entries
are exactly 24 bytes. Projection records are exactly 57 bytes:

```text
index entry
  u64 first_key
  u64 projection_offset
  u64 payload_offset

projection record
  u64 key | u64 version | u8 operation
  u32 tenant | u16 category | u16 flags | i64 quantity
  u64 updated_version | u64 checksum
  u32 frame_local_payload_offset | u32 payload_length
```

The operation is `0` for tombstone and `1` for value. Tombstones require zero
typed fields, zero payload offset, and zero payload length. Values require a
nonzero payload length.

Every projection and payload frame begins with this exact 28-byte header:

```text
4-byte kind magic | u16 format version | u16 flags
u32 group ordinal | u32 total groups | u32 record count
u32 content bytes | u16 proof nodes | u16 reserved
```

Kind magics are `OKP2` and `OKV2`; format version is 2; flags and reserved are
zero. Projection content is the fixed-width records in canonical order.
Payload content is the concatenated opaque value suffixes in that same order.
The complete Merkle proof follows the content as 32-byte nodes ordered from
leaf level to root level.

The leaf hash is:

```text
SHA256(kind-specific domain || complete 28-byte header || frame content)
```

The projection domain is `okv-c5v2-projection-leaf-v1\0`; the payload domain
is `okv-c5v2-payload-leaf-v1\0`. Parent nodes are
`SHA256("okv-c5v2-merkle-node-v1\0" || left || right)`. An odd final node is
paired with itself. A one-leaf tree uses the leaf hash as its root. Proof
direction is derived from the zero-based group ordinal at each level, and the
proof length must equal the number of reductions required to reach one node.
Unknown flags, surplus proof nodes, and trailing frame bytes fail closed.

The index magic is `OKI2`. Its fixed header binds format version, flags,
generation, group target, maximum versions per key, group count, both object
lengths, and both 32-byte Merkle roots. Ordered entries follow, then one
SHA-256 checksum over the header and entries. The manifest magic is `OKVCM2`;
it prefixes the compact JSON manifest in the frozen reference field order and
appends SHA-256 over magic plus JSON. The positive compatibility fixture freezes
the exact index, projection, payload, and manifest bytes.

## Independent physical oracle

Candidate code may consume and verify these artifacts but may not generate or
rewrite them:

| Artifact | SHA-256 |
|---|---|
| independent generator `evals/oracles/t28-aligned-columnar-v2.mjs` | `be32d0ac4374f2f39e8ef7873d396ffef4e95eeeb17dfb5b8a21bfe87273e980` |
| full T28 physical oracle | `f2c2417eea48aa9c30e0c15554e5edb14aaff078e00cd2133066be3a21853b65` |
| positive binary compatibility fixture | `83e97b71674ad93c2359bbdb54628b5ab09ed64fc4021efa230f50a33862304d` |
| corrupted projection compatibility fixture | `40223d127ef436d9453ee05558a43e71332804d95966e4e8c43bbb7058fc5da0` |
| frozen physical and evaluation plan | `5b6f2ee2ceaeabae78ff689f33c42fc2bc2022070970e6bb66a1ea410be17d61` |

The standalone generator imports no objectKV crate or package. It independently
recreates the canonical T28 history, applies the exact grouping and wire rules,
and freezes complete-object digests, Merkle roots, frame geometry, and the
absolute point-byte ceiling before candidate implementation.

The expected full-fixture geometry is:

| Metric | Frozen value |
|---|---:|
| groups | 792 |
| records | 25,014 |
| maximum records per group | 32 |
| Merkle proof depth | 10 |
| resident index bytes | 19,148 |
| projection bytes | 1,701,414 |
| payload bytes | 11,974,176 |
| manifest bytes | 1,028 |
| total C5v2 media bytes | 13,695,766 |
| maximum projection plus payload frame bytes | 17,880 |
| reused C0 maximum point bytes | 65,524 |
| frame-pair/C0 byte ratio | 0.273x |

The absolute frame-pair ceiling is 32,762 bytes, exactly one half of the reused
C0 closure's observed 65,524-byte maximum. The expected index is 0.996x the
reused C0 resident metadata before the small manifest is added. Expected total
C5v2 media is 1.043x C0. These are format-oracle predictions, not performance
receipts.

## Point algorithm

For key `K` and read version `T`:

1. Locate the greatest group fence whose `first_key <= K`.
2. Derive both exact object ranges from entry `N`, entry `N+1`, or the object
   lengths for the final group.
3. Start the projection and payload generation-pinned range GETs in one
   scheduler turn.
4. Join both calls without starting a third call or retrying either call.
5. Verify generation, returned range, frame header, Merkle proof, ordinal, and
   paired record count.
6. Decode the projection and choose the newest record for `K` with
   `version <= T`.
7. Return true absence, tombstone, or reconstruct the exact value from the
   frame-local payload slice.

The point path intentionally fetches the payload frame before it knows whether
the visible result is absent or a tombstone. The extra bounded bytes buy one
provider round trip. A later resident negative/tombstone summary is eligible
only if this experiment shows that those bytes matter.

The measured implementation must expose these counters:

- point operations and exact outcomes by outcome kind;
- projection and payload attempts, bytes, and latency distributions;
- paired-start skew and pair completion latency;
- pairs with both requests in flight before either completes;
- provider max-of-pair latency and end-to-end local residual;
- retries, full-object requests, generation mismatches, and proof failures;
- resident metadata and peak fetch bytes.

For every indexed measured point, both request start events must precede the
first request completion event. A task pool with two sequential requests does
not satisfy this requirement.

## Projected-scan algorithm

DataFusion keeps the RFC-0048 logical query and output oracle. Its source
coalesces adjacent projection frames into at most 256 KiB generation-pinned
ranges. It verifies and decodes every complete frame inside each fetched
range. It issues zero payload requests.

Projection frames become smaller than C5v1 stripes, but coalescing preserves
the low provider-request count. Merkle proofs add media bytes. The unchanged
scan-byte and stored-amplification gates bound that cost.

## Immutable fixture and authority plan

The experiment reuses, by exact generation and digest, the RFC-0048 C0 child
closure. It creates only a C5v2 child and a new typed root authenticating the
shared canonical history and both selected children.

```text
RFC-0048 C0 closure, unchanged -------+
                                      +-> new typed root -> frozen plan
new C5v2 child, writer publication ---+
                                      writer authority revoked
                                      fresh objectViewer positions
```

The canonical history, schema, read versions, point traces, point outcomes,
ordered projected rows, and aggregate oracle remain byte-identical to
RFC-0048. A new physical-format plan binds group rows, frame bounds, Merkle
construction, C5v2 format identity, reused C0 generations, new C5v2
generations, executable identity, and every threshold. The postpublication
execution plan binds both the unchanged logical oracle and the new physical
plan.

Publication, authority revocation, denied-create probe, read-only exact open,
fresh processes, cache state, no-retry transport, paired ordering, OTel
completion, and durable receipt rules remain those of RFC-0048. Logical point
concurrency remains eight for both subjects. The transport wrapper allows at
most eight simultaneous C0 SDK attempts and sixteen simultaneous C5v2 SDK
attempts because every C5v2 logical operation owns one overlapping pair. Both
caps are bound in the plan and receipts.

## Frozen evaluation gates

### Preflight resource guard

One uncounted seed-5701 preflight runs 256 points per subject across eight
tasks and one exact projected scan per subject. It stops before admission if:

- C5v2/C0 end-to-end point p99 exceeds 2.50x;
- C5v2/C0 projected-scan throughput is below 1.25x;
- any correctness, authority, range, retry, proof, byte, metadata, memory, or
  telemetry gate fails.

### Point admission

Three unchanged seeds execute five paired ABBA or BAAB blocks each. Every
block must satisfy:

```text
C5v2 end-to-end point p99 / C0 point p99 <= 2.00
```

Hard gates:

- every point matches the independent RFC-0048 outcome digest;
- zero retries, correctness anomalies, generation mismatches, or proof errors;
- exactly one projection and one payload SDK attempt for every indexed C5v2
  point across value, tombstone, and absent outcomes;
- exactly one C0 data SDK attempt per measured point;
- every C5v2 pair proves concurrent overlap from the shared transport
  wrapper's attempt lifecycle, not candidate-emitted timestamps;
- C5v2 maximum point bytes are at most 0.50x C0;
- no complete-object GET;
- resident metadata is at most 8 MiB and peak fetch memory at most 256 KiB;
- provider pair maximum, end-to-end time, and local residual are reported;
- all six OTel completion checks and independent collector evidence pass.

### Projected-scan admission

The primary statistic remains the nearest-rank median of 15 within-block
throughput ratios:

```text
median(C5v2 rows/s / C0 rows/s) >= 2.00
```

The RFC-0048 scan correctness, zero-payload, at-most-64-GET, at-most-0.50x-C0
bytes, 256 KiB fetch, 128-row batch, concurrency-one, no-retry, and OTel gates
remain unchanged.

### Shared media gates

- C5v2 stored/live amplification is at most 1.10x C0;
- C5v2 compaction write amplification is at most 1.10x C0;
- C5v2 resident metadata is at most 2.00x C0;
- empty-worker recovery reproduces the canonical history digest;
- branch creation references unchanged children without copying their media.

## Required negative controls

The production decoder and controller must reject:

1. a stale or omitted GCS generation;
2. a projection or payload frame with one changed content byte;
3. a valid frame paired with a proof from another group;
4. a projection frame and payload frame with unequal ordinals or counts;
5. a non-monotonic, overlapping, gapped, or non-closing offset directory;
6. a key history split across adjacent groups;
7. more retained versions for one key than the declared format bound;
8. a payload slice outside its paired frame or crossing the frame end;
9. a tombstone with nonzero payload offset or length;
10. a scan that reads any payload byte;
11. one request completing before the second request begins;
12. a measured process with create, delete, or list authority;
13. a receipt, fixture, binary, plan, or root identity mismatch.

Each poison receives a stable error class. Injected failures do not count as
measured performance positions.

## Alternatives

### Add one payload location per MVCC record

Optimizes for: fetching only the selected value's small payload page.

Gives up: the unchanged resident metadata bound. Even a compact entry for each
of 25,014 records materially exceeds the RFC-0048 C0 index.

### Add one payload page per key

Optimizes for: version-aware point routing without reading a projection.

Gives up: bounded metadata for large key counts and a clean scan projection.
It also duplicates part of the MVCC directory in memory.

### Fetch the C5v1 projection and four-page payload slab concurrently

Optimizes for: minimal format change.

Gives up: the frozen point-byte gate. A 128-record slab approaches the C0 row
block before projection bytes and framing.

### Store projection and payload in one interleaved object range

Optimizes for: one point request.

Gives up: projection-only coalesced scans because payload bytes sit between
adjacent projection frames.

### Reintroduce a row sidecar

Optimizes for: familiar cold point geometry.

Gives up: testing whether one typed columnar main can serve both point and scan
paths without duplicated row media.

### Relax the RFC-0048 p99 or metadata gates

Optimizes for: admitting the current implementation.

Gives up: the frozen comparison and hides a measured design defect. The gates
do not change.

## Decision rule

C5v2 advances only if it passes every frozen point, scan, media, authority,
correctness, and telemetry gate. A preflight failure is retained as a new
rejection. No repeated run is allowed without one named code, format,
configuration, or infrastructure change and a new immutable execution plan.

If admitted, C5v2 becomes the typed-object candidate for the next
base-plus-live-tail and cache-refill evaluations. It does not replace C0 for
opaque KV ranges and does not by itself verify the complete RangeEngine hot
path.

Optimizes for: one columnar typed object base with bounded cold points and
material projection-scan leverage.

Gives up: the smallest possible format change, because concurrent point gather
requires alignment and authenticated routing to be encoded into the media.
