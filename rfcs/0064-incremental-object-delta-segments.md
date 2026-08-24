# RFC-0064: Incremental object delta segments

- Status: accepted for the bounded local prototype; compaction and remote-object proof remain active work
- Created: 2026-08-24
- Depends on: RFC-0039, RFC-0058, RFC-0060, RFC-0063

## Decision

`[DECIDED]` Objectification may advance durable object frontier `O` without
rewriting a complete immutable base. The first incremental format stores one
ordered, content-addressed delta segment containing the exact certified commit
records after the prior object frontier.

```text
F = complete immutable base frontier
O = object durable frontier
C = hot committed frontier

F <= O <= C

ObjectState(O)
  = FullBase(F)
  + DeltaSegment(F, D1]
  + DeltaSegment(D1, D2]
  + ...
  + DeltaSegment(Dn, O]

Database(C)
  = ObjectState(O)
  + certified txLog records in (O, C]
```

This format is a conservative first layer, not the intended final row-segment
encoding. It preserves the original commit envelopes and every required txLog
certificate so the existing replay and serving oracles remain authoritative.
Later compaction may convert the same logical history into a fresh SlateDB,
Parquet, Vortex, or another swappable physical base.

## Why not mutate the current SlateDB base

Opening the current database path for writes would make SlateDB publish a newer
internal manifest before objectKV publication authority selects it. A crash or
failed root publication could leave that manifest physically present. A later
writer that follows SlateDB's latest manifest could then adopt unselected state.

The prototype therefore never mutates an authority-pinned base. Every delta is
an independent immutable object. Only an atomic objectKV durable-root update
adds its descriptor to the ordered lineage.

## Delta format

The generic v1 delta payload contains:

```text
format version
cell, tenant, transaction-system generation
prior object frontier and resulting object frontier
prior and final commit-chain SHA-256
ordered certified commit records
```

Its descriptor contains:

```text
content-addressed data-object reference
prior delta-descriptor SHA-256, or zero for the first layer
prior and resulting object frontiers
prior and final commit-chain SHA-256
record count and encoded mutation bytes
```

The content key is:

```text
{database_path}/deltas/{after_version}-{through_version}-{sha256}.segment
```

The segment is valid only when:

- identity matches the full base and every other layer;
- records are strictly version-ordered and nonempty;
- the first record extends the prior commit-chain digest;
- every subsequent record extends the preceding digest;
- the final version and chain digest match the descriptor;
- the object length and SHA-256 match its reference;
- every record has the required certificate coverage when opened for serving.

## Durable-root compatibility

`[DECIDED]` PostgreSQL durable-root format 2 adds an ordered
`object_deltas` field. The new reader accepts legacy format 1 as a full base
with no delta layers. An old reader must reject format 2 before serving because
it cannot reconstruct a txLog prefix that may already have been popped.

The implementation must retain an exact format-1 durable-root fixture and an
exact format-1 delta-segment fixture. Re-encoding the fixtures must preserve
their semantic identity; corruption, omission, reordering, or a broken lineage
must fail closed.

## Scheduling and object count

The checkpoint capture may request at most one segment for the newest pending
frontier. Page writes do not create objects. A segment batches every eligible
record in `(O, B]` for that capture, so one page is never intentionally mapped
to one object.

The prototype may produce one small segment per checkpoint. Production must
add byte, record, and time thresholds plus compaction before this becomes an
object-count claim. Small segments remain visible as compaction debt.

## Publication and deletion

A stable root names the full base closure, every ordered delta object through
`O`, and the certified txLog tail `(O, B]`. Publication authority verifies that
complete closure before selection. Its deletion capability may pop txLog only
through `O`.

Object upload or local durable-root selection alone never authorizes pop. A
failed stable publication leaves the new delta unreachable or locally staged;
stable state and txLog retention remain unchanged.

## Recovery

A replacement KV Runtime:

1. authenticates the full base descriptor and closure;
2. authenticates and orders every delta descriptor and object;
3. concatenates their exact certified records above the full base;
4. loads the retained certified txLog suffix above `O`;
5. opens one serving view from base plus object deltas plus hot tail;
6. refuses before serving on any identity, chain, object, certificate, or
   frontier mismatch.

## Tradeoff

Optimizes for: write amplification proportional to changed history, immutable
publication, exact restart, and a clean separation between flush and
compaction.

Gives up: a minimal long-lived object format. Certificates and full commit
envelopes add bytes; read-open authentication grows with layer count; small
checkpoints can create small objects; old layers require a later compaction and
garbage-collection protocol.

## Eval gate

The owning suite is `evals/suites/postgres-object-delta.toml`. Its primary
metric is objectification rewrite ratio:

```text
encoded delta bytes / logical changed page bytes
```

Correctness, source-free restart, exact stable closure, pop bounds, fixture
compatibility, and corruption controls are hard gates. The first candidate is
kept only when it writes no replacement full-base SST and the object bytes are
independent of untouched relation size for an identical mutation suffix.

The initial local baseline uses five deterministic high-entropy seeds, a
128-page relation, and one changed 8 KiB page. It records delta materialization,
activation, source-free restart, and a reference full-base rewrite for the same
final logical state. Repeated-byte pages are not accepted as economics evidence
because compression would make the comparison artificial.

`[PROPOSED]` The first targets, pending the baseline, are:

- encoded delta bytes at or below 2x logical changed bytes;
- delta bytes at or below 10 percent of a 1 MiB replacement full base;
- local delta materialization p50 at or below 10 ms;
- local activation p50 at or below 25 ms;
- local source-free restart p50 at or below 50 ms.

These are calibration targets, not hard gates. Remote GCS or S3 targets require
a separate networked profile and must not be inferred from the local filesystem.

### Relation-size crossover contract

`[DECIDED]` The first crossover suite is
`evals/suites/postgres-object-delta-crossover.toml`. It holds seed, changed
block 1, changed-page bytes, certificate policy, and mutation suffix constant
while varying the complete base through 2, 128, 4,096, and 65,536 pages. Two
pages is the minimum point because every subject must mutate the same block.

The replacement full base is materialized in an isolated reference root. Its
bytes and duration are observations, never candidate writes, so the candidate
root must still create zero replacement SSTs. The primary metric is:

```text
(delta materialization + delta activation duration)
----------------------------------------------------
isolated replacement full-base materialization duration
```

Exact replay, source-free restart, full row equality, one changed 8 KiB page,
an identical delta suffix within each seed, and zero replacement SSTs in the
candidate root remain hard gates. The existing full-base-in-candidate-root
subject remains a negative control and must discard.

`[PROPOSED]` Calibration expects delta bytes to vary by no more than 5 percent
across relation sizes, latency crossover by 4,096 pages, delta bytes below 1
percent of a 4,096-page rewrite, and below 0.1 percent of a 65,536-page rewrite.
These thresholds select the next experiment; they are not production claims.

`[EXISTS]` Candidate `efa9d54` passes all four calibration targets. Delta bytes
are identical within each seed. At 4,096 pages, the time ratio is 0.3359x and
the byte ratio is 0.2622 percent. At 65,536 pages, the time ratio is 0.2502x and
the byte ratio is 0.01639 percent. The JSON encoding still fails the separate
changed-byte target at 11.106x. The full report is
[`docs/research/postgres-object-delta-crossover-2026-08-24.md`](../docs/research/postgres-object-delta-crossover-2026-08-24.md).

## Open questions

1. At what byte, record, age, and layer thresholds should the objectifier emit
   a segment or compact a lineage?
2. Should production delta segments retain full certificates, or should a
   publication-root certificate replace per-record proof after compaction?
3. How should range splits divide an in-flight segment without copying it?
4. Which compaction output should PostgreSQL point reads prefer before the
   generic row-segment format is available?
5. How are abandoned staged deltas collected without relying on object-store
   listing as authority?
