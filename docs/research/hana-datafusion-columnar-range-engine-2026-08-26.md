# HANA and DataFusion implications for the objectKV RangeEngine

Status: `[EVALUATING]`, 2026-08-26.

## Call

The RangeEngine should be evaluated as a hybrid columnar runtime, not merely a
row cache in front of analytical files. The permanent typed representation can
be columnar if the same primitives support two access modes:

```text
resident primitive
    contiguous in-memory vector, dictionary, index, or bitmap

paged primitive
    the same logical and byte-compatible structure fetched in bounded pages
```

Writes still require a small mutable transaction delta. Immutable columnar
fragments remain the long-lived representation. The point path uses a resident
primary-key index to find a stable row identifier, then gathers only the
required column slots or pages. The scan path streams the same fragments as
Arrow `RecordBatch` values through DataFusion.

This is materially different from asking Parquet to act as a point store.

## What HANA actually establishes

SAP HANA's published unified table progresses records through three physical
stages:

```text
L1 delta
    write-optimized logical rows
    fast insert, delete, update, and record projection
        |
        v
L2 delta
    column format
    unsorted dictionary
    secondary indexes for point and uniqueness checks
        |
        v
Main fragment
    sorted dictionaries
    compressed column vectors
    inverted indexes for selective point access
```

Queries see one table interface over all fragments. Propagation is
asynchronous. HANA keeps a persistent row identifier across merges, even when
physical row positions change.

HANA's later Native Storage Extension is more directly relevant to objectKV.
It uses a unified persistence format whose column primitives can be loaded
fully into memory or accessed through a bounded buffer cache without format
conversion. Dictionaries, data vectors, indexes, bitmaps, and compressed
blocks can each choose their own load unit. Small resident helper indexes bound
the number of pages needed for point access.

Primary sources:

- Sikka et al., *Efficient Transaction Processing in SAP HANA Database: The
  End of a Column Store Myth*, SIGMOD 2012:
  <https://dl.acm.org/doi/10.1145/2213836.2213946>
- Sherkat et al., *Page As You Go: Piecewise Columnar Access in SAP HANA*,
  PVLDB 2019:
  <https://www.vldb.org/pvldb/vol12/p2047-sherkat.pdf>
- SAP HANA column-store memory management:
  <https://help.sap.com/docs/SAP_HANA_PLATFORM/6b94445c94ae495c83a19646e7c3fd56/bd6e6be8bb5710149e34e14608e07b76.html>
- SAP HANA primary-key indexes:
  <https://help.sap.com/docs/SAP_HANA_PLATFORM/9de0171a6027400bb3b9bee385222eff/7f0ed915ee1d45df8905649a1831b0a5.html>

## What DataFusion establishes

DataFusion's in-memory contract is Arrow. A `RecordBatch` is columnar inside a
bounded horizontal row chunk. Execution operators pull streams of immutable
batches, which gives vectorization, partitioned execution, early output, and
bounded memory.

A custom objectKV provider can expose the storage runtime without converting
it into Parquet:

```text
ZebraTableProvider::scan
    -> RangeFragmentExec per key range or object fragment
    -> asynchronous RecordBatchStream
    -> projection and predicate pushdown
    -> SnapshotOverlayExec over L1 and retained tail
```

Planning must use only manifests and statistics. Object I/O belongs in the
execution stream. Natural key ranges become DataFusion partitions, and declared
ordering avoids unnecessary sort operators.

Primary sources:

- DataFusion Arrow and streaming execution:
  <https://datafusion.apache.org/user-guide/arrow-introduction.html>
- DataFusion custom table providers and execution plans:
  <https://datafusion.apache.org/library-user-guide/custom-table-providers.html>
- Arrow columnar memory format:
  <https://arrow.apache.org/docs/format/Columnar.html>

## Corroboration from other vector engines

The execution-side pattern is not unique to DataFusion:

- DuckDB processes fixed-size vectors, 2,048 tuples by default, and lets flat,
  constant, dictionary, and sequence encodings flow from storage into
  execution. Its unified vector view separates an operator's logical contract
  from the physical encoding:
  <https://duckdb.org/docs/current/internals/vector.html>.
- Velox stores one column across many rows in reference-counted contiguous
  buffers. Flat, constant, and dictionary vectors can share buffers or expose
  views into externally managed memory, while a `RowVector` groups child
  columns for operators:
  <https://facebookincubator.github.io/velox/develop/vectors.html>.
- ClickHouse keeps a sparse primary index resident and uses it to select sorted
  granules. That is an efficient range-scan design, but its own documentation
  contrasts it with a dense per-row index used for point lookup. objectKV
  therefore needs the dense key-to-row-ID index that HANA describes, not only
  ClickHouse-style sparse marks:
  <https://clickhouse.com/docs/guides/clickhouse/data-modelling/sparse-primary-indexes>.

The common unit is a bounded horizontal batch containing independently
addressable column buffers. It is neither one column for the entire database
nor one row object per key.

## objectKV adaptation

HANA's local persistence and page cache cannot be copied directly because an
object GET has a much higher fixed cost than a disk or memory page lookup.
objectKV therefore needs larger fetch units, asynchronous prefetch, immutable
object generations, and explicit request accounting.

```text
quorum txLog through C
        |
        v
RangeEngine L1
    bounded mutable row delta
    conflict and recent-version indexes
        |
        v
L2 column delta
    sealed Arrow-sized batches
    append-friendly dictionaries and row IDs
        |
        v
Main object fragments through O
    immutable column primitives
    primary key -> stable row ID index
    dictionaries, vectors, validity and delete bitmaps
    per-primitive checksums and page maps
        |
        +---- resident mode: load selected primitives into RAM
        |
        +---- paged mode: fetch bounded object ranges into RAM or NVMe cache
```

The reconstructable truth remains:

```text
ManifestedColumnState(O) + txLog(O, C]
```

The row delta is not a second permanent database. It is bounded serving and
transaction state rebuilt from the retained suffix. A complete durable row
sidecar is optional rather than assumed.

## Required primitives

1. A stable logical row ID that survives compaction and physical reorder.
2. A primary-key index mapping keys to fragment, row ID, and visibility data.
3. Column vectors split into independently addressable, checksummed pages.
4. A value or blob column with direct offset lookup for generic KV payloads.
5. Delete and MVCC visibility structures that are consulted before predicates.
6. A bounded L1 row delta and append-friendly L2 column delta.
7. A fragment manifest that publishes every primitive atomically.
8. A cache policy that accounts separately for indexes, dictionaries, data
   vectors, decoded batches, and opaque value pages.
9. A DataFusion source that emits Arrow batches directly from resident or paged
   primitives and overlays the exact live tail.

## Candidate C5

`C5 columnar_range_overlay` stores the opaque payload once in a dedicated
column and stores typed fields in independently addressable column groups. Its
resident primary index maps a key to a stable row position. A cold point read
fetches one metadata microstripe and, for a live value, one checksummed payload
page. The RangeEngine caches decoded projection stripes and payload pages only
as disposable state under an explicit byte limit. A projected scan never
fetches the opaque payload column.

Frozen local gates against the indexed row-object control:

| Curve | Gate |
| --- | --- |
| Cold point requests | at most 2.0x |
| Cold point response bytes | at most 0.50x |
| Cold point p99 | at most 2.0x |
| Warm repeated point object requests | exactly zero |
| Projected scan throughput | at least 3.0x |
| Projected scan opaque payload bytes | exactly zero |
| Stored/live amplification | at most 1.05x |
| Compaction write amplification | at most 1.10x |
| Resident index amplification | at most 2.0x |
| Empty-overlay restart | exact manifest, index, point, and full logical digest |

These thresholds test a mechanism, not a production format. Passing locally
admits a clean GCS comparison and a true Arrow/DataFusion source. Failure
identifies whether the fixed object-request cost, index size, page geometry, or
merge amplification makes columnar permanent truth unsuitable.

## Local mechanism result

`[EVALUATING]` Release-local run
`49d6cd06-28e6-4aee-ac5b-bf8f105f53e0` alternated C5 and the indexed-row
control for seeds 5701 through 5703 over three repeats. The tree was dirty and
OTel was disabled, so this is not a verified result. Every frozen local gate
passed:

| Curve | Result | Gate |
| --- | ---: | ---: |
| Cold point requests | 1.982x | at most 2.0x |
| Cold point response bytes | 0.353x | at most 0.50x |
| Cold point p99 | 0.839x | at most 2.0x |
| Projected scan throughput | 4.718x | at least 3.0x |
| Storage amplification | 1.010x | at most 1.05x |
| Compaction write amplification | 1.010x | at most 1.10x |
| Resident index | 1.170x | at most 2.0x |
| Warm replay object requests | 0 | exactly 0 |
| Projected-scan opaque bytes | 0 | exactly 0 |
| Empty-overlay restart anomalies | 0 | exactly 0 |
| Overlay resident / configured bound | 13,186,555 / 16,777,216 bytes | resident at most bound |

The run reconstructed the complete MVCC history from the manifest, compact
index, projection object, and payload object after discarding the cache. Its
suite and profile hashes are
`c92826009b7a4b73577cfc7bf28ae031b73f8e063e2a911051ef4cce035fdf90`
and `fc33b5a08b5cc1afbc0839f201d8a21ad5ed1dfd8476a85b47298e96d59a2324`.

This admits the mechanism to two separate next gates. A DataFusion source must
prove that these projection stripes can become Arrow batches without an
intermediate row materialization. A GCS run must determine whether the second
cold request is tolerable at remote object latency.

## Direct DataFusion source result

`[CODE-COMPLETE]` `RangeStripeTableProvider` and `RangeStripeExec` now expose
the exact C5 `OKCP` projection object to DataFusion. The execution plan emits
one incremental Arrow batch per logical stripe, accepts projection pushdown,
and reads no `OKCV` payload page for a projected aggregate. Each physical range
is verified against the existing per-stripe checksum before decoding.

One physical geometry cannot optimize both point and scan access. A point read
still selects one approximately 7.8 KiB stripe. A scan coalesces adjacent
stripes into a bounded range GET, verifies every nested stripe independently,
then emits the original small Arrow batches:

```text
point
  key index -> one 7.8 KiB projection stripe -> optional payload page

scan
  manifest order -> one at most 256 KiB range GET
                 -> verify each nested stripe
                 -> emit one at most 128-row Arrow batch at a time
```

`[EVALUATING]` Release-local suite
`columnar-range-datafusion-coalesced-v1` ran three seeds over three repeats on
the same dirty source and with OTel disabled:

| Subject | Median source rows/s | Projection GETs | Read bytes | Peak fetch buffer | Peak Arrow batch |
| --- | ---: | ---: | ---: | ---: | ---: |
| One GET per stripe control, `d788c75f` | 1,246,835 | 1,761 | 13,687,710 | 7,818 | 1,646 |
| 256 KiB coalesced candidate, `a7d4f3bf` | 2,543,552 | 54 | 13,687,710 | 257,506 | 1,646 |
| Coalesced plus payload-prefetch poison, `b4fe7c11` | 820,215 | 1,815 | 40,736,670 | 257,506 | 1,646 |

The candidate preserved exact SQL aggregates, projection pushdown, bounded
fetch and Arrow buffers, checksum coverage, zero full-object reads, zero LIST
operations, and zero opaque payload reads. It reached 2.040x the control's
median throughput with 0.0307x its request count and identical useful bytes.
The poison added 1,761 payload requests and cut throughput to 0.322x the
candidate, so the payload-avoidance gate is independently observable.

This proves a direct DataFusion source mechanism, not the complete HTAP path.
The provider currently reads one object base version. Exact base-plus-live-tail
execution, predicate invalidation across the tail, complete-query memory,
parallel range partitions, and GCS latency remain open. The suite and profile
hashes are
`082b8782c4c0566857b14f08562ce4c46d503edf0a3b8716fb4ae0facebf73a1`
and `567d9971abfb1ee02bd528c4c2a56be0dbdeb13a521ce072a3aecff598155a9e`.

## Decision

`[PROPOSED]` Promote the RangeEngine abstraction from "row serving worker" to
"range-local hybrid execution runtime." It owns transaction deltas, stable row
identity, column primitive loading, cache policy, point assembly, and
vectorized scan production. It does not own durable transaction authority or
permanent bytes.

The split row-sidecar design remains the control. It is not the presumed final
architecture.
