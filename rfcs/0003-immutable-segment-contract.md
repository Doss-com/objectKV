# RFC-0003: Immutable segment contract

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

objectKV uses two physical contracts. A transactional segment stores the
kernel's ordered MVCC entry algebra for point and range reads. An analytical
artifact stores a schema-aware, version-aligned materialization for DataFusion.
The narrow waist is a sorted versioned-entry stream plus fenced publication,
not a generic Parquet, Vortex, or SST interface.

## Context and invariant

Row blocks, Parquet, and Vortex have different semantics and cost structures.
Treating them as interchangeable kernel formats would push tombstones, range
deletes, merge behavior, schemas, projection, and compaction policy through one
misleading trait.

Every reader that supports a transactional segment version must produce the
same logical result for the same entry stream and read version.

## Transactional segment contract

### Logical entries

The builder consumes the canonical RFC-0002 stream:

- point set;
- point clear;
- half-open range clear;
- transaction batch boundary and fingerprint evidence.

Entries are ordered by user key ascending, commit version descending, then a
stable entry-kind order. The physical encoder may split point and range indexes,
but the reader must preserve their combined visibility.

### Required metadata

Each segment descriptor contains:

- segment contract version and physical format ID/version;
- exact stored-byte length and SHA-256 of the stored bytes;
- minimum and maximum user key;
- minimum and maximum commit version;
- entry and tombstone counts;
- compression, encryption-key identity, and checksum algorithms;
- optional bloom, zone-map, and block-index capability declarations;
- reader compatibility floor and ceiling;
- provenance: source WAL interval, generation, and builder implementation ID.

The object key includes the digest of the exact stored ciphertext or plaintext
bytes. Reusing a key for different bytes is corruption. A segment is immutable
after its conditional create succeeds.

### Reader interface

The logical interface provides:

- `get(key, read_version)`;
- `scan(begin, end, read_version, direction, limit)`;
- metadata inspection without reading all data blocks;
- checksum verification for every fetched byte range;
- a declared estimate for index/footer GETs and preferred range alignment.

Readers return `unsupported_format`, `corrupt_segment`, or
`history_not_present` explicitly. They never skip a segment because a footer or
index is unavailable.

### Compaction

The kernel owns the crash-safe protocol: immutable inputs, deterministic logical
output, conditional manifest publication, and delayed garbage collection. The
format implementation owns block sizing, split candidates, output-size
estimates, encoding choices, and a cost model. Two builders may choose different
boundaries, but they must emit the same logical stream and stable per-output
provenance.

## Analytical artifact contract

Parquet is the initial control. Vortex is a later candidate. A complete
partition snapshot manifest at `W_p` identifies the full base object closure,
schema and partitioning epoch, primary-key encoding and ordering, row identity,
delete representation, source provenance, and checksums. Individual artifacts
may declare covered intervals, but a bag of interval-labeled files is not by
itself an exact base. Analytical artifacts are never read by the kernel
point-read path and cannot advance `O_cell` for transactional durability.

## Worked failure cases

1. A Parquet reader cannot represent RFC-0002 range tombstones without a
   separate delete stream. It therefore fails the transactional capability test
   instead of claiming to be a drop-in OLTP segment.
2. A builder uploads an object and crashes before manifest publication. The
   object is unreachable garbage; recovery never discovers it through LIST.
3. A range GET returns bytes that fail the segment checksum. The reader returns
   corruption and does not retry against another logical version silently.
4. An old reader sees a newer format with an unsupported range-delete encoding.
   It returns `unsupported_format`; it does not ignore the entries it cannot
   decode.
5. A columnar artifact covers through version 100 while the query requests 105.
   DataFusion must wait or merge the exact `(100, 105]` delta once.

## Alternatives

- One file-format trait appears flexible, but hides that analytical pushdown
  requires schemas and transactional visibility requires MVCC entry kinds.
- A SlateDB-native contract reduces initial code, but would make objectKV's
  durable format and reader lifecycle follow an upstream API with no current
  compatibility promise.
- Generated object names avoid hashing cost, but weaken retry identity and make
  conflicting publication harder to diagnose.

## Eval plan

- Golden logical histories cover points, range scans, clears, range clears,
  overlapping versions, corruption, old readers, and mixed segment formats.
- The same history must produce identical reads across every transactional
  format implementation.
- Separate economics champions measure point GETs, scan throughput, bytes,
  compaction amplification, and open cost.
- A negative one-trait implementation that ignores a range tombstone must fail.

## Compatibility and migration

Every object carries a contract and format version. Writers may emit a new
format only while readers for every live format remain deployed. Migration is
copy-on-write compaction plus manifest publication; existing objects are never
rewritten in place.

## Unresolved questions

- Initial row-block encoding and whether the first stable segment derives from
  SlateDB or an objectKV-owned format.
- Encryption envelope and whether digest identity covers ciphertext only or a
  canonical authenticated envelope.
- Whether deterministic builders require byte-identical output or only
  identical logical streams plus content-addressed output identities.
