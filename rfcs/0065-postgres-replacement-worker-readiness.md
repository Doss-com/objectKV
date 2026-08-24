# RFC 0065: PostgreSQL replacement-worker readiness

- Status: accepted for local OS-warm evaluation; production serving policy unchanged
- Authors: objectKV contributors
- Created: 2026-08-24
- Supersedes: none

## Decision

Measure and expose PostgreSQL replacement-worker recovery as separate phases.
Loading a durable root, authenticating its object-delta lineage, opening one
authority-bound immutable view, serving the first point, and auditing the full
physical closure are different operations. The current eager production helper
continues to audit the complete closure before returning. The first candidate
adds an experimental manifest-bound worker path so the performance effect can
be measured without silently changing that safety policy.

## Context and invariant

The 512 MiB relation-size curve reports 1.106 seconds of delta activation and
4.549 seconds for source-free restart plus a complete snapshot oracle. Code
inspection shows that `open_persistent_range_view` rereads and hashes the
manifest and every live SST before opening the SlateDB reader. The number is
therefore an eager physical-closure audit, not evidence that one point read
requires a relation scan.

The invariant is unchanged:

```text
one authority-selected root
  -> one exact manifest
  -> one exact immutable base closure
  -> one contiguous authenticated delta lineage
  -> one exact snapshot at T
```

No worker may turn malformed root metadata, a changed manifest, a changed
delta segment, a broken commit chain, or a wrong page into visible database
state.

## Proposed contract

One fresh worker process reports these ordered phases:

```text
durable root load
  -> delta object and commit-chain authentication
  -> manifest-bound view ready
  -> first delta-overlay point
  -> first immutable-base point
  -> first bounded ordered range
  -> complete streaming oracle
  -> complete physical-closure audit
```

`view ready` means that root identity, manifest bytes, visible base frontier,
and the complete selected delta lineage are authenticated. It does not mean
that every live SST byte has already been reread by the process.

The experimental path must not replace the eager helper or become a production
default in this RFC. Serving before full closure audit can be admitted later
only when one of these integrity contracts is proven:

1. the backing object provider authenticates the exact immutable generation
   and checksum selected by the authority root;
2. touched objects or blocks carry cryptographic identities that the worker
   verifies before returning rows;
3. the worker has a previously authenticated local copy whose identity is
   bound to the selected root.

The background closure audit remains mandatory in the candidate eval. An audit
failure quarantines the view and must be observable. The v0 local worker exits
after its receipt, so quarantine routing is not yet a production claim.

## Failure model

- Missing or malformed durable root: refuse before view construction.
- Changed manifest bytes: refuse before `view ready`.
- Missing or changed delta object: refuse before `view ready`.
- Broken delta or commit chain: refuse before `view ready`.
- Missing or changed SST: a complete audit must refuse it. Production serving
  before that audit remains unadmitted until a touched-object integrity
  contract exists.
- Worker death during audit: no durable database state changes. A successor
  starts from the authority root again.
- Object-store timeout: the affected phase fails and the worker is not ready.
- Stale root: the publication authority and routing epoch remain responsible
  for refusing it. This eval consumes one already selected immutable root.

## Alternatives

### Audit every SST before readiness

Optimizes for the simplest fail-closed argument. Gives up relation-size
independent worker readiness and rereads cold object bytes before any request.

### Trust manifest identity alone

Optimizes for immediate readiness. Gives up application-layer proof that the
selected SST bytes still match the publication closure. This is not admitted.

### Hash the complete touched SST on first access

Optimizes for lazy work with exact object identity. Gives up bounded first-read
latency when an SST is large. Small immutable objects or per-block hashes may
be required.

### Provider checksum plus background application audit

Optimizes for cloud-native readiness and bit-corruption detection. Gives up a
Byzantine storage threat model unless provider identity and checksum semantics
are strong enough for the selected backend.

## Eval plan

The owning suite is
`evals/suites/postgres-replacement-worker-readiness.toml`. It holds the same
one-page certified mutation across 128, 4,096, and 65,536-page relations and
uses five deterministic seeds. Every correct subject builds the immutable
fixture outside the timed worker, then runs the measured phases in a fresh OS
process without the source heap.

Hard gates require exact delta-overlay and immutable-base points, an exact
bounded range, an exact bounded-memory full oracle, a completed full closure
audit, deterministic semantic replay, and a bounded worker RSS receipt.
Changed-manifest, changed-delta, and skipped-audit controls must discard.

The primary metric is manifest-bound `view ready` duration. Calibration targets
are less than 4x growth from 128 to 65,536 pages, at most 100 ms at 65,536
pages on the declared local OS-warm profile, at most 5 ms for the first base
point, and at most 1 GiB worker RSS. These targets select the next experiment;
they are not production claims.

## Local result

Candidate `e2c9dd5` passed the frozen five-seed release curve and every
correctness gate. The immutable physical closure grew from 1.09 MB to 555.04
MB while manifest-bound view readiness grew from 2.33 ms to 4.75 ms, a 2.04x
increase over about 511x more physical bytes. First immutable-base point reads
were 0.142 to 0.181 ms, first delta-overlay reads were 4.1 to 4.5 microseconds,
and the first eight-page range was 0.57 to 0.62 ms. Median worker RSS stayed at
or below 67.4 MiB.

The complete physical-closure audit grew from 2.12 ms to 1.046 seconds, and the
bounded full-snapshot oracle grew from 8.88 ms to 4.493 seconds. Those two
phases scaled with relation bytes. Changed-manifest, changed-delta, and
skipped-audit controls all discarded.

Decision: keep the phase split and the experimental manifest-bound open seam.
Do not replace the eager production helper. The next admission gate is the
same curve on GCS across metadata-warm, persistent-cache-warm, and cold cache
states with provider generation and checksum identity bound to the root or a
touched-block cryptographic contract.

## Compatibility and migration

No stored format changes. No existing reader or public helper changes. The
candidate adds a new experimental open function, process config, receipt, eval
operation, and metrics. Removing all of them restores the prior eager path.

## Unresolved questions

1. How should objectKV bind GCS generation and CRC32C or S3 checksum metadata
   into a publication root without coupling the format to one provider?
2. How small must SSTs or authenticated blocks be to bound a cold first point?
3. Should a background audit failure terminate the worker, revoke its route,
   or first drain in-flight reads?
4. Which integrity level is required for local filesystem development versus
   production cloud object stores?
