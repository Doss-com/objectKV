# Fable RFC-0046 pre-implementation review, 2026-08-30

- Verdict: `BLOCK`
- Scope: proposed generation-pinned GCS cold-point and layout curve
- Reviewed artifact: `rfcs/0046-generation-pinned-gcs-cold-point.md`
- Implementation state at review: `[PROPOSED]`

## Blocking findings

1. T28 did not define whether it reads only the object base at `O` or composes
   the txLog suffix `(O, C]`. The first point gate must freeze `T = O`.
2. The placement locator does not attest the ambient GCS principal or prove
   that writer authority was revoked. The execution plan must bind a dedicated
   service account, IAM receipt, bucket, role, and credential lifetime.
3. `open_existing_fixture_at_revision` downloads every index and data object.
   A measured cold reader needs a separate lazy metadata opener and must reject
   use of the complete-closure helper.
4. `ObservedBackend` counts logical calls after the provider returns. It cannot
   observe internal GCS retries or physical transferred bytes. The gate must
   separate logical operations from request attempts, status, ranges,
   generations, retries, and bytes.
5. `OKVM` embeds every segment reference and is decoded in full. General
   database-size-independent metadata is not supported by the current format.
   T28.1 may prove constant data-block work only and must report metadata growth
   separately.
6. The raw control and cache treatments did not freeze the source of expected
   ranges, independent value oracle, ABBA order, process isolation, transport
   warmup, or separate metadata, data-cache, and fetch-buffer budgets.
7. C5 currently publishes its own local-filesystem media and has no
   authenticated typed GCS closure shared with the row control. The DataFusion
   lane is not ready to implement under this RFC.

The OTel design is feasible only if it reuses the six-result exporter
completion contract and adds collector-side run-ID evidence.

## Smallest safe slice

Implement T28.0 and T28.1 only:

```text
one immutable RFC-0044 fixture at object frontier O
  -> frozen per-operation point plan
  -> dedicated read-only principal receipt
  -> fresh ABBA candidate and raw-range processes
  -> lazy descriptor, manifest, and selected-index opener
  -> retries disabled and request attempts recorded
  -> exact reads at T = O
```

Defer bounded refill, dataset-size independence, and C5 until their cache,
hierarchical-metadata, and typed-closure contracts are frozen.

This optimizes for obtaining valid 1 GiB GCS point evidence quickly. It gives
up generalizing one result into a complete object-refill or unified HTAP claim.

## Re-review 1

- Verdict: `BLOCK`
- Reviewed artifact: revised RFC-0046 after the seven findings above

Three ambiguities remained:

1. `empty_reader` prohibited metadata cache state while the common warmup text
   still allowed metadata warmup for every position.
2. A wrapper around `object_store` could observe provider SDK calls but not
   redirects, transport retries, or physical HTTP attempts below the library.
3. The 1.25x p99 gate did not freeze aggregation across seeds, paired
   positions, and both execution orders.

The next revision makes warmup state-specific, limits the `empty_reader`
warmup to a disjoint immutable transport canary, renames the measured unit to a
provider SDK attempt, explicitly defers physical-request and billing claims,
and requires every one of 15 paired-block nearest-rank p99 ratios to pass.

## Re-review 2

- Verdict: `SHIP`
- Reviewed artifact: current RFC-0046 after the second revision
- Remaining actionable pre-implementation blockers: none

This verdict accepts the T28.0 and T28.1 contract for implementation after T27
admission. It does not verify code, a GCS read mechanism, or performance.
