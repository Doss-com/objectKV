# RFC-0004: Object-store correctness contract

- Status: proposed
- Authors: DOSS
- Created: 2026-08-22

## Decision

objectKV supports an object-store backend only after it passes a required
semantics profile. The floor is strong named-object read-after-write, immutable
conditional create, generation or ETag-conditional metadata update, byte-range
GET, checksums, bounded retry classification, and guarded delete. The phrase
S3-compatible names an API family, not a correctness guarantee.

## Context and invariant

First-party S3, GCS, and Azure provide strong named-object visibility, but
conditional operations, error behavior, tail latency, throttling, and compatible
implementations differ. Object storage has no multi-object transaction and no
atomic rename that objectKV may depend upon.

A manifest may reference an object only after the exact bytes are readable and
checksum-valid. LIST is never the source of live database state.

## Required operations

### Immutable data

- `put_if_absent(key, bytes, digest)` creates exactly one immutable object.
- A retry that finds the same key and digest is success.
- A retry that finds the key with different length or digest is
  `identity_conflict` and stops publication.
- `get(key)` and `get_range(key, range)` return an identity token, length, and
  checksum evidence sufficient to detect truncation or mixed generations.
- Multipart upload is an implementation detail. Incomplete uploads are not
  visible as committed segment objects.

### Mutable root metadata

- `compare_and_put(key, expected_identity, bytes)` updates a small root only if
  the named generation or ETag still matches.
- Creation requires an explicit absent precondition.
- A lost successful response is resolved by re-reading the named root and
  comparing its full logical transition identity.
- Providers without a sound conditional-update primitive cannot host authority
  metadata, even if they can host immutable segments.

### Delete and LIST

- Delete is guarded by the exact object identity recorded by the GC plan where
  the provider supports it; otherwise the immutable digest key and reachability
  horizon provide the protection.
- LIST may find abandoned uploads and unreachable objects for audit. LIST output
  cannot rebuild a manifest, select a latest version, or prove deletion safety.

## Error and retry contract

Errors are classified as `retryable_unobserved`, `retryable_unknown`,
`precondition_failed`, `not_found`, `corrupt`, `throttled`, `permission_denied`,
or `unsupported`. Retry budgets, jitter, and deadlines come from the eval
profile. `retryable_unknown` always performs an identity read before another
write.

Every backend exports request count, bytes, latency, result class, throttling,
retry count, and estimated price under a versioned price snapshot. Object key,
value, credential, and request identity are forbidden telemetry attributes.

## Worked failure cases

1. A segment PUT succeeds but its response is lost. Retrying the same digest key
   observes identical bytes and succeeds without creating a second segment.
2. A root metadata PUT succeeds but its response is lost. The writer re-reads
   the root and accepts only its intended transition; it does not blindly apply
   the transition again.
3. Provider LIST omits a newly written segment temporarily. Reads remain correct
   because the conditionally published manifest names the segment directly.
4. Two writers update one root from ETag 9. Only one conditional PUT may win;
   the loser receives `precondition_failed` and reloads authority.
5. A compatible store returns a short range with HTTP success. Checksum and
   length validation return `corrupt`; the reader does not parse partial bytes.
6. Thirty minutes of injected 503 responses grows `C - O`. RFC-0005 ratekeeping
   bounds the WAL; the object client does not retry forever at full commit rate.

## Conformance profiles

- `memory`: deterministic model and negative-store fixtures.
- `filesystem`: local durability and lost-response fixtures.
- `minio`: pinned S3 protocol implementation for contributor CI.
- `gcs-dev`: protected `objectKV-dev` bucket with generation preconditions.
- `aws-s3` and `azure-blob`: `[FUTURE]` provider profiles after local gates.

A published support matrix records each backend, exact server/client version,
conditional primitive, checksum behavior, and suite hash. A category-level
claim such as S3-compatible never substitutes for a passing row.

## Alternatives

- Restricting authority to an external consensus service avoids conditional
  object updates, but manifests still need safe idempotent publication.
- Provider-specific clients expose the strongest semantics but increase code
  and compatibility surface. The bootstrap uses Apache `object_store` plus
  narrow provider capability adapters where the shared API is insufficient.
- Eventual-consistency emulation can test old providers, but weak named-object
  visibility is below the required authority floor and is not a launch target.

## Eval plan

The conformance suite injects lost success, duplicate write, conflicting write,
short range, checksum corruption, stale LIST, throttling, timeout, and
precondition races. One negative backend that uses LIST as authority and one
that overwrites immutable keys must fail. Correctness failures are hard gates;
request, byte, latency, and cost curves are lane metrics.

## Compatibility and migration

Backend capability evidence is versioned independently from stored segment
formats. Moving authority to a backend with a different conditional primitive
requires an explicit root handoff while the old authority is still fenced.

## Unresolved questions

- Whether Apache `object_store` exposes every required conditional identity
  without provider-specific adapters at the chosen pinned revision.
- Minimum checksum guarantees for multipart and encrypted objects.
- Delete precondition support matrix and the fallback GC proof per provider.
