# RFC-0014: Real object publication adapter

- Status: proposed
- Authors: objectKV contributors
- Created: 2026-08-23
- Supersedes: none

## Decision

The first physical publication adapter uses the Apache `object_store` client for
content-addressed immutable bytes and a separately reopened, quorum-fsynced
local authority prototype for publication intents, roots, request outcomes, and
deletion reservations. The adapter must execute RFC-0007 without consulting
`LIST` or accounting counters for liveness. This is a process-reopen and real
object-client proof, not a production distributed authority or cloud claim.

## Context and invariant

The admitted `object-publication-gc-v1` lane proves the state machine only in
memory. The physical boundary must now prove that unknown storage outcomes,
durable authority recovery, and unguarded object deletion compose safely.

For every published root `R` and every digest object `o` in its manifest closure:

```text
Published(R) -> durable_authority(R) and exact_named_read(o)
```

For every unguarded deletion of `o`:

```text
complete_mark(o)
and unchanged_root_intent_epoch
and durable_delete_reservation(o)
```

The reservation serializes publication preparation against deletion. It closes
the window in which a publisher could verify an old object after GC
revalidation and publish it immediately before an unguarded delete.

## Proposed contract

### Immutable bytes

- Keys include the SHA-256 digest of exact stored bytes.
- `ObjectClient::put_if_absent` creates or verifies by exact named read.
- A lost successful PUT response is recovered only by length and digest match.
- Manifests are canonical versioned JSON whose child identities include key,
  length, and SHA-256 digest.
- Reader-visible roots are installed only after the full manifest closure has
  been read and verified through `ObjectClient`.

### Local durable authority prototype

The prototype state contains a monotonically increasing revision, a root and
intent domain epoch, retained request outcomes, publication intents, published
roots, snapshot pins, and per-object deletion reservations. Each command writes
one checksummed state transition to all three local WAL files and becomes
eligible for success only after a two-file synchronized quorum. Fresh open
reconstructs the latest contiguous quorum state before serving a request.

One in-process mutex serializes commands. This is deliberately not a claim of
cross-process consensus, independent failure domains, or production authority
availability. The later authority implementation must run the same commands
through the admitted replicated transaction system.

### Publication

1. Durably prepare an intent naming the complete object closure.
2. Write and verify every immutable data object and the manifest.
3. Reopen the authority and prove the intent survived.
4. Publish the root and retire the intent in one state transition.
5. If the response is lost after quorum sync, reopen and resolve by stable
   request identity and exact recorded outcome.

### Mark and sweep

1. Pin the authority domain epoch and root set.
2. Walk every manifest by named verified read. An incomplete walk yields no
   deletions.
3. Use `LIST` only to discover old candidates.
4. In one authority transition, require the pinned epoch, prove no current
   intent names the candidate, and install a durable deletion reservation.
5. Pass the opaque reservation to the object client. Prefer revision-guarded
   delete where supported; otherwise verify exact immutable identity and issue
   an unguarded delete while publication remains blocked.
6. Resolve an unknown delete response by named read, then durably retire the
   reservation.
7. A publisher rejected by the reservation retries after retirement and must
   recreate or reverify the object before publishing.

## Failure model

- Object PUT, authority commit, and object DELETE may succeed while their
  responses are lost.
- A process may restart after intent creation, root publication, or deletion
  reservation.
- The final local WAL frame may tear; a complete corrupt frame without a
  matching quorum fails recovery closed.
- `LIST` may omit live objects or return stale deleted objects.
- Roots or intents may change after mark and before sweep.
- The local filesystem backend has no revision-guarded delete primitive.
- Independent-disk loss, multi-process authority concurrency, network
  partition, and cloud-provider semantics remain outside this adapter gate.

## Alternatives

D1. Store authority in the same object bucket. This reduces components, but it
requires provider-specific conditional-write correctness for every deployment
and does not prove the intended transaction-system authority boundary.

D2. Use revalidation without a deletion reservation. This reduces authority
writes, but leaves a publication-versus-delete TOCTOU window and is rejected.

D3. Never delete on backends without guarded delete. This is safe and simple,
but makes object reclamation unavailable on the common shared client boundary.
The reservation fallback preserves safety while accepting more coordination.

## Eval plan

`object-publication-adapter-v1` uses three fixed seeds, local filesystem objects,
three local authority WAL files, and one correctness lane. The primary metric is
`correctness.anomalies`, which must remain zero within 48 events.

The clean subject must prove:

- durable intent precedes every object upload;
- exact named verification precedes root visibility;
- lost object, authority, and delete responses resolve by identity;
- a fresh authority open recovers intent, root, request outcome, and deletion
  reservation state;
- an incomplete mark yields zero delete permits;
- root or intent epoch changes defer deletion;
- a deletion reservation blocks intersecting publication;
- post-delete publication recreates or reverifies bytes;
- `LIST` and counters do not decide liveness.

The frozen negative subjects are:

1. `publish_root_before_verify`
2. `omit_durable_intent`
3. `forget_unknown_object_outcome`
4. `ram_only_authority`
5. `trust_list_for_liveness`
6. `delete_without_revalidation`
7. `delete_without_reservation`

Each must produce at least one bounded anomaly and a `discard` verdict.

## Compatibility and migration

Authority log payloads and manifest JSON carry format version `1`. Unknown
versions fail closed. The adapter is unpublished and may be removed without a
wire-compatibility promise. Migration to replicated authority preserves command
and state semantics, not the local WAL byte layout.

## Unresolved questions

- How deletion reservations are partitioned without a global authority hot key.
- How abandoned reservations are recovered by a fenced sweeper owner.
- Whether provider-native guarded delete reduces enough coordination to justify
  separate GCS, S3, and Azure implementations.
- How inventory age and quarantine evidence are represented at cloud scale.
