# RFC-0018: Publisher recovery after an ambiguous object PUT

- Status: proposed
- Authors: objectKV contributors
- Created: 2026-08-23
- Supersedes: none

## Decision

The next executable increment of RFC-0016 kills a dedicated publisher after
the first immutable data-object PUT takes effect but its successful response is
replaced by a retryable-unknown error. A replacement publisher starts with an
empty scratch directory, recovers the same quorum-durable publication intent,
replays the same canonical job, verifies the already-present object by exact
named identity, writes the remaining data and manifest objects, verifies the
complete closure, and publishes the root.

This increment extends RFC-0017 by crossing the first object-effect boundary.
It does not add a lost manifest response, a lost `Publish` response, abandoned
intent policy, sweeper recovery, generation handoff, or authority-disk repair.

## Context and invariant

RFC-0017 proved that no worker-local journal is needed after `Prepare` is
quorum durable and before any object effect exists. The next ambiguity is
harder: after a PUT effect exists, neither the replicated authority nor a new
worker knows whether the old worker observed success.

For one publisher job `J` and first data object `O1`:

```text
put_effect(O1)
and put_response(O1) = unknown
and publisher_dies

  -> prepared_intent(J) remains quorum durable
  -> replacement_job = J
  -> named_identity(O1) = expected_identity(J, O1)
  -> remaining_closure(J) is written and verified
  -> root(J) becomes visible exactly once
```

At every point before full named closure verification:

```text
visible_root(J) = false
```

An existing name is not success by itself. Length, digest, object kind, and any
available backend revision must match the immutable job. A missing object is
recreated. A conflicting identity fails closed.

## Fault boundary

The first publisher process uses a test-owned `FaultBackend` around the real
local filesystem `object_store` adapter. For only the first data object it:

1. performs conditional create against the underlying backend;
2. retains the successful physical effect;
3. replaces the response with `RetryableUnknown`;
4. emits a machine-readable `first_put_response_unknown` barrier;
5. waits for the controller to kill the process.

The child does not run named recovery after the injected error and does not
write scratch state. The fault shim is eval infrastructure, not a production
object-store role.

The controller independently proves all of the following before `SIGKILL`:

- the exact `Prepare` intent and request outcome are linearizable;
- the first data object exists with the expected length and digest;
- the second data object and manifest are absent;
- no destination root is visible;
- the old scratch directory contains no correctness state.

## Replacement protocol

The replacement receives the same immutable `PublisherJob` and authority
endpoint set, but a new empty scratch directory.

1. Recompute the job digest plus `prepare` and `publish` request identities.
2. Replay `Prepare` and resolve the original accepted outcome.
3. Require the exact stored intent.
4. Call `put_if_absent` for every data object in canonical order.
5. For `O1`, require `ExistingIdentical` and an exact named verification.
6. Create and verify every missing data object.
7. Create and verify the manifest.
8. Walk the complete named closure.
9. Commit `Publish` with the stable request identity.
10. Read the exact root and intent retirement linearly.

The root transition remains atomic in the replicated authority. Object effects
remain idempotent and may precede root visibility.

## Negative subject

`publish_partial_closure` intentionally treats the first successful object
effect as if the whole job completed. It writes the manifest while omitting the
second data object, skips closure verification, and submits `Publish`.

The authority can accept the syntactically valid manifest reference because it
does not perform object I/O. The reader must then observe a missing closure,
the correctness gate must fail, and the subject must receive `discard`.

This negative control keeps the separation explicit: replicated metadata
orders visibility, while the publisher is responsible for proving physical
closure before requesting that visibility.

## Eval plan

`object-publication-publisher-put-recovery-v1` runs seeds `1103`, `2207`, and
`3301` through twelve labeled checks per seed. The aggregate budget is exactly
36 events. Correctness anomalies are the only primary metric.

The clean subject must prove:

- the active generation authorizes the immutable publisher job;
- `Prepare` and its request outcome are quorum durable;
- the first PUT effect exists while its response is unknown;
- exactly one data object and no root exist at the fault barrier;
- the first publisher receives real `SIGKILL`;
- the replacement starts with empty scratch;
- the replacement reconstructs the same job and transition identities;
- the existing first object passes exact named verification;
- every remaining object is created and verified;
- full manifest closure is verified before root visibility;
- `Publish` installs the exact root and retires the intent atomically;
- a reader walks the exact visible closure;
- two fresh seed-1103 controllers emit byte-identical semantic receipts;
- OTel logs, metrics, and traces have zero drops.

Secondary receipts include authority and publisher process starts, process
kills, PUT attempts, physical object effects, injected unknown responses,
existing-object recoveries, named verification reads, publication-command
attempts, and empty-scratch restarts. Object keys, digests, request identities,
paths, ports, PIDs, and timestamps are forbidden metric attributes.

## Alternatives

D1. Combine lost data PUT, lost manifest PUT, and lost `Publish` response in one
schedule. This reaches the full publisher protocol sooner, but makes a failure
ambiguous between object identity recovery, closure verification, and authority
outcome recovery.

D2. Treat an `AlreadyExists` response as success without a named read. This
reduces one GET but cannot distinguish exact replay from a conflicting or
corrupt immutable identity.

D3. Persist per-object progress in worker scratch. This reduces replay reads but
turns disposable workers into a correctness dependency and creates a second
recovery journal beside the replicated authority and object store.

## Compatibility and migration

No public API or on-disk format changes. The publisher fault schedule and
semantic report are eval-internal. `PublisherJob` remains unpublished format
version `1`. Later changes to job identity or object order require a new fixture
and explicit compatibility decision.

## Unresolved questions

- Whether production publishers should pipeline named recovery reads.
- How retry budgets compose across repeated unknown PUT responses.
- How a backend revision token participates in immutable identity on S3, GCS,
  and Azure.
- How partial multipart uploads are listed, completed, or abandoned safely.
- How abandoned prepared intents are fenced, quarantined, and reassigned.
- Whether a lost manifest PUT should share this worker state machine or receive
  its own narrower gate.
