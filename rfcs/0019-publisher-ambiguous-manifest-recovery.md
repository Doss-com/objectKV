# RFC-0019: Publisher recovery after an ambiguous manifest PUT

- Status: proposed
- Authors: objectKV contributors
- Created: 2026-08-23
- Supersedes: none

## Decision

The next executable increment of RFC-0016 kills a dedicated publisher after all
data objects are verified and the immutable manifest PUT takes effect, but the
manifest response becomes retryable-unknown. A replacement publisher starts
with empty scratch, recovers the same quorum-durable intent, replays the exact
canonical job, verifies every existing data object and the manifest by named
identity, walks the complete closure, and only then requests root publication.

This increment extends RFC-0018 from one ambiguous data object to the object
that names the complete physical closure. It does not combine that ambiguity
with a lost replicated `Publish` response, multipart residue, abandoned-intent
policy, sweeper recovery, generation handoff, or authority-disk repair.

## Context and invariant

A manifest is immutable bytes, not an authority transition. Its existence says
which child names a reader should open, but it does not prove that every child
exists and matches the recorded identity. The replicated root remains the only
visibility decision.

For one canonical job `J`, data closure `D(J)`, and manifest `M(J)`:

```text
verified(D(J))
and put_effect(M(J))
and put_response(M(J)) = unknown
and publisher_dies

  -> prepared_intent(J) remains quorum durable
  -> replacement_job = J
  -> named_identity(M(J)) = expected_identity(J, M(J))
  -> verified(closure(M(J))) precedes visible_root(J)
  -> visible_root(J) changes exactly once
```

At every point before the replacement completes the named closure walk:

```text
visible_root(J) = false
```

Neither manifest existence nor an `AlreadyExists` response may substitute for
child verification.

## Fault boundary

The first publisher process:

1. replays the exact quorum-durable `Prepare` outcome;
2. conditionally creates and verifies each data object in canonical order;
3. wraps the real local filesystem backend in the test-owned `FaultBackend`;
4. conditionally creates the manifest;
5. retains the successful manifest effect but replaces its response with
   `RetryableUnknown`;
6. emits `manifest_put_response_unknown` and waits for `SIGKILL`.

The controller independently proves the exact replicated intent and outcome,
each expected data object, the exact manifest bytes, absence of a destination
root, and empty worker scratch before killing the process.

## Replacement protocol

The replacement receives only the same seed, authority endpoints, object-store
root, and a new empty scratch directory.

1. Recompute the job digest and stable `prepare` and `publish` identities.
2. Replay `Prepare` and require the exact stored intent and accepted outcome.
3. Call `put_if_absent` for every data object in canonical order.
4. Require each existing data object to pass exact named verification.
5. Call `put_if_absent` for the manifest and require exact existing identity.
6. Decode the manifest and walk every named child.
7. Commit `Publish` with the stable request identity.
8. Read the exact root and intent retirement linearly.

The clean fixture expects no new physical object effect after replacement. A
missing object may be recreated by the general protocol, but this fixed gate
requires the controller-proven pre-kill closure so counter drift is detectable.

## Negative subject

`trust_manifest_without_closure` omits the second data object before creating
the manifest and losing the manifest response. Its replacement resolves the
existing manifest identity but neither replays the data-object set nor walks
the closure before `Publish`.

The replicated authority can accept the root transition because it does not
perform object I/O. A reader must fail on the absent child, the correctness
gate must reject the subject, and the subject must receive `discard`.

This isolates the load-bearing rule: manifest identity is necessary, but only a
complete named closure walk is sufficient to request visibility.

## Eval plan

`object-publication-publisher-manifest-recovery-v1` runs seeds `1103`, `2207`,
and `3301` through thirteen labeled checks per seed. The aggregate budget is
exactly 39 events. Correctness anomalies are the only primary metric.

The clean subject must prove:

- the active generation authorizes the canonical publisher job;
- `Prepare` and its request outcome are quorum durable;
- all data objects are exact before the manifest attempt;
- the manifest effect exists while its response is unknown;
- no destination root exists at the fault barrier;
- the first publisher receives real `SIGKILL`;
- the replacement starts with empty scratch;
- the replacement reconstructs the same job and request identities;
- every existing data object is recovered by exact named identity;
- the existing manifest is recovered by exact named identity;
- a complete closure walk precedes root visibility;
- `Publish` installs the exact root and retires the intent atomically;
- a reader walks the exact visible closure;
- two fresh seed-1103 controllers emit byte-identical semantic receipts;
- OTel logs, metrics, and traces have zero drops.

Secondary receipts include authority and publisher starts, process kills, PUT
attempts, physical effects, injected unknown responses, existing-object
recoveries, named verification reads, publication-command attempts, and
empty-scratch restarts. Object keys, digests, request identities, paths, ports,
PIDs, and timestamps remain forbidden metric attributes.

## Alternatives

D1. Combine ambiguous manifest PUT and ambiguous `Publish` in one schedule.
This shortens the implementation sequence but makes object identity recovery
and replicated command-outcome recovery indistinguishable in a failure.

D2. Treat the manifest digest as a transitive proof of child presence. The
digest authenticates manifest bytes only; it says nothing about current child
availability in the object store.

D3. Record a per-object completion bitmap in worker scratch. This reduces
named reads but makes disposable scratch a correctness dependency and creates a
second recovery journal.

## Compatibility and migration

No public API or on-disk format changes. The fault schedule and semantic report
are eval-internal. `PublisherJob` remains unpublished format version `1`.

## Unresolved questions

- Whether a production publisher should pipeline data and manifest recovery
  reads while preserving the closure-before-root proof.
- Whether a manifest object needs an explicit child-count and aggregate-length
  bound before decoding.
- How retry budgets compose across repeated unknown manifest responses.
- How multipart upload completion and abandoned upload identifiers interact
  with immutable manifest naming.
- How abandoned prepared intents are leased, fenced, and reassigned.
- Whether the next lost `Publish` response gate reuses this controller or owns
  a distinct authority-outcome fixture.
