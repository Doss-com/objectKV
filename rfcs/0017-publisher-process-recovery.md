# RFC-0017: Publisher process recovery after prepare

- Status: proposed
- Authors: objectKV contributors
- Created: 2026-08-23
- Supersedes: none

## Decision

The first executable increment of RFC-0016 introduces one dedicated publisher
OS process. The controller kills it immediately after `Prepare` is quorum
committed and before the first object PUT. A replacement process starts with an
empty scratch directory, recovers the exact prepared intent and request outcome
from the replicated publication authority, writes and verifies immutable bytes
through Apache `object_store`, and completes publication.

This increment does not add sweeper recovery, generation handoff, lost object
responses, or authority-disk replacement. It proves the first worker boundary
without weakening the complete RFC-0016 destination.

## Context and invariant

The admitted real-object adapter runs publication inside one process. The
admitted authority gate runs publication commands through three coordinator
processes but does not execute object effects. A production publisher must be
disposable and cannot depend on a local journal to know whether `Prepare`
committed.

For one immutable publisher job `J`:

```text
first_object_put(J) -> quorum_committed_prepare(J)

publisher_restart(J)
  -> same_job_identity(J)
  -> same_transition_request_identities(J)
  -> exact_authority_outcome(J)
```

No object may exist for `J` at the kill boundary. After recovery, the visible
root must name the exact verified closure and the prepared intent must be
retired atomically.

## Publisher job

The controller gives each process one immutable, canonical `PublisherJob`:

```text
format_version
cell_id
generation_credential
publication_id
destination_root
expected_prior_root
data objects with canonical bytes
manifest bytes and exact object reference
authority endpoints
object-store profile and root
scratch directory
```

The publication and object identities derive only from canonical job bytes.
Request identities derive from the job digest plus a bounded transition label:

- `prepare`
- `publish`

The derivation rejects a zero identity and a collision between labels. It does
not use a PID, wall clock, random UUID, local counter, or scratch contents.

## Authority client

`okv-consensus` exposes a production-shaped publication client that performs:

- leader-directed publication writes;
- linearizable publication-state reads;
- linearizable publication-outcome reads;
- bounded retry across a supplied coordinator endpoint set;
- exact request-identity and response decoding.

The client does not perform object I/O and does not infer a transaction outcome
from object existence. The existing process-contract harness is migrated to the
same client where doing so does not change its frozen semantics.

## Publisher process protocol

The worker executes these states:

```text
Start
  -> PrepareRecoveredOrCommitted
  -> ObjectsVerified
  -> ClosureVerified
  -> Published
```

1. Derive the canonical job and transition identities.
2. Read the active publication authority linearly.
3. Submit `Prepare`, or recover its exact outcome after an unknown response.
4. Re-read the exact prepared intent and reject any mismatch.
5. Emit a machine-readable `prepared_committed` barrier to the controller.
6. Wait for an explicit continue signal. The clean schedule kills the first
   process at this barrier.
7. The replacement repeats steps 1 through 4 from empty scratch.
8. Write every immutable data object and the manifest through
   `ObjectClient::put_if_absent`.
9. Verify every exact named object and the complete manifest closure.
10. Submit `Publish`, then verify the destination root and intent retirement
    through a linearizable read.

The worker writes no correctness state to scratch. The directory exists only to
prove that later worker features cannot accidentally depend on a retained local
file.

## Failure model

The controller sends a real `SIGKILL` after the replicated authority and an
independent linearizable controller read both confirm the exact intent. It then
deletes the first worker's scratch directory and creates a new empty directory
for the replacement.

The negative subject `upload_before_prepare_ack` writes one immutable data
object before the prepare response and controller verification. The controller
kills the worker at that boundary and must observe an object with no durable
intent. That bounded anomaly discards the subject.

Outside this increment are lost PUT or Publish replies, partial multi-object
upload, publisher death after object effects, authority leader loss during the
worker schedule, G1 to G2 handoff, and mark or sweep behavior.

## Eval plan

`object-publication-publisher-process-v1` runs seeds `1103`, `2207`, and `3301`
through ten labeled events per seed. The aggregate budget is exactly 30 events.
Correctness anomalies are the only primary metric.

The clean subject must prove:

- the active generation authorizes the publisher;
- a dedicated publisher process reaches the prepare barrier;
- the exact intent and prepare outcome are quorum durable;
- no job object exists before the prepare barrier;
- the first publisher receives `SIGKILL`;
- the replacement starts with an empty scratch directory;
- the replacement reconstructs the same job and request identities;
- data and manifest objects pass exact named verification;
- publish installs the exact root and retires the intent atomically;
- a reader walks the exact visible closure;
- two fresh seed-1103 controller runs emit byte-identical semantic receipts;
- OTel logs, metrics, and traces have zero drops.

The negative subject `upload_before_prepare_ack` must emit at least one anomaly
and a `discard` verdict. Metrics reuse the bounded registry. Object keys,
digests, request identities, paths, ports, PIDs, and timestamps are not metric
attributes.

## Alternatives

D1. Add every publisher and sweeper boundary in one harness. This reaches the
eventual failure matrix sooner, but hides whether a failure belongs to worker
identity, object outcomes, GC receipts, effect fencing, or generation takeover.

D2. Simulate the publisher as controller calls. This is simpler but does not
prove process death, empty-scratch reconstruction, or a production-shaped
authority client.

D3. Let the replacement create a new publication identity. This avoids stable
identity derivation but can leave two independently publishable intents for the
same physical work.

## Compatibility and migration

`PublisherJob` begins at unpublished format version `1`. Unknown versions fail
closed. The worker protocol is an eval-internal boundary until later publisher
kill points pass. No package publication or stable CLI promise follows from
this RFC.

## Unresolved questions

- How the publisher resumes after a subset of object PUTs.
- How unknown PUT and Publish outcomes share one retry budget.
- How abandoned intents are fenced and quarantined.
- How publisher work is assigned and rebalanced across ranges.
- Whether authority endpoint discovery belongs in the metacluster or cell
  control plane.
