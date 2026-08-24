# RFC-0020: Publisher recovery after a lost replicated Publish response

- Status: proposed
- Authors: objectKV contributors
- Created: 2026-08-23
- Supersedes: none

## Decision

The next executable increment of RFC-0016 kills a dedicated publisher after
its `Publish` command is quorum applied, the reader-visible root is installed,
and the prepared intent is retired, but the successful response is dropped.
The controller then kills the authority leader that accepted the command. A
replacement publisher starts with empty scratch, reconstructs the same job and
stable request identity, resolves the original outcome through the successor
authority, retries the exact command once, and requires the retained response
to replay without a second state transition or any object PUT.

This increment separates replicated authority ambiguity from the data and
manifest object ambiguity admitted by RFC-0018 and RFC-0019. It does not combine
the fault with repeated unknown responses, outcome expiry or snapshot
compaction, a later root that supersedes this publication, multipart residue,
abandoned-intent policy, sweeper recovery, or generation handoff.

## Context and invariant

Object identity cannot resolve the outcome of an authority command. After a
lost `Publish` response, an exact visible root is evidence about current state,
but it is not a durable receipt for the client invocation. The root may later
advance. The stable request outcome must survive leader loss and exact retry.

Tigris's public consistency scripts make the distinction concrete. They poll
until ETags and bodies converge, but do not relate client responses to one
durable serialized operation. objectKV must prove both final visibility and
acknowledgement-aligned history.

For canonical job `J`, stable publish request `P(J)`, and authority position
`V(P)`:

```text
quorum_apply(P(J))
and publish_response(P(J)) = unknown
and publisher_dies
and accepting_leader_dies

  -> durable_outcome(P(J)) = accepted at V(P)
  -> root(J) = manifest(J)
  -> intent(J) = retired
  -> replacement_publish_request = P(J)
  -> retry(P(J)) = original_outcome(P(J))
  -> authority_position_after_retry = authority_position_before_retry
  -> object_puts_after_replacement = 0
```

The reader-visible root and intent retirement are one replicated transition.
The retained request result includes the exact command fingerprint, status,
outcome, and applied log position. Reusing an identity with different bytes
fails closed.

## Fault boundary

The first publisher process:

1. replays the quorum-durable `Prepare` outcome;
2. creates or exactly recovers every data object and the manifest;
3. walks the complete named closure;
4. sends `Publish` to the current authority leader with the stable identity;
5. lets the real OpenRaft command reach quorum and apply;
6. has the test-owned RPC boundary close without returning the response;
7. emits `publish_response_unknown` and waits for `SIGKILL`.

The controller independently proves the exact root, retired intent, retained
request outcome, and applied position before killing the publisher. It then
kills the accepting authority leader and elects a successor. Dropping the reply
is eval infrastructure, not a production client mode.

## Replacement protocol

The replacement receives the same seed, surviving authority endpoint set,
object-store root, and a new empty scratch directory.

1. Recompute the canonical job digest and stable `prepare` and `publish`
   identities.
2. Perform no object mutation.
3. Read the publish outcome linearly from the successor authority.
4. Require the accepted status, `Applied` outcome, original log position, exact
   command fingerprint, root installation, and intent retirement.
5. Retry the exact `Publish` command once.
6. Require byte-exact replay of the retained response and no authority revision
   or root-intent-epoch change.
7. Walk the visible manifest closure by exact named reads.

The replacement may use object reads to validate the reader-visible closure.
It may not replay data or manifest PUTs because this gate begins after the
complete closure and root transition already exist.

## Negative subject

`convergence_only_duplicate_publish` combines an unsafe authority with an
unsafe replacement:

- request deduplication and durable outcome lookup are disabled;
- missing-intent and prior-root checks are bypassed for the retry;
- the replacement treats the matching current root as sufficient evidence;
- it reissues `Publish`, causing the root transition and root-intent epoch to
  apply a second time.

The final root and every object remain exact, so a convergence-only checker
passes. The acknowledgement-aligned oracle must reject the subject because the
original invocation has no durable result and the retry applies a second
authority effect. This negative control is deliberately bounded to the eval
authority and is never a supported configuration.

## Eval plan

`object-publication-publisher-publish-recovery-v1` runs seeds `1103`, `2207`,
and `3301` through fourteen labeled checks per seed. The aggregate budget is
exactly 42 events. Correctness anomalies are the only primary metric.

The clean subject must prove:

- the active generation authorizes the canonical publisher job;
- `Prepare` and its request outcome are quorum durable;
- the complete named closure is exact before the publish attempt;
- `Publish` is quorum applied while its response is unknown;
- root installation and intent retirement are atomic at the fault barrier;
- the original publish outcome and applied position are quorum durable;
- the first publisher receives real `SIGKILL` after the publish effect;
- the accepting authority leader receives real `SIGKILL` and a successor wins;
- the replacement starts with empty scratch;
- the replacement reconstructs the same job and publish identity;
- exact retry replays the original outcome and applied position;
- the publish transition applies exactly once;
- the replacement issues zero object PUTs;
- a reader walks the exact visible closure;
- two fresh seed-1103 controllers emit byte-identical semantic receipts;
- OTel logs, metrics, and traces have zero drops.

The negative subject must retain an exact final root and closure while failing
at least durable outcome recovery and exactly-once application. This proves the
correctness oracle is stronger than final-state convergence.

Secondary receipts include authority and publisher process starts, process
kills, authority failovers, object PUT attempts and effects, named reads,
publish-command attempts and applies, dropped replies, recovered outcomes,
exact outcome replays, and empty-scratch restarts. Object keys, digests,
request identities, paths, ports, PIDs, timestamps, and log positions remain
forbidden metric attributes.

## Alternatives

D1. Resolve the unknown response by reading only the current root. This is safe
only while that root remains current and does not preserve the result of the
original invocation after later publications.

D2. Retry with a fresh request identity. A fresh identity cannot retrieve the
original result and may duplicate an authority effect if current-state guards
are weakened or evolve.

D3. Let the normal client hide the unknown response by retrying internally.
That is the production behavior, but it prevents this gate from proving the
physical fault barrier, publisher death, empty-scratch reconstruction, and
successor outcome read independently.

D4. Combine leader death, root supersession, outcome expiry, and snapshot
restore in this schedule. Those cases are necessary but would obscure the first
publisher-to-authority ambiguity boundary.

## Compatibility and migration

No public API or on-disk format changes. The raw dropped-reply client method,
faulty authority mode, counters, and semantic report are eval-internal. The
retained outcome format is already owned by the OpenRaft application journal.
Outcome expiry or snapshot compaction requires a separate compatibility RFC and
must preserve the declared retry window.

## Unresolved questions

- How long publish outcomes remain queryable and how expiry is fenced from
  clients still inside their retry window.
- How outcome records enter and restore from authority snapshots.
- Whether a later root may supersede the publication before its worker has
  observed the original outcome.
- How repeated dropped replies consume one bounded retry budget.
- Whether direct readers require the original applied position in a root
  version token.
- How the same outcome contract composes with G1 to G2 authority recovery.
