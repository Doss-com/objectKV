# RFC-0060: Replicated snapshot-lease and collection authority

- Status: active work, replicated transition and bounded control matrix implemented
- Authors: DOSS
- Created: 2026-08-24
- Depends on: RFC-0009, RFC-0010, RFC-0015, RFC-0016, RFC-0035, RFC-0059

## Decision under test

`[ACTIVE-WORK]` Extend the replicated publication authority into the owner of
durable snapshot leases, the monotonic minimum-readable version `F`, prepared
collection jobs, and the physically collected frontier `G`.

Do not create a separate lease consensus group. Lease admission, manifest
roots, collection intent, replacement publication, and delete-plan invalidation
must be serialized in one authority history. The transaction system still owns
the latest committed version `C`.

The first implementation may mirror `C` into the publication authority from
the same objectKV state machine. A later split into independent authority
groups requires an authenticated, gap-free committed-frontier feed. A worker or
client assertion is never sufficient.

## Why the authority belongs with publication

A snapshot lease is both a read-admission record and an object-GC root. If
those states live in different replicated histories, acquiring a query
snapshot can race object deletion between the two commits. Putting the lease,
root pin, collection job, and root-intent epoch in one authority makes the
following transition atomic:

```text
verify F <= T <= C
  -> register lease identity and epoch
  -> pin the exact snapshot closure
  -> advance the root-intent epoch
  -> return a durable lease token
```

The transaction system and publication authority still have separate jobs.
The transaction system orders user commits and owns `C`. The publication
authority decides whether a historical read is admitted and whether physical
history can be replaced or deleted.

## Authority state

The first cell-wide state is:

```text
observed_commit_frontier C
retention_window W
policy_floor P = max(previous P, C - W)
minimum_readable_version F
physically_collected_through G
lease_clock_tick
lease_epoch
active leases by lease identity
prepared collection jobs by job identity
configured top-level transactional manifest root
root-intent epoch
durable request outcomes
```

Each lease binds:

```text
lease identity
cell and tenant identity
snapshot version T
lease epoch
authority generation and committed log position
owner and purpose
deadline tick
exact pinned manifest closure
```

Each collection job binds:

```text
job identity
authority generation and committed log position
frozen floor F_job
input manifest identity and root revision
range-map epoch
expected collected frontier G_before
output namespace reservation
```

The worker output receipt repeats every job field and adds the immutable output
manifest identity plus checksums. Publication compares the receipt exactly.
The authority does not parse an engine-specific manifest. A trusted physical
binder must re-read the manifest bytes, enumerate every live child object, and
construct the receipt. If the binder omits a live SST, the generic authority
cannot discover the omission from object keys alone.

## Version and floor rules

The authority maintains:

```text
P = max(previous P, C - W)
L = oldest active lease version, when a lease exists
candidate F = min(P, L) when L exists
              P otherwise
F = max(previous F, candidate F)

G <= F_job <= F <= C
```

New leases require `F <= T <= C`. Because a new lease cannot be below the
current floor, recomputing `F` cannot make it retreat. A retention-window
change may hold or advance `P`; it may not lower `F` or resurrect an expired
snapshot.

The first contract uses one cell-wide `F` and `G`. Per-tenant floors are a
future optimization only if measured cross-tenant retention coupling warrants
the added state and compaction complexity.

## Lease clock

`[ACTIVE-WORK]` Expiry is a replicated logical transition, not a worker-local wall
clock decision. The authority accepts a monotonic `AdvanceLeaseClock` command,
expires every lease whose deadline is at or below the committed tick, removes
its root pin, increments the root-intent epoch, and recomputes `F` in the same
state-machine transition.

The bounded gate injects deterministic ticks. Production time-source custody,
maximum clock jumps, and clock-failure availability remain later decisions.
This gate rejects backward ticks and any local worker that treats a lease as
expired before the authority commits expiry.

## Commands

The proposed deterministic actions are:

```text
ObserveCommittedFrontier { certified C }
SetRetentionWindow { expected policy epoch, W }
ConfigureCollectionRoot { expected root name, top-level root name }
AcquireLease { lease id, tenant, T, owner, purpose, deadline, closure }
RenewLease { lease id, expected lease epoch, new deadline }
ReleaseLease { lease id, expected lease epoch }
AdvanceLeaseClock { expected tick, next tick }
PrepareCollection { job id, F_job, input manifest, range epoch, G_before }
PublishCollection { job id, exact worker receipt, replacement manifest }
AbandonCollection { job id, expected job epoch }
```

Every command has a stable request identity. A lost response after commit must
return the retained outcome after leader failover. A conflicting replay must be
rejected.

`G` is cell-wide, so `PrepareCollection` may target only the configured
top-level transactional manifest root whose closure covers all collected
ranges. Advancing a cell-wide `G` from replacement of one arbitrary range root
is invalid. A future per-range `G` requires an explicit frontier map and a
derived cell minimum.

## Crash matrix

| Phase | Injected failure or race | Required result |
|---|---|---|
| acquire | leader dies after lease commit but before reply | retry returns the same lease token; root remains pinned |
| acquire | request uses `T < F` or `T > C` | reject without adding a lease or root |
| acquire | root mark is taken before lease commit | committed lease changes root-intent epoch; stale delete reservation fails |
| renew | leader dies after renewal commit but before reply | retry returns the retained deadline and epoch |
| renew | expiry commits before renewal | renewal rejects; the old token cannot restore the lease |
| expiry | worker-local clock passes deadline first | worker cannot unpin, advance `F`, or publish collection |
| expiry | authority snapshot and leader restart | leases, tick, floor, root pins, and request outcomes restore exactly |
| prepare | `F` advances after job creation | job continues with frozen `F_job`; it never reloads the newer floor |
| prepare | input manifest changes before prepare commit | compare fails; no job token is issued |
| worker | worker dies before output persistence | old manifest remains authoritative; `G` does not move |
| worker | worker dies after output persistence | output remains an intent root or later orphan; old manifest still serves |
| publish | authority leader dies before PublishCollection commits | retry either publishes once or observes the old manifest, never an unknown mixed state |
| publish | leader dies after commit but before reply | retry returns the retained published outcome; replacement root appears once |
| publish | receipt names a stale authority generation, range epoch, input root, floor, or job | reject without changing the root or `G` |
| publish | worker claims `G` without replacement publication | reject; `G` remains unchanged |
| delete | sweep uses a mark from before acquire, renew, release, expiry, prepare, or publish | root-intent epoch mismatch rejects the delete reservation |
| failover | old leader resumes after successor activation | generation fence rejects lease, job, publish, and delete transitions |

## Frozen semantic history

Each seed must execute this bounded history through three authority processes:

1. mirror committed frontier `C = 256`, configure window `W = 64`, and derive
   policy floor `P = 192`;
2. acquire lease A at `T = 200` and lease B at `T = 224` with exact roots;
3. lose the acquire-A response, kill the leader, and recover the same token;
4. attempt and reject a backdated lease at `T = 191`;
5. renew lease A across another lost response and leader failure;
6. advance `C` to 288, which advances `P` to 224 while lease A holds `F` at
   200, then prepare collection job J at frozen `F_job = 200` against M0;
7. advance the lease clock until A expires, causing current `F` to advance to
   224 while J remains frozen at 200;
8. persist J's output, kill the worker, and keep M0 authoritative;
9. publish M1 from a replacement worker receipt, lose the authority reply,
   fail over, and recover the exact published outcome;
10. prove `G = 200`, M1 is current, B remains readable, and M0 is only a
    deletion candidate after every root releases it;
11. release B, take a fresh root mark, reserve exact deletion, restart the
    authority, and retire only the exact permit.

The first process gate may use deterministic manifest fixtures. The following
composition gate must run the RFC-0059 SlateDB worker against the issued job
token and verify exact reads after failover.

## Negative subjects

Each subject must independently replay, produce at least one bounded anomaly,
export OTel, and discard:

1. admit a lease below `F`;
2. expire and unpin from a worker-local clock;
3. let renewal after committed expiry resurrect the same lease epoch;
4. omit the lease root from root-intent epoch changes;
5. reload the current floor during a prepared job;
6. publish against a changed input manifest;
7. accept a stale authority generation or range epoch;
8. advance `G` without replacement publication;
9. apply a lost-response retry twice;
10. restore an authority snapshot without active leases or request outcomes;
11. reserve deletion from a mark made before a lease transition.
12. omit one live SST from an otherwise valid physical output receipt;
13. substitute a semantic digest for the physical output manifest identity.

## Hard gates

- three distinct authority processes and at least one leader replacement;
- committed-frontier observations are monotonic and authenticated;
- `F` and `G` never retreat, and `G <= F_job <= F <= C` always holds;
- acquire, renew, release, expiry, prepare, publish, and delete are exactly
  replayable by request identity;
- a lease transition atomically changes its root and root-intent epoch;
- no worker-local observation can expire a lease or advance `F` or `G`;
- every prepared job keeps one immutable `F_job` and input manifest identity;
- publication changes the root and `G` once or not at all;
- authority snapshot and restart preserve the complete state and outcomes;
- every stale mark, epoch, generation, manifest, and worker receipt fails
  closed;
- all negative subjects discard;
- the process history replays to the same semantic receipt.

## OTel contract

Record:

```text
snapshot_lease.active
snapshot_lease.transition_duration{operation,result}
snapshot_lease.minimum_readable_version
snapshot_lease.clock_tick
mvcc_gc.prepared_jobs
mvcc_gc.collected_through
mvcc_gc.publish_duration{result}
mvcc_gc.root_epoch
correctness.anomalies
```

Every signal includes the cell, authority generation, suite, workload, and
backend attributes. Lease identity, owner, tenant, and object key remain in
bounded artifacts, not metric attributes.

## Implementation sequence

1. `[EXISTS]` Pure lease, floor, job, and collection transitions in
   `okv-publication`, including exact closure roots, namespace reservations,
   stale receipt rejection, expiry-versus-renewal, and old-state defaults.
2. `[EXISTS]` The versioned publication command carries the new
   actions through the OpenRaft state machine. Checksummed snapshot restore
   retains active leases, and the frozen format-v1 fixture remains byte-stable.
3. `[EXISTS]` A three-process authority history covers acquire, renew, expiry,
   prepare, publish, release, delete reservation, process restart, four leader
   replacements, and three lost committed replies. Six independent process
   controls discard missing durable outcomes, backdated admission, omitted
   lease-root epoch changes, stale range epochs, stale input roots, and `G`
   advancement before publication.
4. `[EXISTS]` Compose the authority token with the RFC-0059 SlateDB worker,
   exact input and output physical closures, and authority-leader replacement.
5. `[EXISTS]` Bind read-only immutable-base opens to the exact
   authority-selected manifest identity.
6. `[EXISTS]` Wire immutable bases to replicated authority, real signed txLog
   processes, process-isolated collection, root-aware deletion, persistent
   cache handoff, and fresh-authority validation after lease release.
7. `[EXISTS]` Refuse historical reopen after a bounded live-authority failure
   and discard a stale-state availability fallback.
8. `[ACTIVE-WORK]` Add torn-cache-write and bounded multi-range eviction
   controls.
9. `[FUTURE]` Repeat against MinIO and GCS, then add concurrent writers and
   range-map movement.

## Keep and stop rules

Keep the authority shape only if lease and root state are one atomic replicated
transition, every retry returns one retained outcome, every job preserves its
frozen floor and input root, and `G` advances only with replacement publication.

Stop if a valid lease can be omitted from GC roots, if renewal can resurrect a
committed expiry, if failover loses a root or request outcome, if a worker can
advance any frontier, or if publication cannot bind the exact job token. A stop
reopens authority placement or collapses the roles into one stronger replicated
state machine; it never weakens snapshot semantics.

## Tradeoff

This design optimizes for one serializable answer to read admission, object
reachability, and collection publication. It gives up an independently scalable
lease service, accepts a publication-authority dependency for long reads, and
uses deterministic logical expiry before claiming a production wall-clock
policy.

## Current implementation evidence

`[EXISTS]` The pure authority now enforces monotonic `C`, `P`, `F`, and `G`;
exact lease epochs and deadlines; atomic lease-root epoch changes; frozen
collection tokens; input-root, range-epoch, namespace, and receipt compares;
one configured top-level collection root; and root-aware delete reservation.
The bounded history advances `C` from 256 to 288 before preparing at
`F_job = 200`, then proves expiry of lease A moves `F` to lease B at 224
without changing the frozen job.

`[EXISTS]` The consensus state machine serializes the added fields without
changing the byte-stable empty format-v1 snapshot. A checksummed restart test
restores the exact active lease state.

`[EXISTS]` Candidate `5f62082`, suite hash `9c582fe0`, kept the three-seed
process history in run `78df81e3`. It executed 42 checks through 21 process
starts, 12 kills and leader replacements, nine dropped replies, nine recovered
outcomes, and nine exact retries. The final state was `F = 224`, `G = 200`, no
leases, no prepared jobs, the replacement top-level root, and no delete
reservation. Fresh process replay was semantically exact.

`[EXISTS]` Disabling retained request outcomes discarded in run `bd9b73b9` on
all three seeds: the lease commit survived failover, but the dropped acquire
reply could not be recovered.

`[EXISTS]` Candidate `87794a6`, suite hash `134c97a7`, kept the same positive
history in run `1dc3440f` and independently discarded six unsafe process
subjects on all three seeds. Runs `578e62e8`, `a5cde72d`, `63ff010e`,
`df749dac`, and `95c369e4` prove that backdated admission, omitted lease-root
epoch changes, stale range epochs, frontier advancement before publication,
and stale input-root publication are each visible. Run `90104df9` retains the
lost-outcome control. Fresh replay stayed exact for every subject.

`[ACTIVE-WORK]` The remaining authority gate covers worker-local expiry,
renewal after committed expiry as a real process history, authority snapshot
restore controls that omit leases or outcomes, stale authority generation,
and stale delete-mark acceptance.

`[EXISTS]` Candidate `3c8a52e`, suite hash `aee84768`, kept the three-seed
physical composition in run `a9d1b1f8`. It executed 24 checks, started three
authority processes per seed, replaced the leader once per seed, authorized
only after discovering the exact SlateDB input closure, compacted real MVCC
history, re-read the replacement manifest plus every live SST, and advanced
the authority root and `G = 13` together. Semantic replay was exact even though
fresh SlateDB object identities differed between runs.

`[EXISTS]` Three controls discarded on all seeds. Omitting one output SST was
detected in run `0f0232da`; substituting a semantic digest for the physical
manifest was detected in run `15ecd6ac`; skipping authority failover was
detected in run `ad93d32a`. The first two controls also prove that exact
engine-specific closure construction is part of the trusted computing base.
The generic authority validates the submitted capability but cannot infer
children omitted by a lying binder.

`[EXISTS]` Candidate `b228bd3`, suite hash `86eacf38`, kept run `49d4d445`
after adding an authority-bound read-only SlateDB view. It verifies the exact
manifest object identity, filters the reader's manifest listing at that root,
and disables WAL replay. After compaction advances SlateDB internal latest from
M0 to M1, independent bound readers still return exact floor and latest MVCC
points and scans from M0 and M1. A forged manifest digest and a manifest key
from another database fail before open.

`[EXISTS]` Candidate `f46d632` adds an `AuthorityRangeRoot` and process-local
serving view. The root binds the exact manifest, base frontier, retention floor,
generation, and base commit-chain digest. The view admits only a gap-free
commit-chain suffix with increasing versions, exact target coverage, and a
valid quorum certificate for every required log set. Numeric versions may skip
non-commit log positions. Its local
physical test serves exact version 8 from M0 through version 2 plus commits 5
and 8, swaps to M1 through version 5 plus commit 8, keeps an in-flight M0 reader
exact, and rejects a tampered certificate.

`[EXISTS]` Candidate `fc30e59`, suite hash `9bf20342`, stores the range-root
payload in the replicated authority and reads quorum attestations from real
txLog processes. Correct run `da53cee9` keeps M0 and M1 workers exact across
authority failover and two txLog member failures. Six controls discard on the
same process topology.

`[EXISTS]` Candidate `c79e099`, suite hash `2fb2eb53`, moves collection into a
dedicated child process. The controller re-reads both exact physical closures
after collector exit, then replaces the authority leader and publishes through
the successor. Correct run `3a0e5bfb` keeps; controls `d9baa91e`, `d188aa0a`,
and `4cadcddd` discard.

`[EXISTS]` Candidate `2742400`, suite hash `fd5b52a6`, composes lease pinning
with real physical deletion. Correct run `7805dd6d` blocks every M0 candidate
while leased, refreshes the mark after release, deletes six objects across
three seeds through exact permits, and keeps a post-GC M1 worker exact. Bypassed
pin `83a7544a`, stale mark `257069b4`, and retire-before-delete `206a22e2`
discard. Remote object storage, concurrent serving, worker restart, and
required-profile OTel export remain open.

`[EXISTS]` Candidate `7eae670` adds typed validation of an exact active lease
token for read-side admission. The token names the outer published Range
Engine root, while its closure must also contain the inner immutable-base
manifest. This distinction is required because publication authority and
physical base history can advance independently. Release, expiry, token drift,
or root drift refuses before cache or object-store access.

`[EXISTS]` Candidate `e06a159`, suite hash `f1bfd782`, composes the read rule
with process handoff and physical collection. Correct run `2b1bdc6a` has a
fourth worker read live authority after M0 lease release and refuse the warm
cache reopen. It then reclaims the compacted M0 closure and keeps M1 exact.
Negative run `93773b96` injects the pre-release authority snapshot, reopens M0
in all three seeds, and discards. Authority freshness is therefore part of
worker bootstrap.

`[EXISTS]` Candidate `52ca95e`, suite hash `2beb3824`, adds a bounded authority
read to a fifth worker. Correct run `805cc0cf` records three fail-closed
unavailable-authority receipts and opens zero historical views. Negative run
`1c769733` enables stale fallback after the failed live read, opens M0 in all
three seeds, and discards. Remote behavior and the production deadline policy
remain open.
