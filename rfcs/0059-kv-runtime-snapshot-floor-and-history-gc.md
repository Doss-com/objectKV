# RFC-0059: KV Runtime snapshot floor and MVCC history collection

- Status: local collection and authority composition validated, serving-root binding active work
- Authors: DOSS
- Created: 2026-08-24
- Depends on: RFC-0003, RFC-0031, RFC-0035, RFC-0058

## Decision under test

`[PROPOSED]` Separate snapshot admission from physical history collection.
The replicated Cell authority owns durable query leases and one monotonic
minimum-readable version. A KV Runtime and every compaction worker consume
that floor. SlateDB does not decide which objectKV snapshots remain valid.

For the first cell implementation, use one cell-wide floor:

```text
C = latest committed cell version
W = configured recent-version window
L = oldest unexpired durable snapshot lease, when one exists

policy floor      = max(0, C - W)
admission floor F = min(policy floor, L) when L exists
                    policy floor otherwise
```

`F` may advance but never retreat. A new lease must select `T >= F`; a lease
request cannot resurrect a snapshot after the floor has passed it. Tenant-local
floors can follow only if cross-tenant retention coupling becomes a measured
problem.

## Two related frontiers

The system reports two different values:

```text
minimum_readable_version = F
  versions below F are rejected by every serving path

physically_collected_through = G, where G <= F
  published object state has removed eligible history below G
```

Advancing `F` before a compaction is safe because old history remains present
but inaccessible. Advancing `G` requires a successfully published replacement
manifest. A failed job may delay reclamation; it cannot raise `G`.

## Lease contract

A durable lease record binds:

```text
lease identity
cell and tenant
snapshot version T
authority generation and lease epoch
expiry decided by the authority clock
owner and purpose
```

The read-version service obtains `T` and registers the lease through one
authority decision. Renewal and release are idempotent. Expiry, generation
change, and explicit release are replicated transitions. Local worker clocks
do not authorize collection.

Short OLTP reads do not need individual durable leases. They execute inside
the configured recent-version window. Long DataFusion queries, checkpoints,
backups, clones, and moves use durable roots. Existing publication-root rules
still protect immutable objects referenced by those owners.

## Compaction rule

Each compaction job freezes one floor `F_job` at creation. For every logical
user key, physical entries are ordered newest first. The filter:

1. keeps every version newer than `F_job`;
2. keeps the first value or point tombstone at or below `F_job`;
3. drops older entries for that key;
4. keeps objectKV metadata outside the user namespace;
5. aborts on malformed physical keys or non-descending version order.

The retained anchor reconstructs the exact value at `F_job`, including
absence after a point clear. A compaction whose input begins in the middle of a
logical key conservatively keeps its first below-floor entry. This may retain
extra bytes but cannot delete the only known anchor.

SlateDB internal snapshot retention, when present, lowers the effective job
floor. The filter never collects an entry protected by a lower internal
snapshot boundary.

## Publication and crash order

```text
authority advances admission floor F
  -> compaction job persists F_job plus input manifest identity
  -> worker writes filtered immutable outputs
  -> publication authority validates input, output, epoch, and F_job
  -> replacement manifest publishes with collected frontier G = F_job
  -> superseded objects become candidates for root-aware object GC
```

Worker death before publication leaves the old manifest and history intact.
An output from a stale range epoch or an unregistered floor cannot publish.
Object deletion remains a later root-aware operation, never a side effect of
the filter.

## Frozen semantic cases

1. a read below `F` returns `snapshot_expired` before touching user data;
2. reads in `[F, C]` exactly match the pre-collection reference state;
3. reads above `C` return `snapshot_unavailable`;
4. a floor-visible set survives while older versions disappear;
5. a floor-visible point tombstone survives and prevents resurrection;
6. every version newer than the floor survives;
7. a floor can advance or remain equal but cannot retreat;
8. a new lease below the floor is rejected;
9. an active durable lease prevents the floor from passing its version;
10. close and empty-cache reopen preserve the same read bounds and answers.

## Negative subjects

1. compute the floor without the oldest active lease;
2. admit a new lease below the current floor;
3. drop every version at or below the floor instead of retaining one anchor;
4. drop a point-tombstone anchor and reveal an older value;
5. reload a newer floor while one compaction job is already running;
6. publish a collected frontier when the filtered output did not publish;
7. let a worker-local clock expire a durable lease;
8. delete superseded objects without walking every publication root.

## First implementation slice

`[EXISTS]` The generation-zero mechanism includes a monotonic retention floor,
explicit minimum-readable bounds on SlateEngine point and range reads, and a
SlateDB compaction-filter supplier that freezes the floor per job. Unit tests
exercise the keep-anchor rule, tombstones, malformed keys, internal snapshot
protection, and floor monotonicity.

`[EXISTS]` `evals/suites/cell-kv-runtime-mvcc-history-gc.toml` drives a real
separate compactor against depth-256 history with retained windows of 1, 16,
and 64 versions. Each seed starts in a fresh process, stages eight overlapping
L0 SSTs, publishes one filtered sorted run, reopens independently, and records
manifest bytes, object I/O, collection duration, cold points, cold scans, RSS,
and a semantic replay receipt. Five controls ignore the lease floor, drop the
floor anchor, drop the tombstone anchor, reload the floor during the job, or
claim collection without publication.

The authority-backed lease state machine and crash matrix remain separate
process gates.

`[EXISTS]` Candidate `3c8a52e` now stages the real collector behind one issued
`CollectionJobToken`. The worker first discovers and re-reads the exact input
manifest plus every live SST, then requests authorization. Only after the
token matches the frozen floor and physical input does SlateDB compact. The
binder re-reads the replacement manifest plus every live SST, fails if any
object escapes the authorized namespace, and submits that exact closure after
the publication-authority leader is replaced. The authority root and `G` move
together only after the successor accepts the receipt.

`[EXISTS]` Candidate `b228bd3` adds a read-only authority-bound SlateDB view.
It verifies the selected manifest path, length, and SHA-256, hides every newer
manifest from the embedded reader, and disables WAL replay. The physical gate
now proves that both the input M0 view and replacement M1 view return exact
floor and latest MVCC points and scans after SlateDB internal latest has moved
to M1. Correct run `49d4d445` kept 27 checks across three seeds.

`[EXISTS]` Candidate `f46d632` adds the first Range Engine base-tail view.
An `AuthorityRangeRoot` binds one exact SlateDB manifest to its covered-through
version, minimum-readable version, transaction generation, and commit-chain
digest. Opening the view requires every txLog commit above the base to be
strictly increasing, chain-linked, and covered by a valid quorum certificate
for every required log set. Numeric commit versions may skip non-commit log
positions. The final authenticated commit must equal the requested target. The
local physical test serves the same exact version 8 from M0 through version 2
plus commits 5 and 8, then M1 through version 5 plus only commit 8. An in-flight M0 reader
remains exact after the process-local root pointer changes to M1, and a tampered
certificate fails closed.

`[EXISTS]` Candidate `fc30e59` freezes the composed gate as suite
`cell-range-serving-handoff-v1`. A disposable worker resolves M0 from the
three-node publication authority, verifies the root object, manifest, and live
SST closure, then reconstructs target version 10 from two surviving members of
each required signed txLog set. After publication-leader failover, the
successor accepts M1 only with the exact prior-root compare and a fresh worker
uses only the post-M1 tail. Correct run `da53cee9` kept with zero anomalies and
exact replay across three seeds. Six independent controls discarded.

`[EXISTS]` Candidate `c79e099`, suite hash `2fb2eb53`, moves the physical
collector behind a child-process boundary. Correct run `3a0e5bfb` starts three
collectors across three seeds, keeps all 30 checks, and independently re-hashes
the M0 and M1 closures after each process exits. All three existing controls
discard.

`[EXISTS]` Candidate `2742400`, suite hash `fd5b52a6`, adds root-aware physical
deletion to the composed Range Engine handoff. Correct run `7805dd6d` proves
that M0 objects remain exact while its snapshot lease is live, become
reclaimable only after lease release advances the root epoch, and can be
physically removed through exact permits without breaking a fresh M1 worker.
All nine controls discard with exact replay.

`[ACTIVE-WORK]` Remote storage, concurrent writes and tail application, worker
restart, range tombstones, and serving performance remain open.

## Measured local result

`[EXISTS]` Candidate `3c9f008` kept all three clean points under suite hash
`c288cd4d`; all five controls discarded.

| Retained versions | Post-GC bytes / retained logical bytes | Bytes removed | Compaction | Cold point p99 | Cold scan bytes | Range GETs |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | `1.225x` | `99.57%` | `241 ms` | `0.155 ms` | `0.32 MB` | 7 |
| 16 | `1.111x` | `93.73%` | `239 ms` | `0.161 ms` | `4.66 MB` | 73 |
| 64 | `1.107x` | `75.01%` | `258 ms` | `0.181 ms` | `18.57 MB` | 285 |

Every point began with 74.3 MB of live depth-256 SST state. Compaction read the
same 74.0 MB input at every window. Output bytes and floor-scan bytes scaled
with the retained window, not the original history depth. Exact floor and
latest point and range reads, the tombstone anchor, expired and future refusal,
publication, and reopen all passed.

This is a local filesystem object-store curve. It does not establish GCS or S3
latency, remote request economics, concurrent write behavior, or production
capacity.

## Operational bound found

The pinned SlateDB serving profile allows eight overlapping L0 SSTs per key.
The first diagnostic staged sixteen while compaction was deliberately disabled
and stopped at L0 backpressure before the measurement worker could start. The
accepted fixture stages eight.

Production must run compaction continuously or rate-limit mutation intake
before the per-key L0 bound. Increasing the bound merely moves the debt and is
not the default decision.

## Keep and stop rules

`[DECIDED]` Keep the local filter for the next prototype. Every tested snapshot
at or above the floor remained exact, every control discarded, and physical
amplification converged toward the configured retained window rather than total
historical depth.

Stop if collection can remove a pinned snapshot, if a job observes a changing
floor, if a tombstone anchor can resurrect data, or if publication cannot bind
the exact input manifest and frozen floor. A stop reopens an objectKV-owned
segment rewriter rather than weakening snapshot semantics.

`[EXISTS]` The frozen process-composed Range Engine base plus txLog handoff gate
kept at candidate `fc30e59`; the collector process boundary kept at candidate
`c79e099`; old-root reclamation kept at candidate `2742400`. Candidate
`e06a159` then compacts M0 into an independent M1 closure, requires a fourth
worker to read live authority after M0 lease release, reclaims the old outer
root, inner manifest, and data object, and keeps M1 exact. Its stale-authority
negative reopens M0 and discards. `[ACTIVE-WORK]` Lease renewal and expiry
races, stale generations, remote object storage, concurrent writes, and range
tombstones remain open. Candidate `52ca95e` separately proves that a bounded
authority-read failure refuses historical reopen and that stale fallback is
detected.

## Tradeoff

This design optimizes for bounded long-running storage and scan cost without
making SlateDB the snapshot authority. It gives up arbitrary unleased time
travel, accepts conservative over-retention in partial compactions, and adds a
durable lease service plus version-aware compaction to the Cell control plane.
