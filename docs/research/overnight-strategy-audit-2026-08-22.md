# objectKV 12-hour strategy audit

Status: `[EXISTS]` pinned overnight evidence run completed on 2026-08-23.

## Directional decision

Continue objectKV for one bounded vertical proof cycle. Narrow the current
claim and the current SlateDB posture:

- `[EXISTS]` Individual semantics, replicated recovery, publication, S3
  authority, and HTAP overlay mechanisms pass their isolated gates.
- `[EXISTS]` The untuned SlateDB physical incumbent crosses its stop threshold,
  but the one frozen serving-worker profile removes dataset work from open and
  passes the cold-point budgets across three seeds.
- `[EXISTS]` A bounded local process path now connects `CommitEnvelope`, OCC,
  objectification, a replicated publication root, the `C/O` frontier, a copied
  quorum-WAL suffix, and an empty-state serving read at `T`.
- `[EXISTS]` A second bounded path removes the copied suffix and reads committed
  envelopes directly from the live replicated transaction authority after its
  leader dies.
- `[EXISTS]` A third bounded path retains that envelope in three dedicated
  range-tagged tLog processes. After one process dies, a fresh worker requires
  matching tag-`10` records from both survivors and reaches exact `T=10`.
- `[EXISTS]` A fourth bounded path stages one transaction, survives two commit
  proxy deaths, waits for quorums from two required tagged log sets, then
  publishes and acknowledges once at `T=11`. A fresh worker reconstructs the
  exact visible state from both log sets.
- `[EXISTS]` A fifth bounded path requires policy-bound Ed25519 quorum
  certificates over the exact staged statement. Five forged or stale
  certificate controls are rejected before visibility.
- `[EXISTS]` A sixth bounded path carries the exact fully certified staged head
  through a transaction-system generation fence, voter-set handoff, successor
  activation, lost takeover reply, and exact retry. The original old-generation
  envelope publishes once before the successor commits transaction 12.
- `[EXISTS]` A seventh bounded path durably fences every old-generation tLog
  set, proves quorum absence in an incomplete set, aborts transaction 11
  without advancing visible frontier `10`, and commits successor transaction
  12 from the last committed chain. A restarted tLog quorum rejects late
  old-generation appends.
- `[EXISTS]` An eighth bounded path classifies a four-record staged window from
  exact authenticated tLog inventories. It recovers transactions 11 and 12,
  aborts the first quorum-absent record and its dependent suffix, consumes the
  full window, and commits successor transaction 15. Six unsafe recovery
  policies discard.
- `[EXISTS]` Sustained lag, authenticated pop, failed-tLog repair, replicated
  log-set policy movement, and resumable base-plus-live-tail catch-up pass
  their bounded paths and controls.
- `[EXISTS]` Three ordered resolver processes now match the centralized Cell v0
  conflict oracle for cross-range transactions. This is the first bounded Cell
  v1 scaling indicator, not a throughput or online-map-movement result.
- `[EXISTS]` The intended resolver path no longer requires the RFC-0048
  per-decision journal. Three memory-only resolver processes handle ordered
  batches of eight. Resolver loss ends the old transaction-system generation;
  a replicated fence records its durable floor; empty successors reject old
  traffic and continue from that floor. This is not yet composed with the
  authenticated tLog fence or multiple commit proxies.
- `[PROPOSED]` PostgreSQL and ZebraDB HTAP are two independent proofs. A literal
  PostgreSQL bridge keeps PostgreSQL WAL, LSN, and tuple MVCC authoritative;
  the object page store is subordinate materialization. The HTAP path separately
  requires a durable snapshot manifest, lease, and analytical-tail source.

Confidence is directional, not a product feasibility score: 58 percent that a
bounded FoundationDB-like cell with object-native permanent bulk storage is
feasible, and 20 percent that the current implementation proves a complete
cell.

Those percentages are the frozen entry estimate for candidate `a56442a`. Newer
prototype evidence is recorded below but does not rewrite the running audit's
source identity or its 12-hour receipts.

`[EXISTS]` The latest working estimate after candidate `b69b245` is 98
percent that the bounded cell concept is feasible and 94 percent that the
current implementation proves a complete Cell v0. RFC-0048 adds the first
bounded Cell v1 resolver-scaling indicator. RFC-0049 removes its synchronous
resolver persistence and finalization from the intended path through bounded
whole-generation recovery, but neither result raises the Cell v0 estimate.
These are directional judgments, not statistical posteriors. The
increase from the frozen entry estimate reflects transaction composition,
object recovery, routine repair, the PostgreSQL seam, the local physical cliff,
format-compatible separate-role compaction, and real worker-process reclaim
under overwrite pressure all receiving executable positive evidence. The same
physical serving and compaction contract now also passes through a pinned
MinIO S3-compatible HTTP service. A real coordinator can die after durable
worker output and a fresh coordinator commits the exact output without
rerunning maintenance. A bounded 1,000-transaction concurrent history now also
passes through three real authority processes with leader death and exact lost
reply recovery. Two live compaction coordinators now also advance one durable
epoch and the stale process self-fences before the newer coordinator publishes.
Public-cloud economics, root expiry and abandonment policy, coordinator
election and host partitions, broad generated range histories, multiple proxy
ordering and generation rollover, online resolver split or merge, hot-range curves,
and full PostgreSQL system-state recovery remain the largest reasons not to
raise it further. The
tagged-tLog path now removes the transaction authority from serving recovery,
and the staged commit path waits for both required log-set quorums before
visibility and acknowledgement. Those quorums are now independently verified
against authority-installed member policy and process signatures. Signer key
custody, policy rotation, recovery-fence authorization, moving log sets,
partitioning, and production serving availability remain open. A fully
certified head now survives generation takeover without creating a second
transaction history. An incompletely certified head aborts only after durable
fence and absence quorums rule out a late old-generation commit. A bounded
pipelined window now also recovers only its longest quorum-present prefix and
deterministically aborts the dependent suffix. One failed tLog can now be
rebuilt as a distinct non-voting learner from an active survivor quorum,
restarted, and certified ready without entering capacity or serving counts.
That learner can now move through one replicated policy epoch, persist
authority-certified activation, fence its removed predecessor, and keep
serving after another active member fails. Candidate `254cf421` separately adds
resumable base chunks plus an ordered live tail while the active policy keeps
committing. This remains one local repair, not remote transfer, multiple
simultaneous repair, transfer cleanup, or concurrent policy movement.

### In-run vertical indicator

`[ACTIVE-WORK]` An isolated throwaway prototype now connects OCC, atomic
multi-key mutation, Raft-ordered versions, the existing `CommitEnvelope` codec,
durable outcomes, leader failover, exact retry, retained-log replay, and full
three-process convergence. Seeds `1103`, `2207`, and `3301` each pass 11 checks
with zero anomalies. This closes the semantic-to-Raft composition question for
one centralized Cell v0 prototype. Objectification, frontier advancement, WAL
pop, empty-cache serving, general history checking, distributed proxy behavior,
and partitioned resolvers remain open, so this is not a complete-cell claim.

Subsequent candidates add a durable transaction-authority snapshot, independent
objectification and `O_cell`, safe log pop at `min(O_cell, S_authority)`, exact
empty-cache row reconstruction, and fresh learner recovery through snapshot
plus retained suffix. These are separate admitted gates, not yet one production
cell runtime.

Candidate `1e01b08` adds the first bounded concurrent composition gate. Runs
`9616bf69` and `f66bb379` each evaluate 3,000 logical transactions across three
seeds, including 300 concurrent rounds, three leader process kills, and three
lost replies. Both keep with 2,100 commits, 900 durable conflicts, exact replay,
and zero anomalies. Omitted-read-conflict control `c837f980` commits all 3,000,
produces no conflicts, and discards with two intended anomalies per seed. This
closes the schedule-independent bounded history, not arbitrary histories or
partitioned roles.

Candidate `dea0b20` closes the first local ground-truth GC boundary. Runs
`8d606761` and `26b19dfb` preserve completed but unpublished compaction output
as an active-job root, commit those exact objects after coordinator replacement,
then delete an aged SST absent from every manifest and active job. Dry-run
control `161eac32` leaves only that orphan and discards. This closes one local
root transition, not the complete checkpoint, clone, backup, analytical-lease,
multi-tenant, or public-cloud root graph.

Candidate `a93041f` adds actual read values and non-overlap order to the bounded
transaction history. Run `56a132c6` keeps after checking 1,200 linearizable read
observations, 300 committed actual-read dependencies, and 727,650 real-time
edges across 3,000 transactions. Omitted-conflict control `aa460aa8` commits all
3,000 transactions but discards on the actual-read-dependency class. This
closes one commit-sequence witness, not exhaustive history search, range
phantoms, multiple read-version proxies, or partitioned resolvers.

Candidate `5d4427d` adds the first actual range-read dependency cycle. Run
`04b84730` commits 300 insertions and rejects 300 dependent range writes across
three seeds, checks 600 dependency edges, kills one leader per seed between the
dependent submissions, and converges with zero cycles. Omitted-range control
`f4678cd8` commits all 600 transactions and exposes 300 cycles while its final
rows and process convergence still look valid. This closes one deterministic
empty-range insertion phantom, not generated range histories, range clears,
multiple read-version proxies, or partitioned resolvers.

Candidate `d910d10` moves the cross-proxy causal-floor witness into two
independent OS processes per seed. Run `eec5ca77` honors 300 post-commit minimum
versions, observes all 300 acknowledged values, and survives three authority
leader deaths. Ignore-minimum control `d280df19` returns a valid pre-commit
cache and produces 300 minimum-version violations plus 300 stale observations.
This closes the process and session-token boundary, not concurrent batching,
lag policy, generation rollover, or direct serving-worker reads.

Candidate `d1ce1ec` closes the bounded local publication root graph. Run
`885dfdb4` preserves checkpoint, clone, backup, analytical-lease, and
tenant-move closures through durable authority reopen and mark/sweep. Selective
clone unpin reclaims only its unique closure, shared objects survive, and all
three roots pinned after mark invalidate their stale delete plan. Omitted-lease
control `6e8ce843` deletes the unregistered live closure, produces 12 anomalies,
and discards. This closes the explicit local root vocabulary, not expiry,
abandoned moves, cloud inventory behavior, independent host loss, or a
distributed sweeper.

Candidate `9e733e2` closes the first bounded serving recovery equation. Run
`ed0cdfe8` starts a fresh worker process per seed, resolves the exact replicated
publication root, verifies object state through `O=8`, quorum-recovers the
retained suffix, and reconstructs exact rows at `T=10`. Ignore-suffix control
`690e0844` opens the same valid base but stops at `8`, returns stale rows,
produces nine anomalies, and discards. This closes `ObjectState(O) +
RetainedMutations(O,T]` in one local process fixture, not original OpenRaft log
routing, concurrent range serving, independent hosts, or public-cloud failure.

Candidate `e1c2437` removes the copied suffix and closes the first live
transaction-to-serving role boundary. Run `bf79522d` kills the transaction
leader after `C=10`, starts a fresh worker, and fetches the committed envelope
in `(8,10]` through a linearizable request to the successor at authority
position `11`. All three workers reconstruct exact rows with zero copied WAL
directories. Dropped-envelope control `3db9c604` contacts the same authority but
stops at `8`, returns stale rows, produces nine anomalies, and discards. This
admits committed envelopes as the serving mutation boundary, not a dedicated
tagged tLog, push streaming, range routing, or independent log failure.

Candidate `beec908` adds the first dedicated range-tagged tLog path. Run
`851d0654` starts three tLog processes per seed with private synchronized roots,
copies the exact committed envelope and required tags, rejects a retained-byte
overflow on every node, kills one tLog, and reconstructs exact `T=10` from the
two survivors. Missing-tag control `136b2523` receives both survivor responses
but no tag-`10` quorum record, stops at `8`, produces 12 anomalies, and
discards. This admits one tagged suffix and a hard byte bound, not integrated
commit acknowledgement, multi-record streaming, repair, or partitioned logs.

Candidate `c549587` closes the first integrated commit-visibility boundary.
Run `5a2e5a7f` passes 84 checks across three seeds. Proxy one dies after making
log set `10` durable; proxy two recovers the same envelope, makes log set `20`
durable, and dies before publication. Neither advances the visible frontier
past `10`. Proxy three publishes and acknowledges the same envelope once at
`T=11`, and a retry returns `already_committed` without another append. Eighteen
tLog processes retain eighteen exact records, and fresh workers reconstruct the
visible state from both log-set quorums. One-set acknowledgement control
`0da1a0c1` leaves set `20` empty, stays visibly at `10`, produces 51 anomalies,
and discards. Receipt authentication, staged-head abort, generation takeover,
multi-record lag, repair, and partitioning remain open.

Candidate `6a81821` closes the proxy-forgery boundary. Run `f5e3720a` passes 96
checks across three seeds. Eighteen tLog processes synchronize eighteen exact
records and produce 45 Ed25519 attestations over the staged statement. The
authority verifies both log-set certificates against independently installed
member policies, then publishes once at `T=11`. Unsigned, duplicate-signer,
wrong-log-set, tampered-statement, and obsolete-policy controls each produce 51
anomalies and discard. This admits authenticated local durability evidence, not
key custody, policy rotation, generation takeover of an incomplete staged head,
multi-record lag, repair, or partitioning.

Candidate `f350a12` closes the fully certified staged-head generation-takeover
branch. Run `959a2211` passes 105 checks across three seeds. The old generation
remains visible at `T=10` while its data log is fenced, the voter set changes,
and generation 2 activates. The active successor publishes the original
transaction-11 envelope once, returns the retained result after a lost reply,
rejects old-generation publication, and then commits transaction 12. Early
takeover, missing-certificate, tampered-expectation, skipped-head, and
generation-rewrite controls produce 6, 3, 3, 30, and 27 anomalies and discard.
This admits one fully certified head, not safe abort of an incomplete head or
recovery of a multi-record staged prefix.

Candidate `341beb9` closes the one-head incomplete branch. Run `338ef8b4`
passes 132 checks across three seeds with zero anomalies and exact replay.
Every old-generation tagged-log set returns a durable fence quorum under one
recovery identity. One set lacking a durability certificate also proves a
write-quorum of local absence. The active successor aborts transaction 11,
leaves rows and visible frontier `10` unchanged, replays the lost abort reply,
and commits successor transaction 12 from the last committed chain. Restarted
tLog processes retain their fence and reject six late old-generation appends.
Early abort, one absence signer, a missing log-set fence, forged absence,
volatile fence, and sequence-reuse controls produce 3, 9, 6, 12, 6, and 6
anomalies and discard. This admits one incomplete head under the bounded signer
model, not a multi-record prefix or production fence authorization.

Candidate `900b646` closes the first multi-record staged recovery window. OTel
run `ea3fb589` passes 168 checks across three seeds with zero anomalies and
exact replay. It recovers original transactions 11 and 12, aborts 13 and 14,
replays the lost result, and commits successor transaction 15 in generation 2.
Publishing beyond absence, aborting quorum-present data, skipping a recoverable
record, retaining the dependent suffix, accepting an over-limit window, and
omitting one required inventory produce 24, 21, 18, 27, 12, and 3 anomalies
and discard. This admits one four-record, 16 KiB local window, not sustained
lag, repair, moving log sets, production fence authorization, or partitioned
resolvers.

Candidate `868c3de` closes the bounded sustained-lag ratekeeping gate. Run
`d510af28` passes 180 checks across three seeds with zero anomalies and exact
replay. Fresh signed capacity quorums deny transaction 15 before sequence
allocation, publication through 12 permits durable pop, restarted tLogs retain
that pop, and fresh workers reach exact transaction 16 from base plus tail.
All six unsafe subjects discard. A publication-authority process quorum signs
the replicated root. Every tLog verifies the pinned signer set, exact root
reference, manifest bytes, and embedded snapshot frontier before deletion. The
bounded concept estimate remains 98 percent, and the current complete Cell v0
proof estimate advances from 84 to 86 percent. These are directional judgments,
not statistical posteriors.

Candidate `670ef0a` closes the bounded failed-tLog learner-repair gate. OTel run
`a3c3356a` passes 69 checks across three seeds with zero anomalies and exact
replay. An active survivor quorum certifies one 3,977-byte retained snapshot,
three new learners install 12 records and survive restart, and a second quorum
certifies readiness. Fresh workers count only active nodes `2` and `3` and
recover exact transaction `14` from object frontier `10`. One-source,
post-signature tamper, stale readiness, wrong incarnation, premature counting,
and duplicate live-identity controls produce 2, 2, 2, 1, 1, and 2 anomalies
per seed and discard. The bounded concept estimate remains 98 percent, and the
current complete Cell v0 proof estimate advances from 86 to 89 percent. The
remaining repair gap is concurrent live-tail catch-up and chunked transfer;
the next durability gate is replicated log-set policy movement.

Candidate `b69714c` closes the bounded one-member log-set policy transition.
OTel run `8b8d9705` passes 90 checks across three seeds with zero anomalies and
exact replay. The transaction authority binds learner readiness, successor
tLogs stage one E2 policy, the authority commits it once, and a distinct
authority quorum certifies durable activation. After node `2` fails, nodes `3`
and `4` certify and serve exact transaction `17`; restarted removed node `1`
cannot contribute. Seven readiness, unresolved-stage, skipped-epoch,
mixed-quorum, missing-activation, removed-rejoin, and double-transition
subjects discard. The bounded concept estimate remains 98 percent, and the
current complete Cell v0 proof estimate advances from 89 to 92 percent. This
is a directional judgment, not a statistical posterior. Live-tail catch-up,
chunked transfer, independent hosts, and concurrent movement remain open.

Candidate `254cf421` closes the bounded concurrent live-tail and resumable
chunk-transfer gap. OTel run `28dfe9f4` passes 51 checks across three seeds
with zero anomalies and exact replay. Each learner durably resumes a
three-chunk base, accepts exact retry after restart, and then installs only the
two-record ordered tail while the active policy commits transactions `15` and
`16`. Learners and fresh workers reach exact transaction `16`; learners remain
outside every active quorum. Seven volatile, missing, conflicting, gapped,
stale-readiness, premature-counting, and full-recopy subjects discard. The
bounded concept estimate remains 98 percent, and the current complete Cell v0
proof estimate advances from 92 to 94 percent. These are directional judgments,
not statistical posteriors. Remote repair, multiple simultaneous repairs,
transfer cleanup, independent hosts, signer custody, online resolver-map
movement, and resolver throughput curves remain open.

Candidate `b69b245` closes the bounded memory-only resolver generation-recovery
gate. OTel run `e334c857` keeps 1,800 attempts across 228 ordered batches with
zero anomalies and exact replay. The three seeds record 699 commits, 1,098
conflicts, three safe conservative false conflicts, 2,706 resolver decisions,
three resolver losses, three abandoned candidates, and three replicated fence
markers. Empty successor resolvers start at the exact durable floor, reject old
requests and replies, and continue without a resolver filesystem sync or
finalization RPC. Six unsafe controls discard. The bounded concept estimate
remains 98 percent and the complete Cell v0 estimate remains 94 percent because
this is a Cell v1 transaction-role shape, not a composed complete-cell result.
Authenticated tLog-fence composition, multiple proxy ordering, recovery-time
availability, online resolver-map movement, and independent hosts remain open.

## Final strategy read

The strategy is directionally sound only in its narrowed form: a bounded
FoundationDB-like cell keeps replicated transaction authority and tLogs on the
live commit path, while object storage replaces permanent bulk storage. The
audit does not support the broader claim that object storage itself replaces
transaction authority.

| Scope | Decision | Evidence boundary |
|---|---|---|
| Bounded Cell v0 semantics | continue | `[EXISTS]` local multi-process semantics and recovery; broader history search and independent hosts remain open |
| Object-native permanent bulk storage | continue conditionally | `[EXISTS]` immutable closure, S3-compatible local path, publication, repair, and policy movement; GCS economics and failure remain open |
| Physical segment candidate | constrain | untuned SlateDB fails the 64 MiB reopen boundary; only the separately admitted serving-worker profile remains a candidate |
| PostgreSQL on objectKV | continue as an independent seam | `[ACTIVE-WORK]` compile and restart through a second `smgr` slot; actual objectKV callbacks and crash authority mapping remain open |
| ZebraDB exact HTAP | continue as an independent seam | `[EXISTS]` exact bounded-memory base-plus-tail operator; durable manifests, leases, schema-at-version, and tail cost remain open |
| Full FoundationDB replacement and fleet | not earned | multiple proxies, online range movement, independent failure domains, metacluster operations, and production curves remain `[FUTURE]` |
| OSS launch | locally ready, externally blocked | repository, RFC, eval, OTel, and tracker surfaces exist locally; public GitHub, Apache-2.0 approval, GCP authentication, and Kimi review remain open |

## Phase checkpoint

| Phase | Status | Early indicator | Current constraint |
|---|---|---|---|
| Transaction semantics and quorum | `[EXISTS]` continue | Multi-key OCC, durable rejection, lost-reply retry, leader death, snapshot recovery, and bounded 1,000-transaction concurrent histories pass through real processes. Witnesses check actual reads, one empty-range cycle, and a causal floor across two proxy processes. Three ordered memory-only resolver processes batch cross-range work and recover one resolver loss by replacing the transaction-system generation at an exact durable floor. Staged commits wait for authenticated tagged-log quorums before visibility. Generation takeover publishes a fully present prefix and aborts the first quorum-absent record plus its dependent suffix. Fresh capacity quorums ratekeep before sequence allocation. | No exhaustive generated range history, multiple commit-proxy ordering, composed resolver and tLog recovery, online resolver split or merge, recovery-time curve, production fence authorization, signer custody, or policy rotation. |
| Object-native durability | `[EXISTS]` continue | Committed envelopes become a verified immutable closure. A fresh process reconstructs exact `Database(T)` from object state at `O<T` through two required range-tagged log sets after proxy failures. A bounded lag path verifies a quorum-signed publication root inside each tLog, durably pops both sets, repairs one failed member through resumable base chunks plus an ordered live tail, moves it through a replicated policy epoch, and serves after another member fails. | Remote and simultaneous repair, concurrent movement, partitioning, public-cloud failure, and production retention curves remain open. |
| Routine repair | `[EXISTS]` bounded process proof | Fresh learner recovery passes, same-ID repair fails, and the tagged-log path rebuilds an empty learner from a survivor quorum without counting it early. Candidate `254cf421` resumes durable chunks across learner restart and transfers only the live tail while the active policy continues to commit. | One-machine local files only; remote transfer economics, multiple simultaneous repairs, transfer cleanup, independent failure domains, signer custody, and authority leases remain open. |
| Physical economics | `[EXISTS]` local S3-compatible candidate continues | `objectkv-serving-v1` opens the 64 MiB dataset with 402 read bytes. Separate maintenance writes 1.027x logical bytes. Claimed worker replacement completes in 576 to 618 ms. The same physical contract passes through pinned MinIO. A fresh coordinator adopts persisted worker output after coordinator death, overlapping coordinators self-fence stale epochs in 13.56 to 21.61 ms, and local GC now preserves the explicit five-class root graph while reclaiming a selectively unpinned closure. | One host and local network only. Root expiry and abandonment, coordinator election and host partitions, concurrent writers, GCS, public-cloud distance, and cloud price remain open. |
| PostgreSQL | `[ACTIVE-WORK]` continue seam | Exact PostgreSQL 18.6 fork compiles, bootstraps, checkpoints, restarts, and recovers rows through a second `smgr` slot. | The slot delegates to `md`; objectKV callbacks, AIO, stable barriers, and non-`smgr` state remain unproven. |
| ZebraDB HTAP | `[EXISTS]` continue independently | DataFusion base-plus-tail merge preserves exact target-version rows with bounded operator memory. | Durable manifest, lease, tail source, pruning, and lag cost curve remain open. |

### Frozen audit final checkpoint

`[EXISTS]` The frozen candidate `a56442a` produced 172 admissible receipts
across 24 fixed-cadence cycles plus four startup controls. All 172 verdicts
match their expected outcome and no safety gate has regressed. Every scale lane
repeats the same fresh-open byte count. MinIO authority repeats exactly 44
requests, 351 request bytes, and 441
response bytes per cycle.

| Lane | Runs | Operation relative MAD | Result |
|---|---:|---:|---|
| Generation handoff | 24 | 3.70% | stable |
| HTAP streaming | 24 | 2.94% | stable |
| Publication lost reply | 24 | 3.49% | stable |
| SlateDB 1 MiB | 24 | 1.45% | stable |
| SlateDB 8 MiB | 24 | 1.54% | stable |
| SlateDB 64 MiB | 24 | 0.92% | stable but still over the physical stop threshold |
| MinIO authority | 24 | 18.32% | local wall time noisy; work counters exact |

The MinIO latency lane is not below the proposed 5 percent noise threshold.
Because its request and byte receipt is identical in every cycle, this is an
early scheduler-noise indicator rather than a semantic or cost regression. The
final scorecard keeps the timing lane marked noisy instead of silently
averaging it away.

The frozen scheduler had one deadline-boundary defect. After the valid cycle
24, it started cycles 25 through 52 without the configured 30-minute waits.
Those 196 dense supplemental outcomes also matched their expected verdicts, so
the raw terminal summary is 368 of 368 expected with zero unexpected result.
They are excluded from fixed-cadence counts and latency dispersion. Commit
`e6ec477` makes future runs wait for the deadline instead of starting another
cycle.

Current decision: continue the bounded vertical proof. Use only the configured
SlateDB serving-worker profile as the local segment candidate, keep PostgreSQL
and HTAP as independent proof lanes, and do not claim a complete cell until a
public-cloud physical profile, generated range histories, multiple proxy
ordering and generation rollover, independent-host and multi-repair scheduling,
production fence authorization and signer key custody, root expiry policy, and coordinator
behavior under host partitions are explicitly closed.

## Evidence entering the run

Candidate `361a0fd` repairs the Phase 0 measurement boundary. It measures old
instance close, new instance open, first correct read, cold reads, and final
close independently. Raw artifacts include `run_id` and cannot overwrite a
prior run.

| Phase | Current result | Interpretation |
|---|---|---|
| Physical scale | 1 MiB: 4.85 ms; 8 MiB: 6.19 ms; 64 MiB: 424.13 ms | 64 MiB reopen reads 210,773,938 bytes before the first point read. Stop the untuned incumbent and permit one bounded layout/configuration pass. |
| Physical configuration | Runs `07dad330` and `5a9846fc` keep three seeds; control `c0affb91` discards | Fresh open reads 402 bytes and first correct read takes 3.81 to 4.12 ms. Keep a local candidate; request count and maintenance remain open. |
| Separate compaction | Runs `d6425f5e` and `5431c0fe` keep three seeds; control `af37279a` discards | Eight L0 SSTs become one sorted run at 1.027x maintenance write amplification. Exact fresh reads stay bounded. This closes local role wiring, not process failure or remote economics. |
| Worker-process reclaim | Runs `238de077` and `882b1fcf` keep three seeds; control `af904d02` discards | A persisted Running claim survives worker death through coordinator reclaim and a fresh replacement. Latest overwrite scans stay exact. This closes one local worker failure, not coordinator, host, GC, or remote failure. |
| MinIO physical serving and compaction | Runs `229bfced` and `6f0e194b` keep three seeds; control `d1125f50` discards | Eight L0 SSTs become one sorted run through pinned MinIO at 1.027x maintenance write amplification. Fresh open reads 538 bytes and the first exact point uses five requests. This closes one local S3-compatible physical boundary, not public-cloud economics or provider failure. |
| Coordinator output adoption | Runs `ab8b22d4` and `e73b3458` keep three seeds; control `b2045e82` discards | A worker persists final SST identities, the coordinator dies before manifest publication, and a distinct replacement commits those exact objects without a worker rerun in 29.4 to 30.5 ms. This closes one coordinator crash boundary, not fencing between concurrent coordinators or true orphan GC. |
| Concurrent coordinator fencing | Runs `aaaecbb6` and `85672759` keep three seeds; control `2899bb28` discards | Two live coordinators advance epoch 0 -> 1 -> 2. The stale processes self-fence in 13.56 to 21.61 ms and the newer coordinators compact exact data. This closes local overlap, not election, host partitions, or public-cloud conditional writes. |
| Active-output and true-orphan GC | Runs `8d606761` and `26b19dfb` keep three seeds; control `161eac32` discards | Active compaction state protects completed but unpublished output, replacement coordinators commit those exact objects, and aged unreferenced SSTs are deleted in 1.88 to 1.92 ms. This closes one local root transition, not checkpoint, clone, backup, analytical-lease, multi-tenant, or cloud roots. |
| Concurrent Cell v0 history | Runs `9616bf69` and `f66bb379` keep three seeds; control `c837f980` discards | Each run evaluates 3,000 transactions with 2,100 commits, 900 durable conflicts, three leader deaths, exact lost-reply retry, exact replay, and zero anomalies. This closes one bounded schedule-independent history, not general strict serializability or distributed resolver agreement. |
| Read-value and real-time witness | Run `56a132c6` keeps three seeds; control `aa460aa8` discards | The witness checks 1,200 read values, 300 committed actual-read dependencies, and 727,650 real-time edges. The control commits every transaction but fails the dependency class. This closes one commit-sequence witness, not range phantoms, multi-proxy reads, or exhaustive search. |
| Range-read phantom witness | Run `04b84730` keeps three seeds; control `f4678cd8` discards | The correct subject commits 300 insertions and rejects 300 dependent range writes across three leader failures. The control commits all 600 transactions and exposes 300 dependency cycles. This closes one empty-range insertion shape, not generated range histories, multiple read-version proxies, or partitioned resolvers. |
| Read-version proxy causality | Run `eec5ca77` keeps three seeds; control `d280df19` discards | Two independent proxy processes per seed honor 300 session floors and observe 300 acknowledged writes through three authority leader failures. The control processes return valid older caches and produce 300 floor violations plus 300 stale observations. Batching, generation rollover, lag policy, and proxy-to-serving handoff remain open. |
| Publication GC root graph | Run `885dfdb4` keeps three seeds; control `6e8ce843` discards | All checkpoint, clone, backup, analytical-lease, and tenant-move roots survive authority reopen and mark/sweep. Selective clone unpin reclaims only six unique objects, shared objects survive, and three post-mark lease pins defer stale deletion. Expiry, abandonment, cloud inventory, host loss, and distributed sweep remain open. |
| Serving recovery equation | Run `ed0cdfe8` keeps three seeds; control `690e0844` discards | Fresh worker processes reconstruct exact rows at `T=10` from object state through `O=8` plus one quorum-recovered suffix record. The control returns stale rows at `8`. This closes the bounded equation with a copied WAL fixture, not original log routing or concurrent range serving. |
| Live committed-envelope feed | Run `bf79522d` keeps three seeds; control `3db9c604` discards | After three transaction-leader deaths, fresh workers fetch one committed envelope each from the live successor authority at position `11` and reach exact `T=10` with no copied WAL. The control drops that envelope and stays stale at `8`. Dedicated tLogs, push streaming, range tags, and independent failure remain open. |
| Range-tagged tLog serving | Run `851d0654` keeps three seeds; control `136b2523` discards | Nine dedicated tLog processes synchronize nine exact range-tagged records and reject nine overflow probes. After three process deaths, six survivor responses reconstruct three suffixes and reach exact `T=10`. Omitting required tag `10` recovers no suffix and stays stale at `8`. Commit acknowledgement integration, multi-record lag, repair, and partitioned logs remain open. |
| Tagged-log commit visibility | Run `5a2e5a7f` keeps three seeds; control `0da1a0c1` discards | One staged transaction survives six proxy deaths, waits for quorums from both required log sets, publishes once at `T=11`, returns the retained retry outcome, and recovers exact state in fresh workers. The control acknowledges after one set, remains visibly at `10`, and produces 51 anomalies. Receipt authentication, abort policy, generation takeover, lag, repair, and partitioning remain open. |
| Authenticated tagged-log certificates | Run `f5e3720a` keeps three seeds; five controls discard | Policy-bound Ed25519 quorum certificates cover the exact staged statement. Unsigned, duplicate-signer, wrong-log-set, tampered-statement, and obsolete-policy subjects each produce 51 anomalies. Key custody, policy rotation, incomplete-head abort, lag, repair, and partitioning remain open. |
| Certified staged-head generation takeover | Run `959a2211` keeps three seeds; five controls discard | The active successor publishes the exact fully certified old-generation head once after fencing, voter-set handoff, and activation, then commits transaction 12. Incomplete-head abort, multi-record takeover, key custody, lag, repair, and partitioning remain open. |
| Incomplete staged-head fence and abort | Run `338ef8b4` keeps three seeds; six controls discard | Durable fence quorums on every old tLog set plus an absence quorum in one incomplete set permit the active successor to abort transaction 11 and commit transaction 12 without exposing the aborted envelope. Multi-record recovery, fence authorization, signer custody, lag, repair, and partitioning remain open. |
| S3 authority | MinIO run `3f3489e0` keeps 44 checks with zero anomalies | S3 protocol semantics are plausible locally. This is not cloud durability or latency evidence. |
| Generation recovery | Run `cf46b738` keeps 48 checks with zero anomalies | Certificate fencing and process replay are credible in isolation. |
| Publication recovery | Run `c2beebfe` keeps 42 checks with zero anomalies | Lost Publish reply, authority failover, and empty-scratch publisher recovery are credible in isolation. |
| HTAP overlay | Run `3b43b102` keeps 24 checks, four peak buffered rows, and no spill | The streaming merge is directionally correct. It is not yet a durable snapshot source or a PostgreSQL integration. |

The warm-cache, single-signer, convergence-only publication, and materialized
HTAP controls must each discard once at the start of the overnight run.

## What the 12 hours measure

`experiments/overnight_strategy_audit.sh` pins one clean Git candidate and one
suite hash per lane. The admissible window ran 24 cycles at 30-minute cadence:

1. the repaired SlateDB 1, 8, and 64 MiB scale points;
2. the pinned MinIO authority contract;
3. real-process generation certificate handoff;
4. lost-publication-response recovery through real OpenRaft processes;
5. the bounded DataFusion streaming overlay.

Every run exports OTel signals and writes a compact result, raw physical
artifact, log, append-only JSONL record, rolling summary, and status file under
one `/tmp/okv-overnight-strategy-*` directory. Source identity drift stops the
run instead of producing incomparable receipts.

This frozen run measures reproducibility, safety regressions, latency
dispersion, and physical scale shape in mechanisms that existed at `a56442a`.
It cannot itself prove the later semantic-to-Raft transaction path,
`C/O/WAL` recovery composition, PostgreSQL page bridge, or durable HTAP
snapshot source. Candidate `9e733e2` now supplies a separate bounded local
`C/O/WAL` recovery proof, and candidate `e1c2437` replaces its copied suffix
with a live committed-envelope authority feed. Candidate `6a81821` separately
supplies authenticated tagged-log certificates, and candidate `f350a12`
separately supplies one fully certified staged-head takeover. The frozen audit
cannot retroactively prove either later candidate. Candidate `341beb9`
separately supplies one incomplete staged-head fence and abort proof; the
frozen audit cannot retroactively prove it either. Candidate `900b646`
separately supplies the bounded multi-record prefix proof; it is also outside
the frozen source identity. Candidate `65664bf` separately partitions conflict
resolution across three ordered processes and matches the centralized oracle
for cross-range transactions. Its OTel run `8be62401` is also outside the
frozen source identity and cannot change the audit result.

Candidate `b69b245` separately replaces the intended resolver journal with
memory-only batches and whole transaction-system generation recovery. Its OTel
run `e334c857` is outside the frozen source identity and cannot change the audit
result.

## Morning decision table

| Outcome | Decision |
|---|---|
| Every normal run keeps; every control discards; repaired counters vary under 2 percent; relative latency MAD is at most 5 percent | Continue to one SlateDB layout/configuration pass, then MinIO physical and cloud profiles. Continue the vertical transaction proof. |
| Existing safety gates hold, but physical reopen remains dataset-sized or noise exceeds the thresholds | Narrow SlateDB to a reference or replace it. Continue objectKV semantics and publication work. Do not claim Gate 1. |
| Any acknowledged loss, stale-generation acceptance, publication reconstruction error, mixed HTAP snapshot, or negative control keeps | Stop the affected lane immediately and diagnose before adding scope. |
| The next vertical proof requires synchronous object publication on every commit, unbounded retained WAL, or two commit authorities | Narrow the architecture to an object-native storage/publication layer over an existing transaction authority. |

Selected outcome: the second row. Every safety outcome held, but the untuned
64 MiB SlateDB reopen remains dataset-sized and MinIO wall time remains noisy.
Keep only the separately admitted serving-worker profile as a local candidate,
continue the transaction and publication proofs, and do not claim physical Gate
1 until a public-cloud profile passes.

## Next falsifiers after the overnight receipts

1. Compose the admitted memory-only resolver generation recovery with the
   authenticated tLog fence. Add multiple commit proxies, broader generated
   overlapping ranges, range clears, concurrent in-flight batches, hot-range
   skew, and online split or merge. Race independent read proxies under lag and
   generation changes. Add bounded history search rather than relying only on
   constructed cycles.
2. Add sustained tLog lag and lag-based backpressure to the admitted
   multi-record prefix. Repair one failed log process, move one log set across
   a policy epoch, and move one serving assignment without copying the
   immutable base.
3. Run the accepted serving and compaction path on GCS.
   Add lease expiry, abandoned tenant-move cleanup, independent host loss, and a
   distributed sweeper to the admitted local root graph.
   Stop using SlateDB as the incumbent if reopen reads the dataset, cold 1 KiB
   points exceed eight requests or 512 KiB, recovery loses exact rows, or
   remote request pricing misses the named workload ceiling.
4. Pin a PostgreSQL revision and implement a tracing `smgr` dispatch wrapper
   before routing one non-default tablespace through a Rust page server.
5. Put the existing DataFusion operator behind an immutable manifest, snapshot
   lease, schema transform, partition epoch, and storage-level `(min(Wp), T]`
   tail reads.

## Tradeoff

This sequence optimizes for discovering a fatal composition problem before
building more database roles. It gives up a broader demo and a single blended
score. Each lane can continue, narrow, or stop independently.

## 2026-08-23 rolling strategic checkpoint

Decision: continue the bounded objectKV program. The transaction-kernel thesis
is directionally working in local semantic evaluation. The product thesis is
not yet earned because public-cloud economics, independent failure domains,
and consumer integration remain open.

Evidence added after the frozen overnight source identity:

| Indicator | Status | New evidence | What could reverse it |
|---|---|---|---|
| Multi-writer transaction topology | `[EXISTS]` positive | RFC-0051 orders three commit proxies through one predecessor chain; RFC-0052 splits one hot resolver range online without moving durable database bytes. Both pass three exact-replay seeds and all frozen controls. | Hotspot throughput fails to improve after split, pending work grows without bound, or independent hosts disagree on order. |
| Commit-proxy failure | `[EXISTS]` positive for safety | RFC-0053 fences the full transaction-system generation at three proxy-loss boundaries. Run `1c55dad7` keeps with zero anomalies; nine unsafe controls discard. | Recovery downtime or restart work grows with database size instead of transaction-system tail size, or simultaneous authority failure breaks the boundary. |
| Object-native permanent bytes | `[EXISTS]` conditional | Bounded local immutable publication, root GC, S3-compatible MinIO serving, tLog repair, and policy movement remain exact. Tigris supports transactional metadata plus immutable-byte separation but continues to rely on FoundationDB for transaction authority. | GCS request, byte, latency, or brownout curves miss the declared workload budgets. |
| PostgreSQL path | `[ACTIVE-WORK]` plausible seam | The pinned PostgreSQL 18.6 fork compiles, boots, checkpoints, restarts, and recovers through a second `smgr` slot. | A real objectKV page callback cannot preserve WAL-before-page and checkpoint barrier semantics, or non-`smgr` system state prevents useful object-native recovery. |
| ZebraDB HTAP | `[EXISTS]` positive semantics | Exact bounded-memory DataFusion base-plus-tail overlay reaches one target version. | Durable leases, manifests, schema-at-version, or tail-cost curves require a second truth system or unbounded retention. |
| Full FDB replacement | `[FUTURE]` not earned | The role decomposition is increasingly coherent, but evidence remains same-host and bounded. | Recovery, sequencer, resolver, tLog, or fleet-control curves reveal a non-partitionable serialized bottleneck. |

The next overnight indicators must be curves, not another constructed semantic
scenario:

1. isolate proxy-loss detection, old-generation fencing, tLog inventory,
   recruitment, and resume time;
2. vary pending tickets, retained tLog tail, resolver count, and tLog topology;
3. compare hot-range throughput before and after the admitted resolver split;
4. keep GCS physical economics, PostgreSQL page barriers, HTAP tail cost, and
   Tigris comparison as independent lanes;
5. stop or narrow any lane whose unsafe control keeps, whose work grows with
   total database size, or whose measured curve misses its declared budget.

This checkpoint optimizes for an early architectural stop signal. It gives up
claiming that the local semantic passes predict production throughput or
availability.

## 2026-08-23 recovery-curve checkpoint

RFC-0054 produced the first requested availability curve. The strategic result
is positive with a specific warning:

- `[EXISTS]` Recovery work is independent of permanent database size in the
  instrumented local path. The 1 GiB and 1 PiB logical points perform identical
  work and read zero permanent database bytes.
- `[EXISTS]` Pending-ticket classification is not the bottleneck from 8 through
  512 tickets.
- `[EXISTS]` Role recruitment and tLog inventory grow in their declared
  dimensions. No hidden quadratic work appeared in the admitted subject.
- `[ACTIVE-WORK]` Retained authenticated tLog inventory is the first measured
  performance limit. Total median recovery reaches 3.158 seconds at 65,536
  records per tLog, with 2.870 seconds spent scanning inventory.
- `[PROPOSED]` Keep full generation recovery as the correctness fallback. Test
  compact authenticated summaries, checkpoint cadence, and parallel role
  recruitment before adding within-generation exact-batch takeover.

This raises confidence in the cell boundary but does not raise product
feasibility. The 12-hour cadence now repeats these recovery points beside the
existing object-store, physical storage, publication, generation, and HTAP
receipts. Hot-range throughput, GCS economics, real PostgreSQL callbacks, and
durable analytical-tail cost remain independent stop-or-continue lanes.
