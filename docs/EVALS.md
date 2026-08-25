# objectKV eval system

Status: `[ACTIVE-WORK]` the configurable runner, metric registry, schema-checked
smoke execution, OTel logs, metrics, and traces export, and proposed Phase 0,
serving-model, fault-recovery, and commit-contract suites exist. Phase 0
workload executors,
object-store implementations, replicated-WAL fault injection, and PostgreSQL
oracles remain proposed. The first generation-fencing fault workload runs
through `okv-sim` and records its anomaly count, event budget, trace digest, and
exact-replay gate through the shared result and OTel path.
The `object-store` suite also executes segment and authority capability profiles
against memory, filesystem, MinIO, and GCS adapters. Memory and pinned MinIO
have local authority receipts; filesystem has a segment receipt and an expected
authority failure; GCS awaits authentication.
The `object-publication-adapter-v1` suite composes real local filesystem object
operations with a separately reopened three-file authority. It verifies
intent-before-upload, exact closure-before-root, unknown PUT, authority, and
DELETE recovery, complete marks, epoch revalidation, and deletion reservations.
Seven negative subjects bypass one physical boundary each.
The `object-publication-authority-process-v1` suite moves the same logical
publication state into the existing three-node OpenRaft generation authority.
It drives two real leader deaths, a lost reply, exact retry and conflicting
identity, root and pin compare-and-swap, deletion reservation and retirement,
a legitimate generation transition, an isolated stale-read probe, a
quorumless acknowledgement probe, and exact restarted-node comparison. Ten
negative subjects each disable one authority invariant.
The `mvcc-semantics-v1` suite runs five deterministic 1,000-event histories
against an independently normalized full-snapshot oracle. Seven negative
subjects break range clears, replay ordering, conflict rejection, future-read
availability, inclusive retention, expired-read rejection, and stale-generation
fencing. They receive `discard` verdicts at deterministic steps 2 through 9.
The `cell-commit-contract-v1` suite checks a replayable Cell v0 envelope and
durable retry outcome across five seeds. Six negative subjects break durable
deduplication, request identity, complete resolver acceptance, complete log
tagging, generation fencing, and quorum acknowledgement.
The `persisted-wal-contract-v1` suite writes the same envelope through a
versioned checksummed local frame and reopens a three-file topology. It covers
matching-quorum reconstruction, one-file loss, durable retry outcomes,
leader-only suffixes, torn final frames, chain disagreement, and complete
corruption. Six negative subjects make each unsafe interpretation observable.
The `zebradb-htap-contract-v1` suite checks base-plus-tail exactness across one
query version, unequal partition and table watermarks, schema normalization,
row movement, leases, independent tail retention, and certified writes. Five
negative subjects each break one of those contracts.
The `openraft-cluster-contract-v1` suite runs three actual OpenRaft nodes over a
seeded Turmoil TCP network and real per-node journals. It covers quorum commit,
leader isolation, explicit successor election, stale-suffix replacement,
simulated process crash and bounce, and restarted-node catchup. Three negative
subjects expose early acknowledgement, missing successor election, and missing
restart catchup.
The `openraft-process-contract-v1` suite runs three OpenRaft nodes as separate
OS processes over normal Tokio TCP. It commits a versioned request, drops the
reply only after apply, kills the leader process, elects a successor, retries
the same request identity, restarts the killed node from its retained log, and
requires one logical effect plus the original outcome on every node. Three
negative subjects disable deduplication, acknowledge without quorum, and omit
the killed-node restart.
The `generation-recovery-certificates-v1` suite requires the external authority
to verify Ed25519 quorum certificates for the exact data-log fence and recovered
voter-set positions. It pins active and pending voter public keys in authority
state. Five negative subjects admit a single signer, tampered position,
duplicate signer, stale recovery identity, or wrong membership digest.

## Executable configuration

- `evals/metrics.toml` owns instruments, units, histogram boundaries, attributes,
  and cardinality limits.
- Each suite owns profiles, workloads, lanes, practical thresholds, constraints,
  telemetry requirements, and any additional frozen contract files.
- `okv-eval validate-suite` validates the suite, registry, and result schema as
  one contract.
- `okv-eval run` executes registered workloads, records OTel signals, and refuses
  to emit a result that fails the JSON Schema. It also refuses a dirty source
  tree unless `--allow-dirty` marks the run as diagnostic and non-comparable.
- `infra/otel/` is the pinned local OTLP and Prometheus path.

## Design rule

Correctness is not a weighted metric. A candidate with any correctness failure
is ineligible. After hard gates pass, each research lane optimizes one primary
metric under a fixed budget.

```text
frozen oracle + frozen suite + fixed budget
                    |
              candidate change
                    |
      correctness and contract hard gates
                    |
         one lane-specific primary metric
                    |
          keep / discard / inconclusive
```

This adapts the autoresearch loop to storage systems. A database cannot safely
use one project-wide scalar because latency, cost, durability, and correctness
are not interchangeable.

## Evaluation layers

| Layer | Purpose | Initial mechanism |
|---|---|---|
| E0 | deterministic semantics | `okv-model` unit and smoke histories |
| E1 | differential correctness | generated histories, candidate versus model |
| E2 | object-store contract | memory, filesystem, MinIO/S3, then one cloud profile |
| E3 | physical economics | hot/cold reads, scans, requests, bytes, compaction |
| E4 | fault and recovery | process death, lost replies, retries, empty cache |
| E5 | distributed invariants | WAL quorum, fencing, range move, OCC histories |
| E6 | PostgreSQL compatibility | `pg_regress` subsets plus crash/restart scenarios |
| E7 | HTAP version alignment | exact columnar base plus durable analytical tail checks |
| E8 | serving-model semantics | Redis subset, inverted-index snapshots, PostgreSQL behavior |

## Research lanes

Each lane owns one champion. Champions are not blended automatically.

| Lane | Primary metric | Hard gates |
|---|---|---|
| `cold-point` | remote GETs per successful point read | correct value; bounded bytes; fixed empty cache |
| `hot-point` | operations per second | zero remote GETs after warmup; p99 ceiling; correctness |
| `range-read` | logical rows per second | exact ordered result; request and byte ceilings |
| `objectify` | p99 objectification lag | no lost commit; request/byte ceiling; checksum verified |
| `compaction` | object bytes written per logical byte ingested | read amplification ceiling; snapshot correctness |
| `reopen` | time to first correct read from empty cache | dataset-size-independent setup; no bulk copy |
| `commit` | durable commit p99 | every acknowledged commit survives allowed faults |
| `range-move` | durable database bytes copied | zero impossible reads; bounded cutover time |
| `serializable` | committed transactions per second | every history accepted by the oracle |
| `redis` | p99 command latency | declared Redis subset has zero semantic mismatches |
| `search` | top-k queries per second | version-aligned postings/deletes; recall and freshness gates |
| `pg-compat` | passed PostgreSQL cases per fixed budget | zero acknowledged-loss or impossible recovery histories |
| `htap-overlay` | p99 exact-snapshot latency | exact canonical result with no missed, extra, duplicate, or incorrect row |
| `generation-recovery` | anomaly count under exact seeded replay | zero acknowledged loss, version reuse, or stale publication |
| `object-brownout` | p99 objectification lag | bounded retained WAL and zero acknowledged loss |
| `commit-failover` | durable commit p99 | zero acknowledged loss across leader, disk, and lost-ack faults |
| `takeover` | time to first correct read | stale owner fenced and no durable dataset copy |
| `gc-snapshot-race` | anomaly count | no reachable object reclaimed under snapshot, branch, backup, or CDC interleavings |
| `cell-scale` | committed transactions per second | strict serializability and bounded recovery as roles partition |
| `tenant-move` | unavailable time and durable bytes copied | one writable routing epoch and exact snapshot plus tail |
| `certified-write` | validation retry rate | no write commits from an invalid analytical dependency certificate |

`generation-recovery` has one executable bootstrap probe. The other fault lanes
remain configuration contracts until their owning components exist.

## Cell commit contract gate

```bash
cargo run -p okv-eval -- run evals/suites/commit-contract.toml \
  --profile sim-dev \
  --workload cell-commit-envelope \
  --backend sim-model
```

The suite freezes RFC-0005, RFC-0008, and RFC-0009 into its contract hash. It
records exact seed replay, semantic step coverage, recovered outcomes, retry
count, leader-only attempts, trace digests, and correctness anomalies. CI
requires the correct subject to keep and all six negative subjects to discard.

## Real publication adapter gate

```bash
cargo run -p okv-eval -- run evals/suites/object-publication-adapter.toml \
  --profile local-fs \
  --workload object-publication-real-adapter \
  --backend object-store-local-fs+authority-quorum-fs
```

The suite freezes RFC-0003, RFC-0004, RFC-0007, and RFC-0014. Three seeds each
execute 16 semantic checks through Apache `object_store` and fresh authority
opens. Correctness anomalies are the only primary metric. Object request and
byte counts, authority records, reservations, deferrals, and operation duration
are secondary evidence. This gate does not measure cloud economics or claim
distributed authority durability.

## Replicated publication authority gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/object-publication-authority-process.toml \
  --profile local-fs \
  --workload publication-authority-failover \
  --backend process-local-fs
```

The suite freezes RFC-0015 and uses three seeds for 24 semantic checks each.
Candidate `b530321` kept in run
`550e5585-bf9d-4bc9-b96f-d38aaca9eb49` with zero anomalies at the exact
72-event budget. All ten unsafe subjects discarded. OTel run
`8071bc8a-8a4d-4a29-9118-5a11e22b5e3b` exported logs, metrics, and traces.
This admits replicated authority state and real authority-process failover. It
does not yet admit publisher or sweeper process recovery around object-store
operations, independent-disk loss, outcome expiry, or cloud economics.

## Publisher prepare and restart gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-process.toml \
  --profile local-fs \
  --workload publisher-prepare-restart \
  --backend object-store-local-fs+process-openraft
```

The suite freezes RFC-0017 and uses three seeds for ten semantic checks each.
Candidate `ffc0c84` kept in run
`3b5cb41f-8985-47f4-8e87-4797ad9babef` with zero anomalies at the exact
30-event budget. It started nine authority processes and six publisher
processes, issued three real process kills, and wrote nine immutable objects.
The poisoned upload-before-Prepare subject discarded in run
`26bde1fa-670b-40db-a750-8f363042b10b` with eight anomalies per seed. Two
fresh seed-1103 controllers emitted byte-identical semantic traces. OTel run
`ce7692da-7150-4b6a-81c4-9e680c7e2bb6` exported logs, metrics, and traces.

This admits recovery after quorum-durable `Prepare` and before the first object
PUT. It does not admit partial upload recovery, lost object or `Publish`
replies, abandoned-intent policy, sweeper recovery, object-effect fencing,
authority snapshot repair, independent failure domains, or cloud economics.

## Publisher ambiguous-PUT recovery gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-put-recovery.toml \
  --profile local-fs \
  --workload publisher-first-put-unknown-restart \
  --backend object-store-local-fs+process-openraft
```

The suite freezes RFC-0018 and uses three seeds for twelve semantic checks
each. Candidate `a6dfeed` kept in run
`a4a1aec5-cca9-46e7-864e-de48a7e2c30b` with zero anomalies at the exact
36-event budget. It started nine authority processes and six publisher
processes, issued three real process kills, injected three successful object
effects with unknown responses, recovered three existing immutable objects,
and performed eighteen named verification reads. Two fresh seed-1103
controllers emitted byte-identical semantic receipts. The partial-closure
subject discarded in run `fa9d729b-c861-444a-9989-7127f026058c` with four
anomalies per seed. OTel run `b57f141f-fd8d-4108-b053-da1c2cc9a63d`
exported two log records, one trace span, eight metrics, and eight metric data
points. Prometheus exposed correctness anomalies at zero, availability ratio
at one, and operation duration at 1.39638825 seconds.

This admits one ambiguous data-object PUT followed by real process death and
empty-scratch replay. It does not admit a lost manifest response, a lost
`Publish` response, multipart residue cleanup, repeated unknown-response retry
budgets, abandoned-intent reassignment, sweeper recovery, generation-bound
effect grants, independent-disk loss, or cloud economics.

## Publisher ambiguous-manifest recovery gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-manifest-recovery.toml \
  --profile local-fs \
  --workload publisher-manifest-put-unknown-restart \
  --backend object-store-local-fs+process-openraft
```

The suite freezes RFC-0019 and uses three seeds for thirteen semantic checks
each. Candidate `57e28d4` kept in run
`2660e09d-e2f3-4482-a123-68024779de1a` with zero anomalies at the exact
39-event budget. It started nine authority processes and six publisher
processes, issued three real process kills, made eighteen PUT attempts with
nine physical effects, injected three successful manifest effects with unknown
responses, recovered nine existing immutable objects, and performed twenty-four
named verification reads. Two fresh seed-1103 controllers emitted
byte-identical semantic receipts. The manifest-only subject discarded in run
`7ace2812-aab0-44be-bacc-9f4f992d014c` with four anomalies per seed. OTel run
`5fd6240e-17e8-4fca-b632-c594170a233c` exported two log records, one trace
span, eight metrics, and eight metric data points. Prometheus exposed
correctness anomalies at zero, availability ratio at one, and operation
duration at 1.4893435 seconds.

This admits one ambiguous manifest PUT followed by real process death and
empty-scratch replay through a complete named closure walk. It does not admit a
lost replicated `Publish` response, repeated unknown-response budgets,
multipart residue cleanup, abandoned-intent reassignment, sweeper recovery,
generation-bound effect grants, independent-disk loss, or cloud economics.

## Publisher lost-Publish-response recovery gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/object-publication-publisher-publish-recovery.toml \
  --profile local-fs \
  --workload publisher-publish-unknown-restart \
  --backend object-store-local-fs+process-openraft
```

The suite freezes RFC-0020 and uses three seeds for fourteen semantic checks
each. Candidate `72df70c` kept in run
`a544deff-edec-4885-a0bf-b1217d720328` with zero anomalies at the exact
42-event budget. It started nine authority processes and six publisher
processes, issued six real process kills and three authority failovers, made
nine object PUT attempts with nine effects, dropped three successful `Publish`
replies, recovered three exact outcomes, and replayed them without a second
transition. The empty-scratch replacements issued no object PUTs and performed
forty-five named verification reads. Two fresh seed-1103 controllers emitted
byte-identical semantic receipts. The convergence-only subject reached the same
final root and closure but discarded in run
`82698bdb-443d-4ad7-830f-5bef6927b8f8` with four anomalies per seed, two
`Publish` applications per seed, and no recovered outcomes. OTel run
`50ad5d86-ee3e-4790-9c19-d81383d68002` exported two log records, one trace
span, eight metrics, and eight metric data points. Prometheus exposed
correctness anomalies at zero, availability ratio at one, and operation
duration at 3.723415042 seconds.

This admits one lost successful `Publish` reply followed by publisher death,
accepting-leader death, empty-scratch reconstruction, successor outcome
recovery, and exact retry without a second authority or object effect. It does
not admit repeated lost replies, outcome expiry or snapshot restoration, a
later root superseding the publication, generation handoff at this boundary,
multipart residue cleanup, independent-disk loss, or cloud economics.

## ZebraDB HTAP exactness gate

```bash
cargo run -p okv-eval -- run evals/suites/htap-contract.toml \
  --profile model-dev \
  --workload zebradb-base-plus-tail \
  --backend model
```

The suite freezes RFC-0010 into its contract hash. The correctness lane uses
anomaly count as its admission metric and requires `query.result_exact = 1`.
Tail rows, tail bytes, peak memory, spill bytes, and operation duration remain
separate measurements. They are not freshness proxies and are not yet
DataFusion performance evidence.

## Persisted WAL stable-storage gate

```bash
cargo run -p okv-eval -- run evals/suites/persisted-wal.toml \
  --profile local-fs \
  --workload persisted-wal-reopen \
  --backend local-fs
```

The suite freezes RFC-0005, RFC-0009, RFC-0011, and the version-1 frame fixture.
The correctness lane requires zero anomalies across five seeds and exact replay
of the logical report. It emits `correctness.anomalies`,
`transaction.commits`, `wal.retained_bytes`, and
`availability.success_ratio` through the shared OTel path. Local file bytes and
`sync_all` are real. The retained-byte sample is the maximum local topology
size observed in the scenario with object durability fixed at zero; it is not a
capacity result. Replica transport, consensus commit, leader election,
cross-process crash, and independent-disk durability remain proposed.

## OpenRaft per-node storage gate

```bash
cargo run -p okv-eval -- run evals/suites/raft-storage.toml \
  --profile local-fs \
  --workload openraft-storage-reopen \
  --backend local-fs
```

The suite freezes RFC-0005, RFC-0009, RFC-0011, and the `OKVR` version-1
journal fixture. It requires byte-stable logical replay across five seeds and
real fresh opens of durable vote, committed position, entries, conflict
truncate, purge marker, torn-tail repair, and complete-corruption rejection.
`cargo test -p okv-consensus` also runs OpenRaft `0.9.25`'s upstream storage
conformance suite. Six negative subjects must discard. The separate cluster
gate below exercises network quorum and election. Generation takeover, real OS
process crash, independent-disk failure, and throughput remain proposed.

## OpenRaft three-node failover gate

```bash
cargo run -p okv-eval -- run evals/suites/raft-cluster.toml \
  --profile local-fs \
  --workload openraft-three-node-failover \
  --backend turmoil-local-fs
```

The suite freezes the durability, recovery-generation, and cell-topology RFCs.
Automatic elections and heartbeats are disabled. The recorded controller
initializes membership, elects node 1, commits `A`, partitions node 1, elects
node 2, rejects a stale isolated write acknowledgement, commits `B`, repairs the
partition, and verifies the stale suffix is replaced. It then crashes node 2 in
the simulator, elects node 3, commits `C`, bounces node 2 with the same journal,
and requires every state machine to contain exactly `A, B, C`.

Turmoil drops the node runtime and recreates its host future. This is a
deterministic simulated process crash, not an OS process kill. The per-node
journal uses the real local filesystem, so the gate proves reopen and replay of
synced state across a simulated bounce, not unsynced-disk loss. Exact
fresh-process replay, real process kill, and retained request-outcome recovery
are exercised by the separate gate below. Generation takeover remains a
separate gate.

## OpenRaft real-process lost-reply gate

```bash
cargo run -p okv-eval -- run evals/suites/raft-process.toml \
  --profile local-fs \
  --workload openraft-process-lost-reply \
  --backend process-local-fs
```

The controller starts three child processes over normal localhost TCP, commits
`A`, then submits a versioned `X` request whose server connection closes only
after OpenRaft applies the committed entry. It kills that leader with an OS
process signal, explicitly elects node 2, recovers the request outcome before
retry, retries the same identity, and requires the original response with one
logical `X`. Node 1 then starts with the same journal, rebuilds the outcome by
replaying the retained Raft log into a fresh in-memory state machine, and all
nodes continue to exactly `A, X, B`.

The report excludes ports, paths, PIDs, and timestamps. CI invokes a standalone
trace in two fresh controller processes and requires byte-identical JSON. It
also requires all three unsafe subjects to discard through the same schema and
OTel path.

This proves process isolation, normal TCP, retained-log replay, and bounded
lost-reply semantics. It does not prove separately persisted state-machine
snapshots, retained-outcome expiry, automatic election, transaction-system
generation takeover, independent-disk loss, or object-store recovery.

## Cell v0 concurrent history gate

```bash
cargo run -p okv-eval -- run evals/suites/cell-concurrent-history.toml \
  --profile local-fs \
  --workload cell-concurrent-history \
  --backend process-local-fs
```

RFC-0029 submits 100 rounds of ten transactions through the real three-process
cell. Each round has four hot-key read/write contenders, four disjoint two-key
transactions, and two blind writers. Exactly one hot contender must commit,
every disjoint pair must appear atomically, and the greater blind-write commit
sequence must determine the row. At the midpoint, one committed response is
dropped, the leader process is killed, a successor recovers and exactly retries
the outcome, and the killed process remains absent until final convergence.

Candidate `1e01b08` kept runs `9616bf69` and `f66bb379`. Each run evaluated
3,000 logical transactions across seeds 1103, 2207, and 3301 with 2,100 commits,
900 durable conflicts, exact fresh-process replay, and zero anomalies. The
omitted-read-conflict control `c837f980` committed all 3,000 transactions,
produced no conflict outcomes, and discarded with two intended anomalies per
seed. This is a bounded schedule-independent oracle, not an exhaustive
strict-serializability checker or a partitioned-resolver proof.

## Cell read-value and real-time witness

```bash
cargo run -p okv-eval -- run evals/suites/cell-serializable-history.toml \
  --profile local-fs \
  --workload cell-serializable-history \
  --backend process-local-fs
```

RFC-0032 adds actual read values and non-overlap order to the bounded Cell v0
history. Candidate `a93041f` kept run `56a132c6` after two fresh executions per
seed. Across 3,000 transactions it checked 1,200 values from linearizable reads,
300 committed actual-read dependencies, and 727,650 real-time edges. All prior
atomicity, conflict, failover, retry, replay, and convergence gates remained
green.

Omitted-conflict control `aa460aa8` committed all 3,000 transactions. Its read
values and real-time order were individually valid, but the independent witness
rejected the committed actual-read-dependency class. The OTel stream labels
each constraint class and the control's dependency class as `result=fail`.

This is one commit-sequence witness for a seeded history family. It is not an
exhaustive checker and does not cover range reads, phantoms, multiple
read-version proxies, or partitioned resolvers.

## Cell range-read phantom witness

```bash
cargo run -p okv-eval -- run evals/suites/cell-range-phantom.toml \
  --profile local-fs \
  --workload cell-range-phantom \
  --backend process-local-fs
```

RFC-0033 constructs a two-transaction dependency cycle from actual reads. One
transaction reads an empty prefix and later writes a summary. A second reads
the absent summary and inserts into that prefix. The insertion commits first;
the range reader must conflict at its original read version. At the midpoint,
the leader dies after the insertion and a successor evaluates the range write.

Candidate `5d4427d` kept run `04b84730` with zero anomalies across three seeds.
It attempted 600 transactions, committed 300 insertions, rejected 300 range
writes, checked 600 dependency edges, and converged exactly after three leader
kills. Omitted-range control `f4678cd8` committed all 600 transactions and
discarded with 300 dependency cycles. OTel labels the correct and control
constraint series `pass` and `fail`, respectively.

This admits one deterministic empty-range insertion shape. It does not prove
range clears, overlapping ranges, arbitrary generated histories, multiple
read-version proxies, or partitioned resolver agreement.

## Cell read-version proxy causality witness

```bash
cargo run -p okv-eval -- run evals/suites/cell-read-version-proxy.toml \
  --profile local-fs \
  --workload cell-read-version-proxy \
  --backend process-local-fs
```

RFC-0034 alternates one tenant session across two independent proxy processes.
Each round obtains a common pre-commit view, acknowledges one unique
write, advances the session floor to that commit sequence, and hands the next
read-version request to the other proxy. The target must refresh to at least the
floor and observe the acknowledged value. One authority leader dies between
acknowledgement and handoff per seed.

Candidate `d910d10` kept run `eec5ca77` with zero anomalies across three seeds.
Each exact execution started two proxy processes per seed and exercised 900
proxy requests, 300 writes, and 300 causal handoffs. Every minimum was honored,
every write was observed, and all authority processes converged after restart.
Ignore-minimum control `d280df19` returned its valid pre-commit cache, producing
300 version-floor violations and 300 stale values. OTel labels the correct and
control constraint series `pass` and `fail`.

The read values still come from authority snapshots. This does not prove
concurrent batching, bounded waiting, proxy generation rollover, or direct
serving-worker reads.

## Publication GC root-graph gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/object-publication-root-graph.toml \
  --profile local-fs \
  --workload object-publication-root-graph \
  --backend object-store-local-fs+authority-quorum-fs
```

RFC-0035 represents checkpoint, clone, backup, analytical lease, and tenant
move as durable publication pins. Each seed writes a shared immutable object and
one unique closure per root, reopens the authority, marks and sweeps, selectively
unpins clone, and then races a new lease pin against a stale delete plan.

Candidate `d1ce1ec` kept run `885dfdb4` with zero anomalies at the exact
27-event budget. It preserved all 15 root instances, reclaimed only the six
clone-unique objects, deferred three stale delete plans, and replayed exactly.
Omitted-lease control `6e8ce843` registered 12 roots, deleted the three missing
lease closures, produced 12 anomalies, and discarded. OTel exported anomaly,
object-request, byte, availability, and duration series under suite hash
`7963b684`.

This admits the explicit local root vocabulary and root-epoch revalidation. It
does not prove lease expiry, abandoned-move cleanup, public-cloud behavior,
independent host loss, or a distributed sweeper.

## Serving worker base plus retained-WAL recovery gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-serving-recovery.toml \
  --profile local-fs \
  --workload cell-serving-base-plus-wal \
  --backend process-object-store-local-fs+publication-openraft+wal-quorum-fs
```

RFC-0036 freezes `Database(T) = ObjectState(O) + RetainedMutations(O,T]` for a
fresh serving process. Each seed derives a valid object base through `O=8` from
the admitted Cell v0 history, publishes its exact root through three replicated
authority processes, synchronizes the later envelope to a two-of-three local
WAL quorum, and starts a serving worker with empty private state.

Candidate `9e733e2` kept run `ed0cdfe8` with zero anomalies at the exact
45-event budget. Three fresh worker processes resolved the replicated root,
made six verified object reads, recovered three suffix records, and
reconstructed exact rows through `T=10`. Ignore-WAL control `690e0844` stopped
at frontier `8`, returned stale rows, produced nine anomalies, and discarded.
OTel exported both run IDs under suite hash `a6b66185`, with availability `1`
for the correct subject and `0` for the control.

This admits the bounded recovery equation, not original OpenRaft log
consumption, range routing, concurrent serving, arbitrary historical versions,
independent hosts, or cloud failure behavior.

## Live committed-envelope authority-feed gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-serving-authority-feed.toml \
  --profile local-fs \
  --workload cell-serving-live-authority-feed \
  --backend process-object-store-local-fs+publication-openraft+transaction-openraft
```

RFC-0037 keeps the three transaction-authority processes alive after they reach
`C=10`, publishes only the object base through `O=8`, kills the current
transaction leader, and starts a worker with empty private state. The worker
resolves the publication root and performs a linearizable committed-envelope
request against the surviving transaction-authority endpoints. No copied WAL
directory or controller-supplied suffix exists.

Candidate `e1c2437` kept run `bf79522d` with zero anomalies at the exact
48-event budget. Three successor authorities served one envelope each at
authority position `11`; three workers reached `T=10` with exact rows. The
dropped-final-envelope control `3db9c604` contacted the same feed but applied no
suffix, stopped at `8`, returned stale rows, produced nine anomalies, and
discarded. OTel exported both run IDs under suite hash `18e2250f`.

This admits the live role boundary and establishes that committed envelopes,
not raw transaction proposals, are the storage mutation format. It does not
admit a dedicated partitioned tLog, push streaming, range tags, backpressure,
independent hosts, or a serving-availability curve.

## Range-tagged tLog serving gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-serving-tagged-tlog.toml \
  --profile local-fs \
  --workload cell-serving-range-tagged-tlog \
  --backend process-object-store-local-fs+publication-openraft+transaction-openraft+tagged-tlog
```

RFC-0038 starts three dedicated tLog processes with distinct private roots,
copies the exact committed envelope and its required tags to all three, requires
a two-process durable quorum, and verifies the hard retained-byte limit. It then
kills one tLog and starts an empty-state worker that resolves the object base
and accepts only matching tag-`10` records from both survivors.

Candidate `beec908` kept run `851d0654` with zero anomalies at the exact
69-event budget. Across three seeds, nine tLog processes synchronized nine
records, rejected nine overflow probes without changing their retained prefix,
and survived three process deaths. Six survivor responses reconstructed three
suffix records and every worker reached exact `T=10` from `O=8`.

Missing-tag control `136b2523` contacted the same six survivors but found no
tag-`10` quorum record, stopped at `8`, returned stale rows, produced 12
anomalies, and discarded. OTel exported availability `1` and `0` under suite
hash `1afec3bd`.

This admits one bounded tagged tLog to serving path. It does not admit tLog
participation in transaction acknowledgement, multi-record streaming,
lag-based ratekeeping, repair, partitioned log sets, independent hosts, or
production curves.

## Commit visibility after tagged-log durability gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-commit-visibility.toml \
  --profile local-process \
  --workload cell-commit-after-all-tagged-logs \
  --backend transaction-openraft+two-tagged-tlog-sets+process-proxies
```

RFC-0039 separates ordered and staged state from visible commit. One
transaction requires log sets `10` and `20`; each set has three independent
processes and quorum two. Proxy one stages the exact envelope, makes set `10`
durable, and dies. Proxy two recovers the same identity and envelope, makes set
`20` durable, and dies before publication. Proxy three validates both receipt
sets, publishes once, acknowledges the client, and retries the original
transaction identity without appending another log record. A fresh worker then
requires matching quorums from both sets before reconstructing the visible
state.

Candidate `c549587` kept run `5a2e5a7f` with zero anomalies at the exact
84-event budget. Across three seeds, 21 authority processes, 18 tagged-log
processes, nine proxy processes, and three recovery workers participated. Six
proxy deaths preserved visible frontier `10`; eighteen durable records then
allowed one publication per seed at `T=11`. Every retry returned
`already_committed`, and every fresh worker reconstructed the exact rows.

Control `0da1a0c1` acknowledged after only set `10`, left all nodes in set `20`
empty, stopped at visible frontier `10`, produced 51 anomalies, and discarded.
Both subjects replayed exactly. OTel exported correctness `0` and availability
`1` for the admitted subject, then correctness `51` and availability `0` for
the control under suite hash `22fb3497`.

This admits the bounded ordering-to-durability-to-visibility protocol. It does
not admit authenticated receipt certificates, aborting an abandoned staged
head, generation takeover during staging, multi-record lag and backpressure,
log repair, partitioned routing, independent hosts, or production throughput.

## Authenticated tagged-log certificate gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-tagged-log-certificate.toml \
  --profile local-process \
  --workload cell-commit-with-authenticated-tagged-log-certificates \
  --backend transaction-openraft+signed-tagged-tlog-certificates+process-proxies
```

RFC-0040 requires the authority to install a monotonic member and key policy
for each log set independently of transaction input. Each tLog process signs
only after its exact local record is synchronized. The authority verifies the
cell, tenant, transaction generation, transaction identity, logical commit
sequence, log set, policy epoch, envelope digest, durable position, distinct
signers, signatures, and quorum before recording a certificate.

Candidate `6a81821` kept run `f5e3720a` at the exact 96-event budget with zero
anomalies. Across three seeds, 18 tLog processes synchronized 18 records and
produced 45 attestations. Six proxy deaths preserved visible frontier `10`;
both verified certificates then allowed one publication per seed at `T=11`.
The retained outcome and fresh-worker reconstruction were exact.

Five controls each replayed exactly, produced 51 anomalies, exported
availability `0`, and discarded:

| Subject | Run |
|---|---|
| unsigned node list | `f4425295` |
| duplicate signer | `83fbcf79` |
| wrong log set | `26433766` |
| tampered statement | `1235b238` |
| obsolete policy epoch | `52044094` |

The correct subject exported availability `1` and correctness `0`; every
control exported availability `0` and correctness `51`. The frozen suite hash
is `ffbd31cb`; the profile hash is `30ae0d7c`.

This admits a bounded authenticated durability proof, not production key
custody, policy rotation, process-incarnation binding, incomplete staged-head
recovery, generation fencing, multi-record lag, repair, or partitioning.

## Certified staged-head generation takeover gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-staged-head-generation-takeover.toml \
  --profile local-process \
  --workload cell-publish-certified-head-after-generation-takeover \
  --backend generation-authority+transaction-openraft+signed-certificate-state
```

RFC-0041 composes the authenticated staged-head state with the existing
external generation authority and two real three-voter transaction-system
generations. The old generation commits ten visible transactions, stages
transaction 11, records both verified log-set certificates, and remains at
visible frontier `10`. Its data log is then fenced after that state, three
successor learners catch up, membership moves to the successor voters, and the
external authority activates generation 2 with the recovered-position proof.

Candidate `f350a12` kept run `959a2211` at the exact 105-event budget with zero
anomalies and exact replay. Across three seeds, the reported path started nine
generation-authority processes and eighteen data-voter processes, killed three
authority leaders, admitted nine learners, performed three voter-set changes,
and collected nine fence plus nine recovery signers. The active successor
published the original transaction-11 envelope once, recovered a lost takeover
reply, rejected old-generation publication, and committed transaction 12.

Five controls replayed exactly and discarded:

| Subject | Run | Anomalies |
|---|---|---:|
| takeover during recovery | `81bef774` | 6 |
| missing log certificate | `e086ad66` | 3 |
| tampered envelope expectation | `fd11f355` | 3 |
| skipped staged head | `e6061870` | 30 |
| successor-generation rewrite | `59dffe26` | 27 |

The correct subject exported availability `1` and correctness `0`; every
control exported availability `0`. The frozen suite hash is `79cdd0c1`; the
profile hash is `567d8a98`.

This admits recovery of one fully certified head. It does not admit safe abort
of an incomplete head, a multi-record staged prefix, tLog generation garbage
collection, sustained lag, repair, partitioning, or production key custody.

## Incomplete staged-head fence and abort gate

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-incomplete-staged-head-abort.toml \
  --profile local-process \
  --workload cell-fence-and-abort-incomplete-staged-head \
  --backend generation-authority+transaction-openraft+tagged-tlog-fence-processes
```

RFC-0042 composes the incomplete staged-head state with durable per-process
tLog generation fences. Every old-generation log set must return a signed
fence quorum under one recovery identity. At least one set without a recorded
durability certificate must also return a write-quorum of signed local-absence
observations. Only the active successor may replicate the abort after its
voter-set handoff completes.

Candidate `341beb9` kept run `338ef8b4` at the exact 132-event budget with zero
anomalies and exact replay. Across three seeds, the gate started 45 external
processes, collected 18 fence and 9 absence attestations, restarted three
fenced tLog processes, rejected six late old-generation appends, committed
three aborts, replayed three lost replies, and committed successor transaction
12 from the last committed chain.

Six controls replayed exactly and discarded:

| Subject | Run | Anomalies |
|---|---|---:|
| abort before successor activation | `6a9f4002` | 3 |
| one absence signer | `86eda531` | 9 |
| missing log-set fence | `6b7f30a8` | 6 |
| forged absence over a present record | `af6cc5a5` | 12 |
| volatile fence after restart | `10988118` | 6 |
| reused aborted sequence or chain | `125b71cc` | 6 |

The correct subject exported availability `1` and correctness `0`; every
control exported availability `0`. The frozen suite hash is `1db99836`; the
profile hash is `e528f8cc`.

This admits safe abort for one incomplete head under honest, authenticated,
non-equivocating tLog signers. It does not admit production fence
authorization, signer key custody, arbitrary multi-record prefix recovery,
log-set movement, lag backpressure, repair, partitioning, or independent-host
failure.

## Multi-record staged-prefix recovery gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-multi-record-staged-prefix-recovery.toml \
  --profile local-process \
  --workload cell-recover-certified-prefix-abort-incomplete-suffix \
  --backend generation-authority+transaction-openraft+tagged-tlog-prefix-fence-processes
```

RFC-0043 composes a four-record unresolved window with exact authenticated
inventories from two required tagged-log sets. Every process persists the
old-generation fence before reporting the window. The successor recovers only
the longest prefix present at quorum in every set, aborts the first record
absent at quorum in any set and its dependent suffix, consumes every sequence,
and retains each result for exact retry.

Candidate `900b646` kept OTel-enabled run `ea3fb589` at the exact 168-event
budget with zero anomalies and exact replay. Across three seeds, the path
started 45 external processes, staged 12 records totaling 5,760 bytes,
collected 18 prefix-fence attestations and 72 inventory observations,
restarted three tLog processes, rejected six late old-generation appends,
recovered six records, aborted six records, replayed three lost replies, and
committed successor transaction 15.

Six controls replayed exactly, exported OTel, and discarded:

| Subject | Run | Anomalies |
|---|---|---:|
| publish beyond absent boundary | `fc9dda4e` | 24 |
| abort quorum-present record | `c76e5159` | 21 |
| skip recoverable prefix record | `fa35669d` | 18 |
| retain dependent suffix | `49665db4` | 27 |
| accept over-limit window | `1800d15f` | 12 |
| omit required log-set inventory | `12f27160` | 3 |

Prometheus exposed the admitted pass and all six failing controls under exact
candidate, suite, profile, run, and workload labels. The frozen source suite
hash is `83418bdd`; the evaluated suite hash is `398997f4`; the profile hash
is `ad522697`.

This admits deterministic recovery of one bounded staged window. It does not
admit sustained lag, lag-based backpressure, failed-log repair, moving log
sets, production fence authorization, signer custody, independent-host
failure, partitioned resolvers, or public-cloud operation.

## Sustained tagged-log lag and ratekeeping gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-tagged-log-lag-ratekeeping.toml \
  --profile local-process \
  --workload cell-ratekeeps-lag-pops-and-resumes \
  --backend transaction-openraft+publication-openraft+signed-tagged-tlog-ratekeeper-processes
```

Candidate `868c3de` kept run `d510af28` at the exact 180-event budget with
zero anomalies and exact replay. Across three seeds, nine attempts were
rate-limited before sequence allocation, 18 transactions committed, 180 signed
capacity attestations were checked, 18 durable pop attestations were collected,
and restarted tLogs plus fresh workers recovered exact transaction 16. The
correct path reached 7,820 retained bytes, fell to 3,910 after pop, and never
crossed the 16 KiB hard limit.

Six controls discarded with per-seed anomaly samples of 4, 5, 5, 7, 5, and 3
for partial append, best-node capacity, stale sample, pop beyond object
frontier, resume without pop quorum, and allocate before ratekeeping. OTel
exposed exact candidate, suite, profile, run, workload, retained-byte, and
anomaly labels for the correct path and every control. The frozen source suite
hash is `b6263722`; the evaluated suite hash is `5d8de452`; the profile hash is
`46608348`.

The correct path collects a quorum-signed capability from publication-authority
processes after each observes the exact replicated root. Every tLog verifies
the pinned signer quorum, hashes the referenced manifest bytes, decodes the
embedded cell snapshot, and matches the cell, tenant, generation, and object
frontier before deleting local records. A focused unit control rejects a
certificate without the pinned publication quorum.

## Tagged-log learner repair under retained lag gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-tagged-log-learner-repair.toml \
  --profile local-process \
  --workload cell-repairs-empty-tlog-learner-from-quorum \
  --backend transaction-openraft+signed-tagged-tlog-repair-processes
```

Candidate `670ef0a` kept run `a3c3356a` at 69 of 210 allowed events with zero
anomalies and exact replay across three seeds. The correct subjects started 18
active tLogs, failed three members, installed 12 retained records into three
new learners, restarted every learner from its private root, collected six
repair attestations and six readiness attestations, and started three fresh
workers. Each worker ignored the unpromoted learner and reconstructed exact
transaction `14` from object frontier `10`. The maximum certified snapshot was
3,977 bytes.

| Control | Run | Anomalies per seed |
|---|---|---:|
| one source signature | `6d99de75` | 2 |
| tampered snapshot after signing | `b97e5c23` | 2 |
| stale readiness frontier | `c90d7af6` | 2 |
| wrong learner incarnation | `2deb8392` | 1 |
| count unpromoted learner | `4fe75506` | 1 |
| duplicate live learner identity | `5c00a9ae` | 2 |

All controls replayed exactly and discarded. OTel exposed the correct and
unsafe results under exact candidate, suite, profile, run, workload, and
backend labels. The frozen source suite hash is `b85fbfdb`; the evaluated suite
hash is `5b2db17a`; the profile hash is `6cd2ae37`. Candidate `8ef5c87` and run
`38ac862c` produced no admissible result because the operation-duration metric
omitted its required result label. The telemetry-only correction in candidate
`670ef0a` produced the admitted run.

This admits a complete four-record retained-suffix snapshot, durable learner
restart, and quorum-certified readiness without promotion. The correct path
does not exercise concurrent append or ordered live-tail catch-up. Chunked
transfer, resume, log-set policy movement, independent hosts, external machine
identity, and production key custody remain open.

## Replicated tagged-log policy transition gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-tagged-log-policy-transition.toml \
  --profile local-process \
  --workload cell-moves-repair-ready-tlog-through-policy-epoch \
  --backend transaction-openraft+signed-tagged-tlog-policy-transition-processes
```

Candidate `b69714c` kept OTel run `8b8d9705` at 90 of 300 allowed events with
zero anomalies and exact replay across three seeds. The correct subjects
started nine transaction-authority processes and 18 initial tLog processes,
repaired three failed members as learners, collected six repair and six
readiness attestations, staged E2 with nine successor attestations, committed
three policy transitions, collected six authority activation attestations, and
survived activation restart. After another active member failed, fresh workers
counted only nodes `3` and `4` and reached exact transaction `17`.

| Control | Run | Anomalies per seed |
|---|---|---:|
| missing repair readiness | `c45557f7` | 1 |
| unresolved old-policy stage | `aa1166c8` | 1 |
| skipped policy epoch | `6fad03b0` | 3 |
| mixed-policy quorum | `9363aad0` | 1 |
| missing authority activation quorum | `b89ce548` | 1 |
| removed member rejoins | `1fbd0e47` | 5 |
| transition applies twice | `d92439f4` | 1 |

All controls replayed exactly and discarded. The unresolved-stage subject
fails closed before final transaction visibility. Prometheus observed
availability `1`, correctness anomalies `0`, and membership epoch `2` for the
correct run under exact candidate, suite, profile, run, workload, and backend
labels. The frozen source suite hash is `6bb75c49`; the evaluated suite hash is
`8b287d4e`; the profile hash is `c13048bc`.

This admits a one-member, one-log-set transition with a bounded write pause. It
does not admit concurrent append during repair, live-tail catch-up, chunked or
remote transfer, joint-policy writes, zone replacement, independent hosts,
production key custody, or concurrent policy movement.

## Resumable chunked tagged-log repair with live tail gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-tagged-log-chunked-live-repair.toml \
  --profile local-process \
  --workload cell-resumes-chunked-tlog-repair-and-catches-live-tail \
  --backend transaction-openraft+signed-tagged-tlog-chunked-repair-processes
```

Candidate `254cf421` kept OTel run `28dfe9f4` at 51 of 300 allowed events with
zero anomalies and exact replay across three seeds. The correct subjects
started nine transaction-authority processes and 18 active tLog processes,
failed three members, and created three empty learners. While each resumable
three-chunk base transfer was incomplete, the active policy appended
transaction `15`; transaction `16` followed before readiness. Learners survived
one base and one tail restart, accepted exact retries, installed the separate
two-chunk ordered tail, and remained non-voting. Learners and fresh workers
both reached exact transaction `16`.

| Control | Run | Anomalies per seed |
|---|---|---:|
| lose acknowledged chunk across restart | `97893c13` | 1 |
| finalize with one missing chunk | `30ae3394` | 1 |
| overwrite one durable chunk on retry | `1198e1c0` | 1 |
| install a gapped tail | `d5f85770` | 1 |
| certify stale learner readiness | `25ee028b` | 4 |
| count an uncaught-up learner | `528f1eec` | 1 |
| recopy the base during tail catch-up | `0190688e` | 1 |

All controls replayed exactly and discarded. Prometheus observed availability
`1`, correctness anomalies `0`, and the tail-only retained-byte gauge under
exact candidate, suite, profile, run, workload, and backend labels. The frozen
source suite hash is `a7206d45`; the evaluated suite hash is `3a20363c`; the
profile hash is `a5555c7d`.

This admits bounded same-host chunk persistence, restart recovery, and
tail-only catch-up while two active commits advance the frontier. It does not
admit remote transfer, multiple simultaneous repairs, unbounded append,
transfer lease expiry, orphan chunk collection, zone failure, production key
custody, or simultaneous policy movement.

## Partitioned resolver agreement gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-partitioned-resolver-agreement.toml \
  --profile local-process \
  --workload cell-partitioned-resolvers-match-centralized-oracle \
  --backend transaction-openraft+partitioned-resolver-processes
```

Candidate `65664bf` kept OTel run `8be62401` at 1,800 of 2,400 allowed events
with zero anomalies and exact replay across three seeds. Each seed runs 600
transactions through three authority processes and three independent ordered
resolver processes. The aggregate receipt contains 1,200 commits, 600 durable
conflicts, 3,003 signed resolver decisions, 3,000 finalizations, three resolver
restart replays, exact visible rows, and exact commit-envelope chains against
the centralized Cell v0 oracle.

| Control | Run | Anomalies per seed |
|---|---|---:|
| route crossing range by start key | `0cddd6e2` | 1 |
| commit after partial acceptance | `abfbe8cd` | 1 |
| count duplicate resolver identity | `b7db369a` | 1 |
| combine mixed map epochs | `a4891e60` | 1 |
| acknowledge before durable decision | `92c60192` | 1 |
| skip prior global finalization | `85389fdd` | 1 |
| split over a prepared old-map transaction | `4f5912ca` | 1 |

All controls replayed exactly, passed their negative-control and schema gates,
exported OTel, and discarded. Prometheus observed availability `1`, correctness
anomalies `0`, 1,200 commits, 600 conflicts, 3,003 checked decisions, and
frontier `600` for the correct run. This is a semantic and recovery gate, not a
performance result. The per-decision synchronized journal is intentionally
untuned. Online map movement, concurrent in-flight work per partition,
hot-range curves, independent hosts, and production identity remain open.
RFC-0049 separately tests single-proxy ordered batching without this journal.

## Stateless resolver generation-recovery gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-stateless-resolver-generation-recovery.toml \
  --profile local-process \
  --workload cell-recovers-stateless-resolvers-by-generation \
  --backend transaction-openraft+stateless-resolver-processes
```

Candidate `b69b245` kept OTel run `e334c857` at 1,800 of 2,400 allowed events
with zero anomalies and exact replay across three seeds. The gate sends 1,800
attempts through 228 ordered batches, three replicated transaction-authority
processes, three memory-only range resolvers, and one resolver-loss recovery per
seed. It records 699 commits, 1,098 conflicts, three safe conservative false
conflicts, 2,706 resolver decisions, three abandoned candidates, and three
replicated generation-fence markers.

Every successor generation starts three empty resolver processes at the exact
durable authority floor. Old-generation requests and replies fail closed. The
unresolved old-generation transaction remains invisible and retries with a new
identity and read version. The commit set remains a subset of the centralized
oracle, every centralized conflict is rejected, and rows and commit envelopes
match the authoritative outcomes. The resolver path performs zero durable
synchronizations and zero finalization RPCs.

| Control | Run | Anomalies per seed |
|---|---|---:|
| continue after resolver loss | `d2dde4c1` | 1 |
| activate successor before old fence | `e9551019` | 1 |
| count old-generation resolver reply | `1fa4c4a9` | 1 |
| admit read below recovery floor | `ed58133e` | 1 |
| publish unresolved old work | `0ea78ab3` | 1 |
| omit durable head from recovery | `0cc71d81` | 1 |

All controls replayed exactly, exported OTel, passed schema validation, and
discarded. Prometheus observed availability `1`, correctness anomalies `0`,
699 commits, 1,098 conflicts, and 2,706 checked decisions under the exact run
identity. The frozen source suite hash is `7f2b60eb`; the evaluated suite hash
is `5c74b499`; the profile hash is `1a51aed8`.

This is a semantic and recovery-shape gate, not a throughput result. It replaces
the intended RFC-0048 per-decision resolver persistence with FoundationDB-style
generation-scoped memory. It does not yet compose the authenticated tLog fence,
run multiple commit proxies, measure recovery-time availability, move the
resolver map online, or cross independent hosts.

## Stateless resolver plus authenticated tLog recovery gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-stateless-resolver-authenticated-tlog-recovery.toml \
  --profile local-process \
  --workload cell-recovers-resolver-accepted-authenticated-tlog-prefix \
  --backend generation-authority+transaction-openraft+stateless-resolvers+authenticated-tagged-tlogs
```

Candidate `27a86f1` kept OTel run `0411bfa5` at 210 of 512 allowed events with
zero anomalies and exact replay across three seeds. The aggregate receipt
contains 21 signed resolver decisions, 12 staged records, 51 real tLog appends,
18 prefix-fence attestations, 72 exact inventory observations, six recovered
records, and six aborted suffix records.

The gate proves the composition omitted by RFC-0049. Complete resolver evidence
may stage an envelope but does not publish it. Transaction `12` reaches quorum
in both tLog sets without either certificate reaching the proxy. After resolver
loss, signed tLog inventories recover it at frontier `12`. Transaction `13` is
quorum-absent in one required set, so it and transaction `14` abort. Empty G2
resolvers start at floor `12`; a new crossing-range identity publishes at
version `15` only after real G2 tLog certificates. Resolver synchronization and
finalization counts remain zero.

| Control | Run | Anomalies per seed |
|---|---|---:|
| publish before tLog quorum | `48afad06` | 1 |
| recover from authority marker only | `41e0faf2` | 1 |
| activate successor before tLog prefix fence | `8265bc49` | 1 |
| count old-generation resolver reply | `4f1bea9a` | 1 |
| admit read below authenticated floor | `2f1fe28a` | 1 |
| abort quorum-present record | `a2a75e60` | 1 |
| publish beyond absence boundary | `415d9372` | 1 |

All controls replayed exactly, exported OTel, passed schema and budget gates,
and discarded. Prometheus observed availability `1` and correctness anomalies
`0` for the correct run, then availability `0` and three total anomalies for
each control. The suite hash is `1ed74325`; the profile hash is `cf633bfc`.

This is still a single-proxy semantic gate. It does not admit multiple commit
proxies, online resolver-map movement, independent hosts, recovery-time
objectives, sustained tLog lag, ratekeeping on the partitioned path, or
production signer custody.

## Multiple commit-proxy global ordering gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-multi-commit-proxy-ordering.toml \
  --profile local-process \
  --workload cell-orders-three-commit-proxies-at-resolvers-and-tlogs \
  --backend sequencer-openraft+commit-proxy-processes+stateless-resolvers+authenticated-tagged-tlogs
```

Candidate `674a443` kept OTel run `2c1c8544` at 585 of 1,024 allowed events
with zero anomalies and exact replay across three seeds. The aggregate receipt
contains 72 replicated sequencer tickets, 288 transactions, 180 commits, 108
conflicts, 348 resolver decisions, and 72 durable tLog progress frames.

Three proxy processes per seed batch independently. The three resolvers and six
tLog workers receive different deterministic arrival permutations, then process
only a contiguous `(previous, current]` ticket chain. The largest pending window
is four batches. Conflict-only batches still emit progress frames, so all active
tLogs distinguish an empty result from a missing predecessor. Every batch
acknowledges only after quorum durability in both required log sets.

| Control | Run | Anomalies per seed |
|---|---|---:|
| duplicate current version | `e7a65678` | 11 |
| skip previous version | `00016074` | 10 |
| resolver arrival order | `662ecca2` | 1 |
| tLog arrival order | `b21dfae1` | 2 |
| mutate ticketed batch | `d2e7e3fc` | 1 |
| acknowledge before every tLog set | `1d791160` | 2 |
| stale proxy incarnation | `7313a74c` | 1 |
| omit conflict-only progress | `e5e7d3ce` | 3 |

All controls replayed exactly, exported OTel, passed schema and negative-control
gates, and discarded. Prometheus observed availability `1` and anomalies `0`
for the correct run, then availability `0` with the expected anomaly totals for
the controls. The evaluated suite hash is `0a2640ab`; the profile hash is
`b7e077dd`.

This proves a bounded ordering protocol, not throughput. Proxy failure after
ticket allocation, sequencer scaling, metadata propagation, online resolver
split or merge, independent hosts, recovery-time curves, and production key
custody remain open.

## Commit-proxy generation-recovery gate

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
cargo run -p okv-eval -- run \
  evals/suites/cell-commit-proxy-generation-recovery.toml \
  --profile local-process \
  --workload cell-recovers-three-commit-proxy-loss-boundaries \
  --backend sequencer-openraft+commit-proxy-processes+stateless-resolvers+authenticated-tagged-tlogs+generation-takeover
```

Candidate `bf72639` kept OTel run `1c55dad7` at 1,830 of 2,048 allowed events
with zero anomalies and exact replay across three seeds. The aggregate receipt
contains 108 sequencer tickets, 432 transaction attempts, 336 commits, 1,044
resolver decisions, 510 durable tLog writes, nine generation fences, 24
abandoned tickets, and three exact lost-reply outcome resolutions.

Each seed uses four transaction-system generations. A proxy dies before
resolver delivery, after only one required tLog set reaches quorum, and after
both required sets reach quorum but before its client reply. Recovery publishes
the maximal prefix present at quorum in every required set, abandons the two
incomplete suffixes, preserves the fully durable unknown-result batch exactly
once, and starts each successor above every version issued in its predecessor.

| Control | Run | Anomalies per seed |
|---|---|---:|
| continue same generation | `64afbe20` | 5 |
| replace missing nonempty ticket with no-op | `7435350c` | 6 |
| publish from one required tLog set | `cbdb06b2` | 5 |
| omit fully durable lost-reply ticket | `c65209b1` | 8 |
| execute across missing predecessor | `7aeaba80` | 5 |
| trust incomplete tLog inventory | `e43de6df` | 7 |
| reuse old issued version | `df3c08bd` | 3 |
| accept fenced-generation reply | `4a2ed758` | 2 |
| duplicate unknown-result mutation | `5a3f433f` | 5 |

All controls replayed exactly, exported OTel, passed schema, budget, and
negative-control gates, and discarded. Prometheus observed availability `1`
and anomalies `0` for the closing correct run. The evaluated suite hash is
`9f8dc2a4`; the profile hash is `74034ac6`.

This proves a same-host semantic recovery rule, not operational availability.
The recorded 27.37-second operation median is end-to-end evaluation harness
time for a full seeded history and exact replay, not proxy-recovery downtime.
The next curve must isolate detection, fencing, inventory, recruitment, and
resume durations while varying pending work, retained tails, and tLog topology.

## Cell v0 authority snapshot and log-pop gate

```bash
cargo run -p okv-eval -- run evals/suites/cell-process-snapshot.toml \
  --profile local-fs \
  --workload cell-process-durable-snapshot-pop \
  --backend process-local-fs
```

The controller first proves multi-key OCC, a durable conflict rejection, a
lost reply, exact retry on a successor, and restarted-node convergence. It then
triggers one local state-machine snapshot on every voter at the same applied
position. The gate stops every process, durably purges the covered journal
entries, restarts all voters, requires the exact retained outcome, and commits
one new multi-key transaction. All rows and the complete commit-envelope chain
must converge again. A paired subject performs the same purge without a
snapshot and must fail closed.

The version-1 snapshot fixture, checksum, length checks, duplicate-identity
rejection, metadata-to-state validation, and exact fresh-process replay are hard
gates. This lane proves the transaction-authority half of WAL reclamation on one
machine. It does not admit log pop from object publication alone, object-data
closure, `O_cell`, snapshot transfer to a repairing follower, independent-disk
loss, outcome expiry, or snapshot size and latency limits.

Candidate `09e9344` kept in run
`3ec077dd-4f2e-44a4-add1-880dbe1c250c` with zero anomalies across three seeds
and 42 observed events under the 50-event cap. The no-snapshot subject discarded in run
`4fcd329e-fc9e-496d-895d-b2fd19637491` with four anomalies per seed while exact
replay and operation-coverage gates still passed.

## Cell objectification and empty-cache gate

```bash
cargo run -p okv-eval -- run evals/suites/cell-objectification.toml \
  --profile local-fs \
  --workload cell-objectification-correct \
  --backend process-local-fs
```

The controller first executes the durable Cell v0 transaction and snapshot
scenario. It then writes the exact committed envelope chain into one
content-addressed immutable segment, publishes a range manifest through a
separate three-process generation-fenced authority, and constructs a fresh
object client. That worker may use only the published root and its named
closure to rebuild the exact rows at `C_cell`.

The gate records `C_cell`, the verified object frontier `O_cell`, the durable
authority snapshot frontier `S_authority`, and requires safe log pop to equal
`min(O_cell, S_authority)`. One negative subject publishes a root with a missing
child. The other uses `O_cell` alone as the log-pop frontier. Both must discard
through the same schema, metrics, OTel path, and exact-replay gate.

This is a bounded cross-role composition proof on one machine. It does not
claim a fused cell coordinator, multiple ranges, remote object storage,
independent failure domains, or replacement-voter snapshot installation.

Candidate `4fdf4a0` kept in run
`acdbd621-6aba-4bd6-b533-3efca57be0ed` with zero anomalies across three seeds
and 48 observed checks under the 60-event cap. The incomplete-closure control
discarded in `b4bf435c-2104-476a-84c6-e27d6e81789f`; the object-only-pop
control discarded in `bc4d5d2f-a9bf-4b10-a029-c5120b6ce606`. All three runs
exported the four separate frontier gauges through the configured OTel path.

## Cell-generation takeover gate

```bash
cargo run -p okv-eval -- run evals/suites/generation-process.toml \
  --profile local-fs \
  --workload generation-takeover-authority-failover \
  --backend process-local-fs
```

The controller starts three external generation-authority processes, three G1
data voters, and three G2 data learners over localhost TCP. G2 catches up the
same OpenRaft log before the authority enters `Fencing`. The controller commits
a data-log barrier, proves that even a previously authorized G1 request is
rejected when applied after the barrier, then reserves generation 2 as
`Recovering`. It kills the authority leader, proves the reservation through a
linearizable successor read, changes the data voter set from G1 to G2 while
commits are quiesced, and rejects G2 writes until activation. After activation,
G2 commits `B` and all replacement voters contain exactly `A, B`.

Three seeds execute 48 semantic checks. The standalone command emits canonical
JSON for fresh-process byte comparison. Four negative subjects bypass the stale
commit fence, admit a write during recovery, accept a competing recovery, or
activate with a zero recovery position. Each must discard through the same
schema and OTel path.

This proves one bounded, quiesced voter-set handoff and external authority
leader loss. The data-quorum certificate gate below replaces its bare
controller-reported positions. Automatic failure detection, overlapping old and
new transaction-system traffic, coordinator membership change, object-root
reconciliation, and independent-disk loss remain unproven.

## Data-quorum recovery certificate gate

```bash
cargo run -p okv-eval -- run evals/suites/generation-certificates.toml \
  --profile local-fs \
  --workload generation-certificate-handoff \
  --backend process-local-fs
```

Every data process owns one Ed25519 signing seed in the process contract. The
authority pins public keys for the active voters at bootstrap and for pending
voters at `Prepare`. A G1 voter signs only the exact applied fence-barrier term
and index. A G2 voter signs only the exact applied voter-set transition while
its local generation mirror remains `Recovering`. The authority requires a
majority of distinct pinned voters and verifies canonical statement bytes that
bind purpose, cell, generation, recovery identity, active and pending
transaction-system identities, log position, and membership digest.

Three seeds execute 48 takeover checks with zero anomalies and exact
fresh-process replay. Five negative subjects admit a single-signer fence,
tampered fence position, duplicate recovery signer, stale recovery identity, or
wrong membership digest. Each discards through the shared result and OTel path.

This proves certificate construction, local observation, transport, replicated
authority verification, and authority-leader failover in the bounded process
contract. It does not prove production key custody or rotation, compromised
quorum tolerance, automatic recovery initiation, control-root reconciliation,
or independent-disk recovery. Test signing seeds are passed through process
configuration and are not a production secret-delivery design.

## MVCC semantic gate

```bash
cargo run -p okv-eval -- run evals/suites/model-history.toml \
  --profile generated-dev \
  --workload generated-mvcc-history \
  --backend model
```

The suite freezes RFC-0002 into its contract hash. It records anomaly samples,
event/read/range-clear/replay/availability/retention/generation counts, exact
seed replay, and a trace digest over five seeds. The oracle canonicalizes replay
independently from `CommitBatch::fingerprint`. CI requires the normal workload
to keep and every negative workload to discard for oracle disagreement.

```bash
cargo run -p okv-eval -- run evals/suites/fault-recovery.toml \
  --profile sim-dev \
  --workload overlapping-generation-failures \
  --backend turmoil
```

## Frozen surfaces

An autonomous experiment may not modify:

- `crates/okv-model/`;
- `evals/schema/`;
- the selected suite definition or its held-out seeds;
- fixed runtime/operation budgets;
- result aggregation and keep/discard logic;
- `program.md`.

A benchmark correction is welcome, but it lands separately, invalidates prior
comparisons when necessary, and establishes a new baseline before optimization
resumes.

## Phase 0 workload

`[EXISTS]` RFC-0021 adds the first executable incumbent:

```bash
cargo run -p okv-eval -- run evals/suites/phase0-slate-filesystem.toml \
  --profile local-fs \
  --workload slatedb-filesystem-baseline \
  --backend slatedb-local-fs
```

The local profile writes 8 MiB for each of three seeds through pinned SlateDB,
checks post-flush and warm point reads, checks one ordered 100-row scan, opens a
new database instance over the same filesystem objects, and times the first
verified read. Its `reuse_warm_db_for_reopen` poison must discard even when the
logical value remains exact. Initial open, ingest, oracle verification, cache
prime, warm read, scan, close, reopen, first read, cold reads, and final close
have independent time and I/O deltas. Raw filenames include `run_id`.

`[EXISTS]` RFC-0022 adds the repaired scale curve:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-filesystem-scale.toml \
  --profile scale-64mib \
  --workload slatedb-filesystem-scale-baseline \
  --backend slatedb-local-fs
```

Candidate `361a0fd` kept exact values at all three sizes. Reopen stayed near
flat from 1 to 8 MiB, then the 64 MiB open read 210,773,938 bytes and crossed
the suite's dataset-scan stop threshold. This stops the untuned SlateDB
incumbent, not the objectKV architecture. The suite has no cloud-price or
compaction-cost ceiling yet.

`[EXISTS]` RFC-0024 executes the one bounded configuration pass allowed by
D30:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-bounded-configuration.toml \
  --profile configured-64mib \
  --workload slatedb-bounded-configuration \
  --backend slatedb-local-fs
```

Candidate `7567b99` separates serving from maintenance, uses 64 KiB blocks and
whole-SST Bloom filters, and removes SlateDB's duplicate object WAL beneath the
objectKV transaction log. Seed 1103 and confirmation seeds 2207 and 3301 all
opened with 402 read bytes, used five requests and at most 210,439 bytes for the
first cold point, and reached that value in 3.81 to 4.12 ms. The warm-instance
poison discarded. Total request count increased from 345 to 455, so this keeps
a local segment candidate but does not pass remote request economics or
external compaction.

`[EXISTS]` RFC-0025 adds the local separate-role compaction falsifier:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-external-compaction.toml \
  --profile local-compaction-8mib \
  --workload slatedb-external-compaction \
  --backend slatedb-local-fs
```

Candidate `b240b38` creates eight equal L0 SSTs, runs a coordinator with its
embedded worker disabled, runs a separately built worker with matching 64 KiB
format settings, and then opens a fresh serving handle. Runs `d6425f5e` and
`5431c0fe` kept three seeds at zero anomalies. Every seed finished with zero L0
SSTs and one sorted run, 1.027x maintenance write amplification, a 538-byte
fresh open, and at most 83,264 bytes for the first cold point. Control
`af37279a` skipped both roles and discarded on exactly four maintenance gates.
This is not a real-process or remote-store result.

`[EXISTS]` RFC-0026 adds overwrite pressure and a real worker-process kill:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-compaction-reclaim.toml \
  --profile local-reclaim-8mib \
  --workload slatedb-compaction-reclaim \
  --backend slatedb-local-fs-process
```

Candidate `803de76` writes eight overlapping 8 MiB logical snapshots, observes
a standalone worker's persisted `Running` claim, kills and reaps that process,
waits for the coordinator to reclaim the job, and starts a fresh worker. Runs
`238de077` and `882b1fcf` kept three seeds with exact latest-value scans and
zero anomalies. Kill through committed completion took 576 to 618 ms. Fresh
open read 538 bytes and the first cold point fetched at most 83,264 bytes.
Missing-replacement control `af904d02` discarded only on replacement identity
and completion. Force-killed child I/O is not represented in controller object
counters.

`[EXISTS]` RFC-0027 runs the same serving and separate-role compaction contract
through pinned MinIO:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-minio-compaction.toml \
  --profile local-minio-compaction-8mib \
  --workload slatedb-minio-compaction \
  --backend slatedb-minio-s3
```

Candidate `abb2c64` kept runs `229bfced` and `6f0e194b` across seeds 1103,
2207, and 3301 with zero anomalies. Eight L0 SSTs became one sorted run,
maintenance wrote 1.027x logical bytes, every row remained exact, fresh open
read 538 bytes, and the first cold point used five requests and at most 83,264
bytes. Missing-worker control `d1125f50` discarded on exactly the four intended
maintenance gates. This is a local S3-compatible HTTP receipt, not a GCS,
public-cloud latency, provider-failure, or garbage-collection receipt.

`[EXISTS]` RFC-0028 kills the coordinator after durable worker output and
before manifest publication:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-coordinator-recovery.toml \
  --profile local-coordinator-recovery-8mib \
  --workload slatedb-coordinator-recovery \
  --backend slatedb-local-fs-process
```

Candidate `851decb` kept runs `ab8b22d4` and `e73b3458` across three seeds.
Each worker persisted one final SST, each first coordinator process was killed
before changing the manifest, and each distinct replacement coordinator
committed the exact SST without starting a worker. Kill through commit took
29.4 to 30.5 ms. Every latest overwrite remained exact and bounded fresh reads
held. Missing-restart control `b2045e82` discarded only on replacement identity
and completion while the original L0 state stayed exact.

`[EXISTS]` RFC-0030 overlaps two live coordinator processes and requires the
newer durable epoch to fence the older process:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-coordinator-fencing.toml \
  --profile local-coordinator-fencing-8mib \
  --workload slatedb-coordinator-fencing \
  --backend slatedb-local-fs-process
```

Candidate `2c6a854` kept runs `aaaecbb6` and `85672759` across seeds 1103,
2207, and 3301. The first and second coordinators advanced the shared compactor
epoch 0 -> 1 -> 2 while both processes overlapped. Every epoch-1 process exited
without a controller signal in 13.56 to 21.61 ms. Every epoch-2 process remained
live, compacted eight L0 SSTs to one sorted run, and preserved exact rows and
bounded fresh reads. External-kill control `2899bb28` reached the same data but
discarded only on stale-process self-fencing. This does not prove election,
host-partition behavior, or a public-cloud authority lease.

`[EXISTS]` RFC-0031 preserves active compaction output, then collects a true
orphan:

```bash
cargo run -p okv-eval -- run \
  evals/suites/phase0-slate-orphan-gc.toml \
  --profile local-orphan-gc-8mib \
  --workload slatedb-orphan-gc \
  --backend slatedb-local-fs-process
```

Candidate `dea0b20` kept runs `8d606761` and `26b19dfb` across seeds 1103,
2207, and 3301. GC retained every completed worker output while active
compaction state named it. A replacement coordinator committed those exact
objects, then a second collection deleted an aged SST absent from all manifests
and active jobs in 1.88 to 1.92 ms. Exact latest-value scans and bounded fresh
reads held. Dry-run control `161eac32` retained the same correct data but
discarded only because the true orphan remained. This does not prove the future
checkpoint, clone, backup, analytical-lease, multi-tenant, or public-cloud root
graph.

`[EXISTS]` The fixed-cadence strategy audit repeats the scale curve alongside
MinIO authority, generation recovery, lost-publication-response recovery, HTAP
streaming, and four deliberate negative controls:

```bash
experiments/overnight_strategy_audit.sh
```

Frozen candidate `a56442a` completed 24 admissible fixed-cadence cycles plus
four startup controls with 172 of 172 expected outcomes and zero unexpected
result. A deadline-boundary scheduler defect then emitted 196 dense
supplemental outcomes. They also matched, but are excluded from cadence and
dispersion claims. Commit `e6ec477` fixes future runs by waiting at the audit
deadline instead of starting another cycle.

Use a deterministic 10 GiB logical dataset after the small developer profile is
working. Keys and values are generated from recorded seeds.

Visible workloads:

- uniform point writes;
- uniform and Zipf point reads;
- cold random reads after cache deletion;
- 10, 100, and 10,000-row scans;
- compaction after a fixed overwrite distribution;
- close, delete local cache, reopen, first read.

Held-out coverage changes seeds, key distributions, value-size mix, and operation
ordering without changing semantics. The agent optimizing code does not read the
held-out inputs.

## Fixed profiles

- `dev`: small local filesystem or in-memory run, under 60 seconds.
- `minio`: pinned Docker image and resource limits, fixed operation count.
- `cloud-gcs`: named bucket region, machine type, concurrency, and operation
  count. Credentials are external and never written to results.
- `fault`: deterministic scheduler seed and bounded event count, not wall-clock
  throughput.

Cross-machine performance results are not compared directly. A result is
comparable only when suite hash, profile hash, toolchain, backend, and relevant
hardware identity match.

## Object-store conformance

```bash
cargo run -p okv-eval -- validate-suite evals/suites/object-store.toml
cargo run -p okv-eval -- run evals/suites/object-store.toml \
  --profile memory-authority \
  --workload named-object-authority-contract \
  --backend memory
```

The direct `okv-object` report and the enclosing eval result have separate JSON
Schemas. Every case becomes a hard gate and a bounded-cardinality
`compatibility.cases` measurement. Requests and bytes are split by API and
result class. Object keys and credentials never become attributes.
The object-store suite lists its direct report schema in `contract_files`, so a
schema change also changes the suite contract hash.

## Transaction-system recovery curve

`[EXISTS]` RFC-0054 separates recovery from the complete RFC-0053 semantic
fixture:

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-transaction-system-recovery-curve.toml \
  --profile local-process \
  --workload recovery-tail-4096 \
  --backend local-process+replicated-authority+authenticated-tlog-inventory
```

Candidate `90c1526` produced 210 correct samples across ten frozen points.
Retained-tail totals were 0.292, 0.465, and 3.158 seconds at 256, 4,096, and
65,536 records per tLog. The large point spent 2.870 seconds in authenticated
inventory. Pending 8 and 512 were flat. The 4x5 tLog plus 33-resolver point
took 1.313 seconds, split almost evenly between inventory and sequential role
recruitment. The 1 GiB and 1 PiB logical database points performed identical
work and read zero permanent database bytes.

Permanent-database scan, one-set trust, quadratic inventory, and early-resume
controls discarded. OTel exported total duration, phase duration, inventory
bytes, database bytes read, and deterministic work units. The next performance
experiment must retain suite hash and profile, change only the candidate
surface declared by the suite, run the incumbent in the same batch, and beat
the median by at least 10 percent beyond observed noise.

## KV Runtime accounted resource envelope

`[EXISTS]` RFC-0056 freezes the first KV Runtime density contract:

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-kv-runtime-resource-envelope.toml \
  --profile accounted-v0 \
  --workload kv-runtime-density-1000 \
  --backend model+accounted-resource-envelope
```

The correct 1, 100, and 1,000 Range Engine points pass every hard gate under
suite hash `491c5602`. Fixed accounted RAM is 4,608 bytes per range. With one
shared 128 MiB RAM cache request, total accounted RAM is 134,222,336 bytes at
one range, 134,678,528 bytes at 100 ranges, and 138,825,728 bytes at 1,000
ranges. Accounted NVMe remains one shared 2 GiB cache at every point.

The per-range-cache, refusal-before-eviction, ignored-hard-debt, and
missing-range-movement subjects each fail their owning gate and discard. A
clean candidate receives `keep` for the accounted contract. That verdict is not
a physical performance result because this lane has no physical incumbent and
records no RSS or latency distribution.

This admits the process-wide resource and pressure semantics. It does not
admit 1,000 real SlateDB databases, a routed KV Runtime, a production range
count, or a performance curve. The follow-up physical-density lane must measure
RSS, allocator bytes, threads, async tasks, file descriptors, local files, NVMe
bytes, p50 and p99 point reads, object request amplification, cold
reconstruction time, and objectification debt at the same 1, 100, and 1,000
points.

## KV Runtime physical density

`[EXISTS]` RFC-0057 freezes and passes the physical follow-up:

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-kv-runtime-physical-density.toml \
  --profile local-process \
  --workload kv-runtime-one-db-1000 \
  --backend local-process+slatedb-objectkv-serving-v1
```

Each of the three seeds runs in a fresh child process, plus one same-seed
semantic replay. The child uses one current-thread Tokio runtime, a request and
byte counting object fixture, one bounded filesystem cache, and the accepted
`objectkv-serving-v1` SlateDB settings. It writes real data, explicitly flushes,
measures resident resources, closes every database and decoded cache, then
reopens with empty RAM and NVMe cache state and checks every completed range.

Raw child receipts are written beneath `target/okv-eval-artifacts` with the
candidate identity, run ID, workload, executable SHA-256, physical samples,
file inventories, object I/O, phase durations, and semantic replay. OTel
records the same bounded metric families. Four controls substitute accounted
RSS, misreport private caches as shared, reuse a warm handle, or omit the
safety receipt. Each must discard independently.

The gate selects only the relationship between a Range Engine and the embedded
local engine. Mixed load, range movement, retained txLog debt, MinIO, GCS,
concurrent reads and writes, and production sizing remain separate follow-up
curves.

All nine correct points kept and the four controls discarded under suite hash
`69e079a5`. At 1,000 assignments, one database with logical ranges used 18.2
KiB median RSS per range, 9 live tasks, and 9 object files. Database-per-range
with one shared decoded cache used 131.8 KiB per range, 8,001 tasks, and 9,000
object files. Private decoded caches increased the RSS result to 181.6 KiB per
range. The accepted result makes one database with logical ranges the next
prototype default.

## KV Runtime exact-version read path

`[ACTIVE-WORK]` RFC-0058 and
`evals/suites/cell-kv-runtime-exact-version-read.toml` freeze and execute the
next local curve. The adapter now has the semantic candidate: objectKV-owned
MVCC physical keys, explicit point tombstones, exact `get(key, T)`, exact
ordered `scan(begin, end, T)`, applied-frontier refusal, binary key-order
tests, and close plus reopen coverage.

The child-process lane measures version depths `1`, `16`, and `256`.
Each point records latest, near-latest, and oldest-retained warm and cold point
latency; a 1,024-row ordered scan; object requests and bytes per returned row;
physical bytes per live byte; flush time; and empty-cache reopen time. The
primary question is whether latest and near-latest point amplification stays
bounded as retained history grows. Oldest-retained latency is a reported bound,
not the primary target.

Four controls must discard independently: latest-only storage answering an old
`T`, a skipped point tombstone, an applied frontier claimed beyond physical
state, and a length-prefixed user-key encoding that changes binary scan order.
The local lane admits shape and amplification only. MinIO, GCS, concurrent
application, history GC, range tombstones, and online range movement remain
separate gates.

`[EXISTS]` Candidate `fe2906d` kept the clean depth `1`, `16`, and `256` points
under suite hash `e3bc8644`. Near-latest empty-cache point p99 was 1.85, 2.05,
and 6.69 ms. Physical bytes per live byte were `1.20x`, `17.77x`, and
`283.47x`. At depth `256`, the independently cold 1,024-row near-latest scan
read 74,032,200 bytes through 1,128 range GETs in about 0.92 seconds. The
latest-only, skipped-tombstone, overstated-frontier, and broken-key-order
controls all discarded. Snapshot leases plus bounded MVCC history collection
are required before this path can become a serving default.

## KV Runtime snapshot floor and history collection

`[EXISTS]` RFC-0059 separates the logical admission floor `F` from the
physically collected frontier `G`. The first mechanism now executes in
`okv-slate`: a monotonic generation-zero floor, typed `snapshot_expired` read
refusal, one frozen floor per compaction job, and a version-aware SlateDB
filter. A real separate compactor test proves that older entries are removed
while the floor-visible value, a floor-visible point tombstone, and every newer
version survive close and reopen.

Candidate `3c9f008` kept the clean retained windows `1`, `16`, and `64` under
suite hash `c288cd4d`. Starting from 74.3 MB of depth-256 SST state, post-GC
amplification was `1.225x`, `1.111x`, and `1.107x` retained logical bytes. Cold
floor scans read 0.32, 4.66, and 18.57 MB through 7, 73, and 285 range GETs.
Local cold point p99 stayed between 0.155 and 0.181 ms. Collection took 239 to
258 ms. All five controls discarded.

The primary shape is now admitted locally: physical and scan cost follow the
retained window rather than total historical depth. The first diagnostic also
found the pinned serving profile's eight-SST per-key L0 backpressure boundary.
Production needs continuous compaction or intake ratekeeping before that
limit.

`[EXISTS]` RFC-0060's pure authority rejects a backdated lease, committed
expiry cannot be reversed by renewal, prepared jobs freeze the floor, input
root, range epoch, and namespace, stale receipts fail, and a lease or job root
blocks delete reservation. Cell-wide `G` can advance only against the configured
top-level transactional manifest root. Checksummed restore retains the exact
lease state.

`[EXISTS]` Candidate `5f62082`, suite `cell-snapshot-lease-authority-process-v1`,
kept three seeds with 42 checks, 12 leader replacements, nine dropped replies,
nine recovered outcomes, and semantically exact fresh-process replay. Acquire,
renew, and publish each cross a lost-response failover. Final `F` is 224 and
`G` is 200. The retained-outcome-disabled control discarded on every seed.

`[EXISTS]` Candidate `87794a6` expands the suite to seven workloads under suite
hash `134c97a7`. Correct run `1dc3440f` kept the same 42 checks and zero
anomalies. Six controls discarded on all three seeds: missing retained outcome
`90104df9`, backdated admission `578e62e8`, omitted lease-root epoch
`a5cde72d`, stale range epoch `63ff010e`, `G` advancement without publication
`df749dac`, and stale input-root publication `95c369e4`.

`[EXISTS]` The physical composition suite runs with:

```bash
cargo run -p okv-eval -- run \
  evals/suites/cell-mvcc-gc-authority-composition.toml \
  --profile local-process \
  --workload mvcc-gc-authority-physical-failover \
  --backend local-process+slatedb-mvcc-gc+openraft-publication-authority
```

Candidate `3c8a52e`, suite hash `aee84768`, kept run `a9d1b1f8` with
zero anomalies across three seeds. Each seed discovers and hashes the exact
input manifest and its live SSTs, obtains a replicated token at frozen floor
13, runs real SlateDB compaction, re-reads the exact replacement closure,
replaces the authority leader, and publishes through the successor. The gate
executed 24 checks and three failovers within its 32-event budget. Fresh
physical object identities differ, so replay compares the stable semantic
receipt rather than generated manifest names and bytes.

Three full-path controls discard:

| Control | Run | Anomalies per seed | Detected boundary |
|---|---|---:|---|
| omit one live output SST | `0f0232da` | 1 | exact physical closure |
| use semantic digest as manifest | `15ecd6ac` | 2 | physical closure and exact root |
| skip authority failover | `ad93d32a` | 1 | successor publication history |

The closure controls expose a real trust boundary. The generic replicated
authority validates the submitted token and receipt, but it does not parse a
SlateDB manifest to infer omitted children. The engine-specific binder must
re-read the physical manifest and every referenced SST.

`[EXISTS]` Candidate `b228bd3` upgrades the same suite to hash `86eacf38`.
Correct run `49d4d445` kept 27 checks across three seeds within the 32-event
budget. A read-only authority-bound SlateDB adapter verifies the selected
manifest's path, length, and SHA-256, hides newer manifests, and disables WAL
replay. After internal latest advances to M1, independent M0 and M1 readers
return exact floor and latest points and scans. The local adapter test also
rejects a forged digest, a nonnumeric manifest name, and a manifest from
another database.

The three controls still discarded under the upgraded suite in runs
`d7329c62`, `01d26720`, and `aab0cfec`.

`[EXISTS]` Candidate `fc30e59` freezes suite hash `9bf20342`. Correct run
`da53cee9` kept zero anomalies across three seeds and exact replay. Per seed it
starts seven transaction processes, a three-node publication authority, two
three-node signed txLog sets, and two disposable Range Engine workers. It kills
the authority leader and one member of each txLog set. M0=3 plus commits 5 and
10 and M1=5 plus commit 10 both reach the exact transaction oracle at T=10.
The suite used its full 36-event budget.

Six controls ran on the same process topology and discarded on every seed:

| Control | Run | Anomalies per seed |
|---|---|---:|
| publish M1 before the M0 worker resolves its root | `68d2bc66` | 5 |
| omit the intermediate txLog commit | `5f7441dd` | 3 |
| tamper one quorum signature | `de75ed8e` | 2 |
| advance the certificate policy epoch | `ee85fb34` | 2 |
| publish M1 with the wrong expected prior root | `7f04dbd8` | 7 |
| skip publication-authority failover | `2797bff1` | 1 |

`[EXISTS]` Candidate `2742400` upgrades this suite to hash `fd5b52a6` and adds
old-root reclamation. Correct run `7805dd6d` kept 57 checks across three seeds.
It rejected six delete reservations while M0 leases were live, then issued six
exact permits after release, reclaimed all six unique old-root objects, retired
every permit, and kept three new M1 workers exact.

Three reclamation controls discard:

| Control | Run | Anomalies per seed |
|---|---|---:|
| physically delete despite the live M0 lease | `83a7544a` | 1 |
| reuse the mark epoch from before lease release | `257069b4` | 1 |
| retire the permit before deleting the object | `206a22e2` | 1 |

This is a semantic gate. Its 13.86-second local suite duration includes exact
replay and process startup and is not a serving-latency result.

`[EXISTS]` Candidate `c79e099` updates the adjacent physical-composition suite
to hash `2fb2eb53`. Correct run `3a0e5bfb` kept 30 checks, three dedicated
collector child processes, and three authority failovers. The controller
re-hashes the input and output closures after each collector exits. Omitted SST
`d9baa91e`, semantic-only root `d188aa0a`, and skipped failover `4cadcddd`
discard with exact replay.

The remaining authority subjects are worker-local expiry, renewal
resurrection, incomplete authority snapshot restore, stale authority
generation, and stale delete-mark acceptance. Remote object storage,
concurrent writes, worker restart, and OTel export in a required profile remain
open. The local accepted run registered metrics but did not enable an exporter.

## Authority-bound Range Engine performance curve

`[EXISTS]` `evals/suites/cell-range-serving-performance-curve.toml` measures
the exact RFC-0061 data path in fresh child processes. The suite varies base
size and certified txLog tail length independently, and records view open,
tail authentication, first point, warm point p99, ordered scan throughput,
object request amplification, transferred bytes, and worker RSS.

The cache label is part of the profile identity:

```text
process-cold-os-warm-local-filesystem
```

It means a new Range Engine process and reader over local object files whose
pages may remain in the operating-system cache. It does not mean a GCS cold
miss.

Candidate `1ee9de4`, suite hash `d3e9e9cb`, profile hash `68434458`, and release
executable SHA-256 `3cf310f5` kept all six points:

| Workload | Run | View-open median | Tail-auth median | Scan median |
| --- | --- | ---: | ---: | ---: |
| 1K base, 0 tail | `f127b9a2` | 0.60 ms | 0 ms | 210K rows/s |
| 16K base, 0 tail | `b76d5640` | 0.73 ms | 0 ms | 196K rows/s |
| 64K base, 0 tail | `3915a8d8` | 0.72 ms | 0 ms | 182K rows/s |
| 16K base, 64 tail | `202b5da6` | 4.68 ms | 4.07 ms | 173K rows/s |
| 16K base, 1,024 tail | `ac4f2b46` | 62.82 ms | 62.06 ms | 91K rows/s |
| 64K base, 64 tail | `99d09746` | 4.65 ms | 3.91 ms | 180K rows/s |

All points use three deterministic seeds and one exact semantic replay. They
are a clean release calibration, not a noise-qualified promotion. Performance
admission still requires at least five paired batches under the general rule,
and the RFC calls for 21 fresh processes when fitting the final slopes.

Two bounds became explicit in this baseline. The raw authority-bound reader
makes one object `get_range` per base point read, so local point latency cannot
be extrapolated to GCS. The old scan read `limit + affected_tail_keys` base
rows, so unrelated tail cardinality increased request amplification. Candidates
`7071e33` and `20899e7` address the combined cache path and streaming merge,
respectively. NVMe-only and fully remote cache states remain open.

Candidate `7071e33` extends the same suite to eight workloads and adds
`cache.mode` to every range-serving metric. Suite hash `bc176108`, profile hash
`91f891a1`, and executable SHA-256 `9071fe18` produced the matched clean release
pair:

| Workload | Run | Open | First point | Repeated backend GETs | Scan backend GETs | Scan |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 16K raw | `708b19d4` | 0.51 ms | 129 us | 64 | 80 | 196K rows/s |
| 16K shared cache | `7f628f78` | 2.15 ms | 353 us | 0 | 1 | 248K rows/s |
| 16K + 64 tail raw | `0e5d46ba` | 4.53 ms | 148 us | 64 | 85 | 178K rows/s |
| 16K + 64 tail shared cache | `40a5c041` | 6.10 ms | 330 us | 0 | 1 | 233K rows/s |

The cached workloads hard-fail if the repeated point pass reaches the backend.
The raw workloads hard-fail if they stop observing the expected uncached
request path. This is a combined decoded-RAM and local-block-cache result.
NVMe-only reopen and cache fault controls are not represented yet.

Candidate `20899e7` replaces the enlarged bounded-map scan with two ordered
iterators. One cursor streams visible MVCC rows from the exact authority-bound
base. The other walks the already authenticated resident tail. The merge
suppresses overwritten or deleted base rows, emits tail inserts in order, and
stops at the logical limit.

Suite hash `268beac9`, profile hash `91f891a1`, and release executable SHA-256
`1ff9c635` produced four clean kept points:

| Workload | Run | Open | First point | Scan GETs | Scan |
| --- | --- | ---: | ---: | ---: | ---: |
| 16K raw | `527a947c` | 0.54 ms | 111 us | 80 | 209K rows/s |
| 16K + 1,024 tail raw | `58a99734` | 61.59 ms | 208 us | 80 | 186K rows/s |
| 16K shared cache | `7241c792` | 2.29 ms | 341 us | 1 | 238K rows/s |
| 16K + 64 tail shared cache | `2d02b03a` | 6.13 ms | 347 us | 1 | 236K rows/s |

The raw 1,024-record-tail scan previously used 159 backend range GETs and ran
at 91K rows/s. It now uses the same 80 GETs as the zero-tail raw scan and runs
at 186K rows/s. Raw scans now hard-fail above 96 backend requests; cached scans
hard-fail above four. Tail authentication remains linear at about 61
microseconds per record, and tail memory remains resident. This is local,
OS-warm calibration, not a GCS or noise-qualified result.

Candidate `79afb08` adds two `nvme_reopen` workloads. Each child process first
populates the bounded local block cache, closes the authority view, discards
decoded RAM, reconstructs the cache object from the same directory, and opens
a new view with a fresh decoded cache. Suite hash `c31143ca`, profile hash
`91f891a1`, and release executable SHA-256 `03e1616a` kept both points:

| Workload | Run | Open | First point | Warm p99 | Scan | Scan backend requests |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 16K NVMe reopen | `497b2745` | 0.53 ms | 101 us | 64 us | 262K rows/s | 0 |
| 16K + 64 tail NVMe reopen | `ccc85321` | 4.42 ms | 92 us | 79 us | 237K rows/s | 0 |

The first point transfers zero backend bytes and makes zero successful backend
range GETs on every seed. Cache preparation uses a median 65 range GETs. The
reopen is not backend-independent: view open still makes two successful
manifest GETs, one list, two failed metadata GETs, and transfers 788 bytes.
The first point performs one additional failed metadata GET. The admitted claim
is a persistent NVMe data hit after decoded-RAM loss, not an offline Range
Engine bootstrap.

Candidate `63c9531` adds a focused corruption control. It overwrites every
persisted cache data part, reconstructs the cache and decoded layers, then
permits only failure or the exact value after observed backend range re-fetch.
A wrong value fails the test. The current path repairs from the backend. This
control is deterministic and local. Candidate `505c997` supersedes it with the
process-isolated gate below. Multi-range eviction remains open. The
stale-authority process control is recorded below.

The frozen `cell-range-cache-fault-process-v1` suite exercises overwrite and
torn-file faults through fresh OS processes. A prepare worker builds one real
SlateDB base, opens it through the bounded persistent cache, reads a point and
scan, then exits. The controller mutates every cache part and fsyncs it. A new
worker reconstructs the persistent cache and decoded RAM, opens the
authority-bound view, and may only refuse or return the exact value.

Candidate `505c997`, suite hash `07a33107`, profile hash `bb0068ee`, and
release executable SHA-256 `a74e48ff` produced:

| Workload | Run | Verdict | Median anomalies | Physical result |
| --- | --- | --- | ---: | --- |
| correct | `83a36734` | keep | 0 | 15 overwritten + 15 truncated parts, all exact |
| skip overwrite | `9dc32afc` | discard | 2 | no overwritten parts, exercise gate failed |
| skip torn write | `8863fe8e` | discard | 2 | no truncated parts, exercise gate failed |
| accept wrong after overwrite | `555c59e1` | discard | 1 | unsafe receipt rejected |
| accept wrong after torn write | `dfebb7cd` | discard | 1 | unsafe receipt rejected |

The correct workload executes 24 checks through 12 workers across seeds 1103,
2207, and 3301. Backend repair made 36 successful range GETs and transferred
1,778,994 bytes after cache damage. The two accepted-wrong subjects are oracle
controls. They do not claim that SlateDB actually returned wrong bytes.

The frozen `cell-range-cache-eviction-process-v1` suite then measures physical
capacity, not only configured accounting. Each disposable worker builds eight
logical ranges in one immutable SlateDB base and serves them through one
persistent cache directory. The base holds 64 incompressible 32 KiB values.
The cache is capped at 192 KiB. After scanning every range, the worker closes
the view, creates fresh decoded RAM, and rereads every range in reverse order.

Candidate `5f7bf82`, suite hash `f240110e`, profile hash `e7f6fc24`, and
release executable SHA-256 `9b8556c7` produced:

| Workload | Run | Verdict | Median anomalies | Physical result |
| --- | --- | --- | ---: | --- |
| correct | `9375c874` | keep | 0 | max 131,292 bytes, 130 reread range GETs |
| disable physical bound | `77e7adea` | discard | 2 | 2,105,380 bytes, 0 reread range GETs |
| skip reread | `e92471bc` | discard | 2 | no reread or refill exercised |
| accept wrong value | `3ad2d888` | discard | 1 | exactness gate failed after refill |

The correct run transferred 8,414,900 backend bytes during reverse rereads
across three seeds. Request counts vary with the cache evictor's replacement
choices, so the replay contract hashes dimensions and semantic outcomes rather
than exact refill counts. The accepted-wrong subject is an oracle injection,
not an observed wrong SlateDB value. This is a sequential local-filesystem
gate, not a concurrent fairness or cloud-latency result.

Candidate `f496e8d` adds the same suite's `gcs-dev` profile. Each worker selects
GCS through the standard `object_store` adapter, scopes all database objects to
a unique scratch prefix, and must delete every scratch object before the GCS
cleanup gate passes. Local release regression `2e1ce017` kept under suite hash
`2fb134c2` and release executable SHA-256 `bbafbee0`. The GCS profile is
validated but unexecuted because this session cannot verify the project or
bucket. No cloud latency, request, byte, or cleanup result exists yet.

## Focused concurrent Range Engine publication regression

`[EXISTS]` Candidate `e0f1b12` closes the first same-process publication race.
Sixteen coordinated readers retain an immutable view at `T=5`; the controller
publishes a fully authenticated `T=8` view over the same immutable base; those
readers then complete exact `T=5` scans and later reads complete exact `T=8`
scans. A stale `T=5` publication using the same manifest must fail its full
view-token compare and leave `T=8` current.

This is a focused regression, not a frozen eval lane. It has no process receipt,
load duration, latency distribution, memory curve, failure injection, or OTel
export. The next frozen suite must continuously interleave point and range
reads with repeated authenticated publications and discard at least stale-token,
partial-tail, no-overlap, and mixed-result controls. Required measurements are
publication build and swap latency, read p50/p99, publication rate, live old
view count and bytes, tail records and bytes, cache tier, backend requests and
bytes, anomalies, and telemetry drops.

`[EXISTS]` Candidate `e3866b2` adds the first frozen process lane,
`cell-range-serving-concurrency-process-v1`. Correct run `0aa7c992`, suite hash
`48d82a11`, profile hash `489d83c0`, and release executable SHA-256 `cf469a79`
kept all gates. Across three seeds it started three child processes, performed
18 fully authenticated same-base publications, completed 144 exact retained
old-view reads and 144 exact current-view reads, and observed zero mixed
results. The compare-and-swap section measured 250 ns median and 625 ns p99 in
18 local samples. View construction and authentication occur before that timer.

| Workload | Run | Verdict | Median anomalies | Detected boundary |
| --- | --- | --- | ---: | --- |
| correct | `0aa7c992` | keep | 0 | 18 publications, 288 exact reads |
| accept stale rollback | `fb61c181` | discard | 2 | final target rolled from 9 to 3 |
| skip reader overlap | `20699bf5` | discard | 3 | no retained-old reads |
| accept mixed result | `f6c3a2d8` | discard | 1 | mixed-result oracle |
| skip stale probe | `6778d9f7` | discard | 2 | fence exercise omitted |

This admits coordinated process correctness, not a throughput curve. The
readers are synchronized at each publication, the object store is in-memory,
OTLP export was not enabled, and the receipt does not yet measure view-build
latency, read latency, retained-generation memory, slow-reader pressure, or
worker failure.

Candidate `7eae670` adds the focused stale-root authority control. Publication
state accepts only an exact active snapshot-lease token. Historical raw and
cache-backed Range Engine openers additionally bind the token to the outer
published Range Engine root identity, target version, and closure membership
for both that root and the inner immutable-base manifest before storage
access. Drifted token, expired token, released token, and wrong-root
subjects refuse with typed errors. The released and wrong-root controls
make zero cache or backend requests.

Candidate `e06a159`, suite hash `f1bfd782`, profile hash `40046fbd`, and release
executable SHA-256 `0c81ed42` add that process freshness control to
`cell-range-serving-handoff-v1`. Each seed now starts four disposable Range
Engine workers. M0 warms one persistent cache under an active lease. After M1
publication and M0 lease release, the fourth worker must read live authority
and refuse M0 before storage access. The fixture also compacts M0 into an
independent M1 closure so collection removes one old data object in addition
to the old outer and inner manifests.

Correct run `2b1bdc6a` kept all 60 checks across three seeds, made three
old-root reopen attempts, opened zero, reclaimed all 9 M0-only objects, and
kept the post-GC M1 worker exact. Negative run `93773b96` injected the
pre-release authority snapshot, reopened M0 in all three seeds, and discarded
with exactly one bounded anomaly per seed. This admits fresh-authority
revalidation as part of process handoff. At this point in the evidence
sequence, authority-unreachable behavior, process-isolated corruption, torn
writes, and multi-range eviction were still open. The adjacent controls close
all four.

Candidate `52ca95e`, suite hash `2beb3824`, profile hash `aa483bfe`, and release
executable SHA-256 `58a82868` add one authority-unavailable attempt per seed.
The worker applies a bounded live-authority deadline. Failure persists a
semantic receipt with `live_unavailable`, validates no lease, and opens no
storage. Correct run `805cc0cf` kept 63 checks across 15 workers, with zero
opens from three unavailable-authority attempts. Negative run `1c769733`
enabled stale fallback after the failed live read, reopened M0 in every seed,
and discarded with one bounded anomaly per seed.

This is a correctness and availability-policy result, not a latency admission.
The 50 ms local probe deadline is an eval setting. Production deadline,
backoff, and client-visible error policy still require an operating curve.

## Routed KV Runtime read service

`[EXISTS]` Candidates `6d0cf63` and `6361695` add the first real-TCP point and
single-range scan path above `RangeServingState`. One process-level router owns
two non-overlapping local assignments, validates cell, tenant, range ID,
routing epoch, exact `T`, bounds, scan rows, frames, deadlines, and shared
in-flight capacity, then captures one immutable view for the request.

The focused regression returns an exact point and ordered scan from the left
range and an exact empty point from the right range. It refuses a stale epoch,
returns the left range's split boundary for a crossing scan, and returns
`snapshot_unavailable` for `T` above the applied frontier.

This is not a frozen performance lane. The server and client share one test
process, the protocol opens one TCP connection per request, the object store is
in-memory, and no OTel signals are required. The next suite must use independent
processes, at least two tenants, route refresh at fixed `T`, bounded concurrent
load, saturation, frame, worker-death, and authentication controls. Measure
point and scan p50/p99, throughput, cache state, backend requests and bytes,
retry counts, typed refusals, and telemetry drops.

`[EXISTS]` Candidate `bd9d959` adds frozen suite
`cell-range-read-process-v0`. Correct run `740e7111`, suite hash `64236864`,
profile hash `acef836f`, and release executable SHA-256 `b1bf79ed` kept all
gates:

| Workload | Run | Verdict | Median anomalies | Detected boundary |
| --- | --- | --- | ---: | --- |
| correct | `740e7111` | keep | 0 | 192 points, 48 scans, 3 kills, exact |
| accept stale route | `b0764974` | discard | 1 | prior epoch served |
| accept crossing scan | `2e5d1772` | discard | 1 | assignment boundary widened |
| accept wrong value | `303dd238` | discard | 2 | result oracle |
| skip worker kill | `14ba8d44` | discard | 2 | death exercise omitted |

Release loopback latency with a process-warm in-memory object store was:

| Operation | Samples | p50 | p99 |
| --- | ---: | ---: | ---: |
| exact point, fresh TCP + JSON | 192 | 112 us | 152 us |
| exact single-range scan, fresh TCP + JSON | 48 | 133 us | 231 us |

The scan fixture is small and all requests are sequential. These are not
throughput ceilings. No object miss, shared-cache contention, two-tenant load,
route refresh, replacement worker, TLS, authentication, PostgreSQL executor,
or OTLP export is present.

### Fixed-version route refresh

`[EXISTS]` Candidate `b068256` adds suite
`cell-range-route-refresh-v0`. It begins with one stale route, refreshes to two
ranges split at `k5`, restarts the complete read at the original `T=8`, and
requires a result containing rows from both sides of the split.

| Workload | Run | Verdict | Median anomalies | Detected boundary |
| --- | --- | --- | ---: | --- |
| correct | `7636b6fc` | keep | 0 | 3 refreshes, 21/21 rows, fixed `T=8` |
| keep stale map | `f971d9b2` | discard | 4 | refresh made no progress |
| change snapshot version | `ea34833e` | discard | 3 | retry changed `T=8` to `T=9` |
| skip second range | `f1bc9a90` | discard | 2 | partial fan-out omitted routed rows |

Suite hash is `d74f6e19`, profile hash is `8dc66b3f`, and release executable
SHA-256 is `d30fb2fe`. The correct run starts and kills three independent KV
Runtime workers and uses its full 18-operation budget. The RangeMap refresh
source remains in the controller process, both ranges share one endpoint, and
no concurrent publication, replacement worker, tenant authentication, remote
object miss, or OTLP exporter is present.

### PostgreSQL page-read bridge

`[EXISTS]` Candidate `8fb20e5` adds suite
`postgres-page-read-process-v0`. One independent worker serves three encoded
8 KiB pages across two ranges. The authority base covers objectKV version 1,
and a certified txLog record advances block 8 at version 2. The client refreshes
one stale map and reads all pages at unchanged version 2.

| Workload | Run | Verdict | Median anomalies | Detected boundary |
| --- | --- | --- | ---: | --- |
| correct | `977b368d` | keep | 0 | 3 refreshes, 9/9 pages, fixed version 2 |
| missing page | `7256f045` | discard | 3 | absent block 8 |
| corrupt payload | `d8d0a2a5` | discard | 3 | SHA-256 mismatch |
| change objectKV version | `3332607a` | discard | 3 | retry reads version 1 instead of 2 |
| page LSN ahead | `7dd9189d` | discard | 2 | page LSN 900 exceeds frontier 800 |

Suite hash is `857c3b12`, profile hash is `659609ee`, and release executable
SHA-256 is `1a458df1`. Three direct release samples measured the process-warm
8 KiB point at 247 to 277 microseconds and the three-page stale-route refresh
plus two-range scan at 0.83 to 1.03 milliseconds. These are local in-memory
object-store and fresh TCP plus JSON results, not PostgreSQL, remote-object, or
concurrent-load performance claims.

`[EXISTS]` Candidate `b04b128` adds a manual pinned-fork gate above this frozen
adapter suite. PostgreSQL 18.6 reads one real 148-page heap through
`smgr_startreadv` and the objectKV page service after a restart clears shared
buffers. The cold debug scan returns 2,000 rows and `sum(id)=2001000` in
233.045 ms. The immediate shared-buffer repeat takes 0.299 ms. Unavailable
service and changed-frontier controls both refuse.

This result is not yet a frozen `okv-eval` workload. The external PostgreSQL
source build, patch identity, cluster initialization, relation import, server
restart, callback log, and poison controls must become one reproducible runner
before it can promote `postgres-page-bridge-v0` from `proposed`. Until then,
the patch SHA-256 `910aef1e` and full reproduction record in
`experiments/postgres-smgr-read-probe/` are evidence, not an automated release
gate.

### PostgreSQL page-write admission

`[EXISTS]` Candidate `c3c5df9` adds suite
`postgres-page-write-gate-v0`. It is the semantic effect-boundary gate below a
future PostgreSQL callback. A correct two-page batch carries expected objectKV
version 41, PostgreSQL WAL flush LSN 900, page LSNs 899 and 900, and a stable
request identity. Admission emits deterministic `CellMutation::Set` values and
a domain-separated mutation digest.

| Workload | Run | Verdict | Median anomalies | Detected boundary |
| --- | --- | --- | ---: | --- |
| correct | `0bf18a75` | keep | 0 | 3 batches, 6 mutations, exact replay |
| WAL behind page | `118ba54b` | discard | 4 | flush LSN 899 cannot admit page LSN 900 |
| zero objectKV version | `ee71a5b4` | discard | 4 | absent expected view |
| oversized batch | `b14da383` | discard | 4 | 129 pages exceeds the 128-page bound |
| wrong mutation digest | `c74e05ad` | discard | 1 | admitted bytes do not match the receipt digest |

Suite hash is `0fff8f62`, profile hash is `1594da41`, and release executable
SHA-256 is `5a5e946f`. The correct run uses its exact 15-operation budget. This
suite does not invoke PostgreSQL, commit through the objectKV transaction
system, maintain relation extent, survive restart, publish a stable root, use a
remote object store, or export OTLP. Those remain independent gates.

### PostgreSQL page plus extent Cell commit

`[EXISTS]` Candidate `7de5c4e` adds suite
`postgres-page-commit-process-v0`. Each seed starts the real Cell v0 fixture,
prepares two WAL-admitted pages against its latest objectKV version, and extends
one relation from zero to two blocks. Page values and the versioned extent key
enter one Cell transaction. The same encoded command is retried, the current
leader is killed, and the successor's linearizable state must retain both pages
plus `nblocks=2` at the unchanged committed version.

| Workload | Run | Verdict | Median anomalies | Detected boundary |
| --- | --- | --- | ---: | --- |
| correct | `bb7e18fa` | keep | 0 | 12 starts, 3 handoffs, 6 pages, 3 extents, 3 exact retries |
| omit extent mutation | `5816809e` | discard | 3 | pages exist without authoritative block count |
| change retry identity | `247a6cdb` | discard | 1 | unknown-outcome retry does not return the original response |
| wrong receipt identity | `68282231` | discard | 1 | response identity differs from the planned command |
| non-advancing commit version | `71d18d48` | discard | 1 | receipt does not advance beyond its read version |

Suite hash is `0f3a3a8b`, profile hash is `7e8a0b61`, and release executable
SHA-256 is `9bdb2235`. Correct uses its exact 24-operation budget. The Cell
commit version advances from the prior logical version but is not required to
be adjacent because it is a Raft log position. This suite does not invoke the
PostgreSQL write callback, run the independent tagged txLog protocol, rebuild a
Range Engine from the committed envelope, publish a stable object root, use a
remote object store, or export OTLP.

### PostgreSQL mutable callback and local sidecar recovery

`[EXISTS]` Candidate `3bb2783` composes the external PostgreSQL 18.6 callback
with the Cell, an immutable SlateDB object base, two required signed txLog sets,
and fresh Range Engine construction. The durable proof uses one 148-block,
1,212,416-byte relation. Each txLog set has three independent local processes
and requires two matching durable records and attestations.

| Subject | Verdict | Detected boundary |
| --- | --- | --- |
| source-independent service restart | keep | recovered version 10 from exact base plus two certified tail records while the configured source heap did not exist |
| post-recovery page write | keep | accepted a new PostgreSQL checkpoint through version 11 and returned the changed row |
| second service restart | keep | recovered version 12 with four authenticated tail records and returned both post-base changes |
| missing required txLog quorum | discard | one retained historical node in set 10 cannot establish a unique durable history |
| missing live base SST | discard | exact physical-closure verification refuses startup before serving |
| checkpointer stable publication | keep | version 13 selected at authority term 3, index 4 before PostgreSQL sync completed |
| stable-root service reconciliation | keep | replacement page service recovered version 13 without source heap and verified the same root before serving |
| publication authority unavailable | discard | hot state reached version 14, PostgreSQL checkpoint failed, stable version 13 remained unchanged |

The local heap retained SHA-256
`3770217fa7ca29da2d79580fa5fd68616a9257d6460801f0a1ade6cfc078d7e8`.
The first durable checkpoint took 465.720 ms in a debug build; the first
post-recovery checkpoint took 561.758 ms. These times contain synchronous Cell
and txLog process work plus full Range Engine reconstruction and are shape
evidence only.

The predecessor authority-published checkpoint took 829 ms end to end, split
into 669 ms of page-write work and 160 ms in PostgreSQL's sync phase. The next
proof now materializes a complete relation base, publishes it, and pops both
required txLog sets. Its three-page debug checkpoint took 1.980 seconds; the
sync phase took 703 ms. A no-new-page full rewrite took 440 ms, with 435 ms in
sync, and a one-dirty-page checkpoint took 810 ms, with 448 ms in sync. The
authority-outage control failed after the five-second prototype socket timeout.

The first asynchronous candidate, `54d2510`, permitted stable target `B` to
publish from older object base `O` plus a complete certified suffix `(O, B]`.
It passed source-free restart and capped txLog pop at `O`, but was rejected as a
performance shape. Page writes still scheduled full relation bases, and a
publication-authority timeout inflated one background materialization from
about 400 ms to 6.4 seconds.

Candidate `171b14c` captures owned objectification inputs only at checkpoint
sync and performs relation scan plus materialization without reacquiring the
bridge-state mutex. The fresh proof produced:

| Subject | Verdict | Observed boundary |
| --- | --- | --- |
| first lagging-base stable root | keep | published `B=9/O=5`, popped only through 5, materialized base 9 in 90 ms |
| later ready-base activation | keep | activated base 9, published `B=10/O=9`, popped through 9, materialized base 10 in 75 ms |
| source-free replacement service | keep | recovered `B=10` from base 9 plus one certified record while the source path did not exist |
| publication authority unavailable | discard | hot state reached 11, stable stayed 10, pop stayed 9, checkpoint failed after 6.044 seconds |
| objectifier authority independence | keep | captured base 11 completed in 26 ms during the 6.044-second stable timeout |
| page writes without stable capture | keep | three page-write commits advanced hot state to 14 and created zero new bases |

The last control did not reach the stable callback because PostgreSQL reported
an out-of-extent page read, so it is evidence only for the absence of a
page-write objectification trigger. It is not a successful checkpoint or
multi-page PostgreSQL compatibility result.

The metric registry now separates checkpoint duration, stable-sync duration,
stable object frontier, hot-to-stable lag, objectification duration, ready
version, objectification lag, objectified bytes, publication revision, the
minimum popped-through version, and retained txLog bytes. These metrics are
frozen for the future runner; this manual result did not export them through
OTLP.

`evals/suites/postgres-smgr-write-process.toml` now freezes the durability and
two negative-control gates. `[ACTIVE-WORK]` The literal external PostgreSQL
orchestration is still manual and therefore does not yet emit a schema-validated
`okv-eval` result or OTLP signals. Promotion requires one runner that pins and
verifies PostgreSQL source, applies the patch, builds the fork, creates the
relation, executes both restarts and controls, records exact process and binary
identity, and cleans only its isolated scratch roots.

This result does not recover either authority, objectify incrementally,
aggregate a database-wide root, clear OS/NVMe caches, survive host loss, use a
remote object store, repair txLogs, rotate log policy, move stable publication
I/O outside the state lock, or run PostgreSQL regression tests.

## Incremental PostgreSQL object-delta economics baseline

`[EXISTS]` Candidate `fc88122` runs the schema-valid
`postgres-object-delta-v0` suite over five high-entropy seeds. One changed 8 KiB
page is objectified above a 128-page, 1 MiB immutable base. The correct append
and source-free restart subjects keep. Missing object, corrupt object, broken
chain, omitted closure, pop ahead, and replacement full-base subjects all
discard with exact deterministic replay.

The optimized local result is:

| Measure | p50 or ratio | Read |
| --- | ---: | --- |
| delta bytes / changed page | 11.106x | rejects JSON delta v1 as production format |
| delta bytes / full rewrite | 8.31% | validates the incremental economic direction |
| delta materialization | 11.08 ms | 1.08 ms above the proposed local target |
| delta activation | 12.05 ms | below the 25 ms target |
| source-free reopen | 9.77 ms | below the 50 ms target |
| materialization + activation | 23.13 ms | no end-to-end win over 20.31 ms full rewrite at 1 MiB |

The architecture remains admitted, while the v1 payload encoding is discarded.
The next decision-bearing runs vary relation size, changed-page batch size,
delta-layer count, cache state, and remote object storage. Full evidence and
limits are in
[`research/postgres-object-delta-baseline-2026-08-24.md`](research/postgres-object-delta-baseline-2026-08-24.md).

This run used the runner's normal OTel measurement path, but no local collector
was configured, so `telemetry.enabled=false`. The compact schema-valid JSON
receipts and content hashes are recorded in `experiments/ledger.jsonl`.

## PostgreSQL object-delta relation-size crossover

`[EXISTS]` Candidate `efa9d54` runs the five-seed release crossover at 2, 128,
4,096, and 65,536 relation pages while holding one changed 8 KiB page and its
certified suffix constant. The complete delta identity is byte-identical within
each seed across all four sizes. Correct subjects pass every hard gate. The
full-base-in-candidate-root control discards.

| Relation | Delta / rewrite bytes | Delta / rewrite time | Restart proof |
| --- | ---: | ---: | ---: |
| 16 KiB | 336.07% | 1.788x | 1.88 ms |
| 1 MiB | 8.308% | 1.134x | 10.11 ms |
| 32 MiB | 0.2622% | 0.3359x | 263.43 ms |
| 512 MiB | 0.01639% | 0.2502x | 4.549 s |

The architecture clears every frozen calibration target and has a measured
latency crossover between 1 MiB and 32 MiB. JSON v1 remains discarded at
11.106x changed bytes. The restart proof includes a complete snapshot scan and
must not be reported as worker-ready latency. Full evidence and limits are in
[`research/postgres-object-delta-crossover-2026-08-24.md`](research/postgres-object-delta-crossover-2026-08-24.md).

## PostgreSQL replacement-worker readiness curve

`[EXISTS]` Contract commit `f73e201` froze the phase split before candidate
`e2c9dd5` added the process-isolated measurement seam. The production helper
still audits every live SST before returning. The experimental worker instead
authenticates the selected root, manifest identity, and complete delta lineage,
opens the immutable view, measures exact first reads, performs a bounded full
oracle, and then completes the physical-closure audit.

| Physical closure | View ready p50 | First base point p50 | First 8-page range p50 | Full oracle p50 | Closure audit p50 | Worker RSS p50 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1.09 MB | 2.33 ms | 0.181 ms | 0.570 ms | 8.88 ms | 2.12 ms | 20.0 MiB |
| 34.69 MB | 2.51 ms | 0.156 ms | 0.578 ms | 287.48 ms | 65.16 ms | 67.4 MiB |
| 555.04 MB | 4.75 ms | 0.142 ms | 0.621 ms | 4.493 s | 1.046 s | 61.9 MiB |

Every exact-read, root-identity, delta-lineage, bounded-memory, and closure
gate passed across five deterministic seeds. The 512 MiB relation met the
frozen 100 ms readiness target by 21.1x, and readiness grew 2.04x while the
physical closure grew about 511x. Changed-manifest run `04c43b92`,
changed-delta run `c5002987`, and skipped-audit run `f593ff30` all discarded.

This keeps the architectural performance shape, not the production serving
policy. The local filesystem was OS-warm, no OTel collector was configured,
and no provider checksum or remote object latency participated. Full evidence
and limits are in
[`research/postgres-replacement-worker-readiness-2026-08-24.md`](research/postgres-replacement-worker-readiness-2026-08-24.md).

## Provider-bound GCS cache-state gate

`[EXISTS]` RFC 0066 and suite `provider-bound-range-read-v0` freeze one
provider-identity and cache-state contract for local versioned storage and GCS.
The authority-selected root binds the provider namespace, exact revision,
length, and application SHA-256 of the manifest and every live SST. Every
touched read applies that exact revision. Six changed-generation, same-bytes
new-generation, missing-revision, changed-bytes, changed-namespace, and
unversioned-fallback controls must discard.

Candidate `ae515ec` established the five-seed local release baseline. Empty
cache reached the first exact point in 0.339 ms p50 after eight
revision-checked GETs and 380,519 bytes. Persistent NVMe reached the first
point in 83.8 us p50, and 1,000 measured working-set reads issued zero provider
GETs after fill. This is an OS-warm local result, not a cloud claim.

`[EXISTS]` Candidate `be78904` replaces the GCS workload discard with the real
process worker. Each child receives a validated
`scratch/provider-bound-range/` prefix, writes and reads through Apache
`object_store`, binds GCS generations into the selected closure, and deletes
every live object before returning. The controller repeats cleanup after
worker failure or timeout. `gcs-dev` requires OTel metrics, traces, and logs,
and computes Class B request cost under snapshot
`gcs-us-central1-standard-2026-08-24` at $0.0004 per 1,000 operations.

`[EXISTS]` Candidate `257fe2a` ran the frozen profile on an ephemeral
`us-central1-a` `n2-standard-8` runner. Every correct state kept across five
seeds. Empty-cache first point was 48.6 ms median and 53.4 ms maximum with one
64 KiB data GET. Metadata-warm but data-cold was 40.8 ms median. Persistent
NVMe was 294.5 us median with zero first-point provider reads. Warm p99 medians
were 245 to 284 us. All six identity controls discarded, and OTel exported
metrics, traces, and logs.

The final live listing was empty, but versioning and soft delete retained 218
deleted or noncurrent generations totaling 1,464,840,385 bytes. Zero live
objects is not zero retained cost. The next curves vary reuse distance, cache
capacity, concurrency, worker churn, and dataset size. Stop or redesign if
realistic cache hit rate cannot reach the declared request-cost target or if
remote misses dominate the OLTP latency distribution.

## Provider-bound cache-economics gate

`[EXISTS]` RFC 0067 and suite `provider-bound-cache-economics-v0` implement the
next economic falsifier. The suite measures exact
provider-bound point reads over uniform, Zipfian `0.99`, and moving-hotset
traces while persistent NVMe is fixed at 1, 5, 10, or 25 percent of the 32 MiB
logical dataset. Decoded RAM is fixed separately. One workload reopens the
immutable view every 1,000 reads with fresh decoded RAM and retained persistent
cache.

The primary metric is provider miss ratio. The frozen request-cost target is
at most `0.025`, equivalent to at least `0.975` provider hit ratio and $0.01
per million logical reads under the pinned one-GET-per-miss price model. This
economic constraint may discard a semantically correct result. Discarded
curves remain evidence and must be recorded.

Every receipt must classify each measured point as a hit or provider miss,
record tier-specific latency, provider requests and bytes, settled cache bytes,
reuse distance, requested view reopens, peak RSS, and calculated request cost.
The classified counts must equal logical reads. Four unbounded-cache,
skipped-oracle, skipped-revision, and perturbed-replay controls must discard.

The first local profile uses 20,000 measured reads after 2,000 warmups for five
fixed seeds. The GCS profile uses 2,000 measured reads after 200 warmups. The
first representative cloud points are the 10 percent Zipfian workload, the 25
percent moving-hotset workload, and the decoded-view churn workload. Full
replacement-process churn, concurrent tenants, mixed scans, and sustained
writes remain outside this contract.

`[EXISTS]` The first five-seed local stop points discard passive demand caching
as the complete serving policy. With persistent NVMe equal to 25 percent of
logical data, Zipfian `0.99` missed 26.820 percent of reads and the moving
10-percent hotset missed 14.535 percent. Their projected GCS request costs were
$0.11566 and $0.06226 per million logical reads, against the frozen $0.01
target. Every semantic, provider-identity, physical-bound, trace-replay, RSS,
and cleanup gate passed. All four controls produced schema-valid discards.

`[EXISTS]` Lane constraints are now executed, not only schema-validated. Each
constraint emits a named hard gate with the observed statistic, operator, and
target. A semantically correct workload that misses its economic ceiling is a
`discard`, as required by the research program.

## Provider-bound locality-feasibility gate

`[EXISTS]` RFC 0068, suite `provider-bound-locality-feasibility-v0`, and
candidate `d64a14f` implement a preflight before another physical cache or
prefetch candidate. The gate computes the greatest access probability mass
any capacity-respecting ideal placement can cover, then compares its
irreducible provider miss ratio to the declared target.

At 25 percent local coverage, the RFC 0067 Zipfian `0.99` distribution has an
ideal hit ceiling of `0.838299212912`, so at least `0.161700787088` of reads
miss. The moving 10-percent hotset with 10-percent uniform background has an
ideal hit ceiling of `0.925`, so at least `0.075` of reads miss. Both workload,
capacity, and 2.5-percent-target combinations are infeasible before mechanism
overhead.

The gate does not replace a physical eval. It prevents the autonomous loop
from tuning a mechanism against an impossible target. A feasible pair must
still pass provider identity, request, byte, latency, memory, deterministic
replay, and cleanup gates. Capacity inflation, skipped normalization, and
ignored background reads each produced a schema-valid discard.

## Assigned-range placement gate

`[ACTIVE-WORK]` RFC 0069 and suite
`provider-bound-assigned-range-placement-v0` freeze the first physical test of
the explicit-locality direction. One 32 MiB high-entropy database is divided
into 1, 4, or 16 logical ranges. One range is assigned, hydrated, verified, and
published through a root-specific `placed-ready` receipt.

The primary metric is placed local bytes divided by visible logical bytes in
the assignment. The frozen ceiling is `1.50x`. Hydration may read at most
`2.00x` assigned logical bytes. After readiness, a fresh decoded-RAM view must
open, exhaustively point-read, scan, survive unrelated-range pressure, and
reopen from retained NVMe with zero provider requests and bytes. Post-ready
point p99 must remain at or below 1 ms.

The first implementation measures the current one-database, many-prefixes
SlateDB layout through direct range hydration. This is an incumbent test, not
an assumption that cached blocks form a complete or pinned range image. If it
discards, the first orthogonal candidate is a separate derived range-local
image under the same frozen oracle, workload, and byte thresholds.

The suite also exercises a 64-record authenticated overlay and one root
advance. Premature readiness, stale receipt reuse, corrupted local state, and
provider fallback after readiness must each produce a schema-valid discard.
No GCS performance claim is allowed until one local subject passes readiness,
exactness, post-ready provider isolation, and cleanup.

## Noise and effect rule

1. Run a candidate at least five times before promotion in a performance lane.
2. Report median, median absolute deviation, minimum, and maximum.
3. Mark the result `inconclusive` when the candidate improvement does not clear
   both the declared practical threshold and observed run-to-run noise.
4. Re-run the incumbent in the same batch to detect environment drift.
5. Use complexity as the tiebreaker. Equal performance with less code wins.

## Result artifacts

Every run emits one JSON object conforming to
`evals/schema/result.schema.json`. Large raw logs live outside the repository.
The compact verdict is appended to `experiments/ledger.jsonl` without rewriting
older rows.

Required identity:

- candidate and parent commit;
- suite and contract hash over the suite, metric registry, and result schema;
- machine/profile and backend;
- Rust and dependency lockfile identity;
- seed set;
- budget and elapsed work;
- all hard-gate outcomes;
- primary metric distribution;
- verdict and reason.

## Admission gate for autonomous optimization

Do not begin an open-ended autoresearch run until:

1. the baseline command exists;
2. the candidate edit surface is declared;
3. the oracle and suite are frozen;
4. the result validates against the schema;
5. the incumbent can reproduce within its declared noise band;
6. one intentionally broken candidate fails the correctness gate;
7. one intentionally slower candidate does not become champion.

See `docs/TELEMETRY.md` for signal roles, metric extension, and the local
collector path.
