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
logical value remains exact. The suite records backend calls and byte ranges,
but does not yet assign a cloud-price or compaction-cost ceiling.

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
