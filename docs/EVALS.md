# objectKV eval system

Status: `[ACTIVE-WORK]` the configurable runner, metric registry, schema-checked
smoke execution, OTel logs, metrics, and traces export, and proposed Phase 0,
serving-model, and fault-recovery suites exist. Phase 0 workload executors,
object-store implementations, replicated-WAL fault injection, and PostgreSQL
oracles remain proposed. The first generation-fencing fault workload runs
through `okv-sim` and records its anomaly count, event budget, trace digest, and
exact-replay gate through the shared result and OTel path.

## Executable configuration

- `evals/metrics.toml` owns instruments, units, histogram boundaries, attributes,
  and cardinality limits.
- Each suite owns profiles, workloads, lanes, practical thresholds, constraints,
  and telemetry requirements.
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
| E7 | HTAP version alignment | columnar coverage version plus OLTP delta checks |
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
| `htap-freshness` | covered-through lag | exact results with no missed or double-applied delta |
| `generation-recovery` | anomaly count under exact seeded replay | zero acknowledged loss, version reuse, or stale publication |
| `object-brownout` | p99 objectification lag | bounded retained WAL and zero acknowledged loss |
| `commit-failover` | durable commit p99 | zero acknowledged loss across leader, disk, and lost-ack faults |
| `takeover` | time to first correct read | stale owner fenced and no durable dataset copy |
| `gc-snapshot-race` | anomaly count | no reachable object reclaimed under snapshot, branch, backup, or CDC interleavings |

`generation-recovery` has one executable bootstrap probe. The other fault lanes
remain configuration contracts until their owning components exist.

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
