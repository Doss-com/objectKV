# objectKV real-infrastructure evaluation program

Status: `[VERIFIED]` for the private runner, named local NVMe, regional GCS,
required OTel, machine receipt, schema-valid paired-comparison mechanism, and
GP2.5.3 physical provider-media-loss reconstruction. `[VERIFIED]` GP3.1 and
GP3.1.1 admit the topology-matched native resident read boundary through 32
clients. `[EVALUATING]` cache pressure, same-durability replicated commit, and
the complete transaction plane. No full-cell or cross-stack performance claim
is `[VERIFIED]`.

## The answer we are trying to earn

The native transaction plane keeps ownership only if the same committed
history produces a material win on at least one object-native curve without
becoming structurally noncompetitive on quorum commit, hot point read, resource
use, and recovery. A rejected native mechanism changes the implementation, not
the objectKV program.
There is no single valid "faster than TiKV" number. Each percentage must name
the operation, consistency, durability, topology, cache state, dataset, and
cost boundary it compares.

```text
frozen history + matched environment
  -> candidate run
  -> control run
  -> result-schema validation
  -> identity + hard-gate + noise checks
  -> comparison receipt
  -> better | worse | inconclusive | invalid
```

`invalid` is a useful result. It means the harness prevented an unsupported
performance claim.

The fail-closed evidence-class and workload-envelope rules are defined in
[`docs/EVAL-WORKLOAD-CONTRACT.md`](EVAL-WORKLOAD-CONTRACT.md). New performance
and economics comparisons require both receipts to identify as `workload` and
carry a validated workload-profile hash. A `smoke` result can prove the runner
and infrastructure path, but cannot admit a curve.

## Comparison lanes

| Lane | objectKV subject | Required control | Claim scope | Primary curve |
|---|---|---|---|---|
| Hot point read | admitted RAM or SSD ServingWorker | direct RocksDB on the same machine | mechanism | p50, p99, throughput, CPU/op, bytes/op |
| Quorum commit | replicated-NVMe txLog plus async objectification | TiKV or an equivalent 3-voter RocksDB/Raft stack | solution | commit p50/p99, tx/s, aborts, bytes/commit |
| Cold point read | manifest plus index plus selected object block | full restore and direct indexed-object reader | mechanism | first-correct-read p99, GETs, bytes, index RAM |
| Empty-worker recovery | object base plus retained txLog suffix | full local restore, then a matched incumbent restore | mechanism, then solution | RTO, bytes restored, suffix work |
| Branch | reused immutable closure plus divergent suffix | physical snapshot copy | lifecycle | create latency and new durable bytes |
| Exact HTAP | DataFusion columnar base plus exact tail at `T` | DataFusion base-only and a separate ETL stack | mechanism, then solution | exact-query latency, scan throughput, tail overhead |
| Economics | admitted objectKV deployment | matched TiKV tiered deployment | solution | cost per 1M operations and retained TiB-month |

The economics lane is intentionally closed while
`price_snapshot = "pending-reviewed-snapshot"`. Its hard gate is false, so a
candidate cannot become cheaper by running against an unstated price model.

Direct RocksDB is not a durability-equivalent solution control. It answers how
much the objectKV serving wrapper costs after admission. TiKV is the later
solution control for a replicated transaction path. Keeping those claims
separate prevents the mechanism benchmark from being presented as a system
benchmark.

## Stable percentage rule

For a metric where higher is better:

```text
directional delta = (candidate - control) / abs(control)
```

For a metric where lower is better:

```text
directional delta = (control - candidate) / abs(control)
```

A positive percentage is better. A negative percentage is worse. The receipt
is decisive only when its absolute effect clears both the lane's practical
threshold and the larger of candidate/control `MAD / abs(median)`. Performance
receipts require at least five samples per subject.

Before calculating the percentage, `okv-eval compare-results` requires:

- workload-class candidate and control receipts with workload-profile hashes;
- the exact program gate and declared control;
- matching primary metric, statistic, direction, and unit;
- matching machine, Rust toolchain, lockfile, source revision, and seeds;
- one explicit matching batch ID so environment drift is bounded in time;
- a schema-valid machine receipt whose SHA-256 digest becomes the result's
  machine identity;
- the expected suite, profile, lane, and backend identity for both subjects;
- matching suite and profile hashes wherever the program declares them equal;
- every hard gate to pass and neither run to be discarded or crashed.

Each lane may also declare cross-result constraints over secondary metrics.
The GP3.1 lane maps `single_range.hot_read_p99_ns.median` to
`resident.latency_ns.p99`, declares lower as better, and permits at most a 0.20
regression fraction. Missing, non-finite, or zero-denominator inputs invalidate
the comparison. A failed constraint produces a `worse` verdict even when the
primary throughput result is inside its practical envelope. Result suite hashes
are checked against the current program plan, so changing a constraint makes an
older receipt ineligible for a new claim.

Use:

```bash
okv-eval compare-results \
  evals/programs/objectkv-golden-path-v1.toml \
  --gate GP3.1 \
  --candidate /path/to/objectkv.json \
  --control /path/to/rocksdb.json \
  --output /path/to/g3.1-comparison.json
```

Candidate and control runs receive the same explicit `--batch-id`. A run without
one uses its own run ID and therefore cannot compare against another run.

The program pins `evals/schema/comparison.schema.json`. A caller cannot silently
substitute a looser comparison schema.

## Infrastructure ladder

### R0, one stable runner

`[VERIFIED]` Terraform provisioned one private `n2-standard-8` runner with a
200 GiB `pd-ssd`, plus a separate private OTel collector. Remote state is
isolated from the project/bucket foundation. `create=false` remains the default.
The first bounded smoke completed in 6.925 seconds with 24 passing hard gates,
13 GCS objects, 852,280 durable bytes, and all three required OTel signals. The
receipt and failures are recorded in
`docs/artifacts/eval-receipts/gcp-r0-smoke-2026-08-27/README.md`.

The smoke is not a performance result. Its source identity is explicitly dirty,
it has one sample per subject, and its thresholds are diagnostic. It verifies
the infrastructure path and exposes the next work in the smallest useful unit.

`[VERIFIED]` The next R0 run bound the serving image to a receipt-observed 375
GiB local NVMe device, used a clean frozen source snapshot, captured all three
OTel signals, and produced 15 candidate plus 15 control samples. The public
`SingleRange` path reached 516,973 reads/s median and 2.482 microseconds median
p99. Direct RocksDB reached 702,142 reads/s and 1.749 microseconds. Both paths
had zero incorrect reads and zero object operations in their measured hot
windows. The machine and mechanism are verified; the `worse` comparison keeps
GP3.1 `[EVALUATING]`. See
`docs/artifacts/eval-receipts/single-range-ssd-gcp-r0-2026-08-27/README.md`.

`[VERIFIED]` A second R0 execution tested one focused optimization and reversed
the subject order in a second batch. When a complete serving image is active,
`SingleRange` now avoids per-read manifest location and object-reference
cloning. Candidate throughput increased by 11.18 percent versus the prior
clean candidate. The AB pair measured 575,498 versus 713,304 reads/s and 2,490
versus 1,841 ns p99. The BA pair measured 573,999 versus 717,362 reads/s and
2,427 versus 1,867 ns p99. All comparability checks and mechanism gates passed,
but both executable p99 constraints failed. GP3.1 therefore remains
`[EVALUATING]`, and the predeclared stop condition ends incremental wrapper
optimization. The next bounded experiment materializes the suffix into a
native resident-engine data plane before deciding whether that data plane must
move to TiKV or FoundationDB. See
`docs/artifacts/eval-receipts/single-range-ssd-gcp-r1-2026-08-27/README.md`.

`[VERIFIED]` A third R0 execution corrected candidate and control ownership
semantics, implemented the native resident-engine boundary, and executed the
frozen comparison in both process orders. A diagnostic run showed the existing
wrapper at 0.8819x throughput and 1.092x p99 against the owned-value control,
invalidating the earlier pinned-slice comparison as a clean wrapper-cost
measurement. The final native AB pair measured 589,717 versus 701,119 reads/s
and 2,226 versus 1,839 ns p99. The BA pair measured 587,199 versus 710,184
reads/s and 2,261 versus 1,778 ns p99. All correctness, identity, byte, and OTel
checks passed. Both throughput constraints passed; both p99 constraints failed.
That unmatched result triggered D52. D56 supersedes its permanent pivot because
the control did not pay for the candidate's recovered six-process topology. See
`docs/artifacts/eval-receipts/single-range-native-resident-gcp-r2-2026-08-27/README.md`.

`[VERIFIED]` A fourth R0 execution put native and direct owned-value RocksDB
inside the same recovered six-authority-process topology. Native retained
0.9089x and 0.9197x control throughput in opposite process orders; p99 was
0.9134x and 0.9132x control. All four results passed 64 total hard-gate
evaluations and emitted correlated OTel logs, metrics, and traces. GP3.1 admits
the single-range native read boundary, not replicated commit or a complete
cell. See
`docs/artifacts/eval-receipts/single-range-native-matched-gcp-r0-2026-08-27/README.md`.

`[VERIFIED]` A fifth R0 execution held the same topology and changed only
concurrent read clients. At 8 clients, native retained 0.8798x and 0.8734x
control throughput; p99 was 1.1842x and 1.1220x. At 32 clients, throughput was
0.8803x and 0.8906x control; p99 was 1.1072x and 1.1478x. All four explicit
comparison pairs passed. The eight results contain 120 samples, 24,000,000
measured reads, 128 passing workload gates, zero measured object operations,
and complete OTel correlation. All 384 current scratch objects and all nine
leased resources were removed after evidence capture. See
`docs/artifacts/eval-receipts/single-range-native-concurrency-gcp-r0-2026-08-27/README.md`.

The r0 profile uses a 4 MiB working set to isolate public-path software cost.
It does not measure cache pressure. A 64 MiB attempt was stopped after more
than five minutes because fixture construction serialized 65,536 records
through the local authority before object publication. Reusable frozen fixtures
are required before the 64 MiB and 10 GiB scale points return.

`[VERIFIED]` The incumbent-plane R0 runner executed GP2.5.1 semantic elimination
and GP2.5.2 logical lifecycle. FoundationDB 7.4.6 rejected the frozen
write-skew history and passed all five implemented semantic gates. TiKV 8.5.7
committed both disjoint writers, which matches its snapshot-isolation contract
but fails objectKV P1. TiKV therefore does not advance to lifecycle work.

The frozen FoundationDB plus GCS lifecycle probe produced a 205,262-byte exact
closure for 950 current rows, verified its closure and manifest by named GCS
generation and SHA-256, restored the state into an empty logical generation in
five idempotent chunks, matched the state digest, and fenced a transaction that
began under the old generation. The formal positive path took 512.560 ms inside
the lifecycle probe and 840.212 ms end to end. Three poisons covering missing
retained change, missing durable outcome, and missing generation dependency
were discarded. All seven semantic and lifecycle run IDs occur in the captured
OTel logs, metrics, and traces. These are correctness receipts, not performance
claims.

`[VERIFIED]` GP2.5.3 executed that two-cluster media-loss contract. Candidate
`50c72159781e14d3db06d792beac34838572fc91` wrote a 950-record, 205,248-byte
closure plus a 607-byte manifest to named GCS generations. Terraform deleted
the source VM, boot disk, and 100 GiB provider SSD; the controller observed all
three absent before restore began. A fresh FoundationDB cluster with distinct
cluster, instance, boot-disk, and data-disk identities restored five chunks,
replayed them idempotently, matched the exact source digest and row count,
activated after its ready marker, and accepted one fresh commit. The formal
positive received `keep` with 16 passing gates and zero anomalies. The
same-cluster hidden-media control received `discard`. Both run IDs occur in
captured OTel logs, metrics, and traces. See
`docs/artifacts/eval-receipts/provider-media-loss-r0-2026-08-27/README.md`.

`[VERIFIED]` GP2.5.4's local-process rung separately gates external
provider-incarnation authority. Candidate `b415d502665eff9b6df4c095e33480b628348db2`
kept the zero-anomaly positive and discarded a three-anomaly stale commit,
route, and publication poison with complete OTel signals. `[CODE-COMPLETE]`
the next real-infrastructure rung creates source and destination FoundationDB
providers simultaneously, installs the source fence, activates the exact
destination, restarts the source with unchanged disk identities, and formally
checks the resurrected adapter. Phase order is bound by exact receipt digests,
not cross-VM clock comparison. `[EVALUATING]` that leased run. GP3.1 compares
mandatory retained-write overhead against direct FoundationDB only after this
correctness gate passes.

R0 is enough to answer:

- GCS request, byte, latency, and storage-amplification curves;
- RAM versus SSD serving cost on one machine;
- direct RocksDB wrapper overhead;
- cache admission, cold point reads, and empty-worker reconstruction;
- columnar scan and base-plus-tail operator curves.

R0 cannot prove replicated commit latency, availability under voter loss, or
independent failure domains.

### R1, three data machines plus controller

`[PROPOSED]` R1 adds three equal data machines in separate zones and a controller
outside the data identities. It runs objectKV and direct FoundationDB on the
same shapes in alternating batches. R1 starts only after R0 results reproduce.

```text
controller
  -> data A, zone A, equal local media
  -> data B, zone B, equal local media
  -> data C, zone C, equal local media
  -> regional GCS
  -> separate OTel collector
```

This ordering optimizes for finding read-layout and object-cost failures before
paying the engineering and cloud cost of a distributed comparison. It gives up
early quorum-performance evidence.

## Failure-mode matrix

| Failure | Earliest rung | Required behavior | Primary evidence | Stop condition |
|---|---:|---|---|---|
| OTel endpoint absent | R0 | cloud profile fails closed | invalid/no result receipt | run is admitted without all required signals |
| Runner process crash | R0 | no partial result is promoted; next run uses a new run ID | process exit, run ledger | crash produces a comparable receipt |
| Cold cache | R0 | exact read with bounded metadata and selected-block work | first-read p99, GETs, bytes | work grows with full database bytes |
| Working set exceeds RAM | R0 | bounded eviction or demotion, no swap/OOM policy | resident bytes, hit ratio, p99 | stable p99 requires unbounded RAM |
| GCS latency/error burst | R0 | foreground hot commits remain independent until declared debt cap | foreground object requests, publication lag, backpressure | commit waits on object PUT below the debt cap |
| Unknown object PUT result | R0 | exact named read resolves success or retry | duplicate effects, checksum, recovery time | conflicting bytes or LIST decides authority |
| Corrupt object/block | R0 | checksum rejects data and recovery chooses another valid source or fails loudly | corruption detections, anomalies | silent incorrect value |
| Compaction backlog | R0 | explicit debt/backpressure before storage or memory becomes unbounded | L0 count, write amplification, lag | debt grows without a declared admission response |
| Local data disk loss | R0/R1 | empty worker rebuilds from object base plus retained suffix | RTO, restored bytes, exact digest | acknowledged data is unavailable or full hydration is mandatory for first read |
| One txLog voter loss | R1 | quorum commits continue within degraded target | p99, tx/s, unavailable operations | acknowledgement loss or split outcome |
| Majority loss | R1 | no new acknowledgements | rejected/time-out commits | commit acknowledged without quorum |
| Network partition | R1 | only one fenced generation may acknowledge | generation, conflicting outcomes | two sides acknowledge conflicting history |
| Stale generation restart | R1 | old process cannot serve or publish | stale-generation rejections | stale root or read becomes authoritative |
| Controller loss | R1 | data safety unaffected; experiment aborts and remains resumable | receipt state, cluster health | controller is part of the data quorum |

Every injected failure has a poison subject. A failure test is not admitted if
the correct subject passes but the deliberately broken subject also passes.

## Autoresearch loop

The optimization agent may change one declared surface at a time:

```text
frozen suite + incumbent receipt
  -> one bounded architecture/config change
  -> candidate and incumbent in one alternating batch
  -> correctness and failure poisons
  -> paired comparison receipt
  -> promote, reject, or mark inconclusive
  -> append experiment ledger
```

Promotion requires:

1. schema-valid machine, result, and comparison receipts;
2. exact incumbent reproduction inside its previous noise band;
3. all correctness, resource, durability, and telemetry gates;
4. a decisive improvement on the selected primary metric;
5. no secondary regression beyond the lane's declared cap;
6. one intentionally slower candidate rejected by the champion rule.

The initial edit surfaces are cache admission, object/block sizing, index
fanout, compaction thresholds, commit batch size/delay, and DataFusion stripe
coalescing. Transaction semantics, oracle behavior, result schemas, and failure
schedules are frozen during an optimization campaign.

## Current execution boundary

`[VERIFIED]` R0 can provision private compute, attach measured local media,
reach versioned regional GCS, export required OTel signals, and persist a
schema-valid machine-bound result. The completed lease destroyed nine resources;
zero matching instances, disks, firewall rules, subnetworks, or routers remain.
`[EVALUATING]` the same smoke from one clean, digest-addressed experiment bundle.

The next receipt sequence remains incremental:

1. freeze the cache budget and reusable larger-than-cache fixture;
2. add CPU time, physical bytes, block-cache hits, read amplification, and
   object-fetch attribution;
3. run warm, mixed, and eviction-heavy points in both process orders;
4. build the native three-node replicated commit path;
5. compare it with a same-durability control under normal operation, leader
   loss, and recovery;
6. compose admitted read, commit, publication, and empty-worker recovery into
   the first one-range Cell v0 slice.
