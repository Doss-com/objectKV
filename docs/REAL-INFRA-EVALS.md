# objectKV real-infrastructure evaluation program

Status: `[VERIFIED]` for the private runner, named local NVMe, regional GCS,
required OTel, machine receipt, schema-valid paired-comparison mechanism, and
GP2.5.3 physical provider-media-loss reconstruction. `[VERIFIED]` GP3.1 and
GP3.1.1 admit the topology-matched native resident read boundary through 32
clients. `[VERIFIED]` GP3.1.2 executed both a clean negative 64 MiB
cache-pressure calibration and a corrected rerun that clears the throughput,
p99, CPU/read, and zero physical-read bounds. `[VERIFIED]` the immutable
fresh-process 64 MiB direct-NVMe preflight clears every paired and telemetry
gate in both process orders. `[EVALUATING]` the complete 1 GiB T27 coverage and
skew curve, same-durability replicated commit, and the complete transaction
plane. No full-cell or cross-stack performance claim is `[VERIFIED]`.

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

`[VERIFIED]` A sixth R0 execution completed the 64 MiB cache-pressure
calibration with a 32 MiB explicit RocksDB block cache, Zipf 1.4, eight clients,
15 independently warmed samples per subject and order, and 60 million measured
reads. All 84 workload hard gates passed. Native retained 0.5968x and 0.5659x
direct RocksDB throughput; p99 was 1.3312x and 1.5567x control. Both comparison
receipts returned `worse`. Native CPU time was 1.6685x and 1.7460x control.
Peak RSS was effectively equal, native cache hit ratio was slightly higher,
and Linux reported zero physical read bytes for every subject. This localizes
the immediate regression above physical media but does not isolate an NVMe
curve. Every run ID appeared in OTel logs, metrics, and traces. The run removed
152 current scratch objects and all nine leased resources. See
`docs/artifacts/eval-receipts/native-resident-cache-pressure-gcp-r0-2026-08-28/README.md`.

The initial three-seed 64 MiB attempt was discarded before a receipt because
fixture construction could not fit the bounded command. The completed
calibration reuses its fixture across 15 measured windows, but each of the four
subjects still reconstructs and semantically replays a separate fixture.
Replicated authority scratch reached about 1.2 GiB per 64 MiB logical fixture.
Before the next larger cloud run, the harness must persist one
content-addressed fixture across candidate, control, and both process orders.
The corrected native read path cleared the unchanged calibration; the 1 GiB
admission now waits on fixture reuse and explicit page-cache treatment.

`[VERIFIED]` The seventh R0 execution removed the forced tail SST that caused
two cache probes per untouched latest read, then repeated the same 64 MiB
calibration for 60 million measured reads. Native retained 0.9432x and 0.9735x
control throughput; p99 was 1.0441x and 0.9949x; CPU/read was 1.0586x and
1.0298x. All 84 workload gates and eight explicit comparison constraints
passed. Every run ID occurred in OTel logs, metrics, and traces. The 31 durable
objects total 4,911,598 bytes; all nine leased resources and the 116 MiB local
provider cache were removed. Linux still reported zero physical bytes per read,
so T27 remains `[EVALUATING]` pending the 1 GiB coverage and skew sweep with
explicit page-cache treatment. See
`docs/artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/README.md`.

`[VERIFIED]` An eighth bounded R0 execution verified the explicit direct-table
read treatment on Linux. Native and matched direct RocksDB both reported
`direct_reads=true`, passed 22 of 22 hard gates, returned exact values, and
issued zero measured object operations. With 16 MiB of values, a 4 MiB block
cache, Zipf 0.8, eight clients, and 100,000 reads per subject, Linux attributed
2,960.75 physical bytes per read to native and 2,966.00 to control. The
single-sample native/control ratios were 1.0448x throughput, 0.9651x p99,
1.0337x CPU/read, and 0.9982x physical bytes/read. This verifies the mechanism
that separates page-cache and physical-device curves. It does not admit a
performance point because the smoke profile has one seed, one repeat, no AB/BA
order, and no required OTel correlation. Eight evidence objects totaling
2,681,699 bytes are durable in versioned GCS. All nine resources and the 116
MiB provider cache were removed. See
`docs/artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/README.md`.

`[VERIFIED]` A ninth bounded R0 execution verified the RFC-0044 empty-anchor
precondition before fixture descriptor work. A clean release binary started
20 independent fresh transaction-authority clusters, 60 real OpenRaft
processes total. All assigned `O=2`; each retained one empty record, zero
mutations, and zero live keys. The evaluator observed every commit after its
reply was dropped and before retrying, then recovered the exact original
result. A changed-identity bypass created a second record and the poison oracle
detected it. Both schema-validated receipts returned `keep`. This semantic
contract profile did not require OTel and does not admit a performance point.
The 2.66 MiB evidence set and complete source bundle are durable under
`gs://doss-objectkv-dev-okv-evals/runs/rfc0044-anchor-r0-20260828/`. See
`docs/artifacts/eval-receipts/object-fixture-anchor-gcp-r0-2026-08-28/README.md`.

`[VERIFIED]` A tenth bounded R0 execution verified RFC-0044 phase 1 from clean
source `fc8189e30c2e46d79cc99f3c2068b3cecd8e93e3`. One 4 MiB logical base
reconstructed 4,096 exact records from 11 immutable objects totaling 4,306,945
bytes at anchor `O=2`. The fresh authority retained one empty anchor, zero
anchor mutations, zero anchor live keys, zero base-value records, and one
exact seven-record tail. Native and control received different semantic image
IDs and the same complete tagged logical image digest. The candidate plus
corrupt-descriptor, mutated-anchor, tail-mismatch, and shared-root poisons all
returned `keep`; every formal hard gate passed. This local-filesystem contract
did not require OTel and does not admit a performance point or persisted GCS
reuse. See
`docs/artifacts/eval-receipts/object-fixture-contract-gcp-r0-2026-08-28/README.md`.

`[VERIFIED]` An eleventh bounded R0 execution verified RFC-0044 phase 2 from
clean source `1ae2eded0c7d4da856aa8bbb65d5cacb3c500b28`. Independent empty
native and direct-control workers verified the same 4 MiB fixture and exact
seven-record txLog tail, then built actual RocksDB resident images. The image
IDs differ while the complete logical digest is equal. Native used 8,769,143
local bytes, control used 4,331,990, and both issued zero object requests in
their short semantic read window. The candidate pair and regenerated-control
poison receipts returned `keep` with no failed hard gates. This does not admit
a performance point. See
`docs/artifacts/eval-receipts/object-fixture-resident-process-gcp-r0-2026-08-28/README.md`.

`[VERIFIED]` A twelfth bounded R0 execution verified RFC-0044 phase 4 from
clean source `6f812ddf3d261d30cc9698b6baed3f97876ace45`. One 64 MiB logical fixture
occupied 68,857,626 bytes across 20 regional, versioned GCS objects. Four fresh
ABBA worker processes shared one fixture and tail identity; three opened the
exact persisted descriptor, every authority retained one empty anchor and zero
base values in txLog, every resident image returned one equal complete logical
digest, and hot reads issued zero object requests. The candidate passed 19
gates in 55.906526 seconds; maximum setup was 11.696264 seconds and maximum
transaction-authority scratch was 108,918 bytes, 0.001623x logical data. The
reuse-bypass poison passed by detecting only two exact reopens. Candidate and
poison run IDs occur in OTel traces, metrics, and logs. This is setup evidence,
not T27 performance admission. See
`docs/artifacts/eval-receipts/object-fixture-gcs-preflight-gcp-r0-2026-08-28/README.md`.

`[VERIFIED]` A thirteenth bounded R0 execution verified the RFC-0044 phase-5
cross-invocation boundary on real GCS. One writer invocation persisted a 4 MiB
fixture and a locator pinned to descriptor generation `1787976513990982`.
Separate native and direct-control invocations then exact-opened the same
closure under `roles/storage.objectViewer`. The first native diagnostic
reported 4,080 aggregate mismatches because its validator used the trace seed
instead of fixture seed `4244`. Commit `1cfad27` corrected that seam and added
a fail-closed command gate. The fixed release replay used trace seed `1103`
for both subjects and returned one equal trace, tail, and complete logical
digest, zero aggregate and per-sample correctness failures, zero measured
object requests, and valid counter deltas. The temporary viewer binding, VM,
and firewall were removed. This is correctness and credential-boundary
evidence, not a T27 performance point. See
`docs/artifacts/eval-receipts/t27-gcs-placement-boundary-gcp-r0-2026-08-28/README.md`.

`[VERIFIED]` A fourteenth bounded R0 execution ran the immutable 64 MiB T27
preflight from source `578c6a919cbd2e0e6969eaa134fdfae7d6112446`. One
generation-pinned fixture fed four sequential fresh processes under
`roles/storage.objectViewer`, a 13,421,772-byte RocksDB cache, Zipf 1.4, eight
clients, and direct NVMe table reads. In AB order, native throughput, p99,
CPU/read, and physical bytes/read were 0.8652x, 1.0048x, 1.0718x, and 1.0647x
control. In BA order they were 0.9739x, 0.9882x, 0.9797x, and 1.0638x. Read
amplification was 1.0000x and pressure was observed in both comparisons. Every
gate passed. The sealed receipt records successful flush and shutdown of logs,
metrics, and traces; independent collector inspection found the run ID in all
three. A wrong version-1 anchor and a read-only fixture write were rejected.
The rejected 68,857,626-byte fixture and all nine leased resources were
removed. The canonical version-2 fixture remains for the 1 GiB progression.
See
`docs/artifacts/eval-receipts/t27-fresh-process-preflight-gcp-r0-2026-08-29/README.md`.

`[VERIFIED]` A fifteenth R0 execution completed the first full 1 GiB T27
stratum from source `95dedb0249a69567e7c390f4c191d079f07b6d90`. The exact
215,947,032-byte release executable and 1,981,628-byte source archive were
retained by GCS generation before measurement. Live plan `40d4559a` binds one
private `n2-standard-8` runner, 375 GiB local NVMe, 200 GiB `pd-ssd`, separate
private OTel collector, 1 GiB logical fixture, and 27-stratum workload.
Stratum `c50-z08-s1103` ran 20 unique fresh processes over five ABBA blocks,
200,000 warmup and 1,000,000 measured reads per position, 50 percent cache,
Zipf 0.8, and eight readers. AB and BA throughput were 0.994982x and 0.997260x
direct RocksDB; p99 was 0.999051x and 1.000304x; CPU/read was 1.017306x and
1.011997x; physical bytes/read were 0.997738x and 0.997837x. Every comparison,
pressure, identity, correctness, and telemetry gate passed. The 1 hour 6 minute
run returned subject scratch to 4,096 bytes. The complete evidence archive and
standalone receipt are immutable in GCS. Infrastructure remained leased for
the other 26 strata at that checkpoint, so T27 stayed `[EVALUATING]`. See
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s1103-gcp-r0-2026-08-29/README.md`.

`[VERIFIED]` The same R0 execution then completed `c50-z08-s2207` against the
unchanged source, executable, plan, fixture, machine incarnation, NVMe device,
and lease. Its 20 fresh processes produced AB and BA throughput ratios of
1.012558x and 0.998886x direct RocksDB, p99 ratios of 0.989567x and 0.998296x,
CPU/read ratios of 1.015743x and 1.006168x, physical-read ratios of 0.999567x
and 0.999454x, and read amplification of 1.000000x. All pressure, identity,
correctness, comparison, and telemetry gates passed. Independent collector
inspection found the run ID in logs, metrics, and traces. A first invocation
with a missing bucket environment failed before measurement and remains an
immutable failure artifact. The corrected 1 hour 5 minute run and standalone
receipt are generation-pinned in GCS. The queued driver began the third
stratum only after the second receipt passed and released its host lease. T27
remains `[EVALUATING]` with 25 direct-NVMe strata and two buffered sentinels
open. See
`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s2207-gcp-r0-2026-08-29/README.md`.

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
`[VERIFIED]` one clean digest-addressed 64 MiB ABBA experiment now composes
that infrastructure with read-only fixture consumption and direct NVMe
measurement. `[VERIFIED]` a sealed plan-poison command and schema bind
and reject AABB, missing-position, and option-mismatch artifacts through the
production decoder. A second sealed command binds one real direct-position
receipt and rejects an otherwise isolated hidden native provider through the
production receipt validator. All four structured controls passed against the
exact preflight evidence. The missing-locator process also exited before plan
creation while the versioned GCS fixture manifest remained unchanged.
`[VERIFIED]` source `9cf5014` then published the immutable 1 GiB fixture,
revoked writer authority, exact-opened its generation-pinned descriptor under
object-viewer credentials, and froze the complete 540-position plan. The
fixture contains 266 objects totaling 1,101,701,925 bytes. Plan `b76be02a`
contains 27 strata and exact native/direct parity across every treatment. The
viewer grant and all nine leased resources were removed after evidence
capture. This is setup evidence, not a performance point. `[EVALUATING]` the
execution of the 1 GiB workload envelope.

The next receipt sequence remains incremental:

1. execute the frozen 1 GiB cache-coverage and skew sweep in both process
   orders;
2. run the GCS cold-point and object-layout geometry curve;
3. build the native three-node replicated commit path;
4. compare it with a same-durability control under normal operation, leader
   loss, and recovery;
5. compose admitted read, commit, publication, and empty-worker recovery into
   the first one-range Cell v0 slice.
