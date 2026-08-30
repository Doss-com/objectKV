# objectKV project tracking

Status: `[EVALUATING]` active technical program.

The canonical fabric-to-storage map, RangeEngine profiles, decision-review
split, and layer-to-matrix evidence index live under
[`docs/architecture/`](architecture/README.md). That documentation slice adds
no performance measurement and changes no matrix status.

The canonical living program tracker is
[`docs/artifacts/objectkv-program-tracker/objectkv-program-tracker.html`](artifacts/objectkv-program-tracker/objectkv-program-tracker.html).
It assembles the architecture, dependency frontier, target curves,
systems-to-infrastructure ladder, recent experiment receipts, decisions, and
work log from the current tree and `experiments/ledger.jsonl`.

The canonical living implementation architecture is
[`docs/artifacts/objectkv-architecture/objectkv-architecture.html`](artifacts/objectkv-architecture/objectkv-architecture.html).
It compounds the bottom-up construction, runtime ownership, read and write
paths, current-to-target gaps, and the exact boundary between smoke evidence,
verified mechanisms, admitted workloads, and production credibility.

`[CODE-COMPLETE]` RFC-0047 implements sparse post-object-frontier history
under provider identity `rocksdb-11.8.1-native-resident-v2`. Activation writes
the object base only to `head`; the first post-frontier mutation of a key seeds
that key's value, tombstone, or explicit absence at the object frontier in the
same atomic batch as its tail mutation and applied cursor. `[VERIFIED]` The
V2.1 preflight and 1 GiB diagnostic reduce native local bytes from 2.015239x to
1.000037x direct RocksDB. `[EVALUATING]` A bounded five-native, five-control
replay measured p50 1.026x, p95 1.131x, p99 1.742x, and p99.9 1.032x control.
The complete tail curve is deferred without changing its target. RFC-0046 T28
cold indexed GCS reads are now the active program row.

`[CODE-COMPLETE]` T28 can exact-open a generation-pinned descriptor and
manifest without hydrating the closure, retain selected authenticated indexes
in RAM, seal point ranges, and run concurrent candidate and raw-control reads
through fresh processes. `[EVALUATING]` The real 1 GiB, three-seed GCS curve
completed 15 paired blocks and 30,720 measured reads per subject. Candidate
versus raw-control latency was 26.760 versus 26.758 ms p50, 44.202 versus
43.666 ms p95, 61.752 versus 58.920 ms p99, and 140.124 versus 116.206 ms
p99.9. The pooled p99 ratio was 1.048x, but the frozen every-block gate rejected
two blocks at 1.298x and 1.378x, so row 2 remains `[EVALUATING]`. Provider-only
p99 ratios on those misses were 1.299x and 1.379x; the end-to-provider gap was
about 0.35 ms on both subjects. All reads returned exact values through one
planned range request with zero retries and zero correctness anomalies. T38 is
unchanged because the curve was not admitted. Evidence:
`docs/artifacts/eval-receipts/rfc0046-t28-point-curve-gcp-r0-2026-08-30/README.md`.
`[CODE-COMPLETE]` Each operation now also records its exact GCS time and local
residual. A fresh diagnostic measured candidate/raw local-residual p99 at
428.507/407.242 microseconds, a 21.265-microsecond candidate increment, while
the 33.335 ms end-to-end difference tracked a 33.319 ms provider difference.
This proves attribution, not admission.

The recurring four-panel performance view is tracked with the RFC-0047
diagnostic evidence. Each new admitted curve updates latency shape, concurrency
scaling, footprint, or tier evidence. Unmeasured external controls remain blank.

## Open the playground

The tracker uses a dedicated local port so it does not collide with the
DOSSBOT app page:

```bash
cd "$(git rev-parse --show-toplevel)"
python3 -m http.server 4197 --bind 127.0.0.1 --directory .
```

Open
`http://127.0.0.1:4197/docs/artifacts/objectkv-program-tracker/objectkv-program-tracker.html`.
The architecture tracker is at
`http://127.0.0.1:4197/docs/artifacts/objectkv-architecture/objectkv-architecture.html`.

Rebuild before a review:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 \
  docs/artifacts/objectkv-program-tracker/_assemble.py
PYTHONDONTWRITEBYTECODE=1 python3 \
  docs/artifacts/objectkv-architecture/_assemble.py
```

The former DOSSBOT project-tracker route is a legacy archive. New objectKV
program state is owned here rather than copied into that retired queue.

## Authority boundaries

- `docs/BOOTSTRAP-PLAN.md` owns the program goal, sequencing, and gates.
- The program tracker owns the concise program readout, sequencing, and links
  to evidence.
- The architecture tracker owns the current implementation composition,
  service ownership, pipeline maps, and evidence-maturity boundary.
- `docs/PRODUCT-SPEC-SHEET.md` owns atomic requirements and performance targets.
- `docs/CONTRIBUTOR-BOARD.md` owns bounded contributor tasks.
- `docs/DECISIONS.md` and `rfcs/` own architectural decisions.
- `docs/STATUS-TAXONOMY.md` owns proof-status meaning.
- `docs/REAL-INFRA-EVALS.md` owns paired benchmark claims, GCP rungs, and the
  failure-mode matrix.
- `docs/EVAL-WORKLOAD-CONTRACT.md` owns evidence classes and the minimum
  workload envelope required for performance and economics admission.
- `papers/objectkv-vldb/` owns the working technical paper.
- `experiments/ledger.jsonl` and OTel own empirical receipts.
- Git owns source and exact revision identity.

## Real-infrastructure benchmark checkpoint

`[CODE-COMPLETE]` The product program now pins a paired-comparison schema.
Candidate/control receipts fail comparison when metric, statistic, direction,
machine, toolchain, lockfile, revision, seed, sample count, or hard gates do not
match. A directional percentage must clear both the lane threshold and observed
MAD-based noise.

`[CODE-COMPLETE]` Performance and economics comparisons now also require
workload-class receipts with a validated workload-profile hash. The profile
fails closed unless it declares dataset, operation mix, access pattern,
concurrency, warmup, measurement, cache state, failure schedule, resource
limits, required metrics, matched control, and at least five repeats. Smoke
profiles remain useful infrastructure evidence but cannot admit a curve.

`[CODE-COMPLETE]` The native and matched direct RocksDB read paths now use the
same explicit block-cache budget, deterministic Hotset or Zipf trace, and
measured-window cache counters. Counter resets fail closed. Cache hit ratio,
cache requests, cache bytes, RocksDB read counters, and read amplification are
registered OTel metrics. The runner reuses one recovered fixture across
independently warmed samples and records process CPU, RSS, Linux logical and
physical I/O, and host network-namespace deltas per sample. The local smoke
path emitted exact two-sample native and control receipts with zero object
operations. Mismatched-cache and counter-reset poisons both discarded.

`[EVALUATING]` GP3.1.2 is the first cache-pressure workload profile: 64 MiB
logical data over a 32 MiB block cache, Zipf 1.4, eight clients, one million
measured reads per window, one fixture seed, 15 independently warmed windows,
both process orders, and matched direct RocksDB control. The single-seed
calibration produces 30 samples per subject across two independent fixture
reconstructions. The later 1 GiB admission retains three seeds. The suite and
22-gate program validate. The reusable-fixture runner, resource attribution,
and two required poison workloads are `[CODE-COMPLETE]`.

`[VERIFIED]` The clean GP3.1.2 GCP R0 calibration executed 60 million measured
reads across four receipts and both process orders. All 84 workload hard gates
passed, every run ID occurs in OTel logs, metrics, and traces, 550,853,784
current scratch bytes were removed, and all nine leased resources were
destroyed. The performance claim failed decisively. Native retained 0.5968x
and 0.5659x direct RocksDB throughput; p99 was 1.3312x and 1.5567x control.
CPU time was 1.6685x and 1.7460x control. Both subjects reported zero physical
read bytes, so this is a combined RocksDB plus operating-system-cache curve,
not an isolated NVMe curve. T27 remains `[EVALUATING]`; its complete 1 GiB
admission is deferred after the bounded provider-v2 diagnostic.
Evidence is in
[`docs/artifacts/eval-receipts/native-resident-cache-pressure-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/native-resident-cache-pressure-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` The first CPU attribution found that activation created a base SST
and every small tail advance forced another SST. An untouched latest read then
searched both files, producing two RocksDB cache probes and explaining most of
the measured CPU tax. The bounded correction leaves the disposable recent tail
in the mutable RocksDB layer while txLog remains the durability authority. On
the real R0 host, the focused regression produced exactly 256 cache probes for
256 latest reads; all eight RangeEngine package tests passed. The v2 comparator
now executes throughput, p99, CPU/read, and exact-zero physical-read bounds.

`[VERIFIED]` The corrected 60-million-read A/B plus B/A calibration at source
`a4cf9a8a8d86a1dfa84d5af01eb514149dce1ed8` cleared all eight explicit
comparison constraints. Native retained 0.9432x and 0.9735x control throughput;
p99 was 1.0441x and 0.9949x; CPU/read was 1.0586x and 1.0298x. All 84 workload
gates passed and every run ID occurred in OTel logs, metrics, and traces. The
31-object, 4.68 MiB evidence set is durable in versioned GCS; all nine leased
resources and the 116 MiB local provider cache were removed. T27 and row 1 stay
`[EVALUATING]` because the operating-system page cache kept physical reads at
zero and the broader cache-coverage and skew sweep has not run. Evidence is in
[`docs/artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/native-resident-cache-pressure-optimized-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` The matched RocksDB `direct_reads` mechanism executed on one clean
Linux GCP R0 runner. Both the native RangeEngine and direct RocksDB control
reported `direct_reads=true`, passed 22 of 22 hard gates, returned exact values,
and issued zero measured object operations. The native and control paths read
2,960.75 and 2,966.00 physical bytes per logical read. Their single-sample
throughput, p99, CPU/read, and physical-byte ratios were 1.0448x, 0.9651x,
1.0337x, and 0.9982x. This verifies option parity and physical-device
attribution, not performance. The source bundle, receipts, Linux unit tests,
and machine identity are durable in GCS; all nine leased resources and the 116
MiB provider cache were removed. Evidence is in
[`docs/artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/native-resident-direct-read-preflight-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` The RFC-0044 fixture-bootstrap falsifier ran from clean source
`30f65b566547cbbe151a07fa51eba21df7866ee3` on one disposable GCP host. Twenty
fresh transaction-authority clusters started 60 real OpenRaft processes and
all assigned `O=2`. Every cluster retained one empty record, zero mutations,
and zero live keys; the evaluator observed the commit after dropping its reply
and recovered the exact result on retry. The changed-identity bypass poison
created the forbidden second record and was detected. Both formal receipts
returned `keep`. This verifies the bootstrap boundary only; T27 remains
`[EVALUATING]` pending the descriptor, closure, fresh subject images, and
persisted-fixture preflight. Evidence is in
[`docs/artifacts/eval-receipts/object-fixture-anchor-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/object-fixture-anchor-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` RFC-0044 phase 1 ran from clean source
`fc8189e30c2e46d79cc99f3c2068b3cecd8e93e3` on one disposable GCP builder.
The local 4 MiB base reconstructed 4,096 exact records from 11 immutable
objects at `O=2`; the authority retained one empty anchor, zero anchor
mutations, zero anchor live keys, and zero base-value records. One exact
seven-record tail produced distinct native and control semantic image IDs and
one equal complete logical image digest. The candidate and four descriptor,
anchor, tail, and shared-root poisons all returned `keep` with no failing hard
gate. Local temporary storage reports no persisted cross-subject reuse. This
is semantic setup evidence, not a performance point. The phase-2 slice below
builds the actual fresh-process resident images before GCS persistence.
Evidence is in
[`docs/artifacts/eval-receipts/object-fixture-contract-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/object-fixture-contract-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` RFC-0044 phase 2 ran from clean source
`1ae2eded0c7d4da856aa8bbb65d5cacb3c500b28` on one disposable GCP builder.
Independent empty native and direct-control processes verified the same 4 MiB
fixture and exact seven-record tail. They produced different physical resident
IDs, the same complete logical digest across 4,099 outcomes, nonzero local
images, and zero post-activation object requests. The regenerated-control
poison failed closed. Both formal receipts returned `keep`; no performance
point was admitted. Evidence is in
[`docs/artifacts/eval-receipts/object-fixture-resident-process-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/object-fixture-resident-process-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` RFC-0044 phase 4 ran from clean source
`6f812ddf3d261d30cc9698b6baed3f97876ace45` on one private GCP R0 runner.
The 64 MiB fixture occupied 68,857,626 bytes across 20 regional GCS objects.
Four fresh ABBA subjects used one exact fixture and tail identity; the final
three reopened the persisted descriptor, each authority retained one empty
anchor and zero base values, and every resident image produced the same
complete logical digest with zero measured-window object requests. The
candidate passed 19 gates in 55.906526 seconds and used 108,918 transaction-
authority bytes, 0.001623x logical data. The reuse-bypass poison was detected,
and both run IDs occur in all three OTel signals. This verifies setup mechanics,
not T27 performance admission. Evidence is in
[`docs/artifacts/eval-receipts/object-fixture-gcs-preflight-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/object-fixture-gcs-preflight-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` The RFC-0044 phase-5 cross-invocation boundary prepared one
generation-pinned 4 MiB fixture under writer credentials, then consumed it in
separate native and direct-control invocations under object-viewer credentials.
The first native run reproduced 4,080 aggregate correctness failures because
its validator substituted trace seed `1103` for fixture seed `4244`. Commit
`1cfad27` separated those inputs and made the command fail closed on aggregate
or per-sample correctness, object-I/O, and counter failures. The fixed release
replay returned the same fixture, tail, trace, and complete logical-image
digests across both subjects, with zero correctness failures and zero measured
object requests. The temporary viewer grant, VM, and firewall were removed;
12 immutable fixture objects remain. This verifies the command and credential
boundary, not performance. Evidence is in
[`docs/artifacts/eval-receipts/t27-gcs-placement-boundary-gcp-r0-2026-08-28/README.md`](artifacts/eval-receipts/t27-gcs-placement-boundary-gcp-r0-2026-08-28/README.md).

`[VERIFIED]` The immutable fresh-process 64 MiB preflight ran on one private
R0 runner with local NVMe and a separate private OTel collector. One
generation-pinned GCS fixture crossed all four ABBA positions under
object-viewer credentials. Native retained 0.8652x and 0.9739x direct RocksDB
throughput; p99 was 1.0048x and 0.9882x; CPU/read was 1.0718x and 0.9797x;
physical bytes/read were 1.0647x and 1.0638x; read amplification was 1.0000x.
Every comparison, correctness, cache-pressure, process-identity, and telemetry
gate passed. Collector inspection found the run ID in five log, two metric,
and four trace payloads. The wrong version-1 anchor and a read-only fixture
write were rejected. All nine resources were destroyed and Terraform state is
empty. T27 remains `[EVALUATING]` for the frozen 1 GiB sweep. Evidence is in
[`docs/artifacts/eval-receipts/t27-fresh-process-preflight-gcp-r0-2026-08-29/README.md`](artifacts/eval-receipts/t27-fresh-process-preflight-gcp-r0-2026-08-29/README.md).

`[VERIFIED]` `t27-plan-poison-check` converts three plan-integrity
negative controls into portable immutable artifacts. It first authenticates
the source plan, applies exactly one AABB schedule, missing-position, or
effective-option corruption, recomputes the poisoned plan digest, invokes the
production decoder, and seals the source digest, poisoned bytes, expected
rejection, observed rejection, and receipt digest. Twenty-four focused T27
library tests pass locally, including schema validation and artifact-tampering
rejection. `t27-position-poison-check` also authenticates a
real direct-position receipt, injects exactly one hidden native provider, and
passes only when the production receipt validator returns the intended
runtime-inventory rejection. Twenty-five focused T27 library tests pass
locally, including both receipt schemas and artifact-tampering rejection. The
commands passed against the exact GCP preflight plan and one direct-position
receipt. A missing locator exited before plan creation, and the versioned
fixture manifest was identical before and after. Eight structured artifacts
totaling 22,153 bytes are immutable in GCS. No new performance point was
produced. Only the 1 GiB sweep remains `[EVALUATING]` for T27. Evidence is in
[`docs/artifacts/eval-receipts/t27-preflight-poisons-r0-2026-08-29/README.md`](artifacts/eval-receipts/t27-preflight-poisons-r0-2026-08-29/README.md).

`[VERIFIED]` Source `9cf5014` then built the exact RocksDB-enabled Linux
binary, published the immutable 1 GiB fixture, revoked writer authority, and
derived the complete admission plan under `roles/storage.objectViewer`. The
fixture contains 266 objects totaling 1,101,701,925 physical bytes. Plan
`b76be02a` binds 540 fresh-process positions, 27 strata, three cache levels,
three Zipf skews, three trace seeds, five ABBA blocks, and exact native/direct
treatment parity. The source archive, machine receipt, locator, and plan are
immutable in versioned GCS. The viewer binding and all nine leased resources
were removed after capture. No performance point was produced; execution of
the frozen sweep remains `[EVALUATING]`. Evidence is in
[`docs/artifacts/eval-receipts/t27-1gib-fixture-plan-gcp-r0-2026-08-29/README.md`](artifacts/eval-receipts/t27-1gib-fixture-plan-gcp-r0-2026-08-29/README.md).

`[VERIFIED]` Source `95dedb0` retained the exact release executable in GCS,
sealed a live 540-position plan against one private `n2-standard-8` runner and
375 GiB local NVMe device, and completed stratum `c50-z08-s1103`. Its 20 unique
fresh-process positions cover five ABBA blocks at 50 percent cache, Zipf 0.8,
trace seed 1103, eight clients, and one million measured reads per position.
AB and BA throughput were 0.994982x and 0.997260x direct RocksDB; p99 was
0.999051x and 1.000304x; CPU/read was 1.017306x and 1.011997x; physical
bytes/read were 0.997738x and 0.997837x. Read amplification was 1.000000x.
Every comparison, pressure, correctness, runtime, process, and OTel gate passed.
The complete 42-file evidence set and standalone receipt are immutable in GCS.
T27 remains `[EVALUATING]` with 26 direct-NVMe strata and two buffered
sentinels open. Evidence is in
[`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s1103-gcp-r0-2026-08-29/README.md`](artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s1103-gcp-r0-2026-08-29/README.md).

`[VERIFIED]` The same source, plan, execution envelope, machine incarnation,
and lease then completed stratum `c50-z08-s2207`. Its 20 fresh processes cover
five ABBA blocks at the same 50 percent cache, Zipf 0.8, eight-reader profile
with independent trace seed 2207. AB and BA throughput were 1.012558x and
0.998886x direct RocksDB; p99 was 0.989567x and 0.998296x; CPU/read was
1.015743x and 1.006168x; physical bytes/read were 0.999567x and 0.999454x.
Read amplification was 1.000000x. All comparison, pressure, correctness,
runtime, process, and OTel gates passed. Collector-side evidence contains 21
logs, 63 metrics, and 20 traces for run
`bde16597-8fe7-434b-95b4-dfdc7c58d267`. The initial invocation failed closed
before measurement because its service omitted the GCS bucket environment;
that failure is retained separately. T27 remains `[EVALUATING]` with 25
direct-NVMe strata and two buffered sentinels open. Evidence is in
[`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s2207-gcp-r0-2026-08-29/README.md`](artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s2207-gcp-r0-2026-08-29/README.md).

`[VERIFIED]` Stratum `c50-z08-s3301` then completed under the same source,
plan, workload, execution envelope, machine incarnation, and lease. Its 20
fresh processes produced AB and BA throughput ratios of 1.008552x and
0.981275x direct RocksDB, p99 ratios of 0.987784x and 1.003334x, CPU/read
ratios of 0.999355x and 1.017371x, physical-read ratios of 1.001245x and
1.001029x, and read amplification of 1.000000x. All comparison, pressure,
correctness, runtime, process, and OTel gates passed. Collector-side evidence
contains 21 logs, 63 metrics, and 20 traces for run
`168758be-ca1e-4083-afce-aa981af80b33`. The evidence archive and standalone
receipt are generation-pinned in GCS. T27 remains `[EVALUATING]` with 24
direct-NVMe strata and two buffered sentinels open. Evidence is in
[`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s3301-gcp-r0-2026-08-30/README.md`](artifacts/eval-receipts/t27-1gib-stratum-c50-z08-s3301-gcp-r0-2026-08-30/README.md).

`[VERIFIED]` Stratum `c50-z14-s1103` then raised Zipf skew to 1.4 under the
same source, plan, workload, execution envelope, machine incarnation, and
lease. Its 20 fresh processes produced AB and BA throughput ratios of
0.974144x and 0.976563x direct RocksDB, p99 ratios of 0.875320x and 0.901665x,
CPU/read ratios of 1.022413x and 1.024776x, physical-read ratios of 0.995322x
and 0.995223x, and read amplification of 1.000000x. All gates passed.
Collector-side inspection found 21 log, 65 metric, and 20 trace JSONL exports
for run `a9b6d86a-8680-46af-aae7-a7acebc7844b`. The create-only evidence
archive and standalone receipt were downloaded and hash-verified by generation.
T27 remains `[EVALUATING]` with 23 direct-NVMe strata and two buffered sentinels
open. Evidence is in
[`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s1103-gcp-r0-2026-08-30/README.md`](artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s1103-gcp-r0-2026-08-30/README.md).

`[VERIFIED]` Stratum `c50-z14-s2207` then completed under the same source,
plan, workload, execution envelope, machine incarnation, and lease. Its 20
fresh processes produced AB and BA throughput ratios of 0.965184x and
0.992665x direct RocksDB, p99 ratios of 1.075079x and 1.188676x, CPU/read
ratios of 1.030645x and 1.016604x, physical-read ratios of 1.001707x in both
orders, and read amplification of 1.000000x. All gates passed. Collector-side
inspection found 21 log, 64 metric, and 20 trace JSONL exports for run
`35ae65ce-69f1-4516-b174-cb6054e34f11`. The create-only evidence archive and
standalone receipt were downloaded and hash-verified by generation. T27
remains `[EVALUATING]` with 22 direct-NVMe strata and two buffered sentinels
open. Evidence is in
[`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s2207-gcp-r0-2026-08-30/README.md`](artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s2207-gcp-r0-2026-08-30/README.md).

`[VERIFIED]` Stratum `c50-z14-s3301` completed all 20 fresh-process positions
and rejected the frozen p99 gate in both orders. AB and BA throughput were
0.995760x and 0.956539x direct RocksDB; p99 was 1.307614x and 1.339897x;
CPU/read was 1.017873x and 1.035078x; physical bytes/read were 1.006496x and
1.006547x; read amplification was 1.000000x. Native local state was
2,215,101,820 bytes versus 1,099,175,660 bytes for control, a 2.015239x ratio.
The failure is localized to the cache-hit to cache-miss knee, where the native
format stored the complete object base in both head and history. Collector
evidence contains 21 logs, 63 metrics, and 20 traces for run
`912fb7e5-35db-4f63-99db-cdd8201f23a9`. The failed evidence archive and receipt
are create-only, generation-pinned, and readback hash-verified in GCS. The
controller stopped before `c50-z20-s1103`. Provider v1 retains five passing
strata, this rejection, and 21 unexecuted strata. RFC-0047 owns the sparse
provider-v2 correction and exact failed-stratum replay. Evidence is in
[`docs/artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s3301-failed-gcp-r0-2026-08-30/README.md`](artifacts/eval-receipts/t27-1gib-stratum-c50-z14-s3301-failed-gcp-r0-2026-08-30/README.md).

`[VERIFIED]` The comparator now also binds both results to the suite hash in the
current program plan and evaluates configurable cross-result constraints. The
topology-matched GP3.1 throughput and p99 constraints passed in both process
orders. Missing secondary metrics invalidate the comparison rather than
silently dropping the constraint.

`[CODE-COMPLETE]` The isolated GCP benchmark root defines a private
`n2-standard-8` runner with a 200 GiB `pd-ssd` and a separate private OTel
collector. It uses a separate remote-state prefix and defaults to zero compute
resources. Both real GCS layout profiles now fail closed without OTel; the full
layout profile requires five repeats.

`[VERIFIED]` R0 now has a clean frozen-source machine receipt for one private
`n2-standard-8`, a 375 GiB local NVMe serving volume, a 200 GiB persistent
`pd-ssd`, regional GCS, and a separate OTel collector. All nine leased resources
were destroyed after the receipts were copied. The first paired GP3.1 run is a
valid historical `worse` result that motivated the subsequent matched controls.

`[VERIFIED]` The optimization run used the same R0 shape and a second frozen
source bundle. It executed candidate then control and control then candidate,
15 samples per subject in each pair, with OTel logs, metrics, and traces. All
nine resources and the 116 MB local Terraform provider cache were removed after
the receipts were copied.

`[VERIFIED]` The native resident-engine decision run used a third frozen source
and an owned-value control. Both throughput comparisons passed; both p99
comparisons failed. D56 then identified process topology as a remaining
confounder. The fourth frozen run matched the full six-process recovery
topology. Native retained 0.9089x and 0.9197x throughput, while p99 was 0.913x
control in both orders. GP3.1 now admits the single-range native read boundary.

`[VERIFIED]` GP3.1.1 measured the native and matched direct RocksDB boundaries
at 8 and 32 synchronized clients on clean GCP R0. Native retained 0.8734x
through 0.8906x control throughput and kept p99 between 1.1072x and 1.1842x
control in both process orders. The eight results contain 120 samples and
24,000,000 measured reads. All 128 workload gates passed, hot reads issued zero
object operations, every run ID occurs in all three OTel signals, 550,912,512
scratch bytes were removed, and all nine leased resources were destroyed.

## Master performance matrix

The canonical workload matrix, current measurements, target bands, controls,
next experiments, and unlock conditions live in
[`docs/BOOTSTRAP-PLAN.md`](BOOTSTRAP-PLAN.md#master-performance-matrix).
It is the program scoreboard. Every substantive implementation turn closes by
updating the active row with its latest immutable receipt or by recording that
no new measurement was produced. The HTML tracker and contributor board mirror
that sequence rather than maintaining a second independent plan.

The current critical path is:

```text
RFC-0046 generation-pinned GCS cold point
  -> object-layout geometry
  -> independent-media replicated commit
  -> objectification debt, host loss, and bounded local media
  -> metadata branch and lazy reopen
  -> multi-range cell
  -> RAM profile
  -> okv-fabric workloads
  -> PostgreSQL OLTP
  -> DataFusion HTAP
  -> comparative economics

deferred
  -> row-1 cache hit/miss attribution
  -> complete provider-v2 cache-pressure curve
```

Every step keeps one primary metric and its own correctness and resource hard
gates. A missed curve causes a mechanism or provider-profile redesign, not a
stop decision for objectKV. The immediate owned task is RFC-0046 T28. T27's locator,
read-only consumer, standalone control, separate seeds, fresh-process runner,
raw evidence, runtime binding, telemetry completion, and poison boundaries are
`[VERIFIED]`. The final runner adds one authenticated resumable receipt per
stratum and rejects cross-execution aggregation. The exact provider-v2 release
build passes 122 library and three controller tests. Provider v1 has five
passing strata and one retained p99 rejection after 120 fresh processes.
Provider v2 fixes the footprint and leaves one localized p99 gap in a bounded
diagnostic. The 27 direct-NVMe strata and two sentinels are deferred.
Provider-v1 and provider-v2 results are never combined. T28 now owns the next
clean cloud comparison against direct indexed GCS.

## Current checkpoint

`[VERIFIED]` The program has exact model, local stable-file, pinned local MinIO,
and real local process evidence. The highest verified infrastructure rung is
real operating-system processes using TCP, SIGKILL, OpenRaft election, and
replacement recovery on one machine.

`[CODE-COMPLETE]` The `objectKV-dev` Terraform configuration formats and
validates locally. The PVLDB working paper renders in the official Vol. 20
template. The `okv-log` pure ordered-record substrate is implemented below
`okv-wal`.

`[CODE-COMPLETE]` The golden-path scenario keeps the FoundationDB GP2.5 branch
as semantic oracle and fallback evidence while GP3.1 measures the native
resident read boundary against direct RocksDB. The graph rejects missing
checkpoint coverage, forward dependencies, and undeclared artifact inputs. No
checkpoint has a verified end-to-end golden-path receipt yet; prior component
receipts remain separately scoped.

`[VERIFIED]` The paired Tetris and Chess developer path now passes GP-G0 through
GP-G6 in bounded local scopes. It covers exact differential replay, seven
poison histories per workload, atomic application-record alignment, canonical
envelopes through three OpenRaft processes on one host, disposable RAM serving,
recursive immutable-object publication, copy-on-write forks, and reserved
garbage collection. The release-profile logical-size ratios are 427.6x for
Tetris and 25.0x for Chess. Both apps expose parent, fork version, write head,
and exact historical nodes in a Git-like lineage view.

This does not admit a production cell. GP-G3 is one-host process evidence;
GP-G4 is RAM only; GP-G5 and GP-G6 use the in-memory object adapter and pure
publication authority. GCS, replicated publication authority, independent
hosts, an SSD control, and the continuously integrated path remain
`[EVALUATING]`. GP-G7 is `[FUTURE]`. The design and scope are in
`docs/PLAYGROUND-GOLDEN-PATH.md`; the zoomed-out review is in
`docs/research/playground-g0-g6-architecture-review-2026-08-25.md`.

`[VERIFIED]` The bounded native resident-engine experiment is complete. It
materializes object base plus txLog suffix into `head`, `history`, and
`metadata`, atomically advances data with its frontier, binds snapshots to
generation and assigned range, and preserves older reads across advancement.
The first run caught an object-span versus assigned-range bug; the corrected
engine completed both final orders with zero anomalies and zero measured object
operations.

`[VERIFIED]` The topology-matched GP3.1 rerun admits the single-range native
read boundary. Final AB retained 90.89 percent of control throughput with
0.913x p99. Final BA retained 91.97 percent with 0.913x p99. All four runs
passed 16 hard gates and emitted correlated OTel logs, metrics, and traces.
D56 supersedes D52's permanent pivot conclusion. FoundationDB remains the
semantic oracle and fallback profile; native replicated commit is next.

`[VERIFIED]` The first leased R0 semantic run reduced the provider branch to
FoundationDB. FoundationDB 7.4.6 passed five implemented semantic gates with
zero anomalies. TiKV 8.5.7 committed both transactions in the write-skew
history and failed P1. The provider-neutral `okv-plane` contract, RFC-0041, and
preflight define the remaining lifecycle boundary.

`[VERIFIED]` GP2.5.2 now has a clean `okv-eval` plus OTel receipt at candidate
`ca9195186c4bd85573dddfe2d63a376693a031e9`. The positive FoundationDB plus GCS
run wrote a 205,262-byte closure, verified named GETs, advanced the object
frontier once, restored 950 rows into an empty generation in five idempotent
chunks, matched the state digest, fenced the previous generation, and recorded
zero anomalies. The three closure and generation poisons received `discard`.
All seven final semantic and lifecycle run IDs occur in logs, metrics, and
traces. Its 512.560 ms internal and 840.212 ms end-to-end timings are diagnostic
only. That GP2.5.2 receipt kept source provider media present; the separate
GP2.5.3 receipt below owns physical provider-media loss.

`[VERIFIED]` GP2.5.3 now has a clean real-infrastructure receipt at candidate
`50c72159781e14d3db06d792beac34838572fc91`. The source phase wrote a
950-record closure and manifest to exact GCS generations. Terraform then
deleted the source FoundationDB VM, boot disk, and provider SSD; the controller
observed all three absent before restore. A fresh destination cluster with
distinct provider and GCP identities restored and replayed five chunks,
matched the exact digest and row count, activated after ready, and committed a
fresh transaction. The formal positive received `keep` with 16 passing gates;
the executed same-cluster hidden-media control received `discard`. OTel logs,
metrics, and traces contain both run IDs. At that checkpoint, GP2.5.4
incarnation authority and GP3.1 retained-write overhead were still open.

`[VERIFIED]` GP2.5.4a now has a clean local-process receipt at candidate
`b415d502665eff9b6df4c095e33480b628348db2`. The combined generation and
publication authority positive received `keep` with zero anomalies and exact
fresh-process replay. The stale-source poison bypassed commit, routing, and
publication fences and received `discard` with exactly three anomalies. Both
run IDs occur in OTel logs, metrics, and traces. `[CODE-COMPLETE]` GP2.5.4b now
has a simultaneous dual-provider Terraform shape, a FoundationDB source-fence
and resurrection probe, a strict controller receipt, and formal positive and
poison evaluator workloads. Activation binds the exact source-fence receipt;
resurrection binds the activation and restart receipts, so VM clock skew is not
an admission input. `[EVALUATING]` the real GCP run is blocked on a fresh
operator authentication token. The FoundationDB fallback overhead pair remains
closed until that receipt passes; the native GP3.1 lane is independent.

`[VERIFIED]` The SSD mechanism now composes with the public kernel on real R0
infrastructure. `SingleRange` verifies the complete GCS closure, activates a
bounded RocksDB image with its local WAL disabled, applies the newer txLog
suffix, and serves exact versioned point reads after an empty-worker rebuild.
Across 15 samples it produced 516,973 reads/s median, 2.482 microseconds median
p99, zero object operations, zero anomalies, and a 4,351,739-byte serving
image. Direct RocksDB produced 702,142 reads/s and 1.749 microseconds p99. The
paired result is 26.37 percent worse on throughput and 41.91 percent worse on
p99, so this earlier wrapper candidate kept GP3.1 `[EVALUATING]`. The receipt is
`docs/artifacts/eval-receipts/single-range-ssd-gcp-r0-2026-08-27/README.md`.

`[VERIFIED]` The follow-up optimization bypasses manifest lookup and object
reference cloning once the complete RocksDB serving image is active. AB
measured 575,498 versus 713,304 reads/s and 2.490 versus 1.841 microseconds p99.
BA measured 573,999 versus 717,362 reads/s and 2.427 versus 1.867 microseconds
p99. Throughput entered the frozen envelope twice; p99 failed its new executable
constraint twice. The frozen receipts and architectural consequence are in
`docs/artifacts/eval-receipts/single-range-ssd-gcp-r1-2026-08-27/README.md`.

`[CODE-COMPLETE]` The public row-object point-read pilot now has a checksummed,
content-addressed manifest, data objects bounded below 4 MiB, one separately
cacheable sparse index per object, an exact one-range-GET reader, and a
complete-selected-object scan poison. The release local-filesystem sweep held
candidate reads to one request and about 64 KiB across 1, 8, and 64 MiB range
images. Candidate p50 stayed between 162.708 and 166.666 microseconds while
cached metadata grew from 2,240 to 104,754 bytes. G4.1 remains `[EVALUATING]`
until a clean source run, MinIO or GCS, OTel, concurrency, and cost receipts
exist.

`[EVALUATING]` G4.2 now has a release local-filesystem lazy empty-worker curve.
Across 1, 8, and 64 MiB assigned ranges, the candidate fetched one exact
manifest, one selected index, and one data block without LIST or full-object
hydration. As the complete closure grew 63.96x, candidate response bytes grew
1.19x and p99 grew 1.03x. At 64 MiB, full restore transferred 860.59x more
bytes and was 276.11x slower at p99. The result remains diagnostic because the
source is dirty, OTel is disabled, and local filesystem cache is not cloud
object storage.

`[EVALUATING]` G4.3 now crosses the next local process boundary. Three OpenRaft
authority processes selected generation 7, the authoritative row root, and the
logical txLog root. A first serving-worker process recovered the base and a
three-record post-object suffix, then was killed before its first read. A
distinct empty-scratch process repeated recovery and returned exact base,
update, delete, and tail-only insert reads. The lazy candidate reached 6.542 ms
p99 and transferred 35,811 object bytes. Full hydration was 1.68x slower and
transferred 29.96x more bytes. A skip-tail poison produced nine anomalies. This
remains a one-machine diagnostic with a local quorum-file adapter, not a
production replicated data txLog.

G4.4 targets replacement of that adapter with the actual OpenRaft data log or a
frozen retained-log stream, then holds exactness during concurrent commits.

`[EVALUATING]` G4.4 now crosses that boundary through a product-facing retained
transaction stream rather than raw Raft journal access. Three data-authority
processes retain accepted commands in state-machine snapshots and expose
linearizable frozen-target pages. The killed-worker replacement catches up from
`O = 9` through `C0 = 12`, observes four commits, catches up through `C1 = 16`,
and returns exact `Set`, `Clear`, insertion, and `ClearRange` outcomes. The
candidate reached 120.183 ms p99 from worker entry, moved 6,177 object bytes,
and used no physical journal path. Full hydration was 1.64x slower and moved
6.85x more object bytes. Skipping the concurrent suffix produced 12 anomalies.

`[CODE-COMPLETE]` RFC-0038 now packages the same recovery equation behind the
experimental `okv::SingleRange` API. Suite `single-range-kernel-v1` adds the
post-batching cursor invariant that G4.4 did not cover: a one-record page ends
inside a shared commit version and resumes by batch order. Local diagnostic
run `74b29fe1` passed the commit, process-kill, empty-replacement, exact-read,
bounded-object-I/O, schema, and budget gates. Clean-source OTel replay and
independent-media execution remain the next verification boundary.

`[EVALUATING]` Run `6723ce8a` extended that exact public path to real GCS with
required OTel. It crossed six local authority processes, one killed worker,
one empty replacement, and the exact manifest, index, and ranged-block object
path with zero LIST operations. All 12 gates passed; first correct read was
756.950 ms and total wall time was 7.254 seconds. This is a dirty
local-controller smoke, not a competitive performance result. The next gate is
the same suite from a clean digest-addressed bundle on the machine-bound R0
runner, not a larger distributed topology.

`[EVALUATING]` G4.5 rejects the current monolithic data-authority state shape.
With 256 live keys, the exact serialized snapshot grew from 280,547 to
2,573,288 bytes after an ideal projection removed every retained txLog command
through `O = C`. The 9.172x maximum crossed the frozen 2.0x ceiling. The no-pop
control grew 12.005x, and an incomplete retained-stream-only poison advertised a
flat 1.0x curve but was rejected with nine anomalies.

`[EVALUATING]` G4.6 implements that split. With replicated `R` and `Q(client)`
advancement and projected `O = C`, complete state stayed between 130,671 and
131,047 bytes from 256 to 4,096 lifetime commits, a 1.0029x maximum. The
object-only control grew 9.1694x and the incomplete serving-only poison was
rejected with nine anomalies. Expired retries failed closed and fresh-process
replay was exact. The candidate missed its execution budget, taking 545.726
seconds versus 180 seconds, so no performance or verified-state claim is
admitted.

The next recovery gate is a generation-fenced object-frontier certificate and
crash-safe physical txLog pop. The parallel performance problem is explicit:
the sequential sync-per-entry process path needs commit batching or an
equivalent group-commit mechanism. Safe pop and batching require independent
correctness and performance receipts; neither result can substitute for the
other.

`[EVALUATING]` G4.7 closes that application-level recovery gate on one local
machine. A publication quorum first retained an exact pending row manifest.
The controller validated every manifest, index, and data block, then the data
quorum physically removed all 16 retained transactions through floor 18.
Three data voters certified the exact applied frontier before publication
activation. Object recovery remained exact after both authority leader
failovers and restart of the killed data voter. The candidate reached 160.331
ms protocol p99 and passed its 90-second wall budget.

The controls isolate the invariant. Without a pending frontier, all 16 txLog
records remained. A manifest claiming coverage beyond its actual bytes failed
complete validation before pop. With one voter signature, the pop remained
recoverable from objects but publication activation failed and the pending
manifest stayed protected. These receipts are diagnostic because they use
dirty source, debug processes, one host, and local object files.

`[EVALUATING]` G4.8 isolates the first commit-path performance mechanism. A
release candidate with at most 32 independent transactions in flight preserved
exact final values, retained-stream order, retry outcomes, leader failover, and
killed-voter recovery. It reached 153.708 median durable transactions per
second versus 38.772 for the same-durability sequential control, a 3.964x gain.
The candidate was discarded because it missed the frozen 200 transaction per
second gate, exceeded the 250 ms p99 ceiling at 264.887 ms, and missed the 4x
paired gate. Followers grouped 10 to 15 entries per append, while the leader
still synchronized every transaction entry independently. The early-ack poison
appeared much faster but lost its acknowledged transaction after quorum
recovery and was rejected.

`[EVALUATING]` G4.9 clears that local mechanism gate. One explicit Raft entry
carried 16 independently identified transactions with a shared commit version
and distinct batch orders. The candidate reached 559.511 median durable
transactions per second and 34.016 ms maximum p99 versus 151.944 transactions
per second and 262.174 ms for the one-entry same-durability control. The 3.682x
paired gain cleared the frozen 2.5x requirement, and every seed produced 16
logical transactions per leader stable append.

The candidate also preserved exact paginated recovery across batch boundaries,
individual and whole-batch retry, deterministic in-batch conflicts, final
values, leader failover, and killed-voter restart. A duplicate identity failed
before mutation. The early-ack poison appeared to reach 13,179.572 transactions
per second but lost its acknowledged outcomes after quorum recovery and was
discarded. The mechanism is retained by D40. It is not verified because the
source is dirty, all voters share one host and filesystem, and OTel is disabled.

`[EVALUATING]` G4.10 starts from independent requests and supplies the missing
bounded commit-proxy policy. The original 16-item, 64-caller configuration
reached 581.791 median transactions per second but was discarded because
131.488 ms maximum p99 missed the frozen 100 ms ceiling. The 32-caller control
reached 595.440 transactions per second and 63.398 ms maximum p99, identifying
the local admission knee rather than justifying a weaker latency gate.

`[EVALUATING]` A separately frozen G4.10a.1 candidate uses 32 items at 64
callers. It reached 1,157.369 median transactions per second, 76.101 ms maximum
p99, and exactly 32 logical transactions per leader append. The same-durability
one-entry control reached 182.093 transactions per second, a 6.356x paired
gain. Sparse traffic closed one-item batches on delay. The 128 KiB byte control
closed at eight 8 KiB-value transactions and 119,731 bytes. Overload admitted
and resolved 32 requests while rejecting 480 before replication, and the
oversized poison failed before admission and mutation.

The 32-item configuration remains an experiment envelope, not a public product
limit. `[EVALUATING]` G4.10b now composes it with controlled conflicts and
authenticated object-frontier advancement. The 25% conflict candidate reached
1,075.343 median resolved outcomes per second, 104.274 ms maximum p99, 31.030
minimum outcomes per leader append, and 95.673 ms maximum frontier time. The
same-durability one-entry control reached 37.369 outcomes per second, a 28.776x
paired gain. The no-conflict control reached 1,093.306 outcomes per second.

Every seed reconstructed exact final `C` from objects through frozen `O` plus
retained transactions `(O, C]`. Committed and conflicted retries, both leader
failovers, killed-voter restart, and a fresh controller remained exact. The 75%
conflict curve remained bounded. Moving-frontier and premature-pop poisons were
rejected before unsafe pop, with every prefix record retained. The mechanism is
`[CODE-COMPLETE]`; receipts remain `[EVALUATING]` because the source was dirty,
OTel was disabled, and all six processes shared one host.

`[CODE-COMPLETE]` G4.11a now persists and reopens exact state-machine snapshots
before OpenRaft purge, then compacts each node journal to its canonical vote,
committed marker, purge marker, and retained suffix. Candidate run `7eeaa179`
reduced at most 6,391,575 journal bytes to 879 bytes and preserved exact state,
retained-stream, retry, full-quorum restart, and new-suffix behavior. Poison run
`8fb8a75a` rejected purge before snapshot without changing physical journal
bytes or purge state.

The G4.11a receipts remain `[EVALUATING]` because the tree was dirty, OTel was
disabled, and all voters shared one host. They also exposed a state-shape
failure: three unfrontiered snapshots totaled at most 5,066,472 bytes for
131,072 logical workload bytes, 38.66082x physical amplification. Journal
reclamation works, but the snapshot still retains lifetime recovery and retry
state.

`[CODE-COMPLETE]` G4.11a.1 aligns `R`, a 64-request `Q(client)` window, and
authenticated `O` across four local process cycles, then snapshots, purges,
restarts both full quorums, and proves exact retry plus object-and-suffix
recovery. Candidate `be53d36c` passed the 1.25x growth gate at 1.091759x but
failed the 8x complete-media gate at 19.692719x. No-retry-frontier control
`9b236c46` reached 54.803467x and 2.195933x. Accounting poison `829e35c4`
reported 0.05365x while independent accounting found 19.692719x. The receipts
remain `[EVALUATING]` because the source is dirty and all six processes share
one host. The current physical snapshot shape is discarded.

`[EVALUATING]` D46 now freezes the storage-truth question before cloud topology
work. The row-object base remains the control. A manifested multi-layout LSM
candidate uses row-oriented L0 deltas, random-access columnar compacted runs,
one kernel-owned primary access path, and one authenticated object closure.
The architecture fork must pass exact point, scan, update, compaction, media,
HTAP, and branch curves locally and then on GCS. The research contract is
`docs/research/columnar-lsm-source-of-truth-2026-08-26.md`.

`[CODE-COMPLETE]` The first same-history runner covers the row-object,
indexed-Parquet, and hybrid subjects plus three negative controls. A small
local debug-build preflight preserved exact semantics. Parquet improved the
projected scan rate 1.873x but used 10 point requests and 16.35x the row
control's response bytes. The hybrid used four requests and 1.925x the row
control's stored/live amplification. These `[EVALUATING]` observations reject
plain Parquet as the generic point format and narrow the next work to Vortex,
coalesced checksum-block reads, or an honestly accounted typed sidecar.

`[CODE-COMPLETE]` The follow-up implements both request-coalesced Parquet and a
fully counted split typed run. The split run keeps exact MVCC rows in the
indexed row sidecar and writes only the declared analytical projection to the
columnar object. The parent manifest authenticates the columnar object, its
access index, and the nested row closure.

`[EVALUATING]` Release-local admission run `f5dbba62` alternated the split run
and row control across seeds 5701 through 5703 and three repeats. It passed all
frozen gates in 18.504 seconds: point requests 1.000x, point bytes 1.000x,
median point p99 1.033x, projected scans 9.124x, storage amplification 1.030x,
compaction write amplification 1.035x, and resident index 1.137x versus the row
control. Suite and profile hashes are
`ee5144cd74de848c6a73a3b014d40e247fc80ce14443d073aeafff39dfa9a215`
and `15ca0412b49870a0df3e14bc4eea5572186d7678a5988d8e6a6b70b708fa4fe0`.
The source was dirty and OTel was disabled, so this promotes the mechanism to
clean GCS evaluation only.

Before G4.11b, the split typed run needs namespaced GCS cold and warm point and
scan curves, exact recovery from the complete split closure, and the existing
DataFusion base-plus-tail overlay over its columnar projection.

`[CODE-COMPLETE]` A second candidate now removes the durable row sidecar. The
`columnar_range_overlay_candidate` stores MVCC metadata and typed fields in
checksummed projection stripes, opaque values once in checksummed payload
pages, and a compact resident key-range index. Its RangeEngine cache is
disposable and bounded to 16 MiB in the frozen profile. Empty-cache reopen
loads the manifest and index, performs an exact point read, and reconstructs
the complete logical history without `LIST`.

`[EVALUATING]` Release-local run `49d6cd06` alternated this columnar candidate
with the indexed-row control across three seeds and three repeats. All local
gates passed in 19.002 seconds: point requests 1.982x, point bytes 0.353x,
median point p99 0.839x, projected scans 4.718x, storage amplification 1.010x,
compaction write amplification 1.010x, and resident index 1.170x. Warm replay
issued zero object requests, projected scans read zero opaque payload bytes,
and restart anomalies were zero. The largest observed cache was 13,186,555
bytes under the 16,777,216-byte bound. Suite and profile hashes are
`c92826009b7a4b73577cfc7bf28ae031b73f8e063e2a911051ef4cce035fdf90`
and `fc33b5a08b5cc1afbc0839f201d8a21ad5ed1dfd8476a85b47298e96d59a2324`.
The source was dirty and OTel was disabled, so the result remains
`[EVALUATING]`.

`[CODE-COMPLETE]` The direct DataFusion gate now exposes these exact `OKCP`
projection stripes through `RangeStripeTableProvider` and `RangeStripeExec`.
One-stripe control `d788c75f` reached 1,246,835 median source rows per second
with 1,761 projection requests across nine samples. The bounded 256 KiB scan
fetch candidate `a7d4f3bf` reached 2,543,552 rows per second with 54 requests,
identical projection bytes, zero payload requests, a 257,506-byte maximum fetch
buffer, and a 1,646-byte maximum Arrow batch. Payload-prefetch poison
`b4fe7c11` added 1,761 requests, tripled bytes, and reduced throughput to
820,215 rows per second. The dirty local receipts remain `[EVALUATING]`.

The architecture fork now favors the columnar permanent base with two access
granularities: point-sized stripes and bounded coalesced scan ranges. It is not
closed. The next gates are exact base-plus-live-tail execution over this source
and a namespaced GCS run. The split sidecar remains the fallback control if
remote two-request point latency fails.

`[EVALUATING]` CloudJump III adds a production comparison for the serving
hierarchy. It uses DRAM, volatile local SSD, durable network block buffering,
and asynchronous versioned object publication. It does not use a columnar
source of truth. The review preserves the C5 experiment but adds cache-ratio,
Zipf-skew, admission-policy, publication-unit, background-debt, and recovery
curves. The central falsifier is whether retained quorum `txLog` plus
disposable materialization can replace CloudJump III's durable ESSD page-image
buffer while keeping recovery and foreground p99 bounded.

`[CODE-COMPLETE]` The first CloudJump-derived cache-policy suite now exposes
full admission, a never-admit control, and bounded ghost two-chance around a
scan-pollution phase. `[EVALUATING]` At 20 percent cache and Zipf alpha 1.4,
ghost admission reached a 74.46 percent post-scan local hit ratio versus 71.34
percent for full admission and reduced post-scan requests by 16.2 percent over
three seeds and three repeats. Real GCS canary runs `8574f64c` and `a1c6be8a`
then reached 32.03 and 42.19 percent hit ratios; ghost admission reduced
post-scan GCS requests from 161 to 128 and wall time from 75.06 to 67.14
seconds. Exactness and capacity gates passed, but the dirty source, one GCS
seed, and disabled OTel keep both results inconclusive.

`[CODE-COMPLETE]` Namespaced GCS execution now wraps the existing backend under
`objectkv/evals/storage-layout/<run-id>/<subject>/<seed>/<repeat>`. The frozen
`storage-layout-gcs-admission-v1` suite uses the same history and admission
ratios as the release-local run. `[EVALUATING]` The project
`doss-objectkv-dev` and versioned `us-central1` bucket
`doss-objectkv-dev-okv-evals` were observed through live GCP APIs. The original
16,384-key, nine-repeat serial profile was stopped after five minutes at its
first sample because it was not a bounded cloud diagnostic. The smaller cache
admission canary completed through the same GCS adapter. The full paired layout
admission, object-authority conformance, clean source, and required OTel receipt
remain open.

G4.11b then runs eight frontier cycles from one clean exact revision across
three hosts, each with independent persistent roots for one data voter and one
publication voter, plus a remote GCS backend and required OTLP. It records host,
disk, binary, object, and journal identities; kills hosts during commit and
publication; and proves final object-plus-suffix recovery. That run decides
whether the native authority is credible enough to join the first Cell v0
vertical slice. It starts only after the G4.11 storage-layout fork admits a
bounded local state representation.

An independent three-machine stable-media plus GCS run is not the next storage
gate. It follows only if the resident, cold-read, and object-recovery curves pass
and is required specifically to verify durable quorum and host-loss claims.
GCP access and a bounded real-GCS canary now work; independent-machine
provisioning, required OTel, object-authority conformance, and the continuously
integrated cell remain open. Local durable snapshot and journal work continues
without weakening that external gate.

`[VERIFIED]` RFC-0045 L0 now has a clean-source deterministic protocol receipt
at `efc723e2722a9ad49dedab598349230a49eae51f`. Across seeds 1103, 2207, and
3301, the candidate passed all nine gates with zero anomalies. It covered 21
checks, including three acknowledged appends, three recovered unknown outcomes,
nine writer takeovers, six repaired records, three stale-writer rejections,
three conflicting-retry rejections, three published segments, three ignored
orphan objects, and three bounded-queue rejections. Each of six targeted
poisons produced one anomaly per seed and received `discard`. The receipt is
`docs/artifacts/eval-receipts/staged-txlog-l0-gcp-r0-2026-08-30/README.md`.

This result verifies only deterministic protocol semantics. It does not start
log-node processes, synchronize NVMe, send network appends, publish GCS
segments, measure latency or throughput, verify transaction commit, or replace
OpenRaft.

`[VERIFIED]` RFC-0045 L1 now has a clean-source process receipt at
`8a225cac10c51d65fbe08fe2933bbea9eac782c6`. Across seeds 1103, 2207, and 3301,
the candidate passed every gate with zero anomalies. It started and killed 18
real child processes, completed 12 acknowledged appends through 54 TCP append
requests, recovered every acknowledged record, repaired every injected torn
tail, rejected stale writers, preserved exact retries without journal growth,
and constructed byte-identical segments. Early-acknowledgement, stale-writer,
and divergent-segment controls each received `discard`. The receipt is
`docs/artifacts/eval-receipts/staged-txlog-l1-gcp-r0-2026-08-30/README.md`.

L1 verifies one-host process and local-journal mechanics only. It makes no
independent-media, GCS-publication, append-latency, throughput, transaction
commit, or OpenRaft-replacement claim. L2 same-zone independent-machine
evaluation is the next staged txLog rung after the active read-path rows.

Do not expand MultiRaft, PostgreSQL, or metacluster scope until the resident
read and bounded cold-object lookup curves clear their controls.
