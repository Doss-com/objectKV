# T27 fresh-process 64 MiB preflight, GCP R0, 2026-08-29

Status: `[VERIFIED]` for the four-position 64 MiB preflight, read-only fixture
consumption, direct NVMe treatment, paired admission bounds, collector-side
OTel signals, immutable evidence, and complete infrastructure teardown. T27
and master-matrix row 1 remain `[EVALUATING]` until the frozen 1 GiB coverage
and skew sweep passes.

## Result

```text
project:                       doss-objectkv-dev
runner:                        private n2-standard-8, Intel Cascade Lake
hot scratch:                   375 GiB GCP local SSD, NVMe, ext4
stable volume:                 200 GiB pd-ssd
collector:                     private e2-standard-2, OTel 0.157.0
source revision:               578c6a919cbd2e0e6969eaa134fdfae7d6112446
source archive SHA-256:        645ffae84dfe1d509b3323643c5c8a2d5d1be173ebc72ad944cdc893b3586398
binary SHA-256:                c3dd8e9e087f0ae96733cd3b05a510e08e8a23b5dfdb1fe7c2d13cd1274a5a92
machine receipt SHA-256:       1d4ad0a414c85a7267c5059d4751dd197d6197a6e74cabc4a277dc1bd224ecde
fixture ID:                    dca5bffd1580958aed9a0dba708b6ea59950147936e62b0cb93f52151b72c539
fixture locator envelope:      c176323fcd84be5f06831dd5e06ffe2e31619c0e856ccb8f246d2481c4cba92a
fixture base version:          2
fixture seed:                  4244
fixture logical bytes:         67,108,864
fixture persisted bytes:       68,857,626 across 20 GCS objects
plan SHA-256:                  f44fb88c95495f9cd613db36f79f5841a39f92f86ca860b4c2625a4c03935305
trace seed:                    1103
cache coverage:                20 percent, 13,421,772 bytes
access trace:                  Zipf 1.4
clients:                       8
warmup reads/position:         256
measured reads/position:       1,024
direct table reads:            true
run ID:                        f7209e3e-4d5b-4cc2-83d8-b87ead6d8891
run receipt SHA-256:           57c2074ce4967a851822b67dd3902fafd1f7f12824192e290042ff248ef5f52d
run receipt file SHA-256:      bdd89b59c35ddade40ad399d950938b7cd2f4f20fb17f74fff8a8bfe416560e1
infrastructure teardown:       9 resources destroyed, empty Terraform state
```

The runner service account had exactly `roles/storage.objectViewer` during
plan construction and execution. Each position started a fresh wrapper and a
fresh measured worker against one generation-pinned object fixture. Candidate
and control each owned one RocksDB database and one explicit block cache.

## Position measurements

| Position | Subject | Throughput, reads/s | p99 | CPU/read | Physical bytes/read | Cache misses/read |
|---:|---|---:|---:|---:|---:|---:|
| 0 | Native snapshot | 217,143.85 | 255.264 us | 5,017.58 ns | 592 | 0.0723 |
| 1 | Direct owned RocksDB | 250,979.47 | 254.039 us | 4,681.64 ns | 556 | 0.0674 |
| 2 | Direct owned RocksDB | 208,372.25 | 278.829 us | 5,062.50 ns | 564 | 0.0684 |
| 3 | Native snapshot | 202,924.81 | 275.540 us | 4,959.96 ns | 600 | 0.0732 |

Every position returned exact values, zero correctness failures, zero measured
object requests, an initially empty scratch directory, and measured cache and
physical-read pressure.

## Paired admission

| Order | Native throughput/control | Native p99/control | Native CPU/read/control | Native physical bytes/read/control | Read amplification ratio | Result |
|---|---:|---:|---:|---:|---:|---|
| AB | 0.8652x | 1.0048x | 1.0718x | 1.0647x | 1.0000x | pass |
| BA | 0.9739x | 0.9882x | 0.9797x | 1.0638x | 1.0000x | pass |

Both orders cleared the frozen preflight bounds: throughput at least 0.80x,
p99 at most 1.20x, CPU/read at most 1.25x, physical bytes/read at most 1.25x,
read amplification at most 1.25x, and nonzero cache plus physical pressure.
The sample is deliberately short and admits only the preflight mechanism. It
does not estimate steady-state tail latency or replace the 1 GiB, 540-position
T27 workload.

## Telemetry

The sealed receipt records successful flush and shutdown for logs, metrics,
and traces before its digest is constructed. Independent collector inspection
found the run ID in five log payloads, two metric payloads, and four trace
payloads. The metric payloads include:

```text
okv.eval.correctness.failures
okv.eval.object_store.requests
okv.eval.operation.duration
okv.eval.operation.throughput
okv.eval.process.cpu_per_operation
okv.eval.process.io_bytes
okv.eval.rocksdb.cache_requests
okv.eval.rocksdb.read_amplification
```

The SDK emitted one warning because the controller attempted a final tracing
log after shutting down its logger provider. This did not affect the sealed
signals or result. The post-shutdown log call was removed after the run; 25
focused T27 tests and all 105 RocksDB-featured `okv-eval` tests pass locally.
There is no lightweight local seam that reproduces the full provider-shutdown
warning without a real controller and collector, so the next real run remains
the end-to-end regression check.

## Preserved failures

1. The first 64 MiB fixture used base version 1. The independent oracle
   correctly required canonical empty-anchor version 2, so plan construction
   failed before measurement. Source `578c6a9` now rejects this locator at the
   plan boundary with `fixture placement has the wrong T27 empty-anchor
   version`. The rejected 68,857,626-byte fixture was removed after its locator
   and failure were preserved.
2. A valid 4 MiB preparation attempt under `roles/storage.objectViewer` failed
   with `permission_denied` and created zero objects. This verifies that the
   measured principal could not replace or augment its fixture.

## Durable evidence

```text
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-t27-fresh-preflight-r0-20260829/evidence-v2/
```

`GCS-EVIDENCE.tsv` binds the generation, byte count, and SHA-256 of the runner,
collector, and machine bundles. Bucket versioning is enabled. The canonical
fixture remains under the same run prefix for exact replay. The private runner,
collector, persistent disks, local NVMe, firewall rules, subnet, router, and
NAT were destroyed after evidence capture.

## Next gate

Keep T27 `[EVALUATING]`. Run the remaining capability and schedule poisons,
then execute the immutable 1 GiB plan across 50, 20, and 5 percent cache
coverage and Zipf 0.8, 1.4, and 2.0. Only that sweep can admit master-matrix row
1 and unlock the GCS cold-point and object-layout curve.
