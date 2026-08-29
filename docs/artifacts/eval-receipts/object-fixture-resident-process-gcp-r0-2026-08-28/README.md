# RFC-0044 fresh resident processes, GCP R0, 2026-08-28

Status: `[VERIFIED]` semantic process gate. This is not a T27 performance
receipt.

Clean source `1ae2eded0c7d4da856aa8bbb65d5cacb3c500b28` ran the
`object-fixture-resident-process-v1` release suite on one disposable
`c3-highcpu-22` machine in `us-central1-a`. The candidate pair and regenerated
control poison both returned `keep`; every formal hard gate passed.

The candidate started independent native and direct-control process trees from
empty scratch. Both verified and consumed one 4 MiB content-addressed closure
at `O=2`, then applied the same exact seven-record txLog suffix through version
10.

```text
fixture ID               29d0f21103303e36bba2d91a9415f41e500c0a184bbdcf48ea088994a58a3383
tail SHA-256              c267309a03539fd0d7af546f274fc8572e1ed625421eb52d1c2e01f9341c5d8f
logical image SHA-256     b361a5ec674b6e4928343d5c7360450ff2d5bcf15ccb0783b203a96db08c3922
native resident ID        6a13b7c5021f745d285bc33bbdf3fca28a13a7eff9103314d1b2b1bb6b12c518
control resident ID       9ae1fcacff044cd94ca8bced45a4d434e5fd97fc55775e35e4ddae0fef19b5bf
native local bytes        8,769,143
control local bytes       4,331,990
complete image outcomes   4,099 per subject
txLog response bytes      16,707 per subject
hot-read object requests  0 per subject
hot-read wrong outcomes   0 per subject
```

The complete image digest covers values, tombstones, and one declared absence.
Physical IDs differ because native and direct control use different providers
and encodings. The control is populated from the verified native snapshot, not
from the data generator.

The poison changed one control outcome after deriving the control image. The
worker failed closed with `regenerated control diverges from verified object
fixture`; the formal poison receipt returned `keep` after detecting that exact
failure.

The suite hash is
`ca996bed49229bbfce04104f154b669d7160077f6cfa77241bfc37a0cc3d2f65`.
Rust was `1.88.0`; the release binary SHA-256 was
`9a104cb2eef6bf71d0eda134c255ace0ce9c4c7e2b0fa013a706a5807c044e60`.
The frozen `native-resident-cache-pressure-rerun-v2` suite remained
byte-identical at
`b7f8ca03bfb1104680e9e65681487ec86b0c719d4faa06d14f37b77cfd9227a3`.

```text
control-trace.json  75c019b28b6d217bfe34cb853ee0982937de43479a9bee8fa3438e5357d224db
native-trace.json   c90cce144a4daa3d91e57f04595a89e1db2db9d7c6ccd40a410a46c991e6af35
pair-receipt.json   6a2a76a19a3e75f1e78b5198dbac9e3d667544efcad7398435dcba9ec848b5c1
poison-receipt.json 5cd46686b1c0f62814c1ad18e15db8a891543eb3fb349341fddce24b2413350c
poison.stderr       d2da020cdc9cc604f684cc27c519f7066cd875cb59eb740441ef1ffe46714149
```

The 1,024-read windows are semantic diagnostics only. They are too short and
do not have the frozen T27 cache-coverage, sample-count, telemetry, process
order, or comparison contract. No throughput or latency point is admitted.

The six-file receipt set is durably copied to:

```text
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-resident-process-r0-20260828/
```

The initial upload contains 19,503 bytes. The pair receipt is GCS generation
`1787969788019294`; the poison receipt is generation `1787969798052762`.
Their repository SHA-256 values above bind the exact payloads.

The disposable VM, boot disk, ephemeral external address, firewall rule, and
temporary project SSH key were removed after evidence capture. Exact-name GCP
queries returned no remaining benchmark resource. The original project SSH key
remains installed. The local receipt set is about 17 KiB; the clean source is
pushed to the repository.
