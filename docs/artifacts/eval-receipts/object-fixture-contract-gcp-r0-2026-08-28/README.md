# RFC-0044 phase-1 object fixture contract

Status: `[VERIFIED]` semantic contract. This is not a performance receipt.

## Result

Clean source `fc8189e30c2e46d79cc99f3c2068b3cecd8e93e3` ran the
`object-fixture-contract-v1` release suite on one disposable
`c3-highcpu-22` GCP machine in `us-central1-a`. The candidate and all four
poison workloads returned `keep`; every formal hard gate passed.

The 4 MiB logical base produced 11 immutable objects totaling 4,306,945
bytes. The exact candidate observations were:

```text
base anchor O                 2
anchor txLog records          1
anchor mutations              0
anchor live keys              0
base-value txLog records      0
base-value mutation bytes     0
decoded base records          4,096
canonical tail records        7
fixture_reused                false
immutable PUT reuse verified  true
correctness anomalies         0
```

The local temporary provider correctly reports `fixture_reused=false`.
Persisted cross-subject reuse is reserved for the later GCS preflight. The
second local construction instead proved that every create-if-absent returned
the exact existing bytes and that the descriptor identity was stable.

Candidate identities:

```text
fixture ID                    7cd233a687f25e40931a94ab082956dc048e84041a3816b13a46748627949997
tail SHA-256                   90a82886bf4c86521a4506dd03366bbb3eef02fb3ad97acf37d2605f4032cfe4
native resident image ID      81cd367e08d645d7714ba6f062b00f40d8b1293b5f85295a476ba3325cbe5648
control resident image ID     abff52cb0e003420be740b2ba07f9249643ebe67144a945c9160e1aa306ca5fa
logical resident SHA-256      1c64c6ca9fafc9dd6e3844141fe798179dd2593a9b65c6fe8bb87f5df1c479c9
semantic SHA-256              2e33a8792de96ff116f68740b513d3e8fadcdb5f130674e7b01a4dbe0a7e9f97
```

The native and control resident IDs differ because their provider and codec
identities differ. Their complete tagged logical image digest is identical
across values, tombstones, and declared absence.

## Negative controls

The suite rejected each deliberate invalid construction:

- corrupt descriptor bytes;
- base version different from the authority anchor;
- retained tail with a different commit position;
- native and control sharing one mutable resident root.

## Verification boundary

`cargo build --release -p okv-eval` passed with Rust 1.88.0. The two focused
release tests for fixture identity sensitivity and logical outcome tags
passed. The suite validated against 68 registered metrics, five workloads,
and its result schema. The suite hash is
`240628cfe50209fdbb60c73359013168b690ee8c507921085a864e84e8d80cfa`.
The frozen T27 rerun suite remained byte-identical at
`b7f8ca03bfb1104680e9e65681487ec86b0c719d4faa06d14f37b77cfd9227a3`.

This phase verifies descriptor, closure, version, tail, and semantic resident
identity construction. It does not yet build the native and control RocksDB
images from the fixture, persist cross-subject reuse in GCS, emit required
OTel signals, or measure a workload curve. The next gate is the 4 MiB
fresh-process recovery slice, followed by the 64 MiB GCS setup preflight.

Durable evidence prefix:
`gs://doss-objectkv-dev-okv-evals/runs/rfc0044-fixture-contract-r0-20260828/fc8189e/`.

The disposable VM and boot disk, both temporary SSH firewall rules, the
temporary project SSH key, and 4.5 MiB of local source/key archives were
removed after evidence capture. The worktree remains 7.7 MiB with no local
Cargo target directory.
