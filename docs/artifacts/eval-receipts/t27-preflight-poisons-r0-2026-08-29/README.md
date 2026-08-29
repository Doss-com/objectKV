# T27 preflight poison replay, R0, 2026-08-29

Status: `[VERIFIED]` for the AABB, missing-position, option-mismatch,
hidden-native-provider, and missing-locator negative controls against the exact
64 MiB GCP preflight evidence. T27 and master-matrix row 1 remain
`[EVALUATING]` pending the frozen 1 GiB coverage and skew sweep.

## Boundary

```text
source revision:              9ca447d6dcca7ca4832f489445e0623711ee0748
verifier binary SHA-256:      63752aaa058e02792e6f66e7c83c56172f23421c76059c4cd0c2d7f26633be84
source evidence archive:      17305822c5e13be3499faac620cd269b64ceb8ff3d21a685c6c0cb764964197c
source plan SHA-256:           f44fb88c95495f9cd613db36f79f5841a39f92f86ca860b4c2625a4c03935305
source direct receipt:        ac990b964bc06f8245b1b87376026966da2e24c3669efd287b4b5b031fb7963c
fixture locator envelope:     c176323fcd84be5f06831dd5e06ffe2e31619c0e856ccb8f246d2481c4cba92a
fixture objects and bytes:    20 and 68,857,626
new benchmark compute:        none
```

The exact preflight archive was fetched from its versioned GCS generation and
matched the SHA-256 recorded in the original evidence index. The verifier
binary was built from the named commit with no Rust or schema differences from
that revision. These controls exercise evidence admission. They do not rerun
or replace the measured performance positions.

## Structured poisons

| Poison | Isolated mutation | Expected and observed rejection | Receipt SHA-256 | Result |
|---|---|---|---|---|
| AABB schedule | First ABBA block becomes AABB; subject-specific option hashes are recomputed | `T27 execution plan positions differ from the frozen contract` | `a65049582ff22cafadb9a6793fe62d585c424217adb05fc0fc40afce5eb5bdd2` | pass |
| Missing position | Final position removed; plan digest recomputed | `T27 execution plan positions differ from the frozen contract` | `ec2d53136c6c0e85e6315a125e7afac6fa2639633442779df9c62bf92b0b58d0` | pass |
| Option mismatch | First block-cache budget incremented without changing its treatment identity; plan digest recomputed | `T27 execution plan positions differ from the frozen contract` | `c66dbc93775798ec70e3ba9eb24da1871fa2bf27e56f697ce2f4efe1d60ac255` | pass |
| Hidden native provider | Real direct-position receipt gains one runtime resident-provider field; position digest recomputed | `T27 direct position opened a hidden runtime provider` | `0f2d391cdf21f8e6c5d4e361d8c56bd513fe8b854d49a98c09953fac8f0b6b7d` | pass |

Each command first authenticated the unmodified plan or position receipt. Each
poison recomputed its internal digest, so rejection did not depend on a stale
digest. The poison receipt binds the source, exact poisoned-file digest,
expected rejection, observed rejection, and its own canonical digest. Both
receipt schemas passed their focused tests.

## Missing locator

The same binary invoked `t27-plan-build` with an absent locator and otherwise
complete arguments. It exited 1 with `No such file or directory`, before
reading the Cargo lockfile, machine receipt, scratch root, or object fixture.
The requested output plan did not exist afterward.

The complete versioned fixture listing hashed to
`ca2e4bc1f2ea0f8079ca8702c9965738be2f3458bc1f777265295b100e8e3063`
both before and after the attempt. It retained the same 20 generations and
68,857,626 bytes. This verifies the missing-locator fail-closed boundary and
absence of fixture mutation for this exact process. It does not prove all
possible credential failures or object-store faults.

## Validation and storage

Twenty-five focused `t27_plan` library tests pass. The current binary compiles,
both JSON schemas parse, and all structured poison commands exited 0 only after
observing their required rejection. The full artifacts are immutable in the
versioned bucket at:

```text
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-t27-fresh-preflight-r0-20260829/evidence-v3-plan-poisons-9ca447d/
```

`GCS-EVIDENCE.tsv` binds all eight uploaded generations, byte counts, and file
digests. No benchmark resources were created. The benchmark Terraform state
remained empty.

## Next gate

The 64 MiB negative-control boundary is complete. Prepare one immutable 1 GiB
fixture and its plan under a bounded infrastructure lease, then execute the 27
cache, skew, and seed strata. Row 1 changes to `[VERIFIED]` only if every
stratum passes its correctness, resource, telemetry, throughput, p99, CPU/read,
physical-byte, and read-amplification gates.
