# T27 1 GiB fixture and frozen plan, GCP R0, 2026-08-29

Status: `[VERIFIED]` setup and plan boundary. This is not a T27 performance
result. Master-matrix row 1 remains `[EVALUATING]` until the 540-position
cache-coverage and skew sweep executes and passes.

## Result

One clean source revision built a RocksDB-enabled Linux evaluator on the
private GCP R0 runner. A temporary writer authority published one immutable
1 GiB logical fixture to regional versioned GCS. The writer role was then
replaced with `roles/storage.objectViewer` before the evaluator exact-opened
the generation-pinned closure and froze the complete admission plan.

```text
source revision:               9cf501460cd09e08c41e2f6064667b9fe24e7d9d
source archive SHA-256:        89975feb25537a91bb6989e66370f2816df413298dce05c65aa93a3916bc3315
binary SHA-256:                f3471d07e7bc8fc7efecb63dccfc5d020043547c2c38460b7f4a58ba4d2dd34a
Cargo.lock SHA-256:            85fd5d79ab99965dd3eac6fbba955d57045de2b48e4d2a4bc3ab1d30e2698201
suite contract SHA-256:        3b723980babf4bcbbec895c2cec75a1c1583a819c88d79bbda251438d52460a0
machine receipt SHA-256:       560da2de16020dff23d773cc9ff649b5569746007d535fb2e9edfc4cd002afbc
fixture ID:                    728d2ce3bc996c9786c9d9b194f50542c3edcba2abd417c7def27273788163c2
fixture locator envelope:      768e1a9b8ee91a16615dd69b89d15ba581667a9d5ab6e5190b5de663efcc024d
fixture locator file SHA-256:  c11ee3b383edafce2c73ec68c49ee1559636a46d26e519ce7d4a1b682b64f4cb
descriptor generation:         1788020925446068
descriptor SHA-256:            192b137d99f16cd05310abe4080d98a6e613a9b99cdbc003336cd25ccbbe3312
fixture seed:                  4244
fixture logical bytes:         1,073,741,824
fixture physical bytes:        1,101,701,925 across 266 objects
fixture listing SHA-256:       bae476e0a9dee258959f6c43e5031486d44ac72007d729e94f1b85c5eacb80c5
plan SHA-256:                  b76be02aa012ce3646104e56c1b9c2c6118ee046b33a419103ae7bfdba433de2
plan file SHA-256:             f024e4b5649c1f1245939d224a3ac9657ada831ef0fb9deee58df2d4a820ddbc
```

The suite contract digest is the exact RFC-0044 phase-5 document at the source
revision. The plan itself additionally binds every position, treatment,
engine option, fixture identity, semantic oracle, executable, machine, boot,
NVMe filesystem, and infrastructure lease.

## Frozen plan

```text
3 cache levels x 3 Zipf skews x 3 trace seeds x 5 ABBA blocks x 4 positions
  = 27 independently gated strata
  = 540 fresh-process positions
  = 540,000,000 measured reads when executed
```

The independent check found exactly 540 positions, 27 strata, 270 native
subjects, 270 direct-owned RocksDB subjects, and only the declared cache
budgets of 536,870,912, 214,748,364, and 53,687,091 bytes. Every position uses
direct reads, eight clients, 200,000 warmup reads, and 1,000,000 measured
reads. Trace seeds are 1103, 2207, and 3301; access patterns are Zipf 0.8, 1.4,
and 2.0.

The builder exact-opened the full fixture at descriptor generation
`1788020925446068`, then froze:

```text
tail SHA-256:
  b82caca15c25fa13c93a3cd11bc5cb67998cb150574a7fb7190672e4cf93cda9

resident logical SHA-256:
  2cebeec62407073aab20b4e3a8b00b47bbf33acd1d34c02a134ac1a650378b95
```

## Authority and infrastructure

The private runner was an `n2-standard-8` on Intel Cascade Lake with 375 GiB
of local NVMe, a 200 GiB `pd-ssd` authority volume, and a separate private
`e2-standard-2` OTel collector. All four machine checks passed: runner ready,
binary digest recorded, collector healthy, and no public IPs.

OS Login rejected the operator key, so the already-specified Terraform
break-glass path created a lease-scoped `objectkv` account on the two VMs.
This changed the access path only. It did not change machine, storage, object,
or evaluator treatment.

The runner service account held `roles/storage.objectAdmin` only during
fixture publication. It held only `roles/storage.objectViewer` during plan
derivation. The viewer binding was removed after evidence capture. Terraform
then destroyed all nine leased resources and returned to an empty state.

## Durable evidence

The fixture is retained at:

```text
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-t27-admission-r0-20260829/fixture-9cf5014/
```

The source, machine receipt, locator, and frozen plan are retained at:

```text
gs://doss-objectkv-dev-okv-evals/runs/rfc0044-t27-admission-r0-20260829/evidence-v1/
```

Bucket versioning is enabled. `GCS-EVIDENCE.tsv` binds every evidence object's
generation, byte count, and SHA-256.

## Disposition

`[VERIFIED]` The 1 GiB fixture exists, is content-addressed, has one
generation-pinned locator, and produced the exact immutable 540-position plan
under read-only credentials. No performance claim changed. The next bounded
experiment is execution of the frozen plan, preserving every passing stratum
and every partial failure.
