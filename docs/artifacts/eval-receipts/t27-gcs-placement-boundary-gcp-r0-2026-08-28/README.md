# T27 GCS placement boundary, GCP R0, 2026-08-28

Status: `[VERIFIED]` cross-invocation correctness mechanism. This is not a T27
performance admission.

## Claim

One preparation invocation persisted a content-addressed fixture and emitted a
generation-pinned placement locator. Later native and direct-control
invocations, running with object-read permission only, exact-opened the same
locator and closure, established fresh transaction authorities, applied one
equal tail, built independent resident images, and completed their measured
windows with zero correctness failures and zero object requests.

The fixture seed was `4244`. Both fixed consumers used trace seed `1103`. Their
equal trace digest proves that separating fixture generation from trace
generation did not change the access schedule.

## Identity

```text
project:                 doss-objectkv-dev
bucket:                  doss-objectkv-dev-okv-evals
prefix:                  runs/rfc0044-t27-boundary-r0-20260829/small-28a732f
preparation source:      28a732f
fixed consumer source:   1cfad27454e8901ad6580af183cecf8dbcda8942
fixture seed:            4244
trace seed:              1103
fixture ID:              29d0f21103303e36bba2d91a9415f41e500c0a184bbdcf48ea088994a58a3383
descriptor generation:   1787976513990982
descriptor SHA-256:      1c21a05f696e2b9463f36b1a9650a5cb668ba6bf5e86a94ab7900c9e18b4170b
locator envelope SHA:    f9ba32f0f58a4844c126de517d99971823606ca9bdafde9caa8b2291c1304c26
locator file SHA-256:    033716c3a73e3a01c54762aa957b79cfaf35e459eca803adf6bdebdc139f9ab4
consumer binary SHA-256: 395e22bf07f180ff21a0e626a60a39f242850b937080bf214360f45769ff736c
GCS objects:             12
GCS bytes:               4,307,596
```

The locator also binds:

```text
source SHA-256:      d856d0d7e7fc4ae51947844a025c04ccb1bd73bf48666c236cb0a2c8f53dd0d2
suite SHA-256:       ab05566f0644ed8c28706c33712c6cefe13e73867718d6d9d32384e8264ce95e
preparer binary:     391d0d6d8161494b7594a12296b9d2b219dce6535682d1255025c84982fe06dd
Cargo.lock SHA-256:  85fd5d79ab99965dd3eac6fbba955d57045de2b48e4d2a4bc3ab1d30e2698201
```

## Observed boundary

| Observation | Native | Direct control |
|---|---:|---:|
| Consumer exit code | 0 | 0 |
| Fixture setup | 0.998501 s | 0.865856 s |
| Full closure verification | 0.667646 s | 0.548946 s |
| Setup object requests | 13 | 13 |
| Setup response bytes | 4,310,447 | 4,310,447 |
| Anchor txLog records | 1 | 1 |
| Base-value txLog records | 0 | 0 |
| Applied through | 10 | 10 |
| Complete logical outcomes | 4,099 | 4,099 |
| Aggregate correctness failures | 0 | 0 |
| Sample correctness failures | 0 | 0 |
| Measured object requests | 0 | 0 |
| Counter delta valid | true | true |

Both subjects reported:

```text
trace SHA-256:
  a285882fc5ac65add216b5a5c9fb1376ab468f4f9d8b6b890da75bf62ee2282c

tail SHA-256:
  90a82886bf4c86521a4506dd03366bbb3eef02fb3ad97acf37d2605f4032cfe4

resident logical SHA-256:
  da5ddc6b3fabf3f6855e61ecd3326e58d6d49fab9660aa0bd1d4082ce3170ce2
```

Their physical providers and image identities differed as required:

```text
native provider:  rocksdb-11.8.1-native-resident-v1
native image:     c1f947c23377ed75351cb5b529dcf67d59127088793bd90eff2dd7f591248c6a

control provider: rocksdb-11.8.1-direct-owned-v1
control image:    8a54b01e41d3c1ed0b4055f37d8c6ad01aa688219ffe2fbb08de3dbfda1aeba8
```

Report file hashes before VM deletion were:

```text
native:  d8e551f0da92775902995fd9ab6871cddcabc13120cac639e4f7e632e680e1f3
control: 2c7f6fbf2bbd4483025248146805c9c0318d85a7fae294c2a326c6f2ba480c63
```

The short 256-read windows and buffered I/O were correctness probes. Their
throughput and latency are not performance evidence.

## Preserved failures

1. The default VM service account initially had no object read or write role.
   Preparation and consumption both failed with `permission_denied`.
2. Preparation succeeded only under the dedicated writer service account.
   The consumer later succeeded under exactly `roles/storage.objectViewer`.
3. The first native consumer used fixture seed `4244` but regenerated expected
   base values from trace seed `1103`. It reported 4,080 aggregate mismatches
   while every sampled point read was correct. The direct control reported
   zero because it derived its logical image from the verified closure.
4. Commit `1cfad27` carries fixture seed into the native validator while
   leaving trace generation on trace seed `1103`. A focused remote suite passed
   eight tests. Its regression reports zero failures with the fixture seed and
   48 of 48 failures when the trace seed is substituted deliberately.
5. The placement consumer now fails its CLI invocation when aggregate,
   per-sample, object-I/O, or counter gates fail. Replaying the original GCS
   locator then reduced native aggregate failures from 4,080 to zero.
6. Two startup-script transport mistakes and one Spot preemption occurred
   before the final replay. None entered a product result.

The cold release build took 16 minutes 26 seconds because the `okv-eval`
binary currently compiles the DataFusion surface for a kernel-only replay.
That is build-graph coupling, not a serving runtime measurement.

## Permission and cleanup evidence

The consumer service account's temporary `roles/storage.objectViewer` binding
was removed after the reports completed. Final checks reported:

```text
temporary viewer bindings: 0
disposable VMs:             0
temporary firewall rules:  0
persisted fixture objects:  12
```

## Disposition

`[VERIFIED]` The preparation command, placement envelope, exact GCS generation,
separate consumer invocation, read-only credential boundary, standalone direct
control, and independent fixture and trace seeds now compose correctly on real
GCS.

T27 remains `[EVALUATING]`. The immutable fresh-process ABBA plan, generation
and authority poisons, direct-I/O 64 MiB preflight, 1 GiB cache and skew curve,
OTel admission, and independent T27 performance comparisons remain open.
