# Provider media-loss R0 receipts

Status: `[VERIFIED]` for GP2.5.3 exact object-closure reconstruction after
deletion of the source FoundationDB VM, boot disk, and provider data disk.
This is a bounded correctness result, not an HA or performance result.

## Frozen authority

- Candidate revision: `50c72159781e14d3db06d792beac34838572fc91`
- Candidate tree: `a35306082fbafcbfeaaec90306e6edacce4a72eb`
- Evaluator binary SHA-256: `6eaf31478a3c32eac12a80520e720d0c9f32526b706847dfe6d17a72ef78bdce`
- Source machine receipt SHA-256: `a34eddbb49481e6288930692d7df6add7a587b3e9ea94d8fc07bf017a026b8a8`
- Destination machine receipt SHA-256: `9cfea4d31e7ee386031dadf29e7ff0a3a24c124003a2f2aa14c203ccabaeeb5b`
- Formal suite hash: `cf37f27cec18723b34538bb8b8766ef5ff6371244fc79d315e06d5107ce57d78`
- Provider: `foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337`
- Infrastructure: sequential private `n2-standard-8` source and restore VMs,
  distinct 100 GiB `pd-ssd` provider disks, regional GCS, and a separate
  private OTel collector
- Teardown: nine Terraform-managed resources destroyed by
  `2026-08-27T14:00:28Z`; Terraform state, matching instances, and matching
  disks each returned zero entries while nine named evidence objects remained

The source bundle and named transport objects are under:

```text
gs://doss-objectkv-dev-okv-evals/bundles/provider-media-loss-r0/50c72159781e14d3db06d792beac34838572fc91/
gs://doss-objectkv-dev-okv-evals/results/provider-r0/media-loss/gp253-r0-20260827t1345z/
```

## Executed result

```text
FoundationDB source S
  -> 950-row exact closure in GCS
  -> same-cluster restore poison                  [DISCARD]
  -> delete source VM + boot disk + provider SSD
  -> controller observes all three absent
  -> fresh FoundationDB destination D
  -> named manifest + closure GET
  -> five idempotent restore chunks
  -> exact digest + activation + fresh commit     [KEEP]
```

The source closure contained 950 final records in 205,248 bytes plus a
607-byte manifest. Its state digest was
`9fc46142a6d53447d30c357bdff009944cc7cc554fecdedf89e0f99c74db6d60`.
The controller observed all source media absent at `2026-08-27T13:49:32Z`.
Restore began at `2026-08-27T13:53:19Z` on distinct provider and GCP
identities. The destination read the two named GCS generations, restored and
replayed five chunks, reproduced the digest and row count, activated only
after its ready marker, and committed one fresh transaction.

| Subject | Formal run ID | Verdict | Result |
| --- | --- | --- | --- |
| Fresh-provider reconstruction | `3093a364-b636-4b9e-bb7f-569d79853129` | `keep` | 16/16 hard gates passed, zero anomalies |
| Hidden source media | `865c06d8-9a7d-4874-aa44-c7cd6683c170` | `discard` | same-cluster restore failed three physical-media invariants |

The destination phase took 305.667 ms end to end inside the lifecycle probe:
231.943 ms for named GCS reads, 37.925 ms for restore, 3.422 ms for activation,
and 3.717 ms for the fresh commit. These are single-sample diagnostic timings,
not performance claims.

Both formal run IDs occur in `otel/traces.jsonl`, `otel/metrics.jsonl`, and
`otel/logs.jsonl`. Formal evaluator receipts are in `formal/`; provider,
machine, topology, and loss observations are in `raw/`.

## Scope boundary and shipping path

GP2.5.3 proves that the named immutable object closure is sufficient to
reconstruct this exact logical state after complete source-provider media
loss. It does not prove provider failover, availability during loss, a
resurrected-old-cluster fence, production RPO or RTO, or acceptable write-path
overhead.

```text
GP2.5.1 semantic authority                         [VERIFIED]
  -> GP2.5.2 logical object lifecycle             [VERIFIED]
  -> GP2.5.3 physical provider-media loss         [VERIFIED]
  -> GP2.5.4 external cell-incarnation authority  [PROPOSED]
  -> GP3.1 retained-write overhead vs direct FDB  [PROPOSED]
  -> SSD and RAM disposable serving profiles     [FUTURE]
```

FoundationDB remains the sole candidate transaction plane. GP2.5.3 removes
object-closure sufficiency as a blocker. Provider-incarnation fencing and the
matched overhead curve remain admission gates.
