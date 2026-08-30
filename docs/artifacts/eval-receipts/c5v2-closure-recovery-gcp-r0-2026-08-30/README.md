# C5v2 complete-closure recovery, GCP R0

Status: `[EVALUATING]`. The exact positive GCS recovery and sealed cloud
corruption control passed. Independent OTel confirmation remains before this
becomes an admission receipt.

## Result

A new process opened the generation-pinned RFC-0049 root, fetched every object
in the C5v2 child, authenticated the complete object digests and all frame
proofs, reconstructed the canonical MVCC history, and issued no LIST or write.

```text
typed root
  -> manifest + compact index
  -> complete projection + payload
  -> 792 authenticated frame pairs
  -> 25,014 ordered MVCC records
  -> exact independent history digest
```

| Metric | Observed |
|---|---:|
| full named GETs | 5 |
| response bytes, including root | 13,700,110 |
| range GETs / LIST / writes | 0 / 0 / 0 |
| groups | 792 |
| projection proofs verified | 792 |
| payload proofs verified | 792 |
| retained MVCC records | 25,014 |
| live rows | 15,742 |
| recovery elapsed | 792.221 ms |

Recovered history SHA-256:
`d4be64434f6b69990a2787876f514c6036727b41dcf1c5e120f91b6ce968ecd4`.

## Cloud corruption control

Source `dbfb056a48a0febcd6dd6fc1218575eb6946ac54` opened the same exact
generation-pinned five-object closure through the read-only GCS backend. The
controller changed byte zero of the complete projection object from 79 to 207
with XOR mask 128, then required recovery to fail at the child full-object
digest boundary.

| Boundary | Observed |
|---|---|
| poison | `projection_full_object_byte_0_xor_0x80` |
| exact provider key | `runs/rfc0049-t28-aligned-r0-20260830-94d55bb/c5v2/layout/columnar-v2/projection.okp2` |
| object bytes | 1,701,414 |
| clean projection SHA-256 | `dd67841b2c27a935273478d202d3bb00a506a7fecf522241df369669bb98e24c` |
| poisoned projection SHA-256 | `ab15ec361e94730fceb267254549bae8d85ca794f5c6006e40ccb5a9e4fac352` |
| rejection boundary | `generation_pinned_child_full_object_sha256` |
| exact error | `corrupt: RFC-0048 generation-pinned child read identity mismatch` |
| full GETs / bytes | 5 / 13,700,110 |
| range GETs / LIST / writes | 0 / 0 / 0 |
| rejection elapsed | 455.298 ms |

The runner service account had read-only object authority and could not upload
the receipt archive. The 2,606-byte archive was copied out and uploaded by the
operator. This is additional evidence that the recovery path itself lacked
write authority.

## Identity

| Boundary | Identity |
|---|---|
| source commit | `60a593c59093dbbf3f0be49a1d80957d6199867d` |
| release binary SHA-256 | `f83ea7c67a420d3a40652ea2d1b65624a95dedaa592489b841f7dff19143752b` |
| `Cargo.lock` SHA-256 | `2bea96cd06da295aa6be6c9a55925647044c5a8b70f83ebdee59753c4edc1341` |
| root envelope SHA-256 | `d2bc16dd8b7b58db292bf33763ad8a962ad89259ea28916dca00187f85684550` |
| root SHA-256 | `524cb3303748b2b04f37bc3c25a1e20dc27db82119f8cb357da53780661d23fd` |
| result file SHA-256 | `cd407fcbafe8d8635b1add2bf381b7483ec3357946c2af00e9bdf2867be04805` |
| self-sealed receipt SHA-256 | `ed60cc0fc01b1c989b7af8346a2769ba0e076bf47a3990fb653a3a26a0969e6c` |
| poison source commit | `dbfb056a48a0febcd6dd6fc1218575eb6946ac54` |
| poison release binary SHA-256 | `5cdfd0a2b4cce1aa8eea2cbaec33c16cd28eabf07a33aeca08f6483bc62c0a86` |
| poison build receipt SHA-256 | `b428833fc82ea41ca07b2257612c2fd43078c07502c4eca08257e46bba1b26ae` |
| poison result file SHA-256 | `ca783ad73d8afab0d0a2edc128bea397d3941a1be59c365c2d4aecd56560cceb` |
| poison self-sealed receipt SHA-256 | `029e46017b22f4aac85a0161424831604af04a888f866064297bafa78dfa2ab6` |

Runner: `objectkv-bench-t27a2-r0-runner`, project `doss-objectkv-dev`, zone
`us-central1-a`.

Retained object:
`gs://doss-objectkv-dev-okv-evals/eval-receipts/c5v2-closure-recovery-60a593c-r0/recovery-receipt.tar.gz`,
generation `1788118841432721`, 2,472 bytes, archive SHA-256
`7cbe7614e27e4e4c3ba11066302c066afe8cf871788ca35f7a71191aca657bec`.

Retained poison object:
`gs://doss-objectkv-dev-okv-evals/eval-receipts/c5v2-closure-recovery-poison-dbfb056-r0/recovery-poison-receipt.tar.gz`,
generation `1788121466303674`, 2,606 bytes, archive SHA-256
`7f4b29d3af07d14423d152a4fca4732b1915afc6a0c14a626e6f87f23ed5905f`.

## Claim boundary

`[VERIFIED]` The exact positive and corruption-control receipts bind the frozen logical oracle,
physical oracle, physical plan, root, child inventory, source, binary,
lockfile, Linux process identity, provider attempts, record counts, proof
counts, media bytes, poison byte, clean and poisoned object digests, exact
five-object request graph, and named rejection boundary.

`[EVALUATING]` The cloud run lacks independent OTel collector confirmation. The
runtime principal has viewer IAM, although the measured path issued zero LIST
calls. This recovery intentionally performs full hydration;
its 13.7 MB and 792 ms are a rebuild cost, not point-read cost. Compaction
write amplification and branch-reference reuse remain separate row-3 gates.
