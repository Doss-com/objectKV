# C5v2 complete-closure recovery, GCP R0

Status: `[EVALUATING]`. The exact positive GCS recovery passed. A sealed cloud
poison and independent OTel confirmation remain before this becomes an
admission receipt.

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

Runner: `objectkv-bench-t27a2-r0-runner`, project `doss-objectkv-dev`, zone
`us-central1-a`.

Retained object:
`gs://doss-objectkv-dev-okv-evals/eval-receipts/c5v2-closure-recovery-60a593c-r0/recovery-receipt.tar.gz`,
generation `1788118841432721`, 2,472 bytes, archive SHA-256
`7cbe7614e27e4e4c3ba11066302c066afe8cf871788ca35f7a71191aca657bec`.

## Claim boundary

`[CODE-COMPLETE]` The decoder and receipt bind the frozen logical oracle,
physical oracle, physical plan, root, child inventory, source, binary,
lockfile, Linux process identity, provider attempts, record counts, proof
counts, and media bytes. The local corruption control rejects changed child
bytes before reconstruction.

`[EVALUATING]` The cloud run lacks a sealed poison receipt and OTel collector
confirmation. The runtime principal has viewer IAM, although the measured path
issued zero LIST calls. This recovery intentionally performs full hydration;
its 13.7 MB and 792 ms are a rebuild cost, not point-read cost. Compaction
write amplification and branch-reference reuse remain separate row-3 gates.
