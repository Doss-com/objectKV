# RFC-0048 typed layout publication on GCS

Status: `[VERIFIED]` immutable publication, generation-pinned metadata reopen,
read-only point equality, and runtime writer revocation. The point and scan
performance lanes remain `[EVALUATING]`.

Date: 2026-08-30

## Result

Commit `f3bd0b6` published C0 indexed-row and C5 columnar-main representations
of the same independently generated 25,014-record history. One typed root
binds both complete child inventories, every GCS generation, the schema, the
covered-through version, the independent oracle, and the frozen workload plan.

```text
independent oracle
  -> one Rust logical history
  -> C0 indexed row objects
  -> C5 projection + payload objects
  -> exact generation capture
  -> pinned child reopen
  -> content-addressed typed root
  -> writer revocation
  -> fresh read-only root + C0 + C5 reopen
```

The read-only process reconstructed both metadata closures and returned an
identical value at key 7, version 5. It retained 19,229 C0 metadata bytes and
22,502 C5 metadata bytes. The C0 point fetched one 64,500-byte row block.

## Media

| Subject | Objects | Distinct generations | Total bytes | Stored/live |
|---|---:|---:|---:|---:|
| C0 indexed row | 5 | 5 | 13,125,073 | 1.628x |
| C5 columnar main | 4 | 4 | 13,248,886 | 1.644x |

C5 is 1.009x C0 total bytes, within the frozen 1.10x publication bound. Its
1,527,824-byte projected-scan object is 0.116x C0 total bytes. This is a media
shape, not a scan-performance result. C5 resident metadata is 1.170x C0,
within the frozen 2.00x bound.

## Authority and identity

- Project: `doss-objectkv-dev`
- Bucket: `doss-objectkv-dev-okv-evals`
- Runner: `objectkv-bench-t27a2-r0-runner`, `us-central1-a`
- Runtime principal:
  `objectkv-eval-runner@doss-objectkv-dev.iam.gserviceaccount.com`
- Source commit: `f3bd0b6`
- Release executable SHA-256:
  `4b2a96b7d0872e200b550e4d20f4a533ff8018e8f1efc1bdfff4dfaf6231760e`
- Fixture ID:
  `5d933648e3190b3bd6768c36c1d9022596c69c621c2347fa648a0754dc5431b0`
- Logical root SHA-256:
  `1d4f5ec6ab42e75015f362cea781e9d252c02c1549b4332c82380e0e0e974e7f`
- Root object generation: `1788079307536563`
- Placement envelope SHA-256:
  `1d9ddeff4a4885511f3a4e7cdf11507a45cd47e17525911448e0f094a6343f69`
- Independent oracle SHA-256:
  `b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86`

The runner temporarily held `roles/storage.objectCreator` and
`roles/storage.objectViewer`. After the root was sealed, objectCreator was
removed. The resulting bucket policy retained only objectViewer for the
runtime principal. A new publication attempt then failed with
`permission_denied` and left no object under its named probe prefix.

## Durable evidence

The publication locator, full typed root receipt, read-only reopen receipt,
and denied-create output are archived at:

`gs://doss-objectkv-dev-okv-evals/eval-receipts/rfc0048-t28-layout-r0-f3bd0b6/receipts.tar.gz#1788079370628305`

Archive SHA-256:
`a39f6deaff2b99a6535c1eb6658e60265512ff798ce97c7793d801b7b3e93f60`

## Rejected attempt and cleanup

The first publication attempt found that derived Rust ordering sorted child
objects by role before key, while the frozen format requires key then role.
It stopped before publishing a root. The nine unreachable objects under the
failed prefix were enumerated, confirmed to have no root, and deleted. The
bucket reported no soft-delete or versioning policy, so that cleanup is not
recoverable. Commit `f3bd0b6` corrects the order and the successful root uses
a fresh prefix.

## Remaining gates

1. Execute a complete empty-reader projected scan for both children and match
   the independent ordered projection and aggregate digests.
2. Produce compaction write-amplification and branch-reference receipts.
3. Run the uncounted 256-point and one-scan preflight from fresh read-only
   processes.
4. Run the admitted 15-block point and scan curves with OTel confirmation.
