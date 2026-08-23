# objectKV project tracking

Status: `[ACTIVE-WORK]` registered in the local DOSSBOT project tracker as
`OKV-BOOTSTRAP`.

## Open the playground

```bash
cd /Users/wileyjones/Documents/doss/repos/dossbot
PROJECT_TRACKER_PORT=4187 npm run project:tracker
```

The objectKV playground uses `http://127.0.0.1:4187` so it does not collide with
the DOSSBOT app page on its default port. Filter the canonical queue by the
`objectKV` lane or search for `OKV-BOOTSTRAP`.

## Authority boundaries

- The DOSSBOT tracker entry owns the concise current status, owner, next action,
  acceptance condition, and evidence pointer.
- `docs/BOOTSTRAP-PLAN.md` owns objectKV sequencing and gates.
- `docs/CONTRIBUTOR-BOARD.md` owns bounded contributor tasks.
- `docs/DECISIONS.md` and `rfcs/` own architectural decisions.
- `docs/research/EXPERT-REVIEW-SYNTHESIS.md` owns the independent-review ledger
  and identifies incomplete review paths without implying consensus.
- `experiments/` and OTel own empirical receipts.
- Git owns source and exact revision identity.

Update the tracker when the critical path, owner, or external blocker changes.
Do not copy the full RFC queue into the playground.

## Current checkpoint

`[ACTIVE-WORK]` Tracker revision 103 points at ambiguous-PUT recovery candidate
`a6dfeed13af06d56c30d494d751866bfbdf03a27`. Clean run
`a4a1aec5-cca9-46e7-864e-de48a7e2c30b` passed 36 checks with zero anomalies
across three deterministic seeds. It retained a successful first immutable PUT
while replacing its response with retryable-unknown, killed that publisher,
removed its scratch directory, and recovered the canonical job from replicated
intent in a replacement with empty scratch. The replacement verified the
existing object by exact named identity before completing the closure and
publishing the root. Negative run `fa9d729b-c861-444a-9989-7127f026058c`
discarded partial-closure publication with four anomalies per seed. OTel run
`b57f141f-fd8d-4108-b053-da1c2cc9a63d` exported two log records, one trace
span, eight metrics, and eight data points with correctness anomalies at zero
and availability ratio at one. The next storage critical path covers lost
manifest and `Publish` replies, multipart residue, repeated unknown-response
budgets, complete `MarkReceipt`, sweeper effect fencing, G1/G2 reservation
handoff, and independent empty-disk recovery. The independent HTAP path adds
version-bound manifests and leases, multiple execution ranges, safe pruning,
and the `T - W_p` cost curve.
