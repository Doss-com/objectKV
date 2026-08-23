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

`[ACTIVE-WORK]` Tracker revision 104 points at ambiguous-manifest recovery
candidate `57e28d4e58b1ed2aebebd87e2d3504dbe4ded090`. Clean run
`2660e09d-e2f3-4482-a123-68024779de1a` passed 39 checks with zero anomalies
across three deterministic seeds. It retained a successful immutable manifest
PUT while replacing its response with retryable-unknown, killed that publisher,
and recovered the canonical job from replicated intent in a replacement with
empty scratch. The replacement replayed all data identities, verified the
existing manifest, and walked the complete named closure before publishing the
root. Negative run `7ace2812-aab0-44be-bacc-9f4f992d014c` discarded
manifest-only recovery with four anomalies per seed. OTel run
`5fd6240e-17e8-4fca-b632-c594170a233c` exported two log records, one trace
span, eight metrics, and eight data points with correctness anomalies at zero
and availability ratio at one. The next storage critical path covers a lost
replicated `Publish` reply, multipart residue, repeated unknown-response
budgets, complete `MarkReceipt`, sweeper effect fencing, G1/G2 reservation
handoff, and independent empty-disk recovery. The independent HTAP path adds
version-bound manifests and leases, multiple execution ranges, safe pruning,
and the `T - W_p` cost curve.
