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

`[ACTIVE-WORK]` Tracker revision 105 points at lost-`Publish`-response recovery
candidate `72df70c726797ff3ff7dcd0642a89e2302a7fd7e`. Clean run
`a544deff-edec-4885-a0bf-b1217d720328` passed 42 checks with zero anomalies
across three deterministic seeds. It dropped each successful `Publish` reply
after the replicated root transition, killed the publisher and accepting
authority leader, and started an empty-scratch replacement. Each replacement
recovered the original outcome from the successor, retried the same identity
without a second authority transition or object PUT, and walked the exact
visible closure. Negative run `82698bdb-443d-4ad7-830f-5bef6927b8f8`
discarded convergence-only recovery with four anomalies and two `Publish`
applications per seed while retaining the same final root and closure. OTel run
`50ad5d86-ee3e-4790-9c19-d81383d68002` exported two log records, one trace
span, eight metrics, and eight data points with correctness anomalies at zero
and availability ratio at one. The next storage critical path covers multipart
residue, repeated unknown-response budgets, complete `MarkReceipt`, sweeper
effect fencing, G1/G2 reservation handoff, retained-outcome expiry and snapshot
restore, later-root supersession, and independent empty-disk recovery. The
independent HTAP path adds version-bound manifests and leases, multiple
execution ranges, safe pruning, and the `T - W_p` cost curve.
