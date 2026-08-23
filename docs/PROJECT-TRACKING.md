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

`[ACTIVE-WORK]` Tracker revision 102 points at publisher-recovery candidate
`ffc0c849879ec5ac9a54fa556a067c43e414fbe5`. Clean run
`3b5cb41f-8985-47f4-8e87-4797ad9babef` passed 30 checks with zero anomalies
across three deterministic seeds. It committed `Prepare` through three real
OpenRaft authority processes, killed the publisher before the first object PUT,
removed its scratch directory, and completed publication from a replacement
with empty scratch. Negative run `26bde1fa-670b-40db-a750-8f363042b10b`
discarded upload-before-Prepare with eight anomalies per seed. OTel run
`ce7692da-7150-4b6a-81c4-9e680c7e2bb6` exported logs, metrics, and traces with
correctness anomalies at zero and availability ratio at one. The next storage
critical path covers partial upload, lost PUT and `Publish` replies, complete
`MarkReceipt`, sweeper effect fencing, G1/G2 reservation handoff, and
independent empty-disk recovery. The independent HTAP path adds version-bound
manifests and leases, multiple execution ranges, safe pruning, and the
`T - W_p` cost curve.
