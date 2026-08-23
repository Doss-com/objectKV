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

`[ACTIVE-WORK]` Tracker revision 100 points at admitted object publication head
`77029f843e3c8d2b4bae5357b54e89b74991da60`. Candidate
`602b3174ca35f4dd1d897767e4aed71d8b111fcd` passed clean run
`e83eeb60-29ab-447d-950c-7b533672cc43` with zero anomalies across three seeds
and 48 physical boundary checks. Seven unsafe subjects discarded. OTel run
`beaa7904-f2bd-48a8-93e4-3529cb95f98b` exported logs, metrics, and traces. The
next storage critical path binds the same publication command contract to
replicated authority and kills publisher and sweeper processes at every durable
boundary. The independent HTAP path adds version-bound manifests and leases,
multiple execution ranges, safe pruning, and the `T - W_p` cost curve.
