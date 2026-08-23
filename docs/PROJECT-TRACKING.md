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

`[ACTIVE-WORK]` Tracker revision 101 points at replicated publication-authority
candidate `b530321d114728d92741ca9bdfd7b7a637148e75`. Clean run
`550e5585-bf9d-4bc9-b96f-d38aaca9eb49` passed 72 checks with zero anomalies
across three deterministic seeds, including six leader failovers, 18 process
kills, and 69 publication writes. All ten unsafe subjects discarded. OTel run
`8071bc8a-8a4d-4a29-9118-5a11e22b5e3b` exported logs, metrics, and traces with
correctness anomalies at zero and availability ratio at one. The next storage
critical path kills dedicated publisher and sweeper processes at each durable
object and authority boundary, completes `MarkReceipt` plus G1/G2 handoff, and
proves old-root deletion plus independent empty-disk recovery. The independent
HTAP path adds version-bound manifests and leases, multiple execution ranges,
safe pruning, and the `T - W_p` cost curve.
