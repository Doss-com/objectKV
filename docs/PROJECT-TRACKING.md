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

`[ACTIVE-WORK]` Tracker revision 94 points at admitted generation-takeover receipt
`839c0cd8f48ea4d830c59e5ac06440bd920b1621`. Candidate
`42d01507d0d2b7fd77933caf4253b11f164440b3` passed clean run
`d9dac2f8-dc5a-4d94-b177-2275c17fe462` with zero anomalies across three seeds
and 48 real-process checks. The gate rejected 12 prohibited commit attempts,
survived three authority-leader kills, changed membership three times, and
caught up all nine G2 voter observations. Four unsafe subjects discarded with
exact replay. The next critical path authenticates data-quorum fence and
recovery certificates, then advances the independent Arrow, Parquet, and
DataFusion overlay implementation against the frozen row oracle.
