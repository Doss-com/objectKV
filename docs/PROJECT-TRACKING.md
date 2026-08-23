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

`[ACTIVE-WORK]` Tracker revision 95 points at admitted recovery-certificate
receipt `17d6622ea1ff6c8b79cb29b3ea8ff5704e3e9dfd`. Candidate
`6bad3f85f7a51d0ee5844c73de362f47d7477a91` passed clean run
`6a2989a1-6444-4c87-97ad-e77f16a92475` with zero anomalies across three seeds
and 48 real-process checks. Each seed used three fence signers, three recovery
signers, and rejected five invalid certificates. All five unsafe subjects
discarded with exact replay. OTel run
`e4638e21-f6a0-45b3-abc1-f5aba3467f20` exported the bounded Prometheus series.
The next independent critical path implements Arrow and Parquet fixtures plus
the DataFusion exact-snapshot overlay against the frozen row oracle. Recovery
continues separately with control-root reconciliation and production key
custody.
