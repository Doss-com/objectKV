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

`[ACTIVE-WORK]` Tracker revision 92 advances through the deterministic
three-node replication and failover receipt. Candidate
`d77b7b548a30cdc7534f966cd1f0621e977b1561` passed clean run
`ce63ffd2-502b-4418-88c0-f9e7dd6e1599` with zero anomalies across three seeds,
nine quorum commits, nine elections, six partitions, six repairs, three
simulated process crashes, three bounces, and nine caught-up nodes. Three unsafe
subjects discarded. The next critical path is a real process-kill and durable
request-outcome gate, followed by generation takeover and the Arrow, Parquet,
and DataFusion implementation against the frozen row oracle.
