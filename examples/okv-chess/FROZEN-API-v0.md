# objectKV Chess boundary v0

Status: `[CODE-COMPLETE]`. Frozen on 2026-08-25.

Chess uses the same `objectkv-boundary-v0` transaction envelope and receipt as
Tetris. The stable operations are point read, ordered range read, atomic
mutation batch, exact-version fork, named-branch switch, and txLog replay.

```text
move or lifecycle request
           ↓
objectkv-boundary-v0
           ↓
CommittedEnvelope → okv-log
           ↓
CommitBatch → okv-model
           ↓
snapshot plus CommitReceipt
```

The HTTP adapter is local development transport:

| Method and route | Operation |
| --- | --- |
| `GET /api/state?version=N` | Read one exact snapshot on the active branch. |
| `POST /api/move` | Validate and atomically commit one coordinate move. |
| `POST /api/fork` | Fork the active history at the supplied exact version. |
| `POST /api/switch` | Rebuild and select a named branch. |
| `POST /api/recover` | Discard serving state and replay the selected txLog. |
| `POST /api/reset` | Commit a new initial position on the active branch. |

Normal moves use six mutations: full logical state, source clear, destination
set, turn, ply, and application event. This deliberately contrasts with the
Tetris materialized-view rewrite so byte amplification is visible in the
paired workload.

Current branching copies the retained in-memory log prefix. This verifies
semantics, not the intended object-manifest copy-on-write economics. The
single-process model also does not provide network replication, stable media,
object publication, or multi-process fencing.
