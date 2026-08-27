# objectKV Chess state lab

Status: `[CODE-COMPLETE]` local state-history example. The paired golden run is
`[VERIFIED]` only for the single-process, volatile-memory scope.

Chess makes objectKV history visible. Every accepted move commits an encoded
state, two changed square keys, turn and ply metadata, and one application-log
record through `objectkv-boundary-v0`.

Run the browser app:

```bash
./experiments/run-okv-chess-web.sh
```

Open [http://127.0.0.1:4268/](http://127.0.0.1:4268/). Select a piece and then a
destination. The timeline reads exact MVCC snapshots. Historical states are
read-only until `Fork from this version` creates and selects a new line. Every
named line can be selected again, and `Crash + replay` rebuilds its serving
state only from `okv-log` records.

The rules reducer validates ordinary piece movement, blocking, capture, turn,
and automatic queen promotion. Check, mate, castling, and en passant are not
implemented because the harness evaluates state history rather than chess
engine completeness.

Run the deterministic semantic receipt:

```bash
cargo run -p okv-chess-example -- --golden
```

The shared Tetris and Chess developer path is:

```bash
./experiments/run-okv-playground-golden-path.sh
```

Run only the four-byte Chess action-delta candidate:

```bash
cargo run -p okv-chess-example -- --delta-golden
```

The candidate reconstructs snapshots and named branches from an 81-byte
checkpoint plus ordered move deltas. Its current in-memory fork copies the log
prefix; the object-backed target must share that prefix by reference.

The runner builds only under `/private/tmp` and deletes the build tree when it
exits.
