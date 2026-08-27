# objectKV Tetris example

Status: `[CODE-COMPLETE]` interactive boundary example, not a durable database.

The question is whether a small stateful application feels natural over the
current ordered KV and ordered-log primitives. Every Tetris input is reduced to
one transaction, appended to the real `okv-log` state machine, applied to the
real `okv-model` MVCC oracle, and rendered by reading that model back.

Run the browser app from the repository root:

```bash
./experiments/run-okv-tetris-web.sh
```

Open [http://127.0.0.1:4267/](http://127.0.0.1:4267/). The page stays focused
on three surfaces:

```text
playfield     controls and an MVCC-rendered board
kernel proof commit version, txLog index, branch, recovery, and receipt
vision        running topology, integrated-cell target, metrics, and falsifiers
```

The web runner builds under `/private/tmp`, retains only the executable while
the server is running, and deletes it on exit. The repository does not retain a
`target` directory.

Controls:

```text
a left    d right    w rotate    s tick    f hard drop
n reset   [ older    ] newer     x recover b fork      q quit
```

`x` discards the current serving model and reconstructs it from encoded records
in `okv-log`. `[` and `]` perform MVCC snapshot reads. `b` clones the current
ordered history into a new branch and switches the game to it.

The browser controls use the same keys. Every visible button calls a local HTTP
adapter over the frozen boundary. Deeper transaction and recovery detail stays
in the API and this contract rather than in the game UI.

For a noninteractive terminal smoke run:

```bash
./experiments/run-okv-tetris.sh --script 'a,w,s,f,x,b,d,f,[,]'
```

Run the paired deterministic developer golden path:

```bash
./experiments/run-okv-playground-golden-path.sh
```

Tetris owns the high-rate and byte-amplification half. Chess owns readable
snapshot, branch, switch, and replay semantics. Both report
materialized and action-delta receipts, and the runner requires their final
fingerprints to match.

Run only the two-byte action-delta candidate:

```bash
cargo run -p okv-tetris-example -- --delta-golden 2000
```

The delta path checkpoints the 205-byte game state every 256 actions and
replays only the following tail. It is an application-log prototype over
volatile `okv-log`, not a durable txLog claim.

Run the interactive terminal view with `./experiments/run-okv-tetris.sh`.

The frozen boundary and its current limitations are in
[`FROZEN-API-v0.md`](FROZEN-API-v0.md).
