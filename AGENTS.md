# objectKV contributor instructions

Read `README.md`, `CONTEXT.md`, `docs/DECISIONS.md`, and the RFC owning your
change before editing.

## Hard rules

- Never describe a proposed capability as implemented.
- Active status uses `[CODE-COMPLETE]`, `[VERIFIED]`, `[EVALUATING]`,
  `[PROPOSED]`, or `[FUTURE]`. Do not use `[EXISTS]` or `[ACTIVE-WORK]` in new
  or canonical material. `[VERIFIED]` requires a named metric receipt; code
  presence alone is `[CODE-COMPLETE]`.
- Correctness oracles, held-out eval inputs, and eval result schemas are frozen
  during an experiment. Change them in a separate reviewed change.
- One research commit tests one hypothesis. Record every result, including
  failures and discarded candidates.
- Never optimize a blended project score. Pick one eval lane with one primary
  metric and satisfy all hard gates.
- Do not add distributed roles before the preceding go/no-go gate passes.
- Do not import ZebraDB, ERP, DOSS, PostgreSQL internals, or SQL semantics into
  the kernel API without an accepted RFC.
- Storage-format changes require an RFC plus forward/backward compatibility
  fixtures before implementation.
- Use immutable test seeds and record toolchain, machine profile, backend,
  revision, and suite hash with every benchmark.

## Experiment loop

Follow `program.md`. Use `research/<lane>/<tag>` branches. Preserve discarded
commits and append their result to the ledger. Do not rewrite history to hide a
failed experiment.

## Validation

Run at minimum:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p okv-eval -- smoke
```
