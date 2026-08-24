# Contributing to objectKV

objectKV is in pre-implementation research. The highest-value contributions are
small proofs that close one named uncertainty.

## Before coding

1. Read `README.md`, `CONTEXT.md`, `docs/DECISIONS.md`, and
   `docs/BOOTSTRAP-PLAN.md`.
2. Choose a task from `docs/CONTRIBUTOR-BOARD.md` or an accepted issue.
3. Read the owning RFC. If the invariant is still ambiguous, improve the RFC
   before hardening code around one interpretation.
4. For benchmarks, follow `docs/EVALS.md` and `program.md`.

## Pull requests

- One architectural decision or experiment per pull request.
- State the invariant, evidence, tradeoff, and rollback.
- Label proposed behavior as proposed until the implementation and eval exist.
- Include the exact commands and environment used for validation.
- Never mix an eval change with the implementation it grades.
- Never omit discarded benchmark attempts from the experiment ledger.

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p okv-eval -- smoke
```

## RFC changes

Use `rfcs/0000-template.md`. Material changes to durability, version semantics,
storage formats, manifest publication, transaction isolation, or compatibility
require an RFC before implementation becomes a public contract.

## Research results

Results without candidate commit, suite hash, profile hash, backend, seeds,
budget, hard gates, and raw artifact reference are observations, not comparable
benchmarks.
