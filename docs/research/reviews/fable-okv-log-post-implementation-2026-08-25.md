# Fable post-implementation review: `okv-log` and `okv-wal`

Status: `[COMPLETE]` read-only acceptance review, 2026-08-25.

Reviewer: Claude Code through OpenCode, model `anthropic/claude-fable-5`.

## Verdict

`REVISE`, narrowly. The ordered algebra and WAL delegation passed. One concrete
self-poisoning API edge and one commit-provenance requirement remained.

## Passed cross-checks

- validate cloned state before encode, write, sync, and in-memory commit;
- prefix-closed truncate-plus-append suffix replacement;
- exact fail-closed reads and clamped OpenRaft-compatible reads;
- consumer-selected fresh bases followed by consecutive indexes;
- fully expired zero-byte append plans and straddling purge filtering;
- interleaved WAL metadata and ordered-log replay routing;
- accepted and rejected `OKVR` bytes reproduced against pre-refactor `HEAD`.

## Correctness blocker and disposition

Fable found that `NodeJournal::purge` accepted an empty marker payload,
`encode_record` synchronized the resulting eight-byte body, and `decode_record`
rejected that same body on reopen.

`[RESOLVED]` `NodeJournal::purge` now rejects an empty identity before any bytes
are written. A red-green regression asserts that the physical file stays empty.
An additional codec test round-trips every writable record kind through the
decoder. Empty vote and committed identities already use the same pre-write
rule.

## Provenance blocker and disposition

Fable independently reproduced the new accepted and rejected fixtures against
the committed pre-refactor implementation. The corpus test was also run green
before the local refactor. However, the fixtures and implementation remain in
one uncommitted working tree, so Git history does not yet prove corpus-first
ordering.

`[OPEN-PACKAGING]` Before merge, package the fixture files and pre-refactor
replay test as the first reviewed commit, then package `okv-log` and the
delegation as the second. Do not silently create those commits while unrelated
research work remains in the same dirty tree.

## Recorded compatibility edge

The old journal used saturating arithmetic at `u64::MAX` and could overwrite a
retained max-index entry with another max-index append. `okv-log` uses checked
arithmetic and rejects that history as `IndexExhausted`. This is safer and
restores the consecutive-index invariant, but it is an accepted-to-rejected
edge for a theoretical old journal. Record it as an intentional bootstrap
compatibility break rather than reintroducing the overwrite.

## Final validation

`[PASS]` After resolving the purge issue, the final tree passed:

- `cargo fmt --all -- --check`;
- `cargo clippy --workspace --all-targets -- -D warnings`;
- `cargo test --workspace`;
- `cargo run -p okv-eval -- smoke`, with zero correctness failures.

The final validation used a temporary Cargo target outside the repository. It
was cleaned after the run, removing 7.0 GiB of build output.
