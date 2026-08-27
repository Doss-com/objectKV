# Fable cross-examination: `okv-log` and `okv-wal`

Status: `[COMPLETE]` independent pre-implementation review, 2026-08-25.

Reviewer: Claude Code through OpenCode, model `anthropic/claude-fable-5`, plan
agent with read-only repository access.

Reviewed:

- RFC-0024 and `docs/LOG-ARCHITECTURE.md`;
- the current `okv-wal` node journal and frozen fixtures;
- the consensus adapter and RFC-0002, RFC-0005, and the decision log.

## Verdict

`REVISE` before implementation. The layering was directionally sound, but the
first draft did not fully preserve the current crash and read semantics.

## Blocking corrections

1. Preserve `NodeJournal` ordering as `plan -> dry-apply clone -> encode ->
   write -> sync -> commit state`. The first draft incorrectly showed durable
   write before semantic validation.
2. Require prefix closure for planned command batches. Suffix replacement must
   remain an explicit truncate followed by append records so a torn batch
   recovers to a valid prefix.
3. Expose both clamped reads for OpenRaft compatibility and exact fail-closed
   reads for retained application logs.
4. Freeze raw accepted and rejected `OKVR` histories before refactoring. One
   append fixture is insufficient because replay has five interleavable record
   kinds.
5. State that a fresh log may establish an arbitrary base index. The existing
   journal does not require zero or one.
6. Put below-purge append filtering in the core planner. A fully expired batch
   emits no records; a straddling batch starts at exactly `purge + 1`.

## Required boundary details

- Truncation beyond the current tail is a legal no-op.
- Purge may advance beyond the last retained entry.
- Vote and committed frames remain `okv-wal` metadata and may interleave with
  entry commands during replay.
- Multi-partition application-log reads need one fixed cell read version.
- Versionstamped ordinal allocation remains unproven.
- Producer deduplication needs an explicit retained-outcome lease.
- `save_vote` currently permits an empty payload that replay rejects. The
  adapter should reject it before writing or deliberately poison itself.

## Accepted first slice after correction

0. Freeze accepted and rejected raw `OKVR` histories against current behavior.
1. Add pure `LogState`, planner, errors, exact and clamped reads, and
   prefix-closure tests.
2. Use the frozen corpus as the pre-refactor oracle.
3. Delegate only append, truncate, and purge state transitions from
   `NodeJournal`, preserving validate-before-write and frozen bytes.
4. Run fixture, reopen, OpenRaft storage, negative recovery, and workspace
   gates.

Explicitly excluded: `LocalReplicatedWal` and `OKVW` changes, object segments,
asynchronous I/O, consumer groups, versionstamps, and the transactional task
runtime.

## Load-bearing poison cases

- suffix replacement torn after truncate but before all appends;
- gaps above the retained suffix or purge marker;
- purge regression and conflicting same-index purge identity;
- identical same-index repurge as idempotent success;
- truncate at or below purge;
- entirely expired and straddling append batches;
- fresh bases `0`, `1`, and `7`, then a rejected gap;
- validation attempted only after write;
- exact read below purge versus clamped compatibility read;
- accepted and rejected byte histories before and after delegation.

## Disposition

RFC-0024 and the architecture note incorporate the blocking corrections above.
Implementation may begin at the byte-corpus freeze, not at the new crate.

The implementation and second Fable pass are recorded in
`fable-okv-log-post-implementation-2026-08-25.md`.
