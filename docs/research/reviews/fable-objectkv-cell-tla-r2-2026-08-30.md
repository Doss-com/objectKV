# Fable adversarial review: objectKV cell TLA+ R2

Date: 2026-08-30

Verdict: `SHIP`

## Review sequence

Fable's first pass blocked the model and evidence on concrete counterexamples:

1. a serving image behind the latest commit could answer;
2. RAM or stable-media loss did not consistently clear the serving tuple;
3. an old-generation pending object frontier could activate;
4. the Rust validator checked a seal without independently replaying events;
5. fault controls did not isolate the named weakened contract;
6. five poison runs exited on incomplete successor assignments instead of the
   intended invariant;
7. a RAM-only serving worker could not lose RAM;
8. a deserialized quorum outside the TLA+ assumptions could validate.

Source `7226e81a1dc5964a0fca4d203323f8b78d7e12dc` closes each blocker. Fable
then reproduced the healthy state counts, checked all six exact poison
violations, ran the seven focused Rust tests, and hash-checked the governed R2
receipt.

## Accepted claim

`[VERIFIED]` TLC found no safety violation in the two named finite scopes for
model SHA-256
`55d5bb137b9e3c37deace42f92b4602b022a7583b0a23a801ef707f40618a3ba`.
All six single-fault controls produced their configured named invariant
counterexample.

`[CODE-COMPLETE]` Rust provides hand-written bounded trace conformance. It
replays events and assertions, checks the exact model identity, rejects
constant assignments outside the TLA+ assumptions, and detects derived-state
tampering.

`[EVALUATING]` Mechanical implementation refinement, liveness, unbounded
correctness, algorithm-specific consensus proof, and complete-cell
infrastructure conformance remain open.

## Evidence

- receipt: [`formal/evidence/gcp-r2-2026-08-30.json`](../../../formal/evidence/gcp-r2-2026-08-30.json)
- raw GCS archive:
  `gs://doss-objectkv-dev-okv-evals/eval-receipts/objectkv-cell-tla-r2-55d5bb1/objectkv-cell-tla-r2-55d5bb1.tar.gz`
- archive generation: `1788122230581424`
- archive SHA-256:
  `407bbfe489f9bb699a1f33f33031f7c9cdce5697c44524295acd971dba0167c4`
