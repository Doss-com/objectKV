# objectKV proof-status contract

Status: `[DECIDED]` 2026-08-25.

The active project vocabulary separates code presence from measured evidence.
The scope of every claim is limited to the named revision, suite, profile,
backend, topology, and workload.

| Status | Meaning | Minimum receipt |
| --- | --- | --- |
| `[CODE-COMPLETE]` | The implementation path is present and its code gates pass. It does not imply performance, durability, or operational proof. | Exact revision plus changed-surface checks. |
| `[VERIFIED]` | The stated claim is backed by measurements. | Exact revision, suite and suite hash, profile, backend, immutable seeds, run ID, primary metric, hard-gate result, negative controls when applicable, and telemetry receipt. |
| `[EVALUATING]` | Code or a hypothesis is currently being verified. A required curve, failure result, or infrastructure receipt is still open. | Named question, owner, next run, and acceptance or rejection threshold. |
| `[PROPOSED]` | A reviewable design choice has no admitted implementation claim. | Owning RFC or decision. |
| `[FUTURE]` | Work is outside the active dependency frontier. | Dependency that must clear first. |

`[VERIFIED]` is not a global maturity label. A local filesystem crash-recovery
result is verified for that local topology, but it is not evidence for
independent disks, machines, zones, or cloud object storage. A deterministic
model result is verified model evidence, but it is not systems evidence.

Do not use `[EXISTS]` or `[ACTIVE-WORK]` in new or canonical project material.
Historical review transcripts may retain those tokens when quoting the exact
vocabulary under which the review was written.

## Claim ladder

The systems-to-infrastructure ladder is cumulative:

1. deterministic model and negative subjects;
2. local files with real synchronization calls;
3. real protocol server on one machine;
4. independent operating-system processes with TCP and process kill;
5. independent machines and disks;
6. independent zones with a cloud object store;
7. sustained workload, brownout, recovery, and cost curves.

A result may advance only the rung it actually executes. Passing a lower rung
does not inherit a higher-topology claim.
