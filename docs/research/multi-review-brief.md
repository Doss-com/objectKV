# objectKV independent architecture review brief

Work read-only. Review the objectKV and ZebraDB proposal using:

- the ZebraDB object-native transaction-kernel architecture memo in
  `/Users/wileyjones/Downloads/`
- the repository in `/Users/wileyjones/Documents/doss/repos/okv`

The intended shape is:

1. FoundationDB-inspired ordered transactional KV patterns in `okv`.
2. Distributed operation on object storage with S3-compatible object semantics and swappable physical file formats such as Parquet and Vortex.
3. Initial serving layers for distributed Redis semantics, distributed inverted search, distributed PostgreSQL, then DataFusion for hybrid OLTP and OLAP as ZebraDB.

The team wants outside database experts contributing quickly. Attack the plan, do not sell it. Research current primary sources, papers, and official OSS design docs as needed. Clearly separate sourced observations from your inferences.

Produce a structured Markdown review containing:

1. A one-sentence verdict and the three most dangerous assumptions.
2. The semantic contract and operational limitations that must be specified before implementation: consistency and isolation, durability and acknowledgement, time and version model, range ownership, failures and fencing, recovery, GC, backup and restore, multi-tenancy, object-store semantics, and observability.
3. Which FoundationDB patterns transfer cleanly and which fail or change on high-latency, eventually coordinated object storage.
4. A bottleneck and failure-mode matrix for the `okv` kernel and each target serving model: Redis, inverted search, PostgreSQL, and DataFusion or HTAP. Include likely ceiling, user-visible failure, correctness risk, and mitigation.
5. Whether swappable Parquet and Vortex-style formats are a sound kernel boundary. Identify the narrow interface and where format-specific semantics leak.
6. A prioritized research map of papers and OSS implementations to study or reproduce. Prefer exact components and primary links, not generic project names. Include FoundationDB, Aurora or Neon-style disaggregation, object-store LSM systems, distributed SQL, recovery and consensus, inverted indexes, Redis compatibility, DataFusion, Parquet, and Vortex where relevant.
7. Ten falsifiable experiments or eval lanes, each with invariant, workload or fault, metric, gate, and the architectural decision it resolves. Include cost and object-request curves plus tail latency, not only throughput.
8. A robustness plan for deterministic simulation, linearizability or serializability checking, crash and fault injection, object-store consistency and throttling emulation, long-running recovery and GC tests, and OTel cardinality and cost controls.
9. A phase order that gets credible outside experts contributing quickly without pretending PostgreSQL compatibility exists.
10. Five decisions the maintainers need to make now, five that should remain reversible, and a pre-mortem.

Cite URLs inline. Be precise about what is known versus proposed. Do not edit files.
