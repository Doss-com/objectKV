# objectKV RFCs

Status values: `draft`, `proposed`, `accepted`, `rejected`, `superseded`.

No architecture RFC is accepted during repository bootstrap. Code may explore a
draft behind an internal boundary, but public invariants and storage formats do
not harden until their RFC is accepted.

| RFC | Topic | Status | Opens work |
|---|---|---|---|
| 0001 | project principles | proposed | public contract |
| 0002 | version and MVCC model | proposed, implementation active | reference model and adapter |
| 0003 | immutable segment contract | proposed, point-read pilot implementation active | segment adapter and cold-point curve |
| 0004 | object-store correctness | proposed | backend conformance |
| 0005 | durability and WAL | proposed | replicated fast log |
| 0006 | logical range model | draft | routing and movement |
| 0007 | manifest publication and GC | proposed | objectification and compaction |
| 0008 | transaction isolation | draft | OCC and resolvers |
| 0009 | failure and recovery generations | proposed | fencing and recovery |
| 0010 | OLTP/OLAP snapshot semantics | proposed | HTAP materialization |
| 0011 | cell and tenant topology | draft | bounded distributed cell and metacluster |
| 0012 | DataFusion snapshot overlay implementation | proposed | physical HTAP source and operator |
| 0013 | Streaming DataFusion snapshot overlay | proposed | bounded ordered HTAP merge |
| 0014 | Real object publication adapter | proposed | physical publication and guarded sweep |
| 0015 | Replicated publication authority | proposed | fenced root, pin, intent, and deletion-reservation state |
| 0016 | Publication worker recovery and object-effect fencing | proposed | crash-safe publisher, sweeper, and generation handoff |
| 0017 | Publisher process recovery after prepare | proposed | first disposable object-effect worker boundary |
| 0018 | Publisher recovery after ambiguous object PUT | proposed, implementation active | partial upload and lost-response recovery |
| 0019 | Publisher recovery after ambiguous manifest PUT | proposed, implementation active | closure verification after manifest ambiguity |
| 0020 | Publisher recovery after lost replicated Publish response | proposed, implementation active | acknowledgement-aligned authority outcome recovery |
| 0021 | SlateDB Phase 0 filesystem baseline | proposed, implementation active | first executable physical-economics incumbent |
| 0022 | SlateDB filesystem scale curve | proposed, implementation active | 1 MiB to 64 MiB reopen and object-I/O shape |
| 0023 | Resident ServingWorker hot path | proposed, implementation active | direct RocksDB control and object-fallback poison |
| 0024 | Ordered log substrate and WAL layering | proposed, implementation active | reusable log algebra and WAL adapter |
| 0025 | SSD and RAM serving profiles with independent durability | proposed | common serving-image contract, profile transitions, and explicit durability modes |
| 0026 | ServingWorker process recovery from objects plus txLog | `[EVALUATING]` | authoritative-root, durable-tail, and empty-replacement composition |
| 0027 | Authority-owned retained transaction stream | proposed, implementation active | journal-independent recovery suffix and concurrent catch-up |
| 0028 | Bound transaction-authority state before safe pop | `[EVALUATING]`, current layout discarded | 9.172x ideal-pop snapshot growth requires state-owner split |
| 0029 | Split transaction-authority retention frontiers | `[EVALUATING]` | separate serving, resolver, retry, and recovery ownership |
| 0030 | Authenticated object frontier and crash-safe txLog pop | `[PROPOSED]` | pending-to-active publication proof and physical recovery-stream reclamation |
| 0031 | Bounded concurrent group commit | `[CODE-COMPLETE]`, final mechanism discarded | same-durability pipelining and stable-I/O curve |
| 0032 | Transaction batch entry | `[CODE-COMPLETE]`, local receipt evaluating | shared commit version, ordered versionstamps, and explicit leader-side batching |
| 0033 | Commit-proxy batch closure and admission | `[CODE-COMPLETE]`, G4.10a.1 local receipt evaluating | independent requests, bounded delay and bytes, explicit overload |
| 0034 | Compact transaction and batch wire | `[CODE-COMPLETE]`, local receipt evaluating | backward-readable base64 byte fields for v2 bootstrap wire |
| 0035 | Concurrent commit-proxy and object-frontier composition | `[CODE-COMPLETE]`, G4.10b local receipt evaluating | conflict curve, concurrent safe pop, and exact object-plus-tail recovery |
| 0036 | Independent-media object-frontier convergence | `[PROPOSED]`, G4.11a mechanism code complete | frontiered bounded snapshots, remote objects, and host-loss proof |
| 0037 | Manifested multi-layout LSM | `[PROPOSED]`, evaluation contract frozen | row, Parquet, random-access columnar, and hybrid object-layout fork before G4.11b |
| 0038 | First integrated single-range kernel API | `[PROPOSED]`, implementation code complete, local receipt evaluating | public object-base plus versionstamp-safe txLog-tail composition |
| 0039 | SingleRange serving-image boundary | `[CODE-COMPLETE]`, `[EVALUATING]` | provider-neutral hot-state activation and the public SSD point-read curve |
| 0040 | Native resident-engine data plane | `[VERIFIED]` topology-matched single-range read boundary under D56 | native concurrency and replicated-commit gates, with FoundationDB retained as oracle and fallback |
| 0041 | Incumbent transaction-plane adapter | `[CODE-COMPLETE]` contract and source-pinned preflight, `[EVALUATING]` fallback lifecycle | semantic oracle, object continuity control, and fallback profile |
| 0042 | Native resident concurrent-read curve | `[VERIFIED]` GCP R0 at 8 and 32 clients | paired resident concurrency admission before cache pressure |
| 0043 | Native resident cache-pressure curve | `[EVALUATING]` experiment contract frozen | explicit cache, reusable fixture, and physical-work attribution |
| 0044 | Content-addressed object-frontier fixtures | `[EVALUATING]` | phases 0 through 4 verified; 1 GiB T27 admission remains |
| 0045 | Staged quorum txLog and object-log publication | `[PROPOSED]`, L0 protocol and L1 process mechanics `[VERIFIED]` | one-round-trip log service, bounded tail, object segments, and T29 comparison |
| 0046 | Generation-pinned GCS cold-point and layout curve | `[PROPOSED]` | read-only exact-open, indexed range GETs, bounded refill, and separate DataFusion scan lanes |
| 0047 | Sparse post-frontier resident history | `[PROPOSED]` | remove the 2.015x disposable base duplication, preserve exact snapshots, and replay the rejected T27 stratum |
| 0048 | Generation-pinned typed object-layout curve | `[EVALUATING]`, retained preflight rejection | C5v1 verified scan leverage and localized sequential point-request tail |
| 0049 | Aligned columnar point gather | `[CODE-COMPLETE]` admission; immutable GCS publication, preflight, recovery, corruption, compaction, and branch gates `[VERIFIED]`; full admission `[EVALUATING]` | C5v2 point p99 0.869x C0, scan throughput 31.692x C0, compaction bytes 1.040058x C0, branch root 4,344 bytes with zero child copies; independent OTel and sealed curve verdict next |
| 0050 | Executable cell reference model | `[PROPOSED]`, two finite TLA+ scopes `[VERIFIED]`, Rust trace checker `[CODE-COMPLETE]`, complete-cell conformance `[EVALUATING]` | one integrated architecture contract for concurrency, durability, publication, recovery, and serving tiers |

Use `0000-template.md` for new proposals.
