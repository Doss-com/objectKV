# objectKV RFCs

Status values: `draft`, `proposed`, `accepted`, `rejected`, `superseded`.

No architecture RFC is accepted during repository bootstrap. Code may explore a
draft behind an internal boundary, but public invariants and storage formats do
not harden until their RFC is accepted.

| RFC | Topic | Status | Opens work |
|---|---|---|---|
| 0001 | project principles | proposed | public contract |
| 0002 | version and MVCC model | proposed, implementation active | reference model and adapter |
| 0003 | immutable segment contract | proposed | segment adapter |
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
| 0023 | Routine voter reconfiguration | proposed | fresh-incarnation learner promotion without generation change |
| 0024 | SlateDB bounded physical configuration pass | accepted for local Phase 0 candidate | separate serving from maintenance; retain remote falsifiers |
| 0025 | SlateDB separate-role compaction contract | accepted for local evaluation | test maintenance outside the serving path with format parity |
| 0026 | SlateDB compaction worker process reclaim | accepted for local process-failure candidate | kill, reclaim, and replace one standalone maintenance worker |
| 0027 | SlateDB MinIO serving and compaction | accepted for local S3-compatible evaluation | cross the S3 protocol boundary with format parity |
| 0028 | SlateDB coordinator recovery | accepted for local process evaluation | adopt durable worker output after coordinator death |
| 0029 | Cell concurrent history | accepted for bounded local process evaluation | stress centralized OCC with concurrent clients |
| 0030 | SlateDB concurrent coordinator fencing | accepted for local process evaluation | reject stale overlapping coordinator epochs |
| 0031 | SlateDB active-output and orphan GC | accepted for local process evaluation | protect active work and reclaim true orphans |
| 0032 | Cell read-value and real-time witness | accepted for bounded local process evaluation | check actual reads and serialization order |
| 0033 | Cell range phantom contract | accepted for bounded local process evaluation | detect empty-range dependency cycles |
| 0034 | Cell read-version proxy causality | accepted for bounded local process evaluation | preserve session floors across proxy processes |
| 0035 | Publication GC root graph | accepted for bounded local evaluation | protect checkpoint, clone, backup, lease, and move roots |
| 0036 | Serving worker base plus retained WAL | accepted for bounded local process evaluation | reconstruct an exact target version in a fresh process |
| 0037 | Authoritative committed-envelope feed | accepted for bounded local process evaluation | separate committed serving mutations from Raft proposals |
| 0038 | Range-tagged transaction-log serving | accepted for bounded local process evaluation | reconstruct a tagged suffix after one tLog death |
| 0039 | Commit visibility after tagged-log durability | accepted for bounded local process evaluation | authenticate receipts, then exercise sustained lag, repair, and partitioning |
| 0040 | Authenticated tagged-log durability certificates | accepted for bounded local process evaluation | recover or safely abort a certified staged head during generation takeover |
| 0041 | Certified staged-head generation takeover | accepted for bounded local process evaluation | prove safe abort for an incompletely certified head |
| 0042 | Incomplete staged-head fence and abort | accepted for bounded local process evaluation | compose the admitted multi-record prefix with lag and backpressure |
| 0043 | Multi-record staged-prefix recovery | accepted for bounded local process evaluation | add sustained lag, backpressure, repair, and moving log-set policy |
| 0044 | Sustained tagged-log lag and commit ratekeeping | accepted for bounded local process evaluation | repair one failed tLog while lag is retained |
| 0045 | Tagged-log learner repair under retained lag | accepted for bounded local process evaluation | promote the repaired learner through a replicated moving log-set policy without double counting |
| 0046 | Replicated tagged-log policy transition | accepted for bounded local process evaluation | add live-tail catch-up, chunked transfer, and concurrent append during repair |
| 0047 | Resumable chunked tLog repair with live tail | accepted for bounded local process evaluation | add remote transfer, multiple repairs, transfer cleanup, and independent failure domains |
| 0048 | Partitioned resolver agreement | accepted for bounded local process evaluation | add online split and merge, concurrent in-flight work, batching, hotspot curves, and independent hosts |
| 0049 | Stateless resolver generation recovery | accepted for bounded local process evaluation | compose the tLog fence, multiple proxies, recovery-time curves, and online resolver-map movement |
| 0050 | Stateless resolver recovery from authenticated tLog inventories | accepted for bounded local process evaluation | add multiple commit proxies, recovery-time curves, and online resolver-map movement |
| 0051 | Global batch ordering across multiple commit proxies | accepted for bounded local process evaluation | add proxy-failure gap recovery, online resolver-map movement, and throughput curves |
| 0052 | Online resolver-map split through shadow catch-up | accepted for bounded local process evaluation | add split-controller recovery, merge, concurrent movements, serving-range movement, and hotspot curves |
| 0053 | Commit-proxy loss through transaction-system generation recovery | accepted for bounded local process evaluation | measure recovery-duration curves before adding within-generation takeover |
| 0054 | Transaction-system recovery work and duration curve | accepted for bounded local process calibration | optimize retained-tail inventory and recruitment, then measure independent hosts |
| 0055 | Resolver hotspot throughput curve | proposed, eval frozen before implementation | measure paired source and split throughput across balanced, missed-boundary, and crossing loads |
| 0056 | KV Runtime resource envelope | proposed, implementation active | bound shared cache, retained tail, movement, and refusal |
| 0057 | KV Runtime physical density | proposed, local candidate accepted | select one SlateDB database with logical ranges |
| 0058 | KV Runtime exact-version read | proposed, local candidate accepted | retain objectKV MVCC keys and reject unbounded history |
| 0059 | KV Runtime snapshot floor and history collection | proposed authority, local collection accepted | add replicated lease authority and crash ordering |
| 0060 | Replicated snapshot-lease and collection authority | active work, local unavailable-authority refusal admitted | add remote-object and deadline-policy controls |
| 0061 | Authority-bound range serving performance curve | active work, local cache integrity and bounded multi-range eviction admitted | replay frozen cache states on GCS |
| 0062 | KV Runtime routed exact-read service | active work, local TCP protocol candidate exists | add independent-process routing, refresh, failure, and latency curves |
| 0063 | PostgreSQL WAL-before-page objectKV writes | active work, admission oracle exists | add subordinate commit, relation extent, stable barrier, and literal write callback |
| 0064 | Incremental PostgreSQL object-delta segments | active work, local crossover admitted | replace JSON v1, bound layer growth, and verify remote objects |
| 0065 | PostgreSQL replacement-worker readiness | accepted for local OS-warm evaluation; production policy unchanged | bind provider integrity, then replay the cache-state curve on GCS |
| 0066 | Provider-bound range reads | in-region GCS cache states admitted | measure realistic cache reuse and capacity |
| 0067 | Provider-bound cache economics | implemented, first 25-percent stop points discarded | replace passive caching with an explicit feasible locality model |
| 0068 | Provider-bound locality feasibility | implemented, all 25-percent pairs discarded | define a feasible assigned-range placement and hydration curve |
| 0069 | Assigned-range placement | derived provider-free range image admitted locally | bound application memory and local-file I/O |
| 0070 | Bounded-memory range-image I/O | local-file candidate admitted; OS page cache uncontrolled | measure physical NVMe against a local incumbent |
| 0071 | Physical NVMe range-image and RocksDB incumbent | proposed, eval frozen before implementation | choose or reject a local range-image geometry before GCS hydration |

Use `0000-template.md` for new proposals.
