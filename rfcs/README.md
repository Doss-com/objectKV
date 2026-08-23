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

Use `0000-template.md` for new proposals.
