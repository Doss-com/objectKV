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
| 0007 | manifest publication | draft | objectification and compaction |
| 0008 | transaction isolation | draft | OCC and resolvers |
| 0009 | failure and recovery generations | proposed | fencing and recovery |
| 0010 | OLTP/OLAP snapshot semantics | proposed | HTAP materialization |
| 0011 | cell and tenant topology | draft | bounded distributed cell and metacluster |

Use `0000-template.md` for new proposals.
