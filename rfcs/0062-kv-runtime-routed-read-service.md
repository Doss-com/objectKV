# RFC-0062: KV Runtime routed exact-read service

- Status: active work, local protocol and fixed-version refresh candidates exist
- Created: 2026-08-24
- Depends on: RFC-0011, RFC-0034, RFC-0056, RFC-0058, RFC-0061

## Decision

`[DECIDED]` Direct OLTP reads terminate at one process-level **KV Runtime**,
not at one network server per Range Engine. The runtime owns a tenant-scoped
`RangeMap` view, a shared in-flight request bound, and many local Range Engine
assignments. Each request repeats the cell, tenant, range, routing epoch, and
exact read version selected by the client session.

```text
TenantSession
  -> ReadVersionService: exact T at or above causal floor
  -> RangeMap: key -> {KV Runtime endpoint, RangeId, RoutingEpoch}
  -> KV Runtime TCP service
       -> validate cell + tenant + range + epoch + bounds
       -> acquire one immutable AuthorityBoundRangeView generation
       -> recent authenticated txLog overlay
       -> shared decoded RAM
       -> shared bounded NVMe
       -> authority-selected immutable object base on miss
  <- value/rows + {RangeId, RoutingEpoch, transaction generation,
                   base frontier, applied frontier}
```

A point request must fit one assignment. A range request must fit one
half-open assignment or return its split boundary. The first client can then
fan out and merge several single-range requests at the same `T`. The server
does not silently widen one request across assignments because each assignment
can have a different routing epoch, applied frontier, or destination worker.

## Protocol candidate

`[EXISTS]` Candidate `6d0cf63` adds `KvReadRouter`,
`RoutedRangeReadRequest`, `RoutedRangeReadReply`, typed refusals, and a bounded
length-prefixed TCP/JSON protocol in `okv-object`.

One KV Runtime router owns:

- a fixed cell identity;
- tenant-indexed, non-overlapping half-open range assignments;
- one process-wide semaphore rather than one unbounded queue per range;
- maximum key bytes, scan rows, frame bytes, and request duration;
- an immutable `RangeServingState` pointer per local assignment.

The current protocol opens one TCP connection per request. JSON is a prototype
wire representation, not a stable public format. Persistent multiplexed
connections, cancellation, checksums, compression, zero-copy values, and
protocol compatibility negotiation remain future work.

## Required refusal semantics

| Condition | Result |
| --- | --- |
| unsupported protocol version | `unsupported_protocol` |
| wrong cell | `wrong_cell` |
| tenant absent from this runtime | `tenant_not_assigned` |
| key outside all local assignments | `range_not_assigned` |
| range ID or routing epoch changed | `stale_route` with current identity |
| scan crosses the selected range | `scan_crosses_range` with split boundary |
| requested `T` is below retained history | `snapshot_expired` |
| requested `T` is above the immutable view | `snapshot_unavailable` |
| process concurrency is saturated | `overloaded` |
| request exceeds its deadline | `deadline_exceeded` |
| key, scan, or frame exceeds a bound | typed bound refusal |
| storage or local state cannot prove an exact answer | `storage_unavailable` |

The server captures one immutable view before any storage access. Publication
can replace the current view concurrently, but one request cannot combine rows
from two view generations.

## Tenant and security boundary

The prototype indexes assignments by tenant and verifies that a range's
authority root carries the same tenant. This prevents accidental cross-tenant
routing inside the process. It is not client authentication or authorization.

`[FUTURE]` A public client session must carry an authenticated, cell-bound
tenant capability and deadline. Transport encryption, credential rotation,
audit identity, per-tenant quotas, and cache-encryption policy are separate
admission gates. A caller-provided tenant ID alone is never sufficient for a
public deployment.

## PostgreSQL relationship

This is the read narrow waist needed by the PostgreSQL layer. PostgreSQL
record, index, catalog, or page adapters can translate one executor access into
ordered point/range requests at one transaction snapshot `T`. They must not
open object files or consult SlateDB directly.

For a scan spanning ranges:

```text
PostgreSQL executor snapshot T
  -> route subrange A at T
  -> route subrange B at T
  -> ordered client merge
  -> retry only the stale route, or restart according to transaction policy
```

The adapter still needs write transactions, locks, unique-index enforcement,
catalog semantics, and PostgreSQL isolation mapping. This RFC admits none of
those by itself.

## First evidence and next gate

`[EXISTS]` Follow-on candidate `6361695` expands the focused real-TCP
regression to two local assignments over one
authority-bound view. It returns an exact point, exact single-range scan, and
an empty point from the second range. It refuses a stale epoch, a crossing
scan, and `T` above the applied frontier.

The frozen process gate must add:

1. independent KV Runtime and client processes;
2. at least two tenants and several ranges;
3. correct point and scan latency under bounded concurrency;
4. stale epoch, wrong tenant, crossing scan, oversized frame, saturation, and
   worker-death controls;
5. range-map refresh and retry without changing `T`;
6. request, result, cache-state, backend-request, byte, and OTel receipts;
7. RAM-warm, NVMe-warm, and object-miss profiles;
8. the same object-miss workload on `objectKV-dev` GCS.

`[EXISTS]` Candidate `bd9d959`, suite hash `64236864`, profile hash
`acef836f`, and release executable SHA-256 `b1bf79ed` add the first
independent-process gate. Correct run `740e7111` starts and kills one fresh KV
Runtime per seed. Across three seeds it returns 192 exact points and 48 exact
single-range scans, refuses three stale routes, three crossing scans, and three
unapplied snapshots, then refuses every request after worker death. Semantic
replay is exact.

On local loopback with a process-warm in-memory object store, a fresh TCP
connection and JSON frame per request, point latency was 112 microseconds p50
and 152 microseconds p99. Scan latency was 133 microseconds p50 and 231
microseconds p99. These numbers establish transport shape only. They exclude
concurrent load, object misses, route refresh, TLS, authentication, persistent
connections, and PostgreSQL executor work.

Four controls discard: accepting the prior routing epoch `b0764974`, widening
the left assignment to accept a crossing scan `2e5d1772`, accepting a wrong
result receipt `303dd238`, and omitting the worker kill `14ba8d44`.

`[EXISTS]` Candidates `17d5a5d` and `b068256` add the first tenant-scoped
client route cache, bounded authoritative refresh, and ordered multi-range
fan-out. Correct run `7636b6fc` starts three independent KV Runtimes, begins
from a stale unsplit map, refreshes once per seed to two ranges split at `k5`,
and returns all 21 expected rows across three exact `T=8` histories. Point
reads after refresh remain at `T=8`. Every worker is killed after its history,
and deterministic replay is exact.

Three controls discard: keeping the stale map `f971d9b2`, changing the read
version from 8 to 9 during retry `ea34833e`, and omitting the second range
`f1bc9a90`. This proves the client algorithm and its fixed-version invariant.
It does not yet prove a replicated RangeMap authority, endpoint replacement,
concurrent map publication, multi-tenant isolation, or reroute after worker
death. The two refreshed ranges are served by one process in this gate.

`[EXISTS]` Candidate `8fb20e5` exercises the first real downstream adapter over
this boundary. `okv-postgres` reads three authenticated 8 KiB pages across two
ranges after a stale-route refresh without changing objectKV version 2. It
also enforces a separate PostgreSQL page-LSN frontier. Four missing-page,
payload-corruption, version-drift, and LSN-ahead controls discard. This admits
the read protocol as an adapter substrate, not its security, remote latency,
replacement routing, or sustained-load profile.

## Tradeoff

Optimizes for: direct exact OLTP reads, bounded disposable compute, explicit
stale-route recovery, and one reusable substrate for PostgreSQL, Redis, and
search adapters.

Gives up: transparent server-side cross-range scans and a storage-engine-local
client. The client or higher layer owns fan-out, ordered merge, retries, and
transaction snapshot continuity. The prototype also pays a fresh TCP and JSON
cost per request until the operating curve justifies a more efficient wire
protocol.
