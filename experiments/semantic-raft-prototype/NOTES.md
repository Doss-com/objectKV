# Semantic transaction through process Raft

Status: `[ACTIVE-WORK]` throwaway logic prototype, 2026-08-23.

## Question

Can one centralized Cell v0 state machine preserve serializable multi-key
transactions, durable retry outcomes, and exact recovery when requests flow
through the existing three-process Raft path?

## Run

```bash
cargo run -p okv-eval -- cell-process-prototype --seed 1103
```

The command starts three actual OS processes over TCP, initializes the existing
OpenRaft cluster, commits transactions spanning keys `a` and `z`, rejects a
stale-snapshot conflict, drops a committed reply, kills the leader, elects a
successor, retries exactly, rejects a conflicting identity reuse, restarts the
killed process from its retained journal, and checks exact convergence.

## Result

Seeds `1103`, `2207`, and `3301` each passed 11 of 11 checks with zero
anomalies. Every final process had applied log index 8, rows `a=90` and `z=220`,
and the same three-envelope hash chain. Each run exercised four process starts,
one process kill, three committed transactions, two durable rejections, and one
duplicate retry.

Configured suite run `72a82770-b146-45f1-b625-33930ce45275` kept candidate
`5879252` with 33 checks, zero anomalies, and all OTel-backed hard gates passing.
The `disable_dedup` run `ea6a0b94-79b7-468c-a766-1c0a2edb08e4` discarded with
three anomalies per seed, beginning at exact lost-reply retry recovery.

The first controller revision accidentally retried an expected conflicting
identity 500 times. Semantics remained correct, but the leader reached applied
index 507. The final prototype makes one conflicting attempt and requires equal
applied indices on all processes. This is an early operational warning: a
production commit proxy should resolve retained request identities before
appending an avoidable Raft entry, while the state machine must still recheck
identity for correctness.

## Answer

`yes_within_the_bounded_prototype`

This materially raises confidence in the centralized Cell v0 composition. It
does not prove partitioned resolvers, concurrent throughput, range movement,
objectification, `C/O/WAL` frontier advancement, WAL pop, or empty-cache reads.

## Absorption decision

Absorb the protocol shape, not this dependency graph. The prototype imports the
research `CommitEnvelope` codec from `okv-sim` so it can answer the topology
question quickly. Production code must extract transaction command, outcome,
and envelope codecs into an objectKV-owned shared crate before the process gate
is merged into the main line.

The next vertical falsifier is exact objectification of these committed
envelopes, publication of one immutable root, safe frontier advancement and WAL
pop, then reconstruction of `Database(C)` on an empty worker.

## Tradeoff

The centralized state machine optimizes for a complete correctness path and
deterministic failover. It gives up scalable conflict resolution and permits
rejected or duplicate requests to consume Raft entries until pre-append retry
resolution is added.

## WAL-pop blocker

`[ACTIVE-WORK]` The current process configuration uses
`SnapshotPolicy::Never`, and `StateMachineStore` retains its current snapshot
only in process memory. A bounded `purge_without_durable_snapshot` control
stopped all three nodes after convergence, purged each journal through its last
log ID, and restarted from the remaining durable state. No process became
ready. The gate records four anomalies beginning at
`restarted_node_recovers`.

This is correct fail-closed behavior and an architectural blocker. Object
closure alone cannot authorize WAL pop. The authority must first durably bind a
state-machine snapshot, its applied log position, and the object-durable data
frontier, then prove every voter can restore the snapshot plus retained suffix.

## Durable snapshot result

`[ACTIVE-WORK]` The authority half of that blocker now passes in the bounded
process fixture. Each voter persists a versioned, checksummed `OKVS` snapshot,
synchronizes the file and directory, and restores the complete state before
OpenRaft starts. The snapshot includes applied position, membership, semantic
rows, OCC write history, commit envelopes, generation and publication state,
request fingerprints, and retained outcomes.

The first implementation attempt crashed safely before producing a snapshot.
The latent cause was useful: JSON cannot encode structured `RequestIdentity`
or byte-vector row keys as object keys. The admitted encoding represents those
maps as ordered entry arrays and rejects duplicates during restore. The durable
outer frame is binary rather than a JSON byte array, which avoids a large
second layer of numeric expansion. The failure was not hidden or promoted.

Focused seed `1103` then passed 14 of 14 checks. All three journals were purged
through snapshot index `8`; all three voters restarted; the lost-reply retry
returned its original outcome; a new transaction committed at index `10`; and
the four-envelope chain converged with rows `a=80` and `z=240`. The trace is
`b48d0aa9b4b4993fa4aaea245660d3b3ebd8b6fb4a30e201b85e180f4c48f0ef`.

Configured suite `cell-process-snapshot-v0` kept candidate `09e9344` in run
`3ec077dd-4f2e-44a4-add1-880dbe1c250c`: 42 checks, zero anomalies, 21 process
starts, 12 process kills, and exact replay across seeds `1103`, `2207`, and
`3301`. The no-snapshot control discarded in run
`4fcd329e-fc9e-496d-895d-b2fd19637491` with four anomalies per seed, beginning
at `restarted_node_recovers`. Both runs exported through the configured OTel
endpoint; Prometheus exposed the exact candidate, suite, profile, run, result,
and duration attributes.

## Immutable objectification result

`[ACTIVE-WORK]` The data half of the vertical proof now composes with the
transaction path. The durable-snapshot scenario returns its exact four-envelope
chain and final rows. A publisher writes that chain as a content-addressed
immutable segment, writes a manifest for the complete key range, verifies the
named closure, then uses a separate three-voter publication-authority role to
prepare and install the exact root under the same generation fence.

Focused seed `1103` passed 16 of 16 checks. The transaction frontier is
`C_cell=10`, the verified range closure advances `O_cell=10`, and the durable
authority checkpoint remains `S_authority=8`. The admitted log-pop frontier is
therefore `min(O_cell, S_authority)=8`. A fresh object client resolved only the
replicated root and reconstructed rows `a=80` and `z=240` by decoding and
replaying the canonical mutations in the envelope chain.

Two poisons isolate the boundaries. Publishing a manifest whose segment is
absent leaves `O_cell=0` and fails closure and empty-cache reconstruction even
though the authority root is visible. Treating `O_cell=10` as the sole pop
authority fails because retained retry outcomes and OCC state are checkpointed
only through `8`.

This does not yet prove one fused cell coordinator, incremental range
objectification, concurrent publisher races, multi-range `O_cell`, authority
snapshot transfer, or recovery from object state plus a retained WAL suffix.
The next recovery gate must start a replacement voter from an installed
authority snapshot at `8`, the published object closure through `10`, and no
pre-`8` log.

Configured suite `cell-objectification-v0` kept candidate `4fdf4a0` in run
`acdbd621-6aba-4bd6-b533-3efca57be0ed`: 48 checks, zero anomalies, exact replay,
30 process starts across both roles, 12 process kills, six immutable puts, and
six successful fresh-worker reads across three seeds. The missing-child control
discarded in `b4bf435c-2104-476a-84c6-e27d6e81789f`; the object-only-pop
control discarded in `bc4d5d2f-a9bf-4b10-a029-c5120b6ce606`. Prometheus
exposed `C=10`, `O=10`, `S=8`, and safe pop `8` as independently labeled OTel
gauges for the admitted run.

## Failed same-ID replacement-voter probe

`[ACTIVE-WORK]` The first replacement-voter attempt was discarded before an
eval lane or commit. It killed node `1`, erased its process root, copied a valid
authority snapshot at index `8`, installed the matching purged-log marker, and
restarted the same Raft node ID against peers committed through index `10`.

The replacement opened correctly at the snapshot boundary but did not receive
the retained suffix. The live leader still held volatile replication progress
through `10` for node ID `1`, so the membership identity incorrectly implied
that the new disk incarnation was caught up. A forced leader reset did not make
this safe and introduced connection failures during the fixture.

This falsifies same-ID, file-copy-only repair in the current architecture. The
next proof must add a fresh node ID as a learner, restore the durable authority
snapshot, replay the retained suffix, verify exact rows, OCC history, envelope
chain, and retained outcomes, then use generation-authorized membership change
to replace the destroyed voter. D36 records the boundary.

## Fresh-incarnation learner result

`[ACTIVE-WORK]` The authority-recovery half now passes. Blank node `4` joined
the surviving cell as a learner. Because the live voters had reclaimed their
logs through durable snapshot `8`, OpenRaft installed that snapshot on node `4`
and replayed the retained duplicate-retry and new-transaction suffix through
position `10`, followed by the learner-membership entry at `11`.

Node `4` reproduced rows `a=80, z=240`, the four-envelope chain, OCC and retry
state, and the exact retained outcomes for requests `4` and `5`. Killing and
restarting the learner retained snapshot position `8` and exact state.

Configured run `799804f8-9fe7-4db8-aa13-3ca89dabcc34` kept candidate `693cf26`
with 60 checks, zero anomalies, exact replay, 27 process starts, 15 process
kills, and a 60 of 70 event budget across seeds `1103`, `2207`, and `3301`.
Prometheus exposed zero correctness anomalies and availability ratio `1` under
the exact candidate, suite, profile, and run labels.

The log-only control `671b6db8-f377-4ed5-ad55-22c83800b41a` reached the same
logical state without installing an authority snapshot. It discarded with two
anomalies per seed, beginning at
`authority_snapshot_installed_on_learner`, while exact replay still passed.
This prevents ordinary full-log catchup from being misreported as proof of the
post-reclamation repair path.

The remaining recovery gate is the one-way transition: reserve repair through
the generation authority, prove the fresh learner's exact recovered position,
replace the destroyed voter through authorized membership change, then verify
that removed and stale incarnations cannot commit or rejoin.

## Serving recovery result

`[ACTIVE-WORK]` The disposable read path now passes the bounded recovery
equation in a fresh OS process:

```text
Database(T) = ObjectState(O) + RetainedMutations(O,T]
```

Candidate `9e733e2` kept run `ed0cdfe8-085e-4269-9d2a-6818d1df7b8d` across
three seeds. Each seed derived object state through `O=8` from the admitted
transaction history, published its root through three replicated authority
processes, synchronized the later envelope to three local WAL files with quorum
two, and started one worker with empty private state. The workers reached
`T=10`, reconstructed rows `a=80` and `z=240`, and passed 45 of 45 checks.

Ignore-suffix control `690e0844-b46a-4a93-8868-3f498a99cf23` opened the same
valid base but recovered no suffix. It stopped at frontier `8`, returned stale
rows `a=90` and `z=220`, produced nine anomalies, and discarded. Both subjects
replayed exactly and exported OTel evidence under suite hash `a6b66185`.

This is a copied local quorum-WAL fixture. It established the recovery equation
but left the live transaction-to-storage boundary open. The next result records
the corrected boundary after inspecting the original transaction journal.

## Live committed-envelope feed result

`[EXISTS]` Inspection of the original OpenRaft journal corrected the next
step before implementation. That journal carries transaction proposals,
duplicate retries, durable conflict rejections, blank entries, and membership
changes. It is not a safe serving mutation stream, and asking a storage worker
to replay it would move resolver state into the storage role.

RFC-0037 therefore freezes committed `CommitEnvelope` bytes as the transaction
to storage boundary. Candidate `e1c2437` keeps the original transaction
authority alive, publishes object state only through `O=8`, kills the current
transaction leader, and starts a worker with empty private state. The worker
resolves the object root and fetches `(8,10]` directly from the successor after
a linearizability barrier.

Run `bf79522d-86a1-40af-ab79-01284e4880e5` kept 48 of 48 checks across three
seeds. Every successor served at authority position `11`, every worker applied
one committed envelope and reconstructed rows `a=80` and `z=240`, and no copied
WAL directory existed. Dropped-final-envelope control
`3db9c604-d42e-4932-a4b3-09c748afd20b` stopped at `8`, returned stale rows
`a=90` and `z=220`, produced nine anomalies, and discarded. Both replayed
exactly and exported OTel under suite hash `18e2250f`.

The linearizable pull feed is a correct Cell v0 bridge. The next deeper gate is
a dedicated range-tagged tLog that retains the same envelope bytes, streams
them with backpressure, and survives an independent log-process failure.

## Range-tagged tLog result

`[EXISTS]` RFC-0038 moves the serving suffix into three dedicated tLog
processes. Each process owns a distinct private root, synchronizes the same
committed envelope and its range tags, and rejects the next append before a
4 KiB retained-byte limit is crossed. The worker receives no transaction
authority endpoints.

Candidate `beec908` kept run `851d0654-5d9a-4661-804f-1dc182f9e3be` with
69 of 69 checks across three seeds. Nine tLog processes accepted nine exact
records and rejected nine overflow probes. After three process deaths, the
fresh workers received six survivor responses, reconstructed three quorum
records for tag `10`, and reached exact rows `a=80` and `z=240` at `T=10` from
`O=8`.

Missing-tag control `136b2523-8912-4f99-a21d-abf8c49f335b` accepted the same
envelope without tag `10`. Both survivors answered, but the workers recovered
no suffix, stopped at `8`, returned rows `a=90` and `z=220`, produced 12
anomalies, and discarded. Both subjects replayed exactly and exported OTel
availability `1` and `0` under suite hash `1afec3bd`.

This is an independent-process serving path, not an integrated transaction
commit protocol. The next deeper gate must make transaction acknowledgement
wait for every required tagged log set, then exercise multiple records, lag
backpressure, log repair, and partitioned routing.

## Commit visibility prototype answer

`[ACTIVE-WORK]` The throwaway reducer answered the RFC-0039 state-model
question before the production-shaped path was changed. An explicit
`staged -> every required log set durable -> visible -> acknowledged` sequence
preserved one transaction identity, version, and envelope across proxy deaths.
The injected one-log-set acknowledgement violated both the visibility and
acknowledged-durability invariants.

That answer is now absorbed into the replicated Cell v0 authority and real
process fixture. Seed `1103` passed 28 of 28 checks: the request stayed at
visible frontier `10` while proxy one durably recorded log set `10` and died,
and while proxy two durably recorded log set `20` and died. Proxy three
published and acknowledged the same envelope at version `11`; a retry returned
`already_committed` without another tLog append. Six tLog processes retained
one exact record each, and a fresh worker reconstructed the visible rows at
`11` from both log-set quorums.

The one-log-set acknowledgement control produced 17 anomalies, remained at
frontier `10`, left log set `20` empty, and failed fresh-worker recovery. The
interactive prototype has therefore been removed. Candidate `c549587`
subsequently kept run `5a2e5a7f` with 84 of 84 checks, exact replay, and OTel
availability `1`. Control `0da1a0c1` discarded with 51 anomalies and
availability `0`. RFC-0039 is accepted for bounded local process evaluation.

## Authenticated tagged-log certificate result

`[EXISTS]` RFC-0040 removes the proxy from the durability trust boundary. The
replicated authority installs each log set's signer policy separately, and a
tLog process signs only after finding the exact synchronized local record.
Candidate `6a81821` kept run `f5e3720a` with 96 of 96 checks across three seeds,
45 process attestations, six proxy deaths, exact retry, and exact fresh-worker
recovery at `T=11`.

Unsigned node-list, duplicate-attestation, wrong-log-set, tampered-statement,
and obsolete-policy controls each produced 51 anomalies and discarded. OTel
reported availability `1` and correctness `0` for the admitted path, then
availability `0` and correctness `51` for every control.

The implementation also separated logical transaction commit sequence from
the authority Raft log position because policy installation is a control-plane
command. The envelope still binds its actual log index. The next vertical
falsifier is generation takeover of an incomplete staged head, including a
proof to publish the exact certified transaction or fence the old log
generation before abort.

## Certified staged-head generation takeover result

`[EXISTS]` RFC-0041 joins the authenticated staged-commit state to the existing
external generation authority and real voter-set handoff. Candidate `f350a12`
kept run `959a2211` with 105 of 105 checks across three seeds and exact replay.
The old generation remained visible at `T=10` through fencing and recovery. An
active successor then published the original transaction-11 envelope once,
recovered a lost reply from durable outcome state, and committed transaction 12.

Takeover during recovery, a missing log certificate, a tampered envelope
expectation, skipping the staged head, and rewriting it as a successor
transaction produced 6, 3, 3, 30, and 27 anomalies. Every control discarded and
exported OTel availability `0`; the correct path exported availability `1` and
correctness `0`.

This closes only the fully certified-head branch. A missing certificate still
blocks safely. The next vertical falsifier must prove that the old tLog
generation is fenced strongly enough that no late quorum can appear before the
authority aborts an incomplete head.

## Incomplete staged-head abort result

`[EXISTS]` RFC-0042 closes the one-head incomplete branch without treating
timeout as proof. Candidate `341beb9` kept run `338ef8b4` with 132 of 132
checks, zero anomalies, and exact replay across three seeds. Every old tLog set
was durably fenced under the same recovery identity, one incomplete set proved
quorum absence, and the active successor aborted transaction 11 without
changing rows or visible frontier `10`. The abort reply was replayable, a
restarted tLog quorum rejected late old-generation appends, and successor
transaction 12 chained from the last committed envelope.

Early abort, single absence signer, missing fence, forged absence, volatile
fence, and sequence-reuse controls produced 3, 9, 6, 12, 6, and 6 anomalies.
Every control discarded and exported availability `0`; the admitted path
exported availability `1` and correctness `0`.

This closes one incomplete staged head under the bounded signer model. The
next fatal composition test is a multi-record staged prefix with a certified
prefix, one provably incomplete head, deterministic suffix disposition, and
bounded retained bytes. Production recovery authorization and signer custody
remain separate control-plane requirements.

## Multi-record staged-prefix recovery result

`[EXISTS]` RFC-0043 closes the first pipelined recovery window. Candidate
`900b646` kept OTel-enabled run `ea3fb589` with 168 of 168 checks, zero
anomalies, and exact replay across three seeds. Each seed staged transactions
11 through 14. Prefix-fence inventories proved 11 and 12 present in both
required log sets, 13 absent in one set, and 14 dependent on the abandoned
suffix. The active successor published original envelopes 11 and 12, aborted
13 and 14, replayed the lost disposition reply, and committed transaction 15
from envelope 12 in generation 2.

Publishing beyond absence, aborting quorum-present data, skipping a
recoverable record, retaining the dependent suffix, accepting a fifth record,
and omitting one required inventory produced 24, 21, 18, 27, 12, and 3
anomalies. Every control replayed exactly, exported OTel, and discarded.

This closes one four-record, 16 KiB local window under honest signer keys. The
next fatal composition tests are sustained tLog lag with ratekeeping, repair of
one failed tLog process without losing the inventory boundary, and log-set
movement across a policy epoch. Production recovery authorization, signer
custody, independent hosts, and partitioned resolvers remain open.

## Online resolver-map split result

`[EXISTS]` RFC-0052 closes the first same-generation resolver-map movement.
Candidate `04738b5` kept OTel run `30297004` with zero anomalies and exact
replay across three seeds. The old source remained authoritative through batch
15 while two empty children installed clipped history and shadowed new work.
All proxies and both required tLog sets applied the batch-16 map mutation before
the children served batch 17. Across the histories, 261 of 360 attempts
committed, 87 conflicted, and 12 cutover requests abandoned and retried under
new identities. No durable database bytes moved.

Early cutover, omitted history, mixed epochs, retired source replies, partial
child routing, stale proxy maps, incomplete tLog durability, and descriptor
mutation produced 27, 12, 3, 3, 3, 3, 15, and 9 total anomalies. Every control
replayed exactly, exported OTel, and discarded.

The result admits bounded conflict-metadata movement, not serving-range
movement or a throughput claim. Split-controller recovery, merge, concurrent
movements, history-size and hotspot curves, independent hosts, and production
key custody remain open.

## Commit-proxy generation-recovery result

`[EXISTS]` RFC-0053 closes the first commit-proxy loss composition without an
invented within-generation gap filler. Candidate `bf72639` kept OTel run
`1c55dad7` with zero anomalies and exact replay across three seeds. Each seed
uses four generations and kills one proxy before resolver delivery, after one
required tLog set reaches quorum, and after all required sets reach quorum but
before the client reply. The first two ticket suffixes abandon; the third batch
survives exactly once and resolves through its stable request identity.

Across the histories, 432 transaction attempts use 108 sequencer tickets, 336
commit, 24 abandoned batches retry, 1,044 resolver decisions execute, and 510
tLog records synchronize. Continuing the generation, substituting a no-op,
publishing partial durability, omitting a fully durable unknown result,
crossing a gap, trusting an incomplete inventory, reusing a version, accepting
a fenced reply, and duplicating the unknown mutation all replay exactly and
discard.

The result supports the conservative FoundationDB-style generation boundary.
It does not show that recovery is fast enough. The next falsifier must isolate
recovery downtime and vary pending-window size, tLog count, and retained-tail
length before exact-batch proxy takeover is considered.

## Transaction-system recovery curve result

`[EXISTS]` RFC-0054 replaces the RFC-0053 full-history elapsed time with 210
isolated samples. Candidate `90c1526` keeps all ten correct points with exact
untimed receipts and zero permanent database reads. Tail points 256, 4,096, and
65,536 recover in median 0.292, 0.465, and 3.158 seconds. The large inventory
phase alone takes 2.870 seconds. Pending 8 and 512 are flat. The largest 4x5
tLog and 33-resolver topology takes 1.313 seconds, including 0.616 seconds of
inventory and 0.607 seconds of sequential recruitment.

Database scan, one-set trust, quadratic inventory, and early admission controls
discard. The result keeps full generation recovery as the conservative
fallback while moving the availability work to compact inventory summaries,
checkpoint cadence, parallel recruitment, and independent-host measurement.
