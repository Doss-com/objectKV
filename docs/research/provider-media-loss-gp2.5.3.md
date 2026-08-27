# GP2.5.3 provider-media-loss reconstruction

Status: `[ACTIVE-WORK]` contract and implementation. No provider-media-loss
receipt has passed yet.

## Clarity

Question: Can the FoundationDB-backed objectKV lifecycle reconstruct the exact
logical database after every byte of the source provider cluster is removed?

Punchline: The claim requires a fresh FoundationDB cluster plus controller
evidence that the source instance, boot disk, and data disk were absent before
the first restore write.

Counter: The result is invalid if the restore can reach a source cluster,
provider backup, disk snapshot, copied provider file, or source process memory.

Next: execute one bounded source -> objectify -> delete -> restore run and one
same-cluster poison, then admit only the receipt whose exact state digest comes
from named immutable GCS objects.

Confidence: high for the media-loss evidence boundary. This gate does not prove
multi-cluster incarnation fencing or production HA.

## Decision D54

Split provider-media-loss reconstruction from provider-incarnation fencing.
GP2.5.3 proves that the objectKV closure is sufficient after physical source
media loss. GP2.5.4 will prove that a resurrected old provider identity cannot
acknowledge, route, or publish after a newer cell incarnation activates.

This separation optimizes for one falsifiable claim per gate. It gives up
calling a deleted source process proof of a distributed generation fence.

The current FoundationDB logical-generation key is inside the provider cluster.
It can reject a stale transaction inside that cluster, but it cannot govern a
second cluster after the first cluster is lost. Production incarnation authority
must live outside both provider identities, as already required by RFC-0009.

## BIDEC result

The breadth pass identified provider state, object authority, infrastructure
identity, restore behavior, poisoning, telemetry, and cleanup. These collapse
into three executable workstreams:

1. Freeze the authority contract: distinct cluster and disk identities, named
   object generations and hashes, exact digest, and an explicit scope boundary.
2. Execute the lifecycle: source objectification, same-cluster poison, source
   instance and media deletion, then empty-cluster reconstruction.
3. Admit the receipt: independent evaluator checks, OTel logs/metrics/traces,
   immutable evidence upload, and complete infrastructure teardown.

The order is dependency-first. No cloud run starts until the evaluator rejects
the poison locally.

## Frozen R0 topology

```text
local controller
  -> source phase
       FoundationDB cluster S
       boot disk S + provider data disk S
       objectKV logical state + retained changes
       -> immutable named closure + manifest in regional GCS
  -> poison restore while cluster S media is still reachable
       exact digest is possible
       media-loss gate must reject
  -> Terraform source-to-restore transition
       delete instance S
       delete boot disk S
       delete provider data disk S
       observe all three absent through Compute API
  -> restore phase
       fresh FoundationDB cluster D
       boot disk D + provider data disk D
       named manifest GET -> named closure GET
       -> deterministic idempotent chunks
       -> exact digest
       -> destination activation + fresh commit
  -> evaluator + OTel + immutable receipts
  -> destroy remaining compute
```

Source and destination run sequentially. This keeps the correctness rung to one
data VM plus one existing collector at a time. It also makes the media transition
observable by a controller that is not stored on either provider disk.

## Authority and receipt contract

The admitted receipt binds:

- the clean objectKV source revision and pinned FoundationDB revision;
- source and destination FoundationDB cluster IDs and cluster-file hashes;
- source and destination Compute instance, boot-disk, and data-disk IDs;
- the time at which all source media became absent;
- the destination restore start time, which must be later;
- exact GCS names, generations, byte counts, and SHA-256 digests for the
  manifest and closure;
- the objectified state digest and independently read destination digest;
- empty destination observation, chunk identities, idempotent replay count,
  activation, and one fresh post-restore commit;
- the executed negative-control identity;
- required OTel logs, metrics, and traces under the formal eval run ID.

LIST does not choose an authoritative object. The restore receives the manifest
name, generation, and digest from the controller handoff and follows only the
child name, generation, and digest stored in that manifest.

## Positive hard gates

1. Source and destination cluster IDs differ.
2. Source and destination instance, boot-disk, and data-disk IDs differ.
3. The source instance and both source disks are absent before restore starts.
4. The destination keyspace is empty before the first restore chunk.
5. Named GCS GETs match exact generations, byte counts, and SHA-256 digests.
6. The manifest and closure name the same run, provider stamp, and state digest.
7. Every deterministic chunk applies once and a complete replay applies zero
   additional effects.
8. The destination digest and row count equal the object closure.
9. Activation happens only after the ready digest is durable.
10. One fresh transaction commits through the active destination generation.
11. No provider backup, snapshot, copied data directory, or source endpoint is
    an input to restore.
12. The formal evaluator emits all required OTel signals and a schema-valid
    `keep` receipt.

## Executed poison

`restore_with_hidden_source_media` performs the same named-object restore into
an empty logical generation while source cluster S and its provider media remain
reachable. The reconstructed digest may be exact. The evaluator must still
discard it because physical media loss did not happen.

This poison detects the central false-positive: relabeling GP2.5.2's logical
namespace reset as a provider-media-loss result.

## Scope boundary

Passing GP2.5.3 means objectKV has one exact FoundationDB-to-GCS-to-fresh-
FoundationDB reconstruction mechanism. It does not verify:

- a FoundationDB-supported physical backup or restore;
- quorum durability or failover;
- availability while provider media is lost;
- a resurrected source cluster fence;
- acceptable retained-write overhead;
- a production RPO or RTO.

Those claims remain in GP2.5.4, GP3.1, and the later R1 topology. FoundationDB
is still a candidate until those gates pass.

## Primary sources

- [FoundationDB configuration](https://github.com/apple/foundationdb/blob/e77b64d4c5d01d240931c08c5384a834cae27337/documentation/sphinx/source/configuration.rst)
- [FoundationDB administration and cluster files](https://apple.github.io/foundationdb/administration.html)
- [FoundationDB 7.4.6 release](https://github.com/apple/foundationdb/releases/tag/7.4.6)
