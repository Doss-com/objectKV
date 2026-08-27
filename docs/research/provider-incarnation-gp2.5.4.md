# GP2.5.4 provider-incarnation authority

Status: `[VERIFIED]` for the local compound-fence process mechanism and
`[EVALUATING]` for the real FoundationDB resurrection receipt. GP2.5.3
provider-media-loss reconstruction is `[VERIFIED]` and is an input to this
gate, not a substitute for it.

## Clarity

Question: Can objectKV activate a fresh FoundationDB provider while making a
resurrected source provider unable to acknowledge commits, receive authoritative
routing, or publish an object frontier?

Punchline: The gate requires a compound fence in which the external cell
authority owns incarnation, routing, and object roots while a transaction in the
source FoundationDB cluster durably fences its local generation before the
destination can activate.

Counter: A source image rolled back to before its local fence can still accept
local writes; preventing that case requires a bounded route lease or per-commit
external authorization and changes the GP3.1 hot-path cost.

Next: run a two-provider GCP resurrection using the exact GP2.5.3 closure and
the verified process contract.

Confidence: medium-high for the process authority and medium for the complete
provider handoff. The FoundationDB-specific fence and rollback boundary have
not yet been measured together.

## Decision boundary

The R0 mechanism is:

```text
external cell authority: Active(G1, source)
  -> stop authoritative G1 routing
  -> source FoundationDB fence transaction
       every objectKV commit reads the local generation key
       fence write conflicts with a concurrent stale commit
  -> record exact source fence position
  -> reconstruct and validate destination G2
  -> activate Active(G2, destination)

resurrect source provider from its fenced media
  -> local G1 commit rejected
  -> external route rejected
  -> external object-root publication rejected
```

The source fence and external activation are distinct. The FoundationDB key
prevents a correctly implemented stale adapter from committing on the retained
source media. The external authority prevents that provider identity from
becoming the routed cell or changing reader-visible object roots. Neither GCS
LIST nor a provider-local active-generation key chooses the current cell
incarnation.

## BIDEC result

The breadth pass found authority state, provider-local fencing, routing,
publication, resurrection media, evaluator receipts, telemetry, and cleanup.
These collapse into three executable workstreams:

1. Freeze the compound fence contract and its rollback scope.
2. Execute the existing three-process generation and publication authorities
   with one stale-incarnation poison and exact fresh-process replay.
3. Preserve the source provider disk, activate a fresh destination, resurrect
   the source, and collect a machine-bound GCP receipt before teardown.

The local process contract is necessary but insufficient. It proves that the
objectKV authority composition rejects stale operations. The GCP receipt must
then prove that the FoundationDB adapter installs and retains the provider-local
fence on a real resurrected source identity.

## Verified local-process result

Candidate `b415d502665eff9b6df4c095e33480b628348db2` received `keep` with zero
anomalies across the generation, routing, and publication surfaces. The
`accept_stale_source_incarnation` control received `discard` with exactly three
anomalies while destination operations remained available. The evaluator
reproduced each semantic report in fresh process executions, and both run IDs
were captured in OTLP logs, metrics, and traces.

Evidence:
`docs/artifacts/eval-receipts/provider-incarnation-local-r0-2026-08-27/README.md`.

## Frozen local process contract

The positive subject must prove:

1. The generation authority runs outside both provider identities.
2. `Prepare(G2)` removes G1 from authoritative routing.
3. A quorum-certified source data fence precedes destination activation.
4. A G1 commit cannot receive an authoritative acknowledgement after G2
   activation.
5. A G1 route lookup returns fenced.
6. A G1 object-frontier transition is rejected by the external publication
   authority.
7. G2 can route, commit, and publish after activation.
8. The same seed produces the same semantic report in two fresh process runs.

The `accept_stale_source_incarnation` poison bypasses the stale commit,
routing, and publication checks. The evaluator must record at least one anomaly
for each escaped surface and return `discard`.

## Frozen real-infrastructure topology

```text
controller and external authority roots
  -> source FoundationDB S on persistent provider disk S
  -> object closure and manifest in regional GCS
  -> quiesce and durably fence S without deleting disk S
  -> destination FoundationDB D on provider disk D
  -> exact named-object reconstruction and activation of G2
  -> recreate source process from disk S
  -> attempt stale commit, route, and publication from S
  -> collect positive and poison receipts
  -> destroy both provider identities and authority compute
```

The source disk in this rung is intentionally retained through resurrection.
That differs from GP2.5.3, where every source provider byte was deleted before
restore. The two gates together cover object-closure sufficiency and stale
same-media provider fencing.

## Scope boundary

A passing R0 receipt does not establish Byzantine fencing, protection against a
pre-fence disk snapshot, authority-quorum availability across zones, automatic
failure detection, or acceptable retained-write overhead. A production design
must choose one of two explicit policies for rolled-back provider media:

1. refuse it by provider-media identity and require a current external route
   lease; or
2. add per-commit external authorization and include that cost in GP3.1.

The first policy preserves the intended hot path and is the current candidate.
The second is safer against stale snapshots but risks making FoundationDB plus
objectKV economically pointless.
