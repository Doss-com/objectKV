# RFC-0043: Multi-record staged-prefix recovery

- Status: accepted for bounded local process evaluation
- Authors: DOSS
- Created: 2026-08-23
- Depends on: RFC-0005, RFC-0008, RFC-0009, RFC-0011, RFC-0039, RFC-0040, RFC-0041, RFC-0042

## Decision

A successor transaction-system generation recovers one bounded ordered staged
window by publishing its longest quorum-present prefix, aborting its first
provably incomplete record, and aborting every later record whose envelope
chains through that incomplete record. It may act only after every required
old-generation tagged-log set has durably fenced the generation and returned
an authenticated inventory quorum over the exact staged window.

The successor must block if the next record is neither quorum-present in every
required log set nor quorum-absent in at least one required log set. Timeout,
process unreachability, a partial inventory, or a locally guessed prefix is not
a disposition proof.

## Context and invariant

RFC-0041 recovers one fully certified head. RFC-0042 safely aborts one
incomplete head. A real commit proxy may pipeline multiple ordered staged
transactions before visible publication. Recovery therefore needs one
deterministic answer for the whole ordered window.

The invariant is:

```text
last visible envelope
    -> every recovered staged envelope in original order
    -> successor envelope

first aborted envelope and every later old-generation envelope
    are never visible and never enter the committed chain
```

No successor may publish beyond an aborted record, rewrite a dependent suffix,
or reuse a consumed sequence.

## Proposed contract

### Bounded staged window

The transaction authority retains an ordered unresolved window with exact
transaction identity, commit sequence, immutable envelope bytes, envelope
digest, required log sets, and any prior durability certificates. One recovery
request binds:

```text
StagedWindow {
  first_sequence
  last_sequence
  record_count
  encoded_bytes
  records[] = {
    transaction_identity
    commit_sequence
    envelope_sha256
  }
  window_sha256
}
```

The bounded gate admits at most four records and 16 KiB of encoded envelopes.
An over-limit window blocks before a fence request or replicated disposition.
Production limits remain configurable only through a later policy contract.

### Durable prefix-fence inventory

Each tLog process serializes one prefix-fence request against local appends,
persists and synchronizes the old-generation fence, scans the exact bounded
window, and signs its local presence bit for every named record. The common
statement binds cell, tenant, old generation, recovery identity, log-set ID,
policy epoch, first and last sequence, record count, encoded-byte count, and
the exact staged-window digest.

One process attestation signs the common statement plus its complete ordered
inventory. It cannot omit, reorder, substitute, or add a record. Exact retry
returns the same attestation. A different recovery identity or staged-window
digest for an already fenced generation is rejected. The fence survives
process restart and rejects every later old-generation append.

### Classification

For each staged record in sequence order:

1. `present` means every required log set has at least its write quorum of
   authenticated members reporting the exact record present.
2. `absent` means at least one required log set has at least its write quorum
   reporting the exact record absent.
3. any other observation is `unknown` and blocks recovery.

The recovery boundary is the longest leading run of `present` records followed
by the first `absent` record. A window containing only `present` records is a
publish-only takeover. An `unknown` record has no safe disposition.

Existing recorded durability certificates remain valid evidence, but the
bounded gate still requires the prefix-fence inventory for every record so the
same proof fences later old-generation activity and classifies missing
certificates.

### Replicated disposition

The active successor submits one idempotent action:

```text
TakeoverRecoverPrefix {
  previous_generation
  recovery_id
  staged_window
  log_set_inventories[]
}
```

The deterministic state machine validates the completed recovery identity,
active successor credential, exact unresolved window, byte and record limits,
envelope chain, every required inventory quorum, and the derived boundary.
It then atomically:

1. applies every mutation in the recoverable prefix in original order;
2. appends those original envelope bytes to committed history;
3. marks the first absent record and every later old-generation staged record
   terminally aborted;
4. changes the domain generation to the successor;
5. retains every recovered and aborted outcome for exact retry; and
6. leaves every consumed staged sequence unavailable for reuse.

The next successor transaction uses the first sequence after the entire old
window and chains from the last recovered envelope, or from the prior visible
envelope when the recovered prefix is empty.

### Frozen scenario

The gate starts at visible transaction 10 with four staged records:

- transaction 11 has recorded certificates and quorum presence in both sets;
- transaction 12 lacks recorded certificates but has quorum presence in both
  sets;
- transaction 13 has quorum presence in set 10 and quorum absence in set 20;
- transaction 14 is an unacknowledged dependent suffix record.

Recovery publishes original transactions 11 and 12, aborts 13 and 14, changes
the domain to generation 2, and commits successor transaction 15 chained from
transaction 12. Visible rows must include only transactions 11, 12, and 15.

## Failure model

The bounded gate covers commit-proxy loss, transaction-authority leader loss,
old-voter replacement, tLog process restart after durable fencing, lost
recovery reply, unequal local tLog inventories, stale generation append, and
exact retry. It assumes honest non-equivocating signer keys, one host with
private synchronized process roots, and no disk loss after acknowledged sync.

It does not admit signer compromise, independent-host loss, Byzantine quorum,
public-cloud distance, moving log sets, or production recovery authorization.

## Negative subjects

The frozen suite independently attempts to:

1. publish transaction 13 beyond the first absent boundary;
2. abort transaction 12 even though every required set has a presence quorum;
3. skip recoverable transaction 11 and publish transaction 12;
4. preserve or rewrite transaction 14 after transaction 13 is aborted;
5. admit a fifth staged record beyond the four-record or 16 KiB ceiling; and
6. classify the window without one required log set's complete inventory.

Every subject must replay exactly, produce a correctness anomaly, export OTel,
and discard.

## Eval plan

Freeze `cell-multi-record-staged-prefix-recovery-v0` before implementation.
Reuse seeds `1103`, `2207`, and `3301` with three external
generation-authority processes, three old transaction voters, three successor
voters, and two three-process authenticated tLog sets. Every process owns a
private synchronized root.

The primary metric is correctness anomalies. The event budget is 168 checks
across the three seeds. The receipt separately counts staged records and bytes,
prefix fence attestations, inventory observations, recovered records, aborted
records, process restarts, rejected late appends, recovery attempts, exact
retries, and the successor frontier.

## Alternatives

### Recover only already certified records

This minimizes recovery logic but leaves quorum-present work blocked when a
proxy died before recording its certificate. The inventory already proves the
exact durable fact, so ignoring it reduces availability without adding safety.

### Abort the entire unresolved window

This is simpler and safe after fencing, but throws away a durable ordered prefix
that can be published without rewriting history.

### Recover records after the first abort

This preserves more work but requires rewriting their previous-envelope chain
and can retain conflict decisions made through an abandoned prefix. The kernel
rejects this option.

## Compatibility and migration

The prefix statement, attestation, and replicated action use new versioned
formats. RFC-0041 and RFC-0042 actions remain valid for one-record windows.
Old readers reject the new action rather than partially applying it. Rollback
is permitted only before any successor prefix disposition commits.

## Unresolved questions

1. Which transaction-system policy sets the production record and byte bounds?
2. Can a tLog prove the bounded inventory with a compact authenticated range
   root without increasing recovery verification risk?
3. When can aborted suffix records, fence inventories, and their signer policy
   be garbage collected?
4. How does a moving log set preserve one inventory-policy epoch across the
   staged window?
5. Which independently authorized capability may initiate the durable fence?

## Tradeoff

This contract optimizes for one deterministic history after pipelined commit
failure. It gives up later unacknowledged suffix work, requires bounded inventory
proofs from every required log set, consumes aborted sequences, and blocks on
an unknown record rather than guessing.

## Admission evidence

Candidate `900b646` kept OTel-enabled run `ea3fb589` at the exact 168-event
budget with zero anomalies and exact replay across seeds `1103`, `2207`, and
`3301`. The path started 45 external processes, staged 12 records totaling
5,760 bytes, collected 18 prefix-fence attestations and 72 inventory
observations, restarted three fenced tLog processes, and rejected six late
old-generation appends. It recovered transactions 11 and 12, aborted 13 and
14, replayed every retained result after a lost reply, and committed successor
transaction 15 in generation 2.

Six OTel-enabled subjects discarded at the same source identity:

| Subject | Run | Anomalies |
|---|---|---:|
| publish beyond absent boundary | `fc9dda4e` | 24 |
| abort quorum-present record | `c76e5159` | 21 |
| skip recoverable prefix record | `fa35669d` | 18 |
| retain dependent suffix | `49665db4` | 27 |
| accept over-limit window | `1800d15f` | 12 |
| omit required log-set inventory | `12f27160` | 3 |

Prometheus exposed the exact candidate, suite, profile, run, workload, and
pass or fail labels for the admitted path and every control. The frozen source
suite hash is `83418bdd`; the evaluated suite hash is `398997f4`; the profile
hash is `ad522697`.

This admission covers one four-record, 16 KiB local window under honest signer
keys. It does not admit production fence authorization, key custody, moving log
sets, sustained lag, backpressure, repair, independent-host loss, partitioned
resolvers, or public-cloud operation.
