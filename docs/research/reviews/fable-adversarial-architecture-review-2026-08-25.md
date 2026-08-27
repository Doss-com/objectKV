<!--
Raw external model response captured without editorial changes.
Tool: OpenCode CLI 1.18.20
Model: anthropic/claude-fable-5
Agent: plan, read-only
Target: /Users/wileyjones/Documents/doss/repos/okv
Branch: research/object-publication/process-recovery-v1
HEAD: a56442ad800deedd72a404a0886e88831eb308a0
Session: ses_fc342493effe55dc07M0a8iGPT
Exit: 0
Command:
opencode run --print-logs --model anthropic/claude-fable-5 --agent plan --format default --title "objectKV adversarial architecture review 2026-08-25" "Execute the attached review brief. Inspect the repository files directly. Return the full structured Markdown review in your response. Do not edit files." --file docs/research/reviews/fable-adversarial-architecture-brief-2026-08-25.md
-->

# Fable adversarial architecture review: objectKV at `a56442a` (dirty tree), 2026-08-25

Target: branch `research/object-publication/process-recovery-v1`, HEAD `a56442a`, materially dirty working tree inspected as-is.

---

## 1. Verdict

**BUILD NARROWLY, confidence 0.6.** The decisive reason: nothing observed falsifies the product thesis yet, but the current gate ladder is spending its weeks on publication micro-ambiguities (four RFCs, four suites, four ledger receipts for four kill boundaries of one publisher) while the three claims that can actually kill the product, strict-serializable conflict detection, multi-host commit latency versus a same-durability control, and cloud object economics at real dataset scale, have zero executable evidence and in one case (RFC-0008) no design.

---

## 2. Source ledger

### Observed repository facts (load-bearing)

| # | Fact | Citation |
|---|---|---|
| F1 | Everything is single-machine. Every "cluster" is 3 OS processes on loopback (`transport = "tokio-tcp"`, `127.0.0.1`) or turmoil simulation; the scenario itself says "three-node catch-up on one host" | `evals/suites/raft-process.toml:28-31`; `evals/scenarios/objectkv-playground-golden-path-v3.toml:50` |
| F2 | Consensus elections, heartbeats, and ticks are disabled; leadership changes only when the test controller sends `ELECT`. No autonomous failure detection exists in code | `crates/okv-consensus/src/process_node.rs:186-198` (`enable_tick: false, enable_heartbeat: false, enable_elect: false, snapshot_policy: Never`) |
| F3 | No conflict detection is implemented. `read_conflicts`/`write_conflicts` are opaque unevaluated byte fields; the model has no commit path from `TransactionView`, no read-set tracking, no range/predicate conflicts | `crates/okv-sim/src/commit.rs:85-86`; `crates/okv-model/src/lib.rs:420` |
| F4 | RFC-0008 (transaction isolation, the semantic heart) is a `draft`; conflict-range representation, read-version authority, commit-unknown contracts are all open questions | `rfcs/0008:3,36-45` |
| F5 | The experiments ledger has exactly 19 entries, all recorded in one overnight window 2026-08-22T22:14 to 08-23T06:04; 18 `keep`, 1 `stop_incumbent_configuration` | `experiments/ledger.jsonl` |
| F6 | Every `[VERIFIED]` product-thesis gate rests on toy-scale contract admission: G0.2 on 40 sim events / 0 keys; G8.1 HTAP on 9 keys / 21 base rows / 27 tail rows | `evals/suites/commit-contract.toml:19-27`; `evals/suites/htap-streaming.toml:20`; ledger #11 |
| F7 | Ledger entries omit machine, rustc, and lockfile hash, which `evals/schema/result.schema.json:34` requires and AGENTS.md mandates | all 19 entries; `evals/schema/result.schema.json:34` |
| F8 | SlateDB incumbent stopped: 64 MiB reopen read 210,773,938 bytes (3.1x the dataset) before first correct read; 1 and 8 MiB were metadata-bounded | ledger #19; `docs/BOOTSTRAP-PLAN.md:202-209`; D30 `docs/DECISIONS.md:487-505` |
| F9 | Resident hot path: candidate/control throughput ratio 1.0173, p99 ratio 1.0082, single client, no RPC, self-labeled "inconclusive, dirty source"; G3.1 is `evaluating` with research docs, not a ledger receipt, as evidence | `docs/research/resident-hot-path-g3.1.md:3-27`; `evals/programs/objectkv-product-thesis-v1.toml:149-162` |
| F10 | Real object-store client exists with real conditional writes (ETag `If-Match`, `PutMode::Create/Update`); MinIO passes authority conformance; GCS has never run ("cloud authentication is unavailable"); no AWS profile exists | `crates/okv-object/src/lib.rs:239-244,1658`; `docs/OBJECT-STORE-SUPPORT.md:8-13`; `CONTEXT.md:168` |
| F11 | Real fsync WAL with frozen `OKVR` format, torn-tail recovery, fail-closed corruption; but "replicated" WAL is 3 files in one directory | `crates/okv-wal/src/node_journal.rs:3-5,285`; `crates/okv-wal/src/lib.rs:117-126` |
| F12 | Publisher recovery for all four ambiguity boundaries (process kill, ambiguous PUT, ambiguous manifest, lost Publish reply) exists as real-process contracts with negative controls and ledger receipts | `crates/okv-object/src/publisher_process.rs:551,667,716`; ledger #14-17 |
| F13 | RFC-0006 (logical range model) is 13 lines of questions, yet RFC-0011, 0023, 0025 assume "ordinary range assignment generation" as existing machinery | `rfcs/0006:6-13`; `rfcs/0025:152`; `rfcs/0023:18` |
| F14 | Branch, backup, and CDC roots are load-bearing GC/retention inputs with no producing protocol in any RFC | `rfcs/0002:92-94`; `rfcs/0007:89-93` |
| F15 | The single-log vs MultiRaft decision is explicitly unaudited, yet PRODUCT-SPEC draws MultiRaft as the architecture diagram | `docs/PRODUCT-SPEC.md:28-32,176-182,496`; `docs/SYSTEM-SHAPE.md:8-11` |
| F16 | The playground receipt script emits `status: "VERIFIED"` for GP-G0..G6; none of those receipts are in the ledger | `experiments/run-okv-playground-golden-path.sh:131-146` |
| F17 | The product thesis is self-consciously conditional, with a stop rule: "Stop or pivot to TiKV or RocksDB... if one focused optimization cycle cannot satisfy G3, G4, and at least one of G5 or G8" | `docs/PRODUCT-SPEC.md:7-9`; `docs/BIDEC-EVAL-PROGRAM.md:303` |
| F18 | The internal pivot destination is already named: "narrow the architecture to an object-native storage/publication layer over an existing transaction authority" if the vertical proof requires two commit authorities or unbounded WAL | `docs/research/overnight-strategy-audit-2026-08-22.md:69-73` |

### Inference (labeled)

- I1: Because elections are disabled (F2) and all faults are scripted, no receipt in the tree constrains tail latency or availability under real asynchrony. Inference from F1+F2.
- I2: The 3.1x reopen scan (F8) is weak evidence that object-native reopen cost is a live, not theoretical, failure mode for any LSM-over-objects design, including objectKV's own future manifests. Inference; objectKV's manifest design differs.
- I3: The Aug-22 Fable risk #1 ("strict serializability assembled from parts... the single most likely place the project dies", `docs/research/reviews/fable-2026-08-22.md:17`) is narrowed on the recovery half (generation certificates, publisher recovery, F12) and untouched on the conflict half (F3, F4). Adjudication of a prior hypothesis against the current tree.

---

## 3. Strongest build case

**Why owning objectKV could be rational.** The one property incumbents cannot cheaply retrofit is the `O <= C` invariant integrated end to end: `Database(C) = ObjectState(O) + txLog(O, C]` (`CONTEXT.md:220-230`) where the same authority that assigns commit versions also owns objectification frontiers, branch roots, GC reservations, and serving-image coverage. From that one invariant fall out the four real differentiators: (a) disposable serving, every local byte reconstructable, so recovery and elasticity are metadata operations (`rfcs/0001:13`); (b) metadata-scale branches, demonstrated at bounded scope by GP-G6 (child stores manifest plus divergent suffix only, `docs/PLAYGROUND-GOLDEN-PATH.md:46`); (c) exact version-aligned HTAP, `LogicalTable(T) = ColumnarBase(W) + RowChanges(W, T]` sharing the commit version space (`rfcs/0010:17`); (d) zero-copy range movement, which the Aug-22 review called "the strongest genuinely novel claim" (`fable-2026-08-22.md:69`).

TiKV cannot do this without rewriting its storage ownership model; FoundationDB's storage servers own local disks as the permanent tier; Postgres-over-disaggregated-storage (Neon/Aurora) delivers (a) but not (b)-(d) in one version space. TiDB X is the nearest shape and the repo already treats it as the benchmark, not a novelty claim (`docs/PRODUCT-SPEC.md:47-53`).

**Minimum architecture preserving the upside.** One cell, one ordered txLog (Cell v0, single OpenRaft group), one version authority, the publication authority co-located on the same group (RFC-0015), the two serving profiles behind one `ServingImage` contract (RFC-0025), and the exact overlay reader. No MultiRaft, no metacluster, no Redis/search consumers, no shape-C SQL. Everything beyond this is deferrable, and the docs mostly already say so (`docs/PROJECT-TRACKING.md:95-96`).

**Supporting quality signal.** The engineering discipline is genuinely above average: frozen seeds, suite hashes, mandatory negative controls that must discard, append-only ledger recording failures (D30 stop is in the ledger, not buried), byte-frozen formats with fixtures. This team could plausibly execute the hard parts. That is a reason the option value of continuing is real; it is not a reason the thesis is true.

---

## 4. Strongest do-not-build case

**The alternative: FoundationDB (or TiKV) as the transaction authority, objectKV's publication/branch/HTAP layer built above it.** This is not a hypothetical: it is exactly Tigris's production architecture, and the repo's own study concedes "Tigris still uses FoundationDB as the authoritative transaction... substrate" (`docs/research/tigris-codebase-study.md:13-16`). It is also the repo's own decision-table row (d) pivot (F18).

What it buys: the entire contents of RFC-0005, 0008, 0009, and the unwritten resolver/read-version/conflict machinery arrive free, battle-tested, with real deterministic simulation (FDB) or real production scale (TiKV). That is precisely the region where objectKV today has a draft RFC and zero code (F3, F4), and where the prior review located death (I3). The four differentiators (a)-(d) survive almost intact as a layer: publication authority state, branch roots, GC reservations, and analytical tails are all just keys in the substrate; the disposable-serving thesis holds because permanent bytes are still objects.

What it gives up: (1) commit-path integration, txLog retention (`O` frontier driving WAL pop) becomes two systems' retention policies stitched together, which is where debt bounds get ugly (`rfcs/0005:26-34` becomes a cross-system contract); (2) FDB's 5 s transaction and 100 KB value limits, which Tigris pays for with chunking and continuation leases (`tigris-codebase-study.md:151-207`); (3) version-space ownership, HTAP alignment must map substrate versions to overlay watermarks; (4) control over the hot serving path read semantics. 

**Why the trade can still be better:** the giving-up list is measured in adapters and awkwardness; the keeping list (build your own strict-serializable distributed commit, recovery, and membership) is measured in years and is the historically fatal part. If G6 multi-range coordination or G1 real-quorum latency fails its control, this repo converges to the layered design anyway, having paid the kernel tax first.

The honest counterweight: the layered design has its own falsifier, permanent double authority (substrate WAL plus objectification), which the Tigris study lists as a reversal condition (`tigris-codebase-study.md:414-429`). Neither side of this trade has receipts yet. That is itself the strongest argument for reordering the tree (section 8) rather than deciding today.

---

## 5. Ranked existential risks

**R1. Strict serializability has no design, let alone proof.** Mechanism: every other RFC leans on "a serializable transaction" (`rfcs/0007:62-63`, `0010:139-140`, `0011:183-184`) that RFC-0008 does not yet define; conflict-range representation and GC are open (`0008:41`); code has zero conflict detection (F3). Current evidence: none positive. Missing: a resolver model, phantom handling outside the HTAP certificate sketch, a differential check against a serializability oracle. Earliest decisive test: 1,000 concurrent multi-range histories through the model plus envelope codec, checked by an external oracle (Elle-style), pure in-process, roughly two weeks. Stop gate: if declared conflict ranges cannot express the product's own HTAP dependency certificates without cell-wide coarse tokens dominating, pivot to a substrate that already provides SSI.

**R2. Commit-path economics on real quorum and real media are unmeasured.** Mechanism: `COMMITTED` requires quorum fsync across failure domains (`rfcs/0005:9-13`); every current receipt is loopback with elections disabled (F1, F2); the 25%-of-MultiRaft-control target (`docs/PRODUCT-SPEC-SHEET.md:349`) has no control run. Missing: any cross-machine commit latency number. Earliest decisive test: three real machines, real disks, the existing raft-process contract plus a sustained commit workload versus a TiKV control at identical durability. Stop gate: p99 commit more than 25% over control after one optimization cycle (the repo's own CURVE-COMMIT).

**R3. Cold-read and reopen amplification at real scale.** Mechanism: manifest plus index work per cold point read; the incumbent evidence is negative (F8, 3.1x dataset scan at just 64 MiB); objectKV's own manifest/row-object index design is an open question in both spec sheets (`docs/PRODUCT-SPEC-SHEET.md:423`). Missing: any run above 64 MiB; the planned 10 GiB / 10 M key dataset (`evals/suites/phase0.toml:19-22`) never executed; zero cloud latency data (F10). Earliest decisive test: cold indexed point reads at 10 GiB on MinIO plus one real cloud store, gate "at most one data-range GET after warmup" (`PRODUCT-SPEC-SHEET.md:348`) plus a p99 ceiling. Stop gate: cold GET count or bytes growing with database size (already the repo's invalidation condition, `playground-g0-g6-architecture-review-2026-08-25.md:82-86`).

**R4. The RAM profile may never clear its own 20% bar, and SSD parity may evaporate under composition.** Mechanism: the 1.0173 ratio (F9) is single-client, read-only, no RPC, no overlay, no writes; excluded costs "could dominate the roughly 1.3 microsecond local-engine baseline" (`resident-hot-path-g3.1.md:72-74`). If SSD-resident RocksDB-with-wrapper is the end state, the product is "TiKV with an async uploader", the repo's own named failure (`PRODUCT-SPEC-SHEET.md:23`). Missing: concurrency curves (1/4/16/64 clients), write mix, overlay and tombstone costs, hydration. Earliest decisive test: the admitted SSD vs RAM matrix already specified as the "critical next result" (`docs/PROJECT-TRACKING.md:81-87`). Stop gate: per D33/RFC-0025, no workload where RAM beats admitted SSD by 20% end to end kills `ram_resident` as a product; SSD outside 20% of direct RocksDB kills the wrapper.

**R5. Outage debt and txLog exhaustion behavior exists only as prose.** Mechanism: the four-state lag machine (`rfcs/0005:176-181`) and the 30-minute 503 brownout are the load-bearing availability story; `fault-recovery.toml` (1800 s brownout) has never run. Missing: any measured debt curve. Earliest decisive test: brownout injection on the integrated slice. Stop gate: unbounded objectification debt or acknowledged-commit loss (already listed, `playground-g0-g6:82-86`).

**R6. Two-concept authority creep: the coordinator/generation group is quietly accreting publication intents, pins, reservations, outcome retention (RFC-0015), and eventually range assignment (RFC-0006 vacuum, F13).** Mechanism: a "not a second consensus cluster" (`rfcs/0015:13-14`) becomes the global bottleneck and the single blast radius. Missing: any load or state-size bound for the authority group. Earliest decisive test: authority-state growth accounting in the integrated slice under branch plus GC churn. Pivot gate: if authority state needs partitioning before Cell v1, the "one quorum per cell" decision (D16) must be reopened.

**R7. Retained-identity exactly-once is contract text without a retention design.** Mechanism: 0005 promises stronger-than-FDB dedup (`0005:43-45,57-58`) while outcome expiry vs retry window is open in three RFCs (`0005:251-252`, `0011:220-221`, `0002:167-168`) and OpenRaft snapshots are disabled everywhere (F2), so the promise currently rests on unbounded log replay. Earliest decisive test: outcome-compaction design plus restart-after-snapshot contract. Stop gate: the RFC's own clause, "the public contract must weaken explicitly" (`0005:62-63`).

**R8. PostgreSQL bridge is a named north star with zero executable anything.** "No PostgreSQL integration exists yet" (`docs/POSTGRES-PATH.md:3`). The double-commit-authority question (`BIDEC-EVAL-PROGRAM.md:163`) is exactly the layered-design falsifier. Cheap decisive probe: the `smgr` tracing wrapper the overnight audit already proposed (`overnight-strategy-audit:77-91`), never built.

Below the existential line: multi-tenancy (one constant `tenant_id` in a codec, `crates/okv-sim/src/commit.rs:13`), encryption (absent, and key retirement is a GC liveness root per `rfcs/0007:208-217`), backup/restore (roots without producers, F14), operational control loops (section 6, area 7).

---

## 6. Cross-layer contradiction matrix

| # | Attack area | Sharpest contradiction found | Failure trace |
|---|---|---|---|
| 1 | Semantics | RFC-0008 is a draft while 0007/0010/0011 require serializable transactions (F4); model implements snapshot visibility only (F3) | Two publishers prepare intents whose root-install transactions interleave; nothing today defines the conflict that serializes them; `rfcs/0007:70-72` assumes it |
| 2 | Consensus/durability | 0005 defines `COMMITTED` as WAL-quorum-fsync and rejects synchronous object publication (`0005:206-208`); 0025 re-admits `object_ack` requiring a "fenced durable decision" in object storage that principle 5 forbids ("Object storage is not the coordination system", `rfcs/0001:16`) | An `object_ack` tenant's commit needs an authority no RFC provides; either 0025 silently supersedes 0005 or the profile is unimplementable |
| 3 | Publication/GC | 0014's sweeper retires a reservation by named read (`0014:87-88`); 0016 declares that unsafe without effect-grant fencing ("Process death alone is not proof of fencing", `0016:164`); neither supersedes the other | Sweeper retires per 0014; a zombie publisher with copied credentials deletes a re-published digest; premature delete. Also: branch/backup/CDC roots pinned by GC have no creation protocol (F14), so "walk every root" (`0007:15-18`) is unenumerable |
| 4 | Resident serving | GP-G4 verifies a "RAM serving image" (`PLAYGROUND-GOLDEN-PATH.md:44`) while CONTEXT.md:53 defines "serving image" as `[PROPOSED]`; same name, playground artifact vs production contract. Profile handoff rides "the ordinary range assignment generation" (`0025:152`) which is RFC-0006 vapor (F13) | A profile flip during a range move has no defined ordering between serving generation and range generation; both are called "generation" |
| 5 | Point-read/HTAP cliffs | The only verified HTAP receipt is 21 base rows (F6); tail-scaling gate is `tail <= 1% of base` (`PRODUCT-SPEC.md:400`) with no run above toy scale; 0010's atomic dual effects require multi-effect commit expansion 0002's mutation set (`Set/Clear/ClearRange`, `0002:69-70`) cannot express | A row update must atomically emit its table-change effect (`0010:31-33`); the commit envelope has no such effect kind; the verified overlay contract can never have exercised it |
| 6 | Multi-tenancy/cells | Docs commit to durability profile fixed per tenant domain per generation (`0025:50-52`) and tenant deletion/migration protocols (`0011:159-171`); code has a hardcoded 16-byte constant tenant (F: `commit.rs:13`) | Not a contradiction yet, but every tenant claim is `[PROPOSED]` riding on zero code; honest per taxonomy |
| 7 | Operations | Count of independently correct control loops implied by RFCs: generation coordinator, txLog ratekeeper, objectifier, GC marker, sweeper with effect grants, lease expirer, materializer, admission/eviction ladder, outcome-retention compactor. No doc counts them; `docs/BIDEC-EVAL-PROGRAM.md:225` defers the "small operational contract" | A 30-minute brownout engages at least five of these simultaneously (`0005:200-201`, `0025:221-224`); no test composes even two |
| 8 | API/taxonomy | Three colliding "G" ladders: product G0-G9, bootstrap Gate 1-6B, playground GP-G0-G7; "G3" names resident profiles, disposable serving, and a raft rung simultaneously. 18-gate program vs 10-gate sheet with no mapping (`docs/EVALS.md:66`). `okv-slate` is `[CODE-COMPLETE]` in CONTEXT.md:110 and `[VERIFIED]` in SYSTEM-SHAPE.md:55. Golden-path program `code_complete` with 15/15 gates `proposed` reads as progress | A reader auditing "Gate 3 passed" cannot determine which of three claims was made |
| 9 | Tree ordering | The brief's own question answered by the ledger: all 19 receipts are correctness admissions or a 64 MiB local curve; the program's own text admits "The decisive missing evidence is the bounded resident hot path and public row-object format, not another consensus proof" (`BIDEC-EVAL-PROGRAM.md:300`), yet the four most recent feature commits before the SlateDB run are all publisher-recovery micro-gates | Months of provable component work can continue indefinitely without the thesis ever being exposed to falsification |

---

## 7. Evidence audit: G0-G6 (product) and GP-G0 through GP-G6 (playground)

**Product gates** (using PRODUCT-SPEC-SHEET G-numbers; program gates in `objectkv-product-thesis-v1.toml`):

- **G0 (semantic kernel), verified gates G0.1/G0.2.** Proves: MVCC visibility, replay identity, envelope codec exactness on 16 keys / 40 events with poison controls. Does not compose with: concurrency (single-threaded model), conflict detection (absent), or any physical layer. Taxonomy honest? Yes at the letter ("narrow model"), but "verified" on 0 keys (G0.2) stretches the spirit; it is a codec proof.
- **G1 (durable fast tail), partially verified.** Proves: OpenRaft storage conformance, torn-tail recovery, real process kill with controller-driven election, dedup across retry, on one host. Does not compose with: autonomous elections (disabled, F2), independent media, snapshots (disabled), sustained load. Honest? Mostly; `docs/EVALS.md:557-560` lists the exclusions itself. The label "single-group prototype" is accurate.
- **G2 (object authority), partially verified.** Proves: the four publisher ambiguity boundaries recover exactly with empty scratch, generation fencing at CAS, on local-fs objects. This is the strongest work in the repo and genuinely hard. Does not compose with: real cloud semantics (GCS never run), sweeper/worker fencing (0016 suite `proposed`, never run), multipart, abandoned intents (self-listed, `docs/EVALS.md:357-359`). Honest? Yes.
- **G3 (resident profiles), evaluating.** Proves: nothing yet; the 1.0173 ratio is a dirty-source diagnostic the repo itself refuses to count. Honest? Exemplary, the doc rejected two of its own favorable results (compressible values, order bias, `resident-hot-path-g3.1.md:50-63`).
- **G4, G5, G6 (elastic recovery, branches, multi-range): proposed, zero evidence.** These three are the product thesis. G6 is deliberately last ("should not start until the resident and object leverage gates pass", `BIDEC-EVAL-PROGRAM.md:143`), which I dispute in section 8.
- **G8 (HTAP), gate G8.1 verified.** Proves: plan-shape invariants (invalidation below projection, bounded buffering) on 21 rows. Does not compose with: a storage engine (fixtures are hardcoded), tail growth, leases, or the atomic dual-effect requirement (matrix row 5). "Verified narrow contract" is honest; any reading of it as HTAP capability is not.

**Playground GP-G0..G6: all `[VERIFIED]` within stated bounds, and the bounds are load-bearing.** GP-G3's "three OpenRaft processes" is one host with scripted elections; GP-G4's RAM image is single-process with 125 ns reads explicitly "not network database latency" (`PLAYGROUND-GOLDEN-PATH.md:70-71`); GP-G6's branch/GC proof is the in-memory object adapter. The doc states "No rung inherits proof from an earlier rung" (`:49-50`), which is honest and also an admission: seven verified rungs and zero composed rungs. Two honesty defects: the receipt script emits bare `"VERIFIED"` JSON seven times (F16), and none of these receipts are in the ledger, so the repo's most-cited proof artifact is outside its own evidence system.

**Net taxonomy audit:** the status system is unusually honest at the leaf and systematically flattering at the summary. `[VERIFIED]` has been allowed to mean "a deterministic 30-event script with poisons cannot distinguish this from correct", which is a real and valuable claim, but six "verified" checkmarks on a program dashboard visually outweigh eleven "proposed" rows that contain the entire product.

---

## 8. Technology-tree critique

**Too early / oversubscribed:** publisher ambiguity micro-gates. RFC-0017 through 0020 are four RFCs, four suites, and four ledger receipts for kill boundaries of one process, before any evidence that the surrounding system (conflicts, real quorum, cloud store) is viable. Each is correct and each was cheap individually; collectively they represent the tree's whole recent throughput spent three layers below the thesis. The same instinct is queued next (`object-publication-worker-process.toml`, 11 processes, 9 kill boundaries, proposed).

**Too late:** (1) G6 multi-range strict serializability is sequenced after resident and object leverage (`BIDEC-EVAL-PROGRAM.md:143`). Wrong: it is the highest-mortality unknown (R1) and its first decisive test is a pure-model differential check needing no infrastructure. Semantics can fail on a laptop; there is no reason to defer that failure. (2) Rung 5 (independent machines) is `[PROPOSED]` while rung 6 (cloud) is `[EVALUATING]` on a ladder declared cumulative (`docs/ARCHITECTURE-MAPS.md:167-169`), and both are blocked on non-technical issues (GCP reauth) that gate the two most decisive physical unknowns (R2, R3).

**Redundant:** turmoil raft-cluster and tokio raft-process suites now prove nearly identical scripted scenarios; the turmoil lane's remaining value is entropy exploration, which it is not doing (fixed scripts, `fs_sync_probability=0`). The SlateDB incumbent lane is correctly stopped (D30); do not spend the permitted "one bounded configuration pass" unless it becomes the control for a run objectKV itself will make.

**Non-decisive:** golden-path program v1's fifteen checkpoints validate harness plumbing (`code_complete`, all gates proposed); it measures nothing until the integrated slice exists, so it should not be presented as a program peer of product-thesis-v1.

**Missing gates:** an external serializability oracle (nothing in the tree checks histories against an independent checker; all oracles are self-written models); a cost gate with a reviewed price snapshot (currently `"pending-reviewed-snapshot"`, `product-thesis.toml:178`); an authority-state-size gate (R6); a control-loop composition gate (matrix row 7).

**Reordered tree, optimized for thesis-learning per week:**

1. **Kill-test semantics in-process** (R1): conflict model plus 1,000-history differential vs external oracle. No infra.
2. **Unblock cloud auth and run the existing conformance plus phase0 suites on GCS and S3** (R3): days of work, converts the largest evidence vacuum into data.
3. **Integrated three-machine slice** (R2, R5, R6): real disks, elections enabled, real object backend, one workload; this is GP-G7 pulled forward with machines instead of more local rungs.
4. **Resident SSD vs RAM matrix with concurrency and writes** (R4): the program already calls this the critical result.
5. Only then: worker/sweeper fencing depth, HTAP tail scaling, PG `smgr` probe.
6. Explicitly parked: MultiRaft, metacluster, Redis/search, shape-C SQL (already parked; keep it so).

---

## 9. Ranked experiment plan

Common to all: seeds frozen {1103, 2207, 3301, 4409, 5519}; `correctness.anomalies == 0` hard gate; ledger entries must carry machine, rustc, lockfile hash (fixing F7); record object-store request counts, bytes, and USD from a reviewed price snapshot.

**E1. Multi-range coordination oracle (semantic kill-test).** Invariant: strict serializability of committed multi-range histories. Workload/faults: 1,000 concurrent histories x 3 seeds through model plus envelope codec plus resolver prototype; injected lost replies, duplicate retries, stale generations; conflict rates {0, 0.01, 0.1}. Candidate: okv resolver model. Control: single-lock serial executor (ground truth) plus external checker (Elle or equivalent). Primary metric: anomalies (binary). Correctness gates: zero anomalies; false-conflict rate reported. Cost: engineer-weeks only. Stop: any anomaly, or conflict representation requiring cell-wide coarse tokens for >1% of product-shaped transactions. Resolves: whether RFC-0008 can graduate from draft, i.e. whether the kernel's core claim is designable. (R1)

**E2. Integrated three-host plus object-backend slice (GP-G7 pulled forward).** Invariant: `Database(C) = ObjectState(O) + txLog(O, C]` continuously, with elections enabled. Topology: 3 real machines, independent disks, MinIO plus one real cloud bucket; publisher plus GC plus one serving worker. Workload: sustained mixed read/write at fixed rate for >= 2 h; faults: leader SIGKILL, node replacement from empty disk, 30 s object 503 burst. Candidate: objectKV slice. Control: TiKV 3-node at identical durability, same machines. Primary metric: commit p99 ratio vs control. Correctness gates: zero acknowledged loss, zero stale-generation acceptance, empty-node rebuild without full-dataset download. Cost: 3 machines plus object bill, recorded. Stop: p99 > 1.25x control after one optimization cycle (CURVE-COMMIT), or rebuild bytes ~ dataset bytes. Resolves: R2, R6, and whether "one continuously integrated cell" exists at all.

**E3. Cold indexed point reads at scale.** Invariant: <= 1 data-range GET per cold point read after manifest/index warmup, independent of database size. Workload: 10 GiB / 10 M keys (the never-run `phase0.toml` shape), incompressible values; cold-start worker; 10 k uniform plus Zipfian point reads. Candidate: okv manifest plus sparse index over GCS and S3. Control: the stopped SlateDB curve as floor; RocksDB-on-EBS as latency reference. Primary metric: cold GET count and p99 vs dataset size {1, 10, 50 GiB}. Gates: GET count flat in dataset size; reopen bytes metadata-bounded (the D30 lesson as a gate on ourselves). Cost: object requests and USD per 10 k cold reads. Stop: GET count or reopen bytes growing with dataset (repo's own invalidation condition). Resolves: R3, the manifest open question in both spec sheets.

**E4. Admitted SSD vs RAM matrix.** Invariant: zero object requests after admission (existing hard gate, `product-thesis.toml:204-227`). Workload: 64 MiB then 10 GiB; clients {1, 4, 16, 64}; mixes {100R, 95/5, 50/50}; incompressible values; ABBA ordering; clean committed source. Candidate: `ram_resident` and `ssd_resident` behind ServingImage. Controls: direct NVMe RocksDB (for SSD) and admitted SSD (for RAM). Primary metrics: p99 and throughput ratios. Gates: SSD within 1.20x of RocksDB; RAM >= 1.20x better than SSD on at least one named end-to-end metric, else `ram_resident` demoted per D33. Cost: RAM-hour vs SSD-hour economics recorded. Stop: SSD outside 20% after one cycle triggers the BIDEC stop rule (pivot serving base to RocksDB/TiKV). Resolves: R4, G3.

**E5. Outage-debt brownout.** Invariant: lag state machine order normal -> rate_limited -> commit_refused, hard byte bound never breached, zero acknowledged loss. Workload: E2 slice with `fault-recovery.toml`'s 1800 s 503 brownout plus 10x PUT latency; write-heavy. Candidate: okv ratekeeper. Control: none (binary contract). Primary metric: max retained txLog bytes vs configured bound; time-to-drain after repair. Gates: refusal before bound; reads for reconstructable versions continue during `commit_refused` (`0005:184-185`). Cost: retained-bytes-hours. Stop: unbounded debt or post-repair drain requiring operator action. Resolves: R5.

**E6. Branch plus GC under failure.** Invariant: GC never deletes a reachable object; branch create is metadata-scale at product scale. Workload: 10 GiB base; 100 branches created/deleted during publisher kills, sweeper kills, and one generation takeover; verify every surviving branch byte-exact. Candidate: replicated publication authority (RFC-0015) plus sweeper (RFC-0016), first real run of the worker suite. Control: leak-only mode (GC disabled) for the reachability oracle. Primary metric: branch create p99 and bytes; leaked bytes after full mark. Gates: zero premature deletes (binary); leaked bytes bounded and reclaimed on next complete mark. Cost: orphan storage USD. Stop: any premature delete, or root enumeration requiring full-history walk (violates `rfcs/0009:120-121`). Resolves: R6 plus matrix row 3, and forces the 0014/0016 retirement contradiction to be resolved in code.

**E7. PostgreSQL authority mapping probe.** Invariant: one commit authority; PG acks map onto okv `COMMITTED` without a second durable WAL of record. Workload: the `smgr` tracing wrapper on stock PostgreSQL under pgbench; measure page-write shapes, fsync ordering demands, and where a page-native bridge would double-write. Candidate: paper mapping plus tracing data. Control: stock PG on local disk. Primary metric: double-write bytes ratio; count of PG assumptions with no okv contract. Gates: an explicit answer to "minimal PG hook vs two commit authorities" (`BIDEC-EVAL-PROGRAM.md:163`). Cost: one engineer-week. Stop: if the mapping requires permanent double commit authority, shape A demotes to `[FUTURE]` and G7 leaves the critical path (this is also the layered-pivot tripwire, F18). Resolves: R8.

**E8. Exact HTAP tail scaling.** Invariant: exact snapshot equality at `T` for all `W <= T <= min(C, A)`; bounded memory. Workload: base 10 M rows Parquet; tail grown 0.1% -> 1% -> 10% of base; concurrent materialization advancing `W`; canonical row-multiset comparison vs a batch-recompute control. Primary metric: overlay overhead vs base-only at tail = 1% (gate <= 1.20x); materialization triggers before tail = 10%. Gates: `query.result_exact == 1.0`; `peak_buffered_bytes` bounded; `snapshot_unavailable` (never silent rebase) under lease expiry. Cost: tail storage plus rematerialization compute. Stop: overhead superlinear in tail or memory unbounded. Resolves: G8's composition with a real storage engine, and exercises the dual-effect gap (matrix row 5).

Ranking rationale: E1 costs almost nothing and addresses the highest-mortality unknown; E2/E3 convert the two largest evidence vacuums; E4 is the repo's own declared critical result; E5/E6 compose failure handling; E7/E8 are decisive but only after a slice exists.

---

## 10. Concrete technology-tree changes (do not implement; named targets)

1. **Relabel:** `evals/programs/objectkv-golden-path-v1.toml` `status = "code_complete"` -> a harness-only status (`harness_ready` or keep `proposed`); reserve code_complete for gates. Fix `experiments/run-okv-playground-golden-path.sh:131-146` to emit `VERIFIED (bounded: <scope>)` strings and to append its receipts to `experiments/ledger.jsonl`.
2. **Fix the ledger writer** in `crates/okv-eval` to populate `profile.{machine, rustc, lockfile_hash}` per `evals/schema/result.schema.json:34`; backfill is impossible, so annotate the 19 existing entries as pre-schema in a README note.
3. **Rename the gate ladders:** prefix bootstrap gates as `BP-G1..BP-G6B` in `docs/BOOTSTRAP-PLAN.md`, matching the GP- convention; add the 10-gate to 18-gate mapping table to `docs/EVALS.md` or delete one enumeration.
4. **Merge/resolve RFC conflicts:** amend RFC-0014 to defer reservation retirement to RFC-0016 (supersession note); amend RFC-0025 to either drop `object_ack` or add the missing "fenced durable decision" owner, reconciling with RFC-0005's rejected-alternatives list and RFC-0001 principle 5.
5. **Promote RFC-0008 to the front of the RFC queue** with E1 as its admission evidence; until then, add a banner to RFC-0007/0010/0011 stating their serializable-transaction dependency is unprovided.
6. **Write RFC-0006 for real** (range identity, assignment generation, split/merge publication) before RFC-0025's profile handoff work proceeds; 0025 currently references machinery that does not exist.
7. **Add three missing gates** to `evals/programs/objectkv-product-thesis-v1.toml`: external-oracle serializability (E1), authority-state-size bound (R6), and a cost gate that cannot pass while `price_snapshot = "pending-reviewed-snapshot"` (`evals/suites/product-thesis.toml:178` vs `:598` is currently self-contradictory).
8. **Demote/park:** the proposed `object-publication-worker-process.toml` 11-process suite until after E2; the turmoil raft-cluster lane unless it gains entropy exploration; the SlateDB configuration pass unless repurposed as an E3 control.
9. **Split naming:** "serving image" (production contract, RFC-0025) vs the playground GP-G4 artifact; give the playground one a distinct name in `docs/PLAYGROUND-GOLDEN-PATH.md` and `CONTEXT.md:53`.
10. **Pull GP-G7 forward** as the E2 slice and re-scope `docs/PLAYGROUND-GOLDEN-PATH.md:47` to name three machines explicitly, replacing further one-host rungs.
11. **Docs hygiene:** reconcile `okv-slate` status between `CONTEXT.md:110` and `docs/SYSTEM-SHAPE.md:55`; align the durability-profile lists between `docs/PRODUCT-SPEC-SHEET.md:331-336` and `docs/ARCHITECTURE-MAPS.md:64-67`; add CURVE-PG to `docs/PRODUCT-SPEC.md`'s curve table or remove it from the sheet.

---

## 11. Final call

**BUILD NARROWLY, confidence 0.6.** The thesis is unfalsified, the differentiators are real and not cheaply copyable, the team's evidence discipline is the best predictor available that the hard parts will be measured honestly, and the internal stop rules plus the named layered-pivot destination (F17, F18) bound the downside. But continuation is conditional on reordering: the next quarter must expose the thesis to death, not accumulate more leaf proofs.

**Next three gates (in order, all with existing repo hooks):**

1. **Semantic gate (E1):** 1,000-history multi-range differential vs an external serializability oracle, zero anomalies, conflict representation adequate for HTAP certificates. Fails -> PIVOT SUBSTRATE (FDB/TiKV authority, keep the publication/branch/HTAP layer).
2. **Physical gate (E2 + E3):** three real machines vs TiKV control within 1.25x commit p99, plus cold point reads flat in dataset size at 10 GiB on a real cloud store. Fails after one optimization cycle -> PIVOT SUBSTRATE (the repo's own BIDEC stop rule, `docs/BIDEC-EVAL-PROGRAM.md:303`).
3. **Leverage gate (E4 + E6):** admitted SSD within 20% of direct RocksDB under concurrency and writes, and metadata-scale branches surviving GC under failure at 10 GiB. SSD fails -> the product is TiKV-plus-uploader, STOP owning the kernel per G9's own failure clause (`docs/PRODUCT-SPEC-SHEET.md:413`); branches fail -> the differentiator list shrinks to a point where PIVOT dominates.

**Evidence that would reverse this call to PIVOT SUBSTRATE today:** a credible showing that the conflict/resolver design cannot avoid a global ordering bottleneck (the open question both spec sheets lead with), or a cloud-economics result showing cold-read or objectification request costs scale with database size. **Evidence that would upgrade confidence toward a durable BUILD:** E2 passing with elections enabled on real machines, which would be the first receipt in this repository that composes two verified layers.

Review complete. No files were modified; the deliverable is the review above. If you want, I can next draft the doc/RFC/eval edits from section 10 as a change plan for execution outside plan mode.
