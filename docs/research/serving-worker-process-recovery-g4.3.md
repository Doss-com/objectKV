# G4.3 serving-worker process recovery

Status: `[EVALUATING]`

## Outcome

The one-machine G4.3 diagnostic composes a replicated authority, an immutable
row-object base, a non-empty quorum-file txLog suffix, an operating-system
process kill, and a distinct empty-scratch replacement worker. The replacement
returned exact base, update, delete, and tail-only insert results without LIST
or complete range hydration.

This is evidence that the object-base plus durable-tail recovery equation is
implementable at the local process boundary. It is not evidence for an
independent-machine data txLog, machine-loss durability, GCS latency, concurrent
catch-up, or production economics.

## Executed topology

```text
three OpenRaft authority processes
  -> active generation 7
  -> logical txLog root wal-g7
  -> authoritative row-manifest identity

immutable row-object closure through O = 1
  -> 1,066,354 encoded data bytes
  -> five bounded row segments

quorum-file txLog through C = 4
  -> record 1: objectified prefix marker
  -> record 2: update existing key
  -> record 3: delete existing key
  -> record 4: insert key absent from the object base

first serving-worker process
  -> recovers authority + base + tail
  -> signals recovered_before_first_read
  -> receives SIGKILL

distinct replacement process with empty scratch
  -> repeats authority/root validation
  -> replays txLog mutations in (O, C]
  -> serves exact reads at C
```

The worker sandwiches the publication-root read between two equal linearizable
generation reads. It resolves the logical txLog root from authority state, not
from worker-local metadata. The first-read timer begins at the replacement
node's execution entry after CLI parsing, so the receipt excludes executable
launch and CLI parsing time.

## Frozen fixed-seed result

Build: release. Backend:
`object-store-local-fs+authority-openraft+quorum-wal-files`. Seeds: 1103, 2207,
3301. Source: `a56442ad800deedd72a404a0886e88831eb308a0+dirty`.

| Path | Verdict | p99 first correct read | Median object response bytes | Correctness anomalies |
| --- | --- | ---: | ---: | ---: |
| Lazy object plus tail candidate | inconclusive | 6.542 ms | 35,811 | 0 |
| Full-hydration same-correctness control | inconclusive | 10.965 ms | 1,072,772 | 0 |
| Skip-tail poison | discard | 6.237 ms | 102,169 | 9 |

The candidate's three latency samples were 6.044, 6.542, and 5.777 ms. The
full-hydration control was 1.68x slower at p99 and transferred 29.96x as many
object response bytes. The candidate transferred 3.36 percent of the encoded
row-data closure before completing the selected reads.

For every candidate seed, the replacement used one manifest GET, one selected
index GET, one selected data range GET, zero complete data GETs, and zero LIST
operations. It reconstructed four quorum txLog records totaling 16,158 physical
bytes and applied all three records newer than the object frontier. Each seed
started three authority processes and two worker processes, killed the first
worker once, and proved that the replacement scratch directory was empty.

The poison recovered the same non-empty suffix but applied zero of the three
required tail records. Its base read remained exact, while the update, deletion,
and tail-only insertion were all wrong for every seed. The independent semantic
replay of each trace remained byte exact, which separates deterministic harness
behavior from logical correctness.

## What the result establishes

- `[CODE-COMPLETE]` One replacement worker can discover the active generation,
  publication root, and logical txLog root from a three-process authority.
- `[CODE-COMPLETE]` One real worker process can be killed after recovery and
  before its first read, then replaced by a distinct empty-scratch process.
- `[CODE-COMPLETE]` Point `Set` and `Clear` mutations newer than an immutable
  object frontier override the object base in commit order.
- `[CODE-COMPLETE]` A tail-only key is consulted before base-manifest bounds.
- `[EVALUATING]` The first correct read can avoid complete range hydration and
  hold object transfer to the named manifest, selected index, and selected block.

## What remains unproved

- The txLog adapter synchronizes three local files on one machine. It does not
  use the OpenRaft data group or independent disks and hosts.
- The suite covers point `Set` and `Clear`. It does not cover `ClearRange`,
  historical suffix reads, range scans, or recovery concurrent with commits.
- The 1 MiB closure is too small for a scale or economics conclusion.
- Filesystem page cache is not GCS, S3, SSD, or RAM-profile evidence.
- OTel export was disabled and the source tree was dirty, so the receipts cannot
  promote G4.3 to `[VERIFIED]`.
- The first-read timer excludes executable launch and CLI parsing.

## Next falsification gate

Replace the local quorum-file tail adapter with the actual OpenRaft transaction
authority's retained data log or a frozen streaming interface. Repeat the same
process-kill, empty-replacement, exact-tail, and bounded-read contract while
commits continue. Only after that local boundary passes should the run expand to
1, 8, and 64 MiB closures, GCS, and three independent machines.

## Immutable receipts

- Candidate: `docs/artifacts/eval-receipts/serving-recovery-g4.3-v2/candidate.json`
  (`1de29968596e89393b1bf5dcdee4a62930b41549e5b5ac512f47a3e37e052784`)
- Full-hydration control:
  `docs/artifacts/eval-receipts/serving-recovery-g4.3-v2/control.json`
  (`f89bc10121dda3dfb0c05b6ccce5b86302fa50b1901e938270e192a38752b735`)
- Skip-tail poison:
  `docs/artifacts/eval-receipts/serving-recovery-g4.3-v2/poison.json`
  (`575da85124c3117a4a8f16cd5f85c6ff06efa4e380b1bbeb27e0bf841ded5c35`)

The v1 receipts remain immutable preliminary evidence. The v2 receipts
supersede them because promoting RFC-0026 and the suite to `[EVALUATING]`
changed the frozen contract hash from
`9fefba95a605b3996cd3f96de082d54f059798cee6d55aa9db44732310027e7b`
to `bc6db938e667a83a3b1ed0dd7d714d4f97680c1bd47757e3d0acef878bb90075`.
