# RFC-0045 L2 open-loop matched-media diagnostic

- Date: 2026-08-30
- Result: `[EVALUATING]`
- Correctness within the named runs: `[VERIFIED]`
- Source: `5ddbfcee20b1a274326a89f89bdd1c7226273184`
- Source archive SHA-256: `1d8d5f573ea046a0a7d27f21b9405befb362da3b645a02183524e79f55826b1c`
- Linux binary SHA-256: `e0d1407e1acad808f596c23ffb6c914c577ead4f438081a730c10a61d657d238`
- Immutable archive: `gs://doss-objectkv-dev-okv-evals/eval-receipts/staged-txlog-l2-open-loop-5ddbfce-r1/staged-txlog-l2-open-loop-5ddbfce-r1.tar.gz`
- Object generation: `1788129965074569`
- Archive SHA-256: `bdd2df846460fdfdce0466f6fd5aeedf1fbe158a22e5b09f0641fcebe0ee257a`

## Question

Where does the current staged txLog saturate under bounded open-loop arrivals,
how much of tail latency is queueing versus quorum service, and how does local
NVMe compare with a dedicated persistent-SSD control on the same machines?

## Topology and workload

```text
64 Poisson producer threads, 256 logical streams
                    |
                    v
bounded 32,768-record active-writer queue
                    |
        close at 256 records or 250 us
                    |
      persistent TCP to three log nodes
                    |
          acknowledge after 2 durable
                    |
        exact digest from all 3 nodes

candidate: 3 x 375 GB local NVMe, ext4
control:   3 x 100 GB dedicated pd-ssd, ext4
```

The runner and three `n2-standard-8` log nodes were separate GCE instances in
`us-central1-a`. Every point attempted 131,072 128-byte records. The offered
load sweep was 20k, 40k, 60k, 80k, and 100k records/s. The candidate and control
used the same source, binary, network, nodes, endpoints, record generator,
queue, batch policy, acknowledgement rule, and final digest oracle. Only the
mounted journal root changed.

The producer reached approximately 96.3 percent of each nominal offered rate
because it used 64 operating-system threads rather than a synthetic timestamp
replay. Candidate and control realized the same offered rates to measurement
precision.

## Candidate curve

| Offered | Ack records/s | Batch mean | Record p50 | Record p95 | Record p99 | Record p99.9 | Refused |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 20k/s | 18,843 | 21.8 | 1.688 ms | 3.797 ms | 4.157 ms | 4.584 ms | 0 |
| 40k/s | 36,890 | 80.0 | 3.346 ms | 4.911 ms | 5.434 ms | 5.926 ms | 0 |
| 60k/s | 45,451 | 255.0 | 289.587 ms | 528.153 ms | 539.902 ms | 542.378 ms | 0 |
| 80k/s | 45,033 | 254.7 | 496.522 ms | 695.145 ms | 697.998 ms | 699.096 ms | 20,796 |
| 100k/s | 45,638 | 254.7 | 548.550 ms | 698.632 ms | 704.950 ms | 706.808 ms | 36,337 |

The operating knee is between 40k and 60k offered records/s. At 40k, the queue
never exceeded 153 records and no arrival was refused. At 60k, mean batches
were full, the queue reached 26,602 records, and 534.928 ms of the 539.902 ms
p99 was queue dwell. Saturated throughput remained approximately 45k to 46k
records/s.

At the 20k point, quorum service itself was 0.810 ms p50 and 2.660 ms p99. At
full batches, quorum service was approximately 4.5 ms p50 and 4.8 ms p99. The
current profile therefore does not satisfy the frozen 1 ms record-p99 or 100k
records/s gates.

## Matched media comparison

| Offered | Local-NVMe p99 | `pd-ssd` p99 | p99 ratio | Local-NVMe ack/s | `pd-ssd` ack/s | Throughput ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 20k/s | 4.157 ms | 4.905 ms | 0.847x | 18,843 | 18,843 | 1.000x |
| 40k/s | 5.434 ms | 13.465 ms | 0.404x | 36,890 | 36,897 | 1.000x |
| 60k/s | 539.902 ms | 816.312 ms | 0.661x | 45,451 | 38,414 | 1.183x |
| 80k/s | 697.998 ms | 831.265 ms | 0.840x | 45,033 | 38,619 | 1.166x |
| 100k/s | 704.950 ms | 831.768 ms | 0.848x | 45,638 | 38,544 | 1.184x |

Local NVMe materially moves the saturation boundary. At 60k offered, it
accepted every record while the persistent-SSD control refused 10,858. At
100k, local NVMe refused 27.7 percent versus 35.2 percent for `pd-ssd`.
However, the saturated throughput gain is approximately 1.18x, below the
frozen 1.5x gate.

## Batch-size falsifier

Increasing the local-NVMe cap did not unlock the target throughput:

| Offered | Batch cap | Mean batch | Ack records/s | Quorum p50 | Quorum p99 | Record p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 80k/s | 512 | 506.3 | 47,845 | 8.533 ms | 8.997 ms | 657.291 ms |
| 100k/s | 512 | 506.3 | 47,984 | 8.515 ms | 8.979 ms | 669.098 ms |
| 100k/s | 1,024 | 993.8 | 48,292 | 16.750 ms | 17.858 ms | 669.177 ms |

Doubling batch bytes approximately doubled quorum service time while saturated
throughput stayed near 48k records/s. This is consistent with a per-record
encoding, wire, hashing, or frame-write ceiling rather than an fsync-only
ceiling. The run did not collect CPU profiles, so it does not attribute that
cost to one component.

The current `OKVT` journal writes a separate 128-byte envelope for every
128-byte payload. The JSON wire protocol also encodes byte arrays as numbers.
Those two facts are the first optimization targets. A batch frame can share log
identity, writer epoch, sequence metadata, and checksum while a binary wire
format can remove JSON expansion.

## Correctness and claim boundary

Across the ten candidate and control points plus three batch-size diagnostics:

- 1,703,936 arrivals were attempted;
- 1,476,294 accepted records were acknowledged;
- all 39 final node-digest checks matched the exact accepted history;
- anomaly count was zero;
- normal-path object operations were zero.

`[VERIFIED]` The current open-loop evaluator enforces bounded admission,
quorum acknowledgement, exact final digest comparison, media identity, and
separate queue and quorum latency accounting on the named topology.

`[EVALUATING]` Row 4 is not admitted. Each point has one execution rather than
the frozen five repeats, ordering was candidate then control rather than AB/BA,
OTel and CPU attribution are absent, the control uses zonal `pd-ssd` rather than
a regional availability contract, and no node failure or transaction resolver
was present.

## Decision

Keep the staged txLog lane. Local NVMe has a measurable advantage over the
matched persistent-SSD control, and the current bottleneck exposes a concrete
software target rather than an object-storage limit. Next, replace per-record
`OKVT` envelopes with one checksummed batch frame, replace JSON payload arrays
with a binary wire frame, add node-side encode/write/sync timing, and rerun the
20k through 100k curve. Preserve the 1 ms and 100k gates as falsifiers rather
than relabeling this result as a pass.
