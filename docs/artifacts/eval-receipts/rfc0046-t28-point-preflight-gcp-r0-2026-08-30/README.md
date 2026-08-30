# RFC-0046 T28 GCS point preflight

Status: `[EVALUATING]` mechanism evidence, not an admitted performance curve.

## Result

The generation-pinned lazy reader and raw-range control both returned the
exact value from the existing 1 GiB fixture. Each measured data window issued
one GCS range attempt, returned 65,048 bytes, and produced zero oracle
anomalies.

```text
1 GiB generation-pinned fixture
  -> descriptor + manifest exact-open
  -> sealed point range from authenticated index
  -> candidate: rederive the same range
  -> control: consume the sealed range directly
  -> one GCS data range attempt
  -> exact value at T = O
```

Five points spanning the keyspace produced serialized block lengths from
55,607 to 65,048 bytes. The maximum is below the frozen 65,536-byte data-range
ceiling.

| Subject | Elapsed | Response bytes | Provider attempts | Anomalies |
|---|---:|---:|---:|---:|
| objectKV candidate | 49.220 ms | 65,048 | 1 | 0 |
| raw GCS range | 36.172 ms | 65,048 | 1 | 0 |

The single pair is diagnostic only. Its candidate/control ratio is 1.361x. It
does not satisfy or fail the RFC's admission rule, which requires 15 paired
blocks, 1,024 measured reads per position, three seeds, fresh processes,
read-only IAM attestation, OTel completion, and all poisons.

## Identity

- Project: `doss-objectkv-dev`
- Runner: `objectkv-bench-t27a2-r0-runner`, `us-central1-a`
- Fixture: 1,048,576 keys, 1,024-byte values, 8 MiB target objects, 64 KiB target blocks
- Fixture envelope: `768e1a9b8ee91a16615dd69b89d15ba581667a9d5ab6e5190b5de663efcc024d`
- Descriptor generation: `1788020925446068`
- Plan semantic digest: `de0f47c6a70a2864abfce351ead127e951c264b34e6553cad48dfc39d6b8274b`
- Implementation commits: `1d67897`, `a2d7fff`, `e6dcbe8`, `cb1e44c`, `8c0b39a`

## Immutable evidence

| Artifact | GCS generation | SHA-256 |
|---|---:|---|
| `gs://doss-objectkv-dev-okv-evals/runs/rfc0046-t28-preflight-r0-20260830/t28-r0-plan-cb1e44c.json` | `1788070213676342` | `3a17a93e31078253c7228673c6e190831defff321ad1289983b5c2d1cc997815` |
| `gs://doss-objectkv-dev-okv-evals/runs/rfc0046-t28-preflight-r0-20260830/t28-r0-candidate-8c0b39a.json` | `1788070211334500` | `9d6754d21e4b13a79e628e087144259eec417b549623e65870fe60078fddf11d` |
| `gs://doss-objectkv-dev-okv-evals/runs/rfc0046-t28-preflight-r0-20260830/t28-r0-raw-8c0b39a.json` | `1788070216061401` | `10a842ec6e3f73ecb53e3bf7381b7d7989355b1279a6b8449c371bdafd3c4240` |

The runner principal could read the fixture but could not create the evidence
objects. Evidence was copied through the authenticated operator and uploaded
with create-only generation preconditions. This is consistent with a
read-only runner, but it is not yet the RFC-required IAM and denied-write
attestation.

## Next gate

Complete the negative-control receipt and schema, bind the read-only principal
and denied-write probe, then run the bounded fresh-process paired preflight.
Only that admitted result may add the first object-tier latency point to T38.
