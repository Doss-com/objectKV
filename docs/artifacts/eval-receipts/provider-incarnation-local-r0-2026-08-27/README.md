# Provider-incarnation local-process R0 receipts

Status: `[VERIFIED]` for the bounded local-process compound-fence mechanism.
GP2.5.4 remains `[EVALUATING]` until the same contract includes a resurrected
FoundationDB provider on GCP. This is a correctness result, not an HA or
performance result.

## Frozen authority

- Candidate revision: `b415d502665eff9b6df4c095e33480b628348db2`
- Parent revision: `a1b3efed16c9e15cc5aaea9234c7f84e6cbfde7d`
- Synthetic eval ref: `refs/okv-evals/provider-incarnation-r0-2026-08-27`
- Formal suite hash: `e3c579056005b3af3d7078f6fa1e40643a9b61493c14e06ea8e76dd8067c4d26`
- Seed: `2026082704`
- Profile: `local-process`
- Backend: `external-cell-incarnation-authority`

## Executed result

```text
three-process generation authority + two data generations
  -> source data fence
  -> authority failover
  -> destination activation
  -> stale commit and stale route checks

three-process publication authority
  -> destination root publication
  -> authority failover
  -> stale generation publication check

combined positive                              [KEEP]
accept stale commit + route + publication      [DISCARD]
```

| Subject | Formal run ID | Verdict | Result |
| --- | --- | --- | --- |
| Compound fence | `15260d66-01a9-452c-988b-73091c38ce91` | `keep` | zero anomalies, all 12 emitted hard gates passed |
| Stale-source poison | `b05dd054-7544-4dca-8b96-080df73c6113` | `discard` | exactly three anomalies across commit, route, and publication |

The positive run executed six data-process starts, twelve authority or
publication process starts, seven process kills, three authority failovers,
four stale-commit rejections, and 23 publication writes. The same semantic
report reproduced exactly in the evaluator's two fresh executions. The poison
left destination operations working while bypassing all three stale-source
fences, which is the required false-positive control.

Both formal run IDs occur in `otel/logs.jsonl`, `otel/metrics.jsonl`, and
`otel/traces.jsonl`. The formal receipts are in `formal/`. These timings are
single-machine correctness diagnostics and are not performance claims.

## Scope and shipping path

The receipt verifies the objectKV authority composition with real OS processes
and durable process roots. It does not prove that a FoundationDB provider
retains its local generation fence after VM restart, or that the external
authority is integrated with a real provider handoff.

```text
GP2.5.1 semantic authority                         [VERIFIED]
  -> GP2.5.2 logical object lifecycle             [VERIFIED]
  -> GP2.5.3 physical provider-media loss         [VERIFIED]
  -> GP2.5.4a local compound-fence processes      [VERIFIED]
  -> GP2.5.4b resurrected FoundationDB provider   [EVALUATING]
  -> GP3.1 retained-write overhead vs direct FDB  [PROPOSED]
```

The next admitted action is one dual-provider GCP correctness run. Performance
work does not start until the resurrected source fails commit, route, and
publication checks while the destination remains writable.
