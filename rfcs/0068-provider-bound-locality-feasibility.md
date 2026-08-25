# RFC 0068: Provider-bound locality feasibility

- Status: proposed, eval frozen before implementation
- Authors: objectKV contributors
- Created: 2026-08-25
- Supersedes: none

## Decision

Run a closed-form locality-feasibility gate before implementing a cache,
placement, or prefetch candidate. For a declared access distribution, local
capacity, and provider-request target, calculate the greatest probability mass
that any capacity-respecting local placement can cover without provider work.
Discard an impossible workload-target pair before spending a remote run or
hardening a serving mechanism.

Do not weaken the provider-request target when the bound fails. Change one of
the actual product variables: traffic concentration, local-data fraction,
serving assignment, intermediate tier, or workload scope.

## Context and invariant

RFC 0067 measured the following provider miss ratios with persistent NVMe
bounded to 25 percent of logical bytes:

```text
Zipfian theta 0.99        26.820 percent
moving 10 percent hotset  14.535 percent
request-cost target        2.500 percent
```

That result initially suggested optimizing admission or prefetch. The frozen
distributions reveal a stronger constraint. Even an ideal placement cannot
cover 97.5 percent of either workload with 25 percent of equal-sized keys.

For `N=4096`, `c=0.25`, and Zipfian `theta=0.99`, the best static placement is
the 1,024 highest-probability keys:

```text
ideal hit mass       = 0.838299212912
irreducible miss     = 0.161700787088
```

For a hotset receiving `q=0.90` of reads, a uniform background, hotset fraction
`h=0.10`, and `c=0.25`, the complete hotset fits. The ideal hit mass is:

```text
q + (1 - q) * c      = 0.925
irreducible miss     = 0.075
```

The invariant is:

> No serving candidate may claim a provider miss target below the locality
> bound produced by its declared workload and local capacity.

Prefetch provider requests count as provider work. A candidate cannot turn a
miss into a hit by moving the same request earlier and omitting it from the
receipt.

## Proposed contract

The model accepts:

- key count `N` and equal logical value bytes;
- local capacity fraction `c`;
- distribution and its complete parameters;
- provider misses allowed per logical read `m_target`;
- price snapshot and request-cost target used to derive `m_target`.

It emits:

- ideal local hit ratio;
- irreducible provider miss ratio;
- target gap `m_floor - m_target`;
- maximum request-cost-compatible coverage result;
- normalized probability mass and exact model identity;
- a deterministic receipt digest.

The first model supports the three RFC 0067 distributions.

### Uniform

For independent uniform reads:

```text
H_max = c
M_min = 1 - c
```

### Zipfian

For rank probabilities `p_i = i^-theta / sum(j^-theta)` and
`K = floor(c * N)`:

```text
H_max = sum(p_i, i=1..K)
M_min = 1 - H_max
```

The evaluator calculates the distribution independently in two forms and
requires their results to agree within `1e-12`.

### Moving hotset with uniform background

For hot-read fraction `q`, hotset fraction `h`, and uniform selection within
both the hotset and background component:

```text
H_max = q * min(1, c / h) + (1 - q) * c
M_min = 1 - H_max
```

This is a per-window ideal that knows the active hotset before the first read.
It gives placement more information than a production online controller. A
real policy can equal it but cannot use this model to claim a better provider
request ratio.

## Serving-model consequence

`[PROPOSED]` A Range Engine should treat the active immutable image for an
assigned key range as placed local state, not as an accidental collection of
recent LRU entries. Object storage remains authoritative for durability,
history, and empty-cache rebuild. Local NVMe is disposable but intentionally
populated for the ranges the worker is expected to serve.

This changes the economic question from:

```text
Can 25 percent of arbitrary cell bytes cache 97.5 percent of reads?
```

to:

```text
What fraction of active assigned ranges must be locally complete, and what
fraction of cell traffic can those assignments cover?
```

A fully active dataset may still require nearly one local serving copy. The
advantage over a replicated-local incumbent then comes from fewer authoritative
local replicas, disposable workers, colder unplaced ranges, object-native
history, and faster reconstruction. Those benefits must be measured rather
than assumed.

## Failure model

- Probability parameters do not normalize to one.
- Capacity is rounded or overcommitted in the candidate's favor.
- A moving-hotset model ignores its uniform background.
- A prefetch request is omitted from provider work.
- A model uses future trace knowledge unavailable to the declared policy.
- Physical part granularity holds fewer logical values than the model assumes.
- Compression or metadata overhead changes physical coverage.
- A finite synthetic trace is presented as the distribution expectation.

The feasibility gate is necessary but not sufficient. A passing bound only
says that a mechanism could meet the target. The normal provider-bound suite
must still measure its physical request, byte, latency, memory, and correctness
curves.

## Alternatives

### Implement range prefetch immediately

Optimizes for code progress. Gives up knowing whether the frozen workload can
ever meet its target. Rejected for the current 25-percent experiment.

### Increase cache capacity until the existing suite passes

Optimizes for a green result. Gives up the fixed economic question and violates
the autonomous-research contract. A larger local fraction is a separate
product candidate with its own cost curve.

### Replace the synthetic workload with a more concentrated one

Optimizes for a feasible benchmark. Gives up comparison to the discarded
baseline. Add PostgreSQL, Redis, and search traces as named workload contracts;
do not rewrite RFC 0067.

## Eval plan

Freeze suite `provider-bound-locality-feasibility-v0` with the RFC 0067 dataset,
capacity, distributions, and 2.5-percent target. The primary metric is
`provider_bound.irreducible_miss_ratio`. A workload-target pair keeps only when
the bound is at or below the target and every model gate passes.

The independent checks are:

- total probability mass equals one within `1e-12`;
- closed-form and enumerated hit mass agree within `1e-12`;
- modeled resident keys and bytes do not exceed capacity;
- ideal hit plus irreducible miss equals one;
- request-cost target reproduces the 2.5-percent miss ceiling;
- deterministic replay produces the same receipt digest.

Three unsafe controls inflate capacity, skip probability normalization, or
ignore moving-hotset background reads. Each must discard.

### Candidate surface

The implementation experiment may change only:

- `crates/okv-eval/src/locality_feasibility.rs`;
- the minimum module export and operation-dispatch plumbing in
  `crates/okv-eval/src/lib.rs` and `crates/okv-eval/src/main.rs`;
- focused tests for that model.

The RFC, suite, metric registry, result schema, equations, distribution
parameters, capacity, target, controls, and budget are frozen during the
implementation experiment. A discovered contract defect starts a new contract
commit before another candidate; it is not repaired inside a measured result.

## Compatibility and migration

This adds no storage format or public API. It is an eval preflight and a design
constraint. Existing RFC 0067 results remain valid. Future physical candidates
reference both suites: feasibility defines whether the target is possible;
provider-bound cache economics measures whether the mechanism reaches it.

## Unresolved questions

1. How should physical part packing replace equal-sized logical keys in the
   next bound without coupling the oracle to SlateDB internals?
2. Which public PostgreSQL buffer-access, Redis, and search traces provide a
   reviewable traffic-concentration contract?
3. How much complete assigned-range coverage is economical relative to one,
   two, and three local RocksDB replicas?
4. Does a regional shared cache create a useful middle tier, or reproduce the
   latency and coordination cost objectKV is trying to remove?
