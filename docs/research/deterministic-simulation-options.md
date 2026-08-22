# Deterministic simulation options

Status: `[EXISTS]` primary-source observations and `[PROPOSED]` bootstrap call,
2026-08-22.

## Observations

- Turmoil 0.7.2 runs multiple hosts on one thread, provides a seeded RNG,
  virtual time, network latency/loss/partition controls, host crash/restart, and
  an unstable crash-consistent filesystem simulation. Its current crate remains
  on Tokio 1.x. [Turmoil README](https://github.com/tokio-rs/turmoil),
  [crate 0.7.2](https://crates.io/crates/turmoil/0.7.2)
- Turmoil seeds its own scheduler and fault choices through `Builder::rng_seed`.
  Tokio runtime RNG seeding is compiled only with `tokio_unstable`, so a build
  that omits that cfg can look seeded without closing every wake-order source.
  [Turmoil builder](https://github.com/tokio-rs/turmoil/blob/main/crates/turmoil/src/builder.rs),
  [Turmoil runtime](https://github.com/tokio-rs/turmoil/blob/main/crates/turmoil/src/rt.rs)
- OpenRaft's current Turmoil fuzzer treats byte-identical trace replay as a
  separate invariant. It records that runtime RNG, `futures_util::select!`
  shuffle state, RPC deadlines, and build flags each caused replay divergence.
  It fails compilation when the deterministic build cfg is absent.
  [OpenRaft Turmoil harness](https://github.com/databendlabs/openraft/blob/main/tests-turmoil/README.md)
- MadSim 0.2.34 supplies a replacement async runtime and simulator variants for
  Tokio, Tonic, etcd, Kafka, and S3 dependencies. Its documented integration
  requires cfg-based dependency substitution and several patched crates.
  [MadSim README](https://github.com/madsim-rs/madsim),
  [crate 0.2.34](https://crates.io/crates/madsim/0.2.34)
- The pinned SlateDB revision has a private `slatedb-dst` package that runs real
  `Db` instances with a mock clock, deterministic object stores, actors, and
  fault injection. The package is unpublished and requires internal cfg and
  Tokio settings, so it is design evidence rather than a dependency contract.
  [SlateDB DST at the pinned revision](https://github.com/slatedb/slatedb/blob/e0161973d8d7ffdede7c44725729838811674e99/slatedb-dst/README.md)

## Not observed

- No reviewed source proves that using upstream Turmoil alone makes arbitrary
  application code deterministic. Application RNG, UUIDs, wall-clock reads,
  unordered collections, external clients, and dependency-internal scheduling
  remain the application's responsibility.
- No stable public SlateDB API exposes its full DST harness to an external
  storage engine at the pinned revision.
- No current evidence shows that objectKV needs MadSim's whole dependency-graph
  substitution before a real WAL, object-store wrapper, and recovery protocol
  exist.

## Clarity

Question: Which simulation dependency should objectKV adopt before WAL work?

Punchline: Pin Turmoil first behind an objectKV-owned seed, trace, invariant, and
replay interface that refuses non-deterministic builds.

Counter: Switch to MadSim or a pinned Turmoil fork if real objectKV dependencies
retain entropy or cannot run through the narrower Tokio-compatible seam.

Next: make two identical fresh-process runs produce byte-identical canonical
traces, then introduce one known fencing bug and prove one seed detects and
replays it.

This call optimizes for a small Tokio-compatible integration surface. It gives
up MadSim's broader dependency substitution until evidence shows that breadth is
required.
