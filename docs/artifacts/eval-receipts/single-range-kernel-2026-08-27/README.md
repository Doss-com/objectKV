# Single-range kernel diagnostic receipt, 2026-08-27

Status: `[CODE-COMPLETE]` mechanism and `[EVALUATING]` dirty single-host receipt.

```text
run:                   74b29fe1-5b46-4cd1-923a-ee548a2f780c
suite:                 single-range-kernel-v1
suite receipt hash:    9a214673398bb44c7be3fe8770c713458867dd6384a00affbd6aca12f468ad54
suite file sha256:     9698d8196026b90ead2ef66df8282ebd9df0db4f7d1c0b1b56d45127d9ae5867
result sha256:         50a3d0e4306bdce834f526cb7e6d342d962a68b77bc026b0bceacae5ac27fc11
source:                a56442ad800deedd72a404a0886e88831eb308a0+dirty
machine:               Apple M4 Max, 48 GiB, arm64
rustc:                 1.88.0
first correct read:    123.002125 ms
operation duration:    1.844010041 s
hard gates:            12 pass, 0 fail
```

The run used three real publication-authority and three real transaction-
authority processes, killed the first disposable range process, and opened a
distinct replacement from the published object base plus retained txLog. A
one-record page bound split a transaction batch whose items shared one commit
version. The replacement resumed with batch order, applied all seven tail
records across two catch-up rounds, and returned exact `Set`, `Clear`, insert,
and `ClearRange` outcomes.

The read path issued one manifest GET, one index GET, one data range GET, zero
complete data GETs, and zero LIST requests. The result is not comparable
because the source was dirty, OTel was disabled, and all processes shared one
machine.

The stored JSON normalizes the runner's temporary output path to this durable
receipt directory. All metric, gate, identity, and timing fields are unchanged.
