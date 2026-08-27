#!/usr/bin/env python3
"""Live FoundationDB semantic preflight for RFC-0041.

This is an R0 provider probe, not the objectKV adapter and not an HA receipt.
It emits one JSON object and exits nonzero when any hard gate fails.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
import time
from dataclasses import dataclass
from typing import Any, Callable

import fdb


FDB_API_VERSION = 740
PROVIDER_REVISION = "foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337"
PLACEHOLDER = bytes(10)

fdb.api_version(FDB_API_VERSION)


def prefix_end(prefix: bytes) -> bytes:
    """Return the exclusive lexicographic end for a nonempty prefix."""
    value = bytearray(prefix)
    for index in range(len(value) - 1, -1, -1):
        if value[index] != 0xFF:
            value[index] += 1
            return bytes(value[: index + 1])
    raise ValueError("prefix has no finite lexicographic successor")


def versionstamped_parameter(prefix: bytes, suffix: bytes = b"") -> bytes:
    """Build the C API parameter with one ten-byte versionstamp placeholder."""
    offset = len(prefix)
    return prefix + PLACEHOLDER + suffix + struct.pack("<I", offset)


def bytes_value(value: Any) -> bytes:
    return bytes(value)


def error_code(error: BaseException) -> int | None:
    return getattr(error, "code", None)


@dataclass
class Gate:
    id: str
    passed: bool
    detail: str


class Probe:
    def __init__(self, database: Any, run_id: str) -> None:
        self.database = database
        self.root = b"\x02okv-provider-preflight/" + run_id.encode("ascii") + b"/"
        self.gates: list[Gate] = []

    def clear(self) -> None:
        transaction = self.database.create_transaction()
        transaction.clear_range(self.root, prefix_end(self.root))
        transaction.commit().wait()

    def gate(self, gate_id: str, operation: Callable[[], str]) -> None:
        try:
            detail = operation()
            self.gates.append(Gate(gate_id, True, detail))
        except BaseException as error:  # The receipt must classify provider errors.
            self.gates.append(
                Gate(
                    gate_id,
                    False,
                    f"{type(error).__name__}: {error}; code={error_code(error)}",
                )
            )

    def strict_serializable_write_skew(self) -> str:
        left = self.root + b"skew/left"
        right = self.root + b"skew/right"
        first = self.database.create_transaction()
        second = self.database.create_transaction()

        first_version = first.get_read_version().wait()
        second.set_read_version(first_version)
        first_values = (first[left].wait(), first[right].wait())
        second_values = (second[left].wait(), second[right].wait())
        if any(value.present() for value in first_values + second_values):
            raise AssertionError("write-skew keys were not empty")

        first[left] = b"committed"
        second[right] = b"must-conflict"
        first.commit().wait()
        second_code = None
        try:
            second.commit().wait()
        except fdb.FDBError as error:
            second_code = error.code
        if second_code != 1020:
            raise AssertionError(
                f"second disjoint write did not fail with not_committed: {second_code}"
            )
        return f"one of two attempts committed at shared read version {first_version}"

    def ordered_versionstamp_and_atomic_outcome(self) -> str:
        data_key = self.root + b"data/account-42"
        change_prefix = self.root + b"changes/"
        request_id = b"request-0001"
        outcome_key = self.root + b"outcomes/" + request_id
        payload = b"set account-42 700"

        transaction = self.database.create_transaction()
        if transaction[outcome_key].wait().present():
            raise AssertionError("request outcome already existed")
        transaction[data_key] = b"700"
        transaction.set_versionstamped_key(
            versionstamped_parameter(change_prefix, b"/" + request_id), payload
        )
        outcome_payload = hashlib.sha256(payload).digest() + b"/committed"
        transaction.set_versionstamped_value(
            outcome_key, versionstamped_parameter(b"", outcome_payload)
        )
        versionstamp_future = transaction.get_versionstamp()
        transaction.commit().wait()
        committed_stamp = bytes_value(versionstamp_future.wait())

        read = self.database.create_transaction()
        outcome = bytes_value(read[outcome_key].wait())
        changes = list(read[change_prefix : prefix_end(change_prefix)])
        value = bytes_value(read[data_key].wait())
        if len(changes) != 1:
            raise AssertionError(f"expected one retained change, found {len(changes)}")
        change_stamp = bytes_value(changes[0].key)[len(change_prefix) : len(change_prefix) + 10]
        outcome_stamp = outcome[:10]
        if not (change_stamp == outcome_stamp == committed_stamp):
            raise AssertionError("change, outcome, and commit versionstamps differ")
        if bytes_value(changes[0].value) != payload or value != b"700":
            raise AssertionError("atomic user or retained value differs")
        return f"atomic stamp={committed_stamp.hex()} retained_records=1"

    def exact_unknown_result_retry(self) -> str:
        change_prefix = self.root + b"retry-changes/"
        request_id = b"request-lost-reply"
        outcome_key = self.root + b"retry-outcomes/" + request_id
        data_key = self.root + b"data/retry-counter"
        payload = b"set retry-counter 1"

        first = self.database.create_transaction()
        first[data_key] = b"1"
        first.set_versionstamped_key(
            versionstamped_parameter(change_prefix, b"/" + request_id), payload
        )
        first.set_versionstamped_value(
            outcome_key,
            versionstamped_parameter(b"", hashlib.sha256(payload).digest()),
        )
        first.commit().wait()

        retry = self.database.create_transaction()
        retained_outcome = retry[outcome_key].wait()
        if not retained_outcome.present():
            raise AssertionError("retry could not recover the durable outcome")
        retry.commit().wait()

        verify = self.database.create_transaction()
        changes = list(verify[change_prefix : prefix_end(change_prefix)])
        if len(changes) != 1 or bytes_value(verify[data_key].wait()) != b"1":
            raise AssertionError("lost-reply retry duplicated or lost the logical effect")
        return "discarded first reply; retry recovered one outcome and one retained change"

    def ordered_range_and_range_clear(self) -> str:
        data = self.root + b"range/"
        transaction = self.database.create_transaction()
        for key in (b"a", b"b", b"c", b"d"):
            transaction[data + key] = key.upper()
        transaction.commit().wait()

        clear = self.database.create_transaction()
        clear.clear_range(data + b"b", data + b"d")
        clear.commit().wait()

        read = self.database.create_transaction()
        read_version = read.get_read_version().wait()
        observed = [bytes_value(item.key) for item in read[data : prefix_end(data)]]
        expected = [data + b"a", data + b"d"]
        if observed != expected:
            raise AssertionError(f"ordered range after clear was {observed!r}")
        return f"read_version={read_version} keys=a,d"

    def compare_and_advance_frontier(self) -> str:
        frontier = self.root + b"metadata/object-frontier"
        first = self.database.create_transaction()
        second = self.database.create_transaction()
        if first[frontier].wait().present() or second[frontier].wait().present():
            raise AssertionError("frontier already existed")
        first[frontier] = b"manifest-1"
        second[frontier] = b"manifest-unsafe"
        first.commit().wait()
        second_code = None
        try:
            second.commit().wait()
        except fdb.FDBError as error:
            second_code = error.code
        if second_code != 1020:
            raise AssertionError(f"stale frontier CAS was not rejected: {second_code}")
        read = self.database.create_transaction()
        if bytes_value(read[frontier].wait()) != b"manifest-1":
            raise AssertionError("winning frontier was not retained")
        return "one frontier advanced; stale compare failed with not_committed"

    def execute(self) -> dict[str, Any]:
        started = time.perf_counter_ns()
        self.clear()
        self.gate("strict_serializable_write_skew", self.strict_serializable_write_skew)
        self.gate(
            "ordered_versionstamp_and_atomic_outcome",
            self.ordered_versionstamp_and_atomic_outcome,
        )
        self.gate("exact_unknown_result_retry", self.exact_unknown_result_retry)
        self.gate("ordered_range_and_range_clear", self.ordered_range_and_range_clear)
        self.gate("compare_and_advance_frontier", self.compare_and_advance_frontier)
        duration_ns = time.perf_counter_ns() - started
        failed = [gate for gate in self.gates if not gate.passed]
        return {
            "schema_version": 1,
            "kind": "foundationdb_semantic_preflight",
            "provider": PROVIDER_REVISION,
            "api_version": FDB_API_VERSION,
            "run_id": self.root.split(b"/")[-2].decode("ascii"),
            "duration_ns": duration_ns,
            "correctness_anomalies": len(failed),
            "eligible_for_lifecycle_spike": not failed,
            "gates": [gate.__dict__ for gate in self.gates],
            "scope": "single-process R0 semantics, not HA or production durability",
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-file")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output")
    arguments = parser.parse_args()

    database = fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    result = Probe(database, arguments.run_id).execute()
    encoded = json.dumps(result, sort_keys=True, indent=2) + "\n"
    if arguments.output:
        with open(arguments.output, "w", encoding="utf-8") as output:
            output.write(encoded)
    print(encoded, end="")
    return 0 if result["eligible_for_lifecycle_spike"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
