#!/usr/bin/env python3
"""Bounded FoundationDB plus GCS lifecycle probe for RFC-0041.

The probe objectifies one exact FoundationDB snapshot, reconstructs it into an
empty logical generation using only the named GCS objects, and verifies that a
transaction from the previous generation is fenced. It does not destroy the
FoundationDB process or its media, so it is not a media-loss or HA receipt.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import struct
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable

import fdb
from google.cloud import storage


FDB_API_VERSION = 740
PROVIDER_REVISION = "foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337"
PLACEHOLDER = bytes(10)

fdb.api_version(FDB_API_VERSION)


def prefix_end(prefix: bytes) -> bytes:
    value = bytearray(prefix)
    for index in range(len(value) - 1, -1, -1):
        if value[index] != 0xFF:
            value[index] += 1
            return bytes(value[: index + 1])
    raise ValueError("prefix has no finite lexicographic successor")


def versionstamped_parameter(prefix: bytes, suffix: bytes = b"") -> bytes:
    offset = len(prefix)
    return prefix + PLACEHOLDER + suffix + struct.pack("<I", offset)


def canonical_json(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def unb64(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii"), validate=True)


def bytes_value(value: Any) -> bytes:
    return bytes(value)


def error_code(error: BaseException) -> int | None:
    return getattr(error, "code", None)


def chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


@dataclass
class Gate:
    id: str
    passed: bool
    detail: str


@dataclass
class Timing:
    id: str
    duration_ns: int


class LifecycleProbe:
    def __init__(
        self,
        database: Any,
        run_id: str,
        bucket_name: str,
        object_prefix: str,
        record_count: int,
        restore_chunk_records: int,
        negative_control: str,
    ) -> None:
        self.database = database
        self.run_id = run_id
        self.root = b"\x02okv-provider-lifecycle/" + run_id.encode("ascii") + b"/"
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix.strip("/")
        self.record_count = record_count
        self.restore_chunk_records = restore_chunk_records
        self.negative_control = negative_control
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        self.gates: list[Gate] = []
        self.timings: list[Timing] = []
        self.last_source_stamp = ""
        self.frontier_manifest_uri = ""
        self.closure_uri = ""
        self.manifest_uri = ""
        self.closure_bytes = 0
        self.manifest_bytes = 0
        self.restored_chunks = 0
        self.replayed_chunks = 0

    def generation_root(self, generation: int) -> bytes:
        return self.root + f"generations/{generation}/".encode("ascii")

    def data_prefix(self, generation: int) -> bytes:
        return self.generation_root(generation) + b"data/"

    def changes_prefix(self, generation: int) -> bytes:
        return self.generation_root(generation) + b"changes/"

    def outcomes_prefix(self, generation: int) -> bytes:
        return self.generation_root(generation) + b"outcomes/"

    @property
    def active_generation_key(self) -> bytes:
        return self.root + b"metadata/active-generation"

    @property
    def object_frontier_key(self) -> bytes:
        return self.root + b"metadata/object-frontier"

    def timed(self, timing_id: str, operation: Callable[[], Any]) -> Any:
        started = time.perf_counter_ns()
        value = operation()
        self.timings.append(Timing(timing_id, time.perf_counter_ns() - started))
        return value

    def gate(self, gate_id: str, operation: Callable[[], str]) -> None:
        try:
            detail = operation()
            self.gates.append(Gate(gate_id, True, detail))
        except BaseException as error:
            self.gates.append(
                Gate(
                    gate_id,
                    False,
                    f"{type(error).__name__}: {error}; code={error_code(error)}",
                )
            )

    def reset_namespace(self) -> None:
        transaction = self.database.create_transaction()
        transaction.clear_range(self.root, prefix_end(self.root))
        transaction.commit().wait()
        initialize = self.database.create_transaction()
        initialize[self.active_generation_key] = b"1"
        initialize.commit().wait()

    def _commit_batch(
        self,
        request_id: str,
        puts: list[tuple[bytes, bytes]],
        deletes: list[bytes],
    ) -> str:
        transaction = self.database.create_transaction()
        active = transaction[self.active_generation_key].wait()
        outcome_key = self.outcomes_prefix(1) + request_id.encode("ascii")
        outcome = transaction[outcome_key].wait()
        if bytes_value(active) != b"1":
            raise AssertionError("source generation is not active")
        if outcome.present():
            return bytes_value(outcome)[:10].hex()

        change_prefix = self.changes_prefix(1)
        operations: list[dict[str, str]] = []
        for key, value in puts:
            transaction[self.data_prefix(1) + key] = value
            operations.append({"op": "put", "key": b64(key), "value": b64(value)})
        for key in deletes:
            transaction.clear(self.data_prefix(1) + key)
            operations.append({"op": "delete", "key": b64(key)})

        payload = canonical_json({"request_id": request_id, "operations": operations})
        if not (
            self.negative_control == "omit_retained_change"
            and request_id == "update-head"
        ):
            transaction.set_versionstamped_key(
                versionstamped_parameter(
                    change_prefix, b"/" + request_id.encode("ascii")
                ),
                payload,
            )
        transaction.set_versionstamped_value(
            outcome_key,
            versionstamped_parameter(b"", hashlib.sha256(payload).digest()),
        )
        if (
            self.negative_control == "accept_unknown_without_outcome"
            and request_id == "update-head"
        ):
            transaction.clear(outcome_key)
        stamp_future = transaction.get_versionstamp()
        transaction.commit().wait()
        return bytes_value(stamp_future.wait()).hex()

    def seed_source_generation(self) -> str:
        batch_size = min(100, self.restore_chunk_records)
        for batch_index, batch in enumerate(
            chunks(list(range(self.record_count)), batch_size)
        ):
            puts = []
            for row in batch:
                key = f"row/{row:08d}".encode("ascii")
                value = hashlib.sha256(
                    f"{self.run_id}:initial:{row}".encode("ascii")
                ).digest()
                puts.append((key, value))
            self.last_source_stamp = self._commit_batch(
                f"seed-{batch_index:06d}", puts, []
            )

        update_count = max(1, self.record_count // 10)
        updates = []
        for row in range(update_count):
            key = f"row/{row:08d}".encode("ascii")
            value = hashlib.sha256(
                f"{self.run_id}:updated:{row}".encode("ascii")
            ).digest()
            updates.append((key, value))
        self.last_source_stamp = self._commit_batch("update-head", updates, [])

        delete_count = max(1, self.record_count // 20)
        deletes = [
            f"row/{row:08d}".encode("ascii")
            for row in range(self.record_count - delete_count, self.record_count)
        ]
        self.last_source_stamp = self._commit_batch("delete-tail", [], deletes)
        expected = self.record_count - delete_count
        return (
            f"source_generation=1 rows={expected} through={self.last_source_stamp}"
        )

    def capture_closure(self) -> tuple[dict[str, Any], bytes, str]:
        transaction = self.database.create_transaction()
        read_version = transaction.get_read_version().wait()
        active = bytes_value(transaction[self.active_generation_key].wait())
        state_rows = [
            [b64(bytes_value(item.key)[len(self.data_prefix(1)) :]), b64(bytes_value(item.value))]
            for item in transaction[
                self.data_prefix(1) : prefix_end(self.data_prefix(1))
            ]
        ]
        changes = [
            [
                b64(bytes_value(item.key)[len(self.changes_prefix(1)) :]),
                b64(bytes_value(item.value)),
            ]
            for item in transaction[
                self.changes_prefix(1) : prefix_end(self.changes_prefix(1))
            ]
        ]
        outcomes = [
            bytes_value(item.key)[len(self.outcomes_prefix(1)) :].decode("ascii")
            for item in transaction[
                self.outcomes_prefix(1) : prefix_end(self.outcomes_prefix(1))
            ]
        ]
        if active != b"1":
            raise AssertionError("generation changed during closure capture")
        if not changes:
            raise AssertionError("closure has no retained changes")
        change_requests = {
            json.loads(unb64(encoded_change).decode("utf-8"))["request_id"]
            for _, encoded_change in changes
        }
        outcome_requests = set(outcomes)
        if change_requests != outcome_requests:
            missing_changes = sorted(outcome_requests - change_requests)
            missing_outcomes = sorted(change_requests - outcome_requests)
            raise AssertionError(
                "retained change and outcome request sets differ: "
                f"missing_changes={missing_changes} missing_outcomes={missing_outcomes}"
            )
        state_digest = sha256(canonical_json(state_rows))
        closure = {
            "schema_version": 1,
            "kind": "objectkv_provider_logical_closure",
            "run_id": self.run_id,
            "source_generation": 1,
            "provider_read_version": read_version,
            "through_provider_stamp": self.last_source_stamp,
            "state_digest": state_digest,
            "state": state_rows,
            "retained_changes": changes,
        }
        encoded = canonical_json(closure)
        return closure, encoded, sha256(encoded)

    def upload_named_object(self, name: str, payload: bytes) -> tuple[str, str]:
        blob = self.bucket.blob(name)
        blob.upload_from_string(
            payload,
            content_type="application/json",
            if_generation_match=0,
        )
        return f"gs://{self.bucket_name}/{name}", str(blob.generation)

    def objectify(self) -> tuple[dict[str, Any], dict[str, Any]]:
        closure, closure_payload, closure_sha = self.capture_closure()
        closure_name = f"{self.object_prefix}/{self.run_id}/closure-{closure_sha}.json"
        closure_uri, closure_generation = self.upload_named_object(
            closure_name, closure_payload
        )
        manifest = {
            "schema_version": 1,
            "kind": "objectkv_provider_logical_manifest",
            "run_id": self.run_id,
            "source_generation": 1,
            "through_provider_stamp": self.last_source_stamp,
            "state_digest": closure["state_digest"],
            "record_count": len(closure["state"]),
            "retained_change_count": len(closure["retained_changes"]),
            "closure": {
                "uri": closure_uri,
                "generation": closure_generation,
                "sha256": closure_sha,
                "bytes": len(closure_payload),
            },
        }
        manifest_payload = canonical_json(manifest)
        manifest_sha = sha256(manifest_payload)
        manifest_name = f"{self.object_prefix}/{self.run_id}/manifest-{manifest_sha}.json"
        manifest_uri, manifest_generation = self.upload_named_object(
            manifest_name, manifest_payload
        )
        manifest["object"] = {
            "uri": manifest_uri,
            "generation": manifest_generation,
            "sha256": manifest_sha,
            "bytes": len(manifest_payload),
        }
        self.closure_uri = closure_uri
        self.manifest_uri = manifest_uri
        self.closure_bytes = len(closure_payload)
        self.manifest_bytes = len(manifest_payload)
        return closure, manifest

    def named_get(self, uri: str, generation: str, expected_sha: str) -> bytes:
        prefix = f"gs://{self.bucket_name}/"
        if not uri.startswith(prefix):
            raise AssertionError(f"unexpected object URI {uri}")
        name = uri[len(prefix) :]
        payload = self.bucket.blob(name, generation=int(generation)).download_as_bytes()
        actual = sha256(payload)
        if actual != expected_sha:
            raise AssertionError(f"named GET hash {actual} != {expected_sha}")
        return payload

    def verify_named_gets(self, manifest: dict[str, Any]) -> tuple[dict[str, Any], str]:
        manifest_object = manifest["object"]
        manifest_payload = self.named_get(
            manifest_object["uri"],
            manifest_object["generation"],
            manifest_object["sha256"],
        )
        persisted_manifest = json.loads(manifest_payload)
        closure_object = persisted_manifest["closure"]
        closure_payload = self.named_get(
            closure_object["uri"],
            closure_object["generation"],
            closure_object["sha256"],
        )
        closure = json.loads(closure_payload)
        if closure["state_digest"] != persisted_manifest["state_digest"]:
            raise AssertionError("closure and manifest state digests differ")
        return closure, (
            f"manifest_bytes={len(manifest_payload)} closure_bytes={len(closure_payload)}"
        )

    def advance_frontier(self, manifest: dict[str, Any]) -> str:
        encoded = canonical_json(
            {
                "manifest_uri": self.manifest_uri,
                "manifest_sha256": manifest["object"]["sha256"],
                "source_generation": 1,
                "through_provider_stamp": self.last_source_stamp,
            }
        )
        first = self.database.create_transaction()
        stale = self.database.create_transaction()
        if first[self.object_frontier_key].wait().present():
            raise AssertionError("object frontier already exists")
        if stale[self.object_frontier_key].wait().present():
            raise AssertionError("stale object frontier already exists")
        first[self.object_frontier_key] = encoded
        stale[self.object_frontier_key] = b"unsafe-stale-frontier"
        first.commit().wait()
        stale_code = None
        try:
            stale.commit().wait()
        except fdb.FDBError as error:
            stale_code = error.code
        if stale_code != 1020:
            raise AssertionError(f"stale frontier was not rejected: {stale_code}")
        verify = self.database.create_transaction()
        if bytes_value(verify[self.object_frontier_key].wait()) != encoded:
            raise AssertionError("frontier did not retain named manifest")
        self.frontier_manifest_uri = self.manifest_uri
        return "named manifest advanced once; stale compare failed with not_committed"

    def restore_chunk(self, destination_generation: int, rows: list[list[str]]) -> bool:
        payload = canonical_json(rows)
        chunk_id = sha256(payload)
        marker = (
            self.generation_root(destination_generation)
            + b"restore-chunks/"
            + chunk_id.encode("ascii")
        )
        transaction = self.database.create_transaction()
        existing = transaction[marker].wait()
        if existing.present():
            if bytes_value(existing) != chunk_id.encode("ascii"):
                raise AssertionError("restore chunk marker has another digest")
            return False
        for encoded_key, encoded_value in rows:
            transaction[self.data_prefix(destination_generation) + unb64(encoded_key)] = (
                unb64(encoded_value)
            )
        transaction[marker] = chunk_id.encode("ascii")
        transaction.commit().wait()
        return True

    def state_digest(self, generation: int) -> tuple[str, int]:
        transaction = self.database.create_transaction()
        rows = [
            [
                b64(bytes_value(item.key)[len(self.data_prefix(generation)) :]),
                b64(bytes_value(item.value)),
            ]
            for item in transaction[
                self.data_prefix(generation) : prefix_end(self.data_prefix(generation))
            ]
        ]
        return sha256(canonical_json(rows)), len(rows)

    def restore_empty_generation(self, closure: dict[str, Any]) -> str:
        destination = 2
        before_digest, before_rows = self.state_digest(destination)
        if before_rows != 0:
            raise AssertionError("destination generation was not empty")
        expected_empty = sha256(canonical_json([]))
        if before_digest != expected_empty:
            raise AssertionError("empty destination digest was not canonical")

        restore_chunks = list(chunks(closure["state"], self.restore_chunk_records))
        for rows in restore_chunks:
            if not self.restore_chunk(destination, rows):
                raise AssertionError("new restore chunk was unexpectedly present")
            self.restored_chunks += 1
        for rows in restore_chunks:
            if self.restore_chunk(destination, rows):
                raise AssertionError("restore chunk replay was not idempotent")
            self.replayed_chunks += 1

        restored_digest, restored_rows = self.state_digest(destination)
        if restored_digest != closure["state_digest"]:
            raise AssertionError(
                f"restored digest {restored_digest} != {closure['state_digest']}"
            )
        ready_key = self.generation_root(destination) + b"restore-ready"
        ready = self.database.create_transaction()
        ready[ready_key] = canonical_json(
            {
                "state_digest": restored_digest,
                "through_provider_stamp": closure["through_provider_stamp"],
            }
        )
        ready.commit().wait()
        return (
            f"generation=2 rows={restored_rows} chunks={self.restored_chunks} "
            f"replays={self.replayed_chunks} digest={restored_digest}"
        )

    def activate_and_fence(self, closure: dict[str, Any]) -> str:
        stale_key = self.data_prefix(1) + b"stale-writer"
        stale = self.database.create_transaction()
        if self.negative_control != "restore_without_generation":
            active = stale[self.active_generation_key].wait()
            if bytes_value(active) != b"1":
                raise AssertionError("source generation not active before flip")
        stale[stale_key] = b"must-not-commit"

        ready_key = self.generation_root(2) + b"restore-ready"
        activate = self.database.create_transaction()
        current = activate[self.active_generation_key].wait()
        ready = activate[ready_key].wait()
        if bytes_value(current) != b"1" or not ready.present():
            raise AssertionError("destination generation was not ready")
        ready_payload = json.loads(bytes_value(ready))
        if ready_payload["state_digest"] != closure["state_digest"]:
            raise AssertionError("ready marker has another state digest")
        activate[self.active_generation_key] = b"2"
        activate.commit().wait()

        stale_code = None
        try:
            stale.commit().wait()
        except fdb.FDBError as error:
            stale_code = error.code
        if stale_code != 1020:
            raise AssertionError(f"stale generation commit was not rejected: {stale_code}")

        verify = self.database.create_transaction()
        if bytes_value(verify[self.active_generation_key].wait()) != b"2":
            raise AssertionError("destination generation is not active")
        if verify[stale_key].wait().present():
            raise AssertionError("stale source-generation write is visible")
        digest, rows = self.state_digest(2)
        if digest != closure["state_digest"]:
            raise AssertionError("active destination state differs after flip")
        return f"active_generation=2 rows={rows}; stale commit failed with not_committed"

    def execute(self) -> dict[str, Any]:
        started = time.perf_counter_ns()
        self.timed("reset_namespace", self.reset_namespace)
        self.gate(
            "source_generation_and_retained_changes",
            lambda: self.timed("seed_source_generation", self.seed_source_generation),
        )
        if not self.gates[-1].passed:
            return self.result(started)

        objectified: dict[str, Any] = {}

        def objectify_gate() -> str:
            closure, manifest = self.timed("objectify", self.objectify)
            objectified["closure"] = closure
            objectified["manifest"] = manifest
            return (
                f"closure={self.closure_uri} manifest={self.manifest_uri} "
                f"through={self.last_source_stamp}"
            )

        self.gate("immutable_named_object_closure", objectify_gate)
        if not self.gates[-1].passed:
            return self.result(started)

        downloaded: dict[str, Any] = {}

        def named_get_gate() -> str:
            closure, detail = self.timed(
                "named_get",
                lambda: self.verify_named_gets(objectified["manifest"]),
            )
            downloaded["closure"] = closure
            return detail

        self.gate("named_get_hash_verification", named_get_gate)
        self.gate(
            "compare_and_advance_object_frontier",
            lambda: self.timed(
                "advance_frontier",
                lambda: self.advance_frontier(objectified["manifest"]),
            ),
        )
        if any(not gate.passed for gate in self.gates[-2:]):
            return self.result(started)

        self.gate(
            "empty_generation_exact_idempotent_restore",
            lambda: self.timed(
                "restore_empty_generation",
                lambda: self.restore_empty_generation(downloaded["closure"]),
            ),
        )
        if not self.gates[-1].passed:
            return self.result(started)
        self.gate(
            "atomic_generation_activation_and_stale_fence",
            lambda: self.timed(
                "activate_and_fence",
                lambda: self.activate_and_fence(downloaded["closure"]),
            ),
        )
        return self.result(started)

    def result(self, started: int) -> dict[str, Any]:
        failed = [gate for gate in self.gates if not gate.passed]
        return {
            "schema_version": 1,
            "kind": "foundationdb_objectkv_lifecycle_r0",
            "provider": PROVIDER_REVISION,
            "api_version": FDB_API_VERSION,
            "run_id": self.run_id,
            "duration_ns": time.perf_counter_ns() - started,
            "correctness_anomalies": len(failed),
            "empty_logical_generation_lifecycle_passed": not failed,
            "media_loss_verified": False,
            "ha_verified": False,
            "record_count_requested": self.record_count,
            "restore_chunk_records": self.restore_chunk_records,
            "negative_control": self.negative_control or None,
            "restored_chunks": self.restored_chunks,
            "replayed_chunks": self.replayed_chunks,
            "closure_bytes": self.closure_bytes,
            "manifest_bytes": self.manifest_bytes,
            "closure_uri": self.closure_uri,
            "manifest_uri": self.manifest_uri,
            "frontier_manifest_uri": self.frontier_manifest_uri,
            "through_provider_stamp": self.last_source_stamp,
            "gates": [asdict(gate) for gate in self.gates],
            "timings": [asdict(timing) for timing in self.timings],
            "scope": (
                "single-process R0 logical lifecycle on one existing FoundationDB "
                "cluster; source provider media remained present"
            ),
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-file")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--object-prefix", default="results/provider-r0/lifecycle")
    parser.add_argument("--record-count", type=int, default=1_000)
    parser.add_argument("--restore-chunk-records", type=int, default=200)
    parser.add_argument(
        "--negative-control",
        choices=[
            "omit_retained_change",
            "accept_unknown_without_outcome",
            "restore_without_generation",
        ],
        default="",
    )
    parser.add_argument("--output")
    arguments = parser.parse_args()
    if arguments.record_count < 20:
        parser.error("--record-count must be at least 20")
    if arguments.restore_chunk_records < 1:
        parser.error("--restore-chunk-records must be positive")

    database = fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    probe = LifecycleProbe(
        database=database,
        run_id=arguments.run_id,
        bucket_name=arguments.bucket,
        object_prefix=arguments.object_prefix,
        record_count=arguments.record_count,
        restore_chunk_records=arguments.restore_chunk_records,
        negative_control=arguments.negative_control,
    )
    result = probe.execute()
    encoded = json.dumps(result, sort_keys=True, indent=2) + "\n"
    if arguments.output:
        with open(arguments.output, "w", encoding="utf-8") as output:
            output.write(encoded)
    print(encoded, end="")
    return 0 if result["empty_logical_generation_lifecycle_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
