#!/usr/bin/env python3
"""Phase-separated FoundationDB media-loss lifecycle for GP2.5.3.

The source phase creates one exact objectKV closure and executes the hidden-
source-media poison. The restore phase runs against a separately created empty
FoundationDB cluster and reads only named immutable GCS objects. Assembly joins
those provider observations with controller-captured GCP media identities.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import time
from datetime import datetime, timezone
from typing import Any

import fdb

from foundationdb_lifecycle_r0 import (
    FDB_API_VERSION,
    PROVIDER_REVISION,
    LifecycleProbe,
    b64,
    bytes_value,
    canonical_json,
    prefix_end,
    sha256,
    versionstamped_parameter,
)


MEDIA_LOSS_KIND = "foundationdb_objectkv_media_loss_r0"
HIDDEN_SOURCE_CONTROL = "restore_with_hidden_source_media"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def read_json(path: str) -> dict[str, Any]:
    with open(path, "rb") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} does not contain a JSON object")
    return value


def write_json(path: str, value: dict[str, Any]) -> None:
    encoded = json.dumps(value, sort_keys=True, indent=2) + "\n"
    with open(path, "w", encoding="utf-8") as output:
        output.write(encoded)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def cluster_identity(cluster_file: str | None) -> dict[str, str]:
    path = pathlib.Path(cluster_file or "/etc/foundationdb/fdb.cluster")
    payload = path.read_bytes()
    text = payload.decode("ascii").strip()
    if ":" not in text or "@" not in text:
        raise ValueError("FoundationDB cluster file has an invalid shape")
    cluster_id = text.split(":", 1)[1].split("@", 1)[0]
    if len(cluster_id) != 32 or any(character not in "0123456789abcdef" for character in cluster_id):
        raise ValueError("FoundationDB cluster ID is not 32 lowercase hex characters")
    return {
        "cluster_id": cluster_id,
        "cluster_file_sha256": hashlib.sha256(payload).hexdigest(),
    }


def closure_authority(
    closure: dict[str, Any], manifest: dict[str, Any]
) -> dict[str, Any]:
    manifest_object = manifest["object"]
    closure_object = manifest["closure"]
    return {
        "manifest_uri": manifest_object["uri"],
        "manifest_generation": manifest_object["generation"],
        "manifest_sha256": manifest_object["sha256"],
        "manifest_bytes": manifest_object["bytes"],
        "closure_uri": closure_object["uri"],
        "closure_generation": closure_object["generation"],
        "closure_sha256": closure_object["sha256"],
        "closure_bytes": closure_object["bytes"],
        "state_digest": closure["state_digest"],
        "through_provider_stamp": closure["through_provider_stamp"],
        "record_count": len(closure["state"]),
    }


def manifest_reference(authority: dict[str, Any]) -> dict[str, Any]:
    return {
        "object": {
            "uri": authority["manifest_uri"],
            "generation": authority["manifest_generation"],
            "sha256": authority["manifest_sha256"],
            "bytes": authority["manifest_bytes"],
        }
    }


def fresh_generation_commit(probe: LifecycleProbe) -> str:
    request_id = "fresh-destination-commit"
    value = hashlib.sha256(f"{probe.run_id}:{request_id}".encode("ascii")).digest()
    key = b"post-restore/00000000"
    payload = canonical_json(
        {
            "request_id": request_id,
            "operations": [{"op": "put", "key": b64(key), "value": b64(value)}],
        }
    )
    outcome_key = probe.outcomes_prefix(2) + request_id.encode("ascii")
    transaction = probe.database.create_transaction()
    active = transaction[probe.active_generation_key].wait()
    if bytes_value(active) != b"2":
        raise AssertionError("destination generation is not active for fresh commit")
    transaction[probe.data_prefix(2) + key] = value
    transaction.set_versionstamped_key(
        versionstamped_parameter(
            probe.changes_prefix(2), b"/" + request_id.encode("ascii")
        ),
        payload,
    )
    transaction.set_versionstamped_value(
        outcome_key,
        versionstamped_parameter(b"", hashlib.sha256(payload).digest()),
    )
    stamp = transaction.get_versionstamp()
    transaction.commit().wait()
    committed_stamp = bytes_value(stamp.wait()).hex()
    verify = probe.database.create_transaction()
    if bytes_value(verify[probe.data_prefix(2) + key].wait()) != value:
        raise AssertionError("fresh destination user value is absent")
    if not verify[outcome_key].wait().present():
        raise AssertionError("fresh destination outcome is absent")
    return committed_stamp


def activate_fresh_destination(probe: LifecycleProbe, closure: dict[str, Any]) -> None:
    ready_key = probe.generation_root(2) + b"restore-ready"
    activate = probe.database.create_transaction()
    if activate[probe.active_generation_key].wait().present():
        raise AssertionError("fresh destination already has an active generation")
    ready = activate[ready_key].wait()
    if not ready.present():
        raise AssertionError("destination ready marker is absent")
    ready_payload = json.loads(bytes_value(ready))
    if ready_payload["state_digest"] != closure["state_digest"]:
        raise AssertionError("destination ready marker has another digest")
    activate[probe.active_generation_key] = b"2"
    activate.commit().wait()


def source_phase(arguments: argparse.Namespace) -> int:
    started_ns = time.perf_counter_ns()
    started_at = utc_now()
    database = fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    probe = LifecycleProbe(
        database=database,
        run_id=arguments.run_id,
        bucket_name=arguments.bucket,
        object_prefix=arguments.object_prefix,
        record_count=arguments.record_count,
        restore_chunk_records=arguments.restore_chunk_records,
        negative_control="",
    )
    probe.timed("reset_namespace", probe.reset_namespace)
    probe.timed("seed_source_generation", probe.seed_source_generation)
    closure, manifest = probe.timed("objectify", probe.objectify)
    downloaded, _ = probe.timed(
        "named_get", lambda: probe.verify_named_gets(manifest)
    )
    if downloaded["state_digest"] != closure["state_digest"]:
        raise AssertionError("named source closure differs from captured closure")
    probe.timed("advance_frontier", lambda: probe.advance_frontier(manifest))
    authority = closure_authority(closure, manifest)
    identity = cluster_identity(arguments.cluster_file)
    source_receipt = {
        "schema_version": 1,
        "kind": "foundationdb_objectkv_media_source_r0",
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "cluster": identity,
        "object_closure": authority,
        "timings": [vars(timing) for timing in probe.timings],
        "duration_ns": time.perf_counter_ns() - started_ns,
    }
    write_json(arguments.output, source_receipt)

    poison_started_ns = time.perf_counter_ns()
    poison_started_at = utc_now()
    probe.timed(
        "poison_restore", lambda: probe.restore_empty_generation(downloaded)
    )
    restored_digest, restored_count = probe.state_digest(2)
    probe.timed(
        "poison_activate", lambda: probe.activate_and_fence(downloaded)
    )
    fresh_stamp = probe.timed("poison_fresh_commit", lambda: fresh_generation_commit(probe))
    poison_receipt = {
        "schema_version": 1,
        "kind": "foundationdb_objectkv_media_restore_phase_r0",
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "started_at": poison_started_at,
        "finished_at": utc_now(),
        "cluster": identity,
        "restore": {
            "started_at": poison_started_at,
            "finished_at": utc_now(),
            "destination_empty_before_restore": True,
            "named_object_hashes_match": True,
            "restored_chunks": probe.restored_chunks,
            "replayed_chunks": probe.replayed_chunks,
            "restored_record_count": restored_count,
            "restored_state_digest": restored_digest,
            "activated_after_ready": True,
            "fresh_commit_succeeded": bool(fresh_stamp),
            "source_provider_inputs": ["source-cluster", "source-provider-media"],
        },
        "duration_ns": time.perf_counter_ns() - poison_started_ns,
    }
    write_json(arguments.poison_output, poison_receipt)
    return 0


def restore_phase(arguments: argparse.Namespace) -> int:
    started_ns = time.perf_counter_ns()
    started_at = utc_now()
    source = read_json(arguments.source_receipt)
    if source.get("kind") != "foundationdb_objectkv_media_source_r0":
        raise ValueError("source receipt has another kind")
    if source.get("provider") != PROVIDER_REVISION:
        raise ValueError("source receipt has another provider")
    if source.get("run_id") != arguments.run_id:
        raise ValueError("source receipt has another run ID")
    authority = source["object_closure"]
    database = fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    probe = LifecycleProbe(
        database=database,
        run_id=arguments.run_id,
        bucket_name=arguments.bucket,
        object_prefix=arguments.object_prefix,
        record_count=authority["record_count"],
        restore_chunk_records=arguments.restore_chunk_records,
        negative_control="",
    )
    destination_identity = cluster_identity(arguments.cluster_file)
    if destination_identity == source["cluster"]:
        raise AssertionError("destination FoundationDB cluster equals source cluster")
    empty = database.create_transaction()
    destination_empty = not list(empty[probe.root : prefix_end(probe.root)])
    if not destination_empty:
        raise AssertionError("destination namespace is not empty")
    closure, _ = probe.timed(
        "named_get", lambda: probe.verify_named_gets(manifest_reference(authority))
    )
    if closure["state_digest"] != authority["state_digest"]:
        raise AssertionError("named object closure has another state digest")
    probe.timed("restore", lambda: probe.restore_empty_generation(closure))
    restored_digest, restored_count = probe.state_digest(2)
    probe.timed(
        "activate", lambda: activate_fresh_destination(probe, closure)
    )
    fresh_stamp = probe.timed("fresh_commit", lambda: fresh_generation_commit(probe))
    restore_receipt = {
        "schema_version": 1,
        "kind": "foundationdb_objectkv_media_restore_phase_r0",
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "cluster": destination_identity,
        "restore": {
            "started_at": started_at,
            "finished_at": utc_now(),
            "destination_empty_before_restore": destination_empty,
            "named_object_hashes_match": True,
            "restored_chunks": probe.restored_chunks,
            "replayed_chunks": probe.replayed_chunks,
            "restored_record_count": restored_count,
            "restored_state_digest": restored_digest,
            "activated_after_ready": True,
            "fresh_commit_succeeded": bool(fresh_stamp),
            "source_provider_inputs": [],
        },
        "timings": [vars(timing) for timing in probe.timings],
        "duration_ns": time.perf_counter_ns() - started_ns,
    }
    write_json(arguments.output, restore_receipt)
    return 0


def identity_from_topology(
    topology: dict[str, Any], phase_receipt: dict[str, Any]
) -> dict[str, str]:
    if topology.get("kind") != "objectkv_provider_media_identity_r0":
        raise ValueError("provider topology has another kind")
    identity = dict(topology["identity"])
    if identity["cluster_id"] != phase_receipt["cluster"]["cluster_id"]:
        raise ValueError("topology and provider phase cluster IDs differ")
    if (
        identity["cluster_file_sha256"]
        != phase_receipt["cluster"]["cluster_file_sha256"]
    ):
        raise ValueError("topology and provider phase cluster-file hashes differ")
    return identity


def gate(gate_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"id": gate_id, "passed": passed, "detail": detail}


def assemble_positive(arguments: argparse.Namespace) -> int:
    source_phase_receipt = read_json(arguments.source_receipt)
    restore_phase_receipt = read_json(arguments.restore_receipt)
    source_topology = read_json(arguments.source_identity)
    destination_topology = read_json(arguments.destination_identity)
    loss = read_json(arguments.loss_observation)
    if source_phase_receipt["run_id"] != restore_phase_receipt["run_id"]:
        raise ValueError("source and restore run IDs differ")
    source = identity_from_topology(source_topology, source_phase_receipt)
    destination = identity_from_topology(
        destination_topology, restore_phase_receipt
    )
    restore = restore_phase_receipt["restore"]
    authority = source_phase_receipt["object_closure"]
    loss_precedes_restore = parse_time(loss["observed_at"]) <= parse_time(
        restore["started_at"]
    )
    identities_distinct = all(
        source[field] != destination[field]
        for field in [
            "cluster_id",
            "cluster_file_sha256",
            "instance_id",
            "boot_disk_id",
            "data_disk_id",
        ]
    )
    source_absent = all(
        bool(loss[field])
        for field in [
            "source_instance_absent",
            "source_boot_disk_absent",
            "source_data_disk_absent",
        ]
    )
    exact = (
        restore["restored_state_digest"] == authority["state_digest"]
        and restore["restored_record_count"] == authority["record_count"]
    )
    idempotent = (
        restore["restored_chunks"] > 0
        and restore["replayed_chunks"] == restore["restored_chunks"]
    )
    gates = [
        gate("provider_identities_distinct", identities_distinct, "source and destination provider identities differ"),
        gate("source_provider_media_removed_before_restore", source_absent and loss_precedes_restore, f"loss_observed_at={loss['observed_at']} restore_started_at={restore['started_at']}"),
        gate("destination_empty_before_restore", bool(restore["destination_empty_before_restore"]), "destination namespace was empty"),
        gate("named_object_hashes_match", bool(restore["named_object_hashes_match"]), "manifest and closure named GETs matched"),
        gate("exact_state_digest", exact, f"expected={authority['state_digest']} actual={restore['restored_state_digest']}"),
        gate("restore_chunks_idempotent", idempotent, f"restored={restore['restored_chunks']} replayed={restore['replayed_chunks']}"),
        gate("activated_after_ready", bool(restore["activated_after_ready"]), "destination activated after ready marker"),
        gate("fresh_destination_commit", bool(restore["fresh_commit_succeeded"]), "fresh destination transaction committed"),
        gate("restore_has_no_source_provider_inputs", not restore["source_provider_inputs"], f"inputs={restore['source_provider_inputs']}"),
    ]
    failures = [item for item in gates if not item["passed"]]
    receipt = {
        "schema_version": 1,
        "kind": MEDIA_LOSS_KIND,
        "provider": PROVIDER_REVISION,
        "run_id": source_phase_receipt["run_id"],
        "source": source,
        "destination": destination,
        "media_loss": {
            "observed_at": loss["observed_at"],
            "source_instance_absent": bool(loss["source_instance_absent"]),
            "source_boot_disk_absent": bool(loss["source_boot_disk_absent"]),
            "source_data_disk_absent": bool(loss["source_data_disk_absent"]),
            "removed_before_restore": source_absent and loss_precedes_restore,
        },
        "object_closure": authority,
        "restore": restore,
        "correctness_anomalies": len(failures),
        "media_loss_verified": not failures,
        "negative_control": None,
        "gates": gates,
        "timings": restore_phase_receipt.get("timings", []),
        "scope": "R0 exact logical reconstruction after source GCP instance, boot disk, and provider data disk deletion; not HA or incarnation fencing",
    }
    write_json(arguments.output, receipt)
    return 0 if not failures else 1


def assemble_poison(arguments: argparse.Namespace) -> int:
    source_phase_receipt = read_json(arguments.source_receipt)
    poison_phase_receipt = read_json(arguments.poison_receipt)
    source_topology = read_json(arguments.source_identity)
    source = identity_from_topology(source_topology, source_phase_receipt)
    destination = identity_from_topology(source_topology, poison_phase_receipt)
    restore = poison_phase_receipt["restore"]
    authority = source_phase_receipt["object_closure"]
    exact = (
        restore["restored_state_digest"] == authority["state_digest"]
        and restore["restored_record_count"] == authority["record_count"]
    )
    idempotent = (
        restore["restored_chunks"] > 0
        and restore["replayed_chunks"] == restore["restored_chunks"]
    )
    gates = [
        gate("provider_identities_distinct", False, "poison restored inside the source provider cluster"),
        gate("source_provider_media_removed_before_restore", False, "poison kept the source instance and disks reachable"),
        gate("destination_empty_before_restore", bool(restore["destination_empty_before_restore"]), "empty logical generation in source cluster"),
        gate("named_object_hashes_match", bool(restore["named_object_hashes_match"]), "manifest and closure named GETs matched"),
        gate("exact_state_digest", exact, f"expected={authority['state_digest']} actual={restore['restored_state_digest']}"),
        gate("restore_chunks_idempotent", idempotent, f"restored={restore['restored_chunks']} replayed={restore['replayed_chunks']}"),
        gate("activated_after_ready", bool(restore["activated_after_ready"]), "logical destination activated after ready marker"),
        gate("fresh_destination_commit", bool(restore["fresh_commit_succeeded"]), "fresh logical-generation transaction committed"),
        gate("restore_has_no_source_provider_inputs", False, f"inputs={restore['source_provider_inputs']}"),
    ]
    failures = [item for item in gates if not item["passed"]]
    receipt = {
        "schema_version": 1,
        "kind": MEDIA_LOSS_KIND,
        "provider": PROVIDER_REVISION,
        "run_id": source_phase_receipt["run_id"],
        "source": source,
        "destination": destination,
        "media_loss": {
            "observed_at": poison_phase_receipt["started_at"],
            "source_instance_absent": False,
            "source_boot_disk_absent": False,
            "source_data_disk_absent": False,
            "removed_before_restore": False,
        },
        "object_closure": authority,
        "restore": restore,
        "correctness_anomalies": len(failures),
        "media_loss_verified": False,
        "negative_control": HIDDEN_SOURCE_CONTROL,
        "gates": gates,
        "timings": [],
        "scope": "executed same-cluster hidden-source-media poison; exact logical restore is not provider-media-loss evidence",
    }
    write_json(arguments.output, receipt)
    return 1


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    source = commands.add_parser("source")
    source.add_argument("--cluster-file")
    source.add_argument("--run-id", required=True)
    source.add_argument("--bucket", required=True)
    source.add_argument("--object-prefix", default="results/provider-r0/media-loss")
    source.add_argument("--record-count", type=int, default=1_000)
    source.add_argument("--restore-chunk-records", type=int, default=200)
    source.add_argument("--output", required=True)
    source.add_argument("--poison-output", required=True)
    source.set_defaults(handler=source_phase)

    restore = commands.add_parser("restore")
    restore.add_argument("--cluster-file")
    restore.add_argument("--run-id", required=True)
    restore.add_argument("--bucket", required=True)
    restore.add_argument("--object-prefix", default="results/provider-r0/media-loss")
    restore.add_argument("--restore-chunk-records", type=int, default=200)
    restore.add_argument("--source-receipt", required=True)
    restore.add_argument("--output", required=True)
    restore.set_defaults(handler=restore_phase)

    positive = commands.add_parser("assemble-positive")
    positive.add_argument("--source-receipt", required=True)
    positive.add_argument("--restore-receipt", required=True)
    positive.add_argument("--source-identity", required=True)
    positive.add_argument("--destination-identity", required=True)
    positive.add_argument("--loss-observation", required=True)
    positive.add_argument("--output", required=True)
    positive.set_defaults(handler=assemble_positive)

    poison = commands.add_parser("assemble-poison")
    poison.add_argument("--source-receipt", required=True)
    poison.add_argument("--poison-receipt", required=True)
    poison.add_argument("--source-identity", required=True)
    poison.add_argument("--output", required=True)
    poison.set_defaults(handler=assemble_poison)
    return root


def main() -> int:
    arguments = parser().parse_args()
    if getattr(arguments, "record_count", 20) < 20:
        raise ValueError("record count must be at least 20")
    if getattr(arguments, "restore_chunk_records", 1) < 1:
        raise ValueError("restore chunk records must be positive")
    return int(arguments.handler(arguments))


if __name__ == "__main__":
    raise SystemExit(main())
