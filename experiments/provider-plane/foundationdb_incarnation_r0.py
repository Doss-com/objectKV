#!/usr/bin/env python3
"""Phase-separated FoundationDB provider-incarnation probe for GP2.5.4.

The source and destination are distinct FoundationDB clusters. The controller
retains the source media, installs a provider-local fence, activates the ready
destination only after a real-process authority report, restarts the source VM,
and proves the source adapter still rejects generation-one commits.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from typing import Any

import fdb

from foundationdb_lifecycle_r0 import (
    FDB_API_VERSION,
    PROVIDER_REVISION,
    LifecycleProbe,
    bytes_value,
    canonical_json,
    prefix_end,
    versionstamped_parameter,
)
from foundationdb_media_loss_r0 import (
    activate_fresh_destination,
    closure_authority,
    cluster_identity,
    fresh_generation_commit,
    manifest_reference,
    read_json,
    utc_now,
    write_json,
)


KIND = "foundationdb_objectkv_provider_incarnation_r0"
SOURCE_KIND = "foundationdb_objectkv_incarnation_source_r0"
RESTORE_KIND = "foundationdb_objectkv_incarnation_restore_r0"
FENCE_KIND = "foundationdb_objectkv_incarnation_fence_r0"
ACTIVATION_KIND = "foundationdb_objectkv_incarnation_activation_r0"
RESURRECTION_KIND = "foundationdb_objectkv_incarnation_resurrection_r0"
NEGATIVE_CONTROL = "accept_stale_source_incarnation"
FENCE_VALUE = b"fenced:2"


def document_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def same_cluster(expected: dict[str, str], actual: dict[str, str]) -> bool:
    return (
        expected["cluster_id"] == actual["cluster_id"]
        and expected["cluster_file_sha256"]
        == actual["cluster_file_sha256"]
    )


def probe_for(arguments: argparse.Namespace, database: Any) -> LifecycleProbe:
    return LifecycleProbe(
        database=database,
        run_id=arguments.run_id,
        bucket_name=getattr(arguments, "bucket", "unused"),
        object_prefix=getattr(
            arguments, "object_prefix", "results/provider-r0/incarnation"
        ),
        record_count=getattr(arguments, "record_count", 1000),
        restore_chunk_records=getattr(arguments, "restore_chunk_records", 200),
        negative_control="",
    )


def source_phase(arguments: argparse.Namespace) -> int:
    started_at = utc_now()
    started_ns = time.perf_counter_ns()
    database = (
        fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    )
    probe = probe_for(arguments, database)
    probe.timed("reset_namespace", probe.reset_namespace)
    probe.timed("seed_source_generation", probe.seed_source_generation)
    pre_fence_stamp = probe.timed(
        "unfenced_probe",
        lambda: probe._commit_batch(
            "pre-fence-probe", [(b"incarnation/pre-fence", b"accepted")], []
        ),
    )
    closure, manifest = probe.timed("objectify", probe.objectify)
    downloaded, _ = probe.timed(
        "named_get", lambda: probe.verify_named_gets(manifest)
    )
    if downloaded["state_digest"] != closure["state_digest"]:
        raise AssertionError("named closure differs from source closure")
    probe.timed("advance_frontier", lambda: probe.advance_frontier(manifest))
    receipt = {
        "schema_version": 1,
        "kind": SOURCE_KIND,
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "cluster": cluster_identity(arguments.cluster_file),
        "object_closure": closure_authority(closure, manifest),
        "unfenced_probe_succeeded": bool(pre_fence_stamp),
        "unfenced_probe_provider_stamp": pre_fence_stamp,
        "timings": [vars(timing) for timing in probe.timings],
        "duration_ns": time.perf_counter_ns() - started_ns,
    }
    write_json(arguments.output, receipt)
    return 0


def restore_phase(arguments: argparse.Namespace) -> int:
    started_at = utc_now()
    started_ns = time.perf_counter_ns()
    source = read_json(arguments.source_receipt)
    if source.get("kind") != SOURCE_KIND or source.get("run_id") != arguments.run_id:
        raise ValueError("source receipt does not match this incarnation run")
    database = (
        fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    )
    probe = probe_for(arguments, database)
    destination = cluster_identity(arguments.cluster_file)
    if same_cluster(source["cluster"], destination):
        raise AssertionError("destination FoundationDB cluster equals source")
    transaction = database.create_transaction()
    destination_empty = not list(
        transaction[probe.root : prefix_end(probe.root)]
    )
    if not destination_empty:
        raise AssertionError("destination namespace is not empty")
    closure, _ = probe.timed(
        "named_get",
        lambda: probe.verify_named_gets(
            manifest_reference(source["object_closure"])
        ),
    )
    probe.timed("restore", lambda: probe.restore_empty_generation(closure))
    restored_digest, restored_records = probe.state_digest(2)
    if restored_digest != source["object_closure"]["state_digest"]:
        raise AssertionError("ready destination has another state digest")
    receipt = {
        "schema_version": 1,
        "kind": RESTORE_KIND,
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "cluster": destination,
        "destination_empty_before_restore": destination_empty,
        "restored_state_digest": restored_digest,
        "restored_record_count": restored_records,
        "restored_chunks": probe.restored_chunks,
        "replayed_chunks": probe.replayed_chunks,
        "ready_not_active": not database.create_transaction()[
            probe.active_generation_key
        ].wait().present(),
        "timings": [vars(timing) for timing in probe.timings],
        "duration_ns": time.perf_counter_ns() - started_ns,
    }
    write_json(arguments.output, receipt)
    return 0


def fence_phase(arguments: argparse.Namespace) -> int:
    started_at = utc_now()
    source = read_json(arguments.source_receipt)
    database = (
        fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    )
    identity = cluster_identity(arguments.cluster_file)
    if not same_cluster(source["cluster"], identity):
        raise AssertionError("source fence ran against another provider cluster")
    probe = probe_for(arguments, database)
    stale_key = probe.data_prefix(1) + b"concurrent-stale-fence-probe"
    stale = database.create_transaction()
    if bytes_value(stale[probe.active_generation_key].wait()) != b"1":
        raise AssertionError("source provider is not generation one before fence")
    stale[stale_key] = b"must-not-commit"

    stamp_key = probe.root + b"metadata/provider-fence-stamp"
    fence = database.create_transaction()
    if bytes_value(fence[probe.active_generation_key].wait()) != b"1":
        raise AssertionError("source provider changed before fence commit")
    fence[probe.active_generation_key] = FENCE_VALUE
    fence.set_versionstamped_value(stamp_key, versionstamped_parameter(b""))
    stamp_future = fence.get_versionstamp()
    fence.commit().wait()
    provider_stamp = bytes_value(stamp_future.wait()).hex()

    stale_error_code = None
    try:
        stale.commit().wait()
    except fdb.FDBError as error:
        stale_error_code = error.code
    adapter_rejected = False
    try:
        probe._commit_batch(
            "post-fence-adapter-probe", [(b"incarnation/post-fence", b"unsafe")], []
        )
    except AssertionError:
        adapter_rejected = True
    verify = database.create_transaction()
    persisted_value = bytes_value(verify[probe.active_generation_key].wait())
    persisted_stamp = bytes_value(verify[stamp_key].wait()).hex()
    if persisted_value != FENCE_VALUE or persisted_stamp != provider_stamp:
        raise AssertionError("source provider fence did not persist")
    if stale_error_code != 1020 or not adapter_rejected:
        raise AssertionError("source provider fence admitted a stale commit")
    receipt = {
        "schema_version": 1,
        "kind": FENCE_KIND,
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "cluster": identity,
        "fence_value": FENCE_VALUE.decode("ascii"),
        "fence_provider_stamp": provider_stamp,
        "concurrent_stale_commit_error_code": stale_error_code,
        "concurrent_stale_commit_rejected": stale_error_code == 1020,
        "post_fence_adapter_commit_rejected": adapter_rejected,
    }
    write_json(arguments.output, receipt)
    return 0


def valid_authority_report(report: dict[str, Any]) -> bool:
    required = [
        "external_authority_is_separate_from_provider_identities",
        "source_provider_fence_precedes_destination_activation",
        "newer_incarnation_fences_old_commit_authority",
        "newer_incarnation_fences_old_routing",
        "newer_incarnation_fences_old_object_publication",
        "destination_incarnation_routes_commits_and_publishes",
    ]
    return (
        report.get("mode") == "correct"
        and report.get("anomaly_count") == 0
        and all(report.get("checks", {}).get(item) for item in required)
    )


def activate_phase(arguments: argparse.Namespace) -> int:
    started_at = utc_now()
    started_ns = time.perf_counter_ns()
    source = read_json(arguments.source_receipt)
    restore = read_json(arguments.restore_receipt)
    fence = read_json(arguments.fence_receipt)
    authority = read_json(arguments.authority_report)
    if not valid_authority_report(authority):
        raise AssertionError("external authority report did not authorize activation")
    if (
        fence.get("kind") != FENCE_KIND
        or fence.get("run_id") != arguments.run_id
        or not fence.get("concurrent_stale_commit_rejected")
        or not fence.get("post_fence_adapter_commit_rejected")
    ):
        raise AssertionError("destination activation did not consume a valid source fence")
    if restore.get("kind") != RESTORE_KIND or not restore["ready_not_active"]:
        raise AssertionError("destination is not ready and inactive")
    if restore["restored_state_digest"] != source["object_closure"]["state_digest"]:
        raise AssertionError("ready destination digest differs from closure")
    database = (
        fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    )
    probe = probe_for(arguments, database)
    closure, _ = probe.verify_named_gets(
        manifest_reference(source["object_closure"])
    )
    probe.timed(
        "activate", lambda: activate_fresh_destination(probe, closure)
    )
    fresh_stamp = probe.timed(
        "fresh_commit", lambda: fresh_generation_commit(probe)
    )
    receipt = {
        "schema_version": 1,
        "kind": ACTIVATION_KIND,
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "cluster": cluster_identity(arguments.cluster_file),
        "authority_trace_sha256": authority["trace_sha256"],
        "source_fence_receipt_sha256": document_sha256(fence),
        "source_fence_provider_stamp": fence["fence_provider_stamp"],
        "state_digest": restore["restored_state_digest"],
        "fresh_commit_succeeded": bool(fresh_stamp),
        "fresh_commit_provider_stamp": fresh_stamp,
        "timings": [vars(timing) for timing in probe.timings],
        "duration_ns": time.perf_counter_ns() - started_ns,
    }
    write_json(arguments.output, receipt)
    return 0


def resurrect_phase(arguments: argparse.Namespace) -> int:
    probed_at = utc_now()
    source = read_json(arguments.source_receipt)
    fence = read_json(arguments.fence_receipt)
    activation = read_json(arguments.activation_receipt)
    restart = read_json(arguments.restart_observation)
    if (
        activation.get("kind") != ACTIVATION_KIND
        or activation.get("run_id") != arguments.run_id
        or not activation.get("fresh_commit_succeeded")
    ):
        raise AssertionError("source resurrection did not consume a valid activation")
    if not all(
        restart.get(item) is True
        for item in ["stop_succeeded", "start_succeeded", "identities_retained"]
    ):
        raise AssertionError("source resurrection did not consume a valid restart")
    database = (
        fdb.open(arguments.cluster_file) if arguments.cluster_file else fdb.open()
    )
    identity = cluster_identity(arguments.cluster_file)
    if not same_cluster(source["cluster"], identity):
        raise AssertionError("resurrected source has another provider identity")
    probe = probe_for(arguments, database)
    stamp_key = probe.root + b"metadata/provider-fence-stamp"
    verify = database.create_transaction()
    fence_value = bytes_value(verify[probe.active_generation_key].wait())
    fence_stamp = bytes_value(verify[stamp_key].wait()).hex()
    adapter_rejected = False
    try:
        probe._commit_batch(
            "resurrected-source-probe", [(b"incarnation/resurrected", b"unsafe")], []
        )
    except AssertionError:
        adapter_rejected = True
    if (
        fence_value != FENCE_VALUE
        or fence_stamp != fence["fence_provider_stamp"]
        or not adapter_rejected
    ):
        raise AssertionError("resurrected source did not retain its commit fence")
    receipt = {
        "schema_version": 1,
        "kind": RESURRECTION_KIND,
        "provider": PROVIDER_REVISION,
        "api_version": FDB_API_VERSION,
        "run_id": arguments.run_id,
        "probed_at": probed_at,
        "cluster": identity,
        "activation_receipt_sha256": document_sha256(activation),
        "restart_observation_sha256": document_sha256(restart),
        "fence_value": fence_value.decode("ascii"),
        "fence_provider_stamp": fence_stamp,
        "fence_persisted": True,
        "stale_source_adapter_commit_rejected": adapter_rejected,
    }
    write_json(arguments.output, receipt)
    return 0


def gate(gate_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"id": gate_id, "passed": passed, "detail": detail}


def topology_identity(topology: dict[str, Any]) -> dict[str, str]:
    if topology.get("kind") != "objectkv_provider_media_identity_r0":
        raise ValueError("provider topology has another kind")
    return dict(topology["identity"])


def assemble_positive(arguments: argparse.Namespace) -> int:
    source = read_json(arguments.source_receipt)
    restore = read_json(arguments.restore_receipt)
    fence = read_json(arguments.fence_receipt)
    activation = read_json(arguments.activation_receipt)
    resurrection = read_json(arguments.resurrection_receipt)
    authority = read_json(arguments.authority_report)
    source_before = topology_identity(read_json(arguments.source_identity_before))
    source_after = topology_identity(read_json(arguments.source_identity_after))
    destination = topology_identity(read_json(arguments.destination_identity))
    restart = read_json(arguments.restart_observation)
    distinct = all(
        source_before[field] != destination[field]
        for field in ["cluster_id", "instance_id", "boot_disk_id", "data_disk_id"]
    )
    same_source = (
        source_before == source_after
        and same_cluster(source["cluster"], source_before)
        and same_cluster(resurrection["cluster"], source_after)
    )
    same_destination = same_cluster(restore["cluster"], destination) and same_cluster(
        activation["cluster"], destination
    )
    activation_consumed_fence = activation.get(
        "source_fence_receipt_sha256"
    ) == document_sha256(fence)
    resurrection_consumed_activation = resurrection.get(
        "activation_receipt_sha256"
    ) == document_sha256(activation)
    resurrection_consumed_restart = resurrection.get(
        "restart_observation_sha256"
    ) == document_sha256(restart)
    matching_fence_stamp = (
        activation.get("source_fence_provider_stamp")
        == fence.get("fence_provider_stamp")
        == resurrection.get("fence_provider_stamp")
    )
    exact = (
        restore["restored_state_digest"] == source["object_closure"]["state_digest"]
        and activation["state_digest"] == source["object_closure"]["state_digest"]
    )
    checks = authority.get("checks", {})
    gates = [
        gate("provider_identities_distinct", distinct, "source and destination provider media differ"),
        gate("external_authority_process_contract", valid_authority_report(authority), f"trace={authority.get('trace_sha256')}"),
        gate("source_provider_fence_committed", fence["concurrent_stale_commit_rejected"] and fence["post_fence_adapter_commit_rejected"], f"stamp={fence['fence_provider_stamp']}"),
        gate("source_fence_precedes_destination_activation", activation_consumed_fence and matching_fence_stamp, f"fence_receipt_sha256={document_sha256(fence)}"),
        gate("destination_exact_and_writable", exact and same_destination and activation["fresh_commit_succeeded"], f"digest={activation['state_digest']}"),
        gate("source_vm_restarted_without_media_replacement", restart.get("stop_succeeded") is True and restart.get("start_succeeded") is True and restart.get("identities_retained") is True and same_source and resurrection_consumed_restart, "source instance and disks retained their IDs; resurrection consumed the restart receipt"),
        gate("resurrection_follows_destination_activation", resurrection_consumed_activation, f"activation_receipt_sha256={document_sha256(activation)}"),
        gate("newer_incarnation_fences_old_commit_authority", resurrection["fence_persisted"] and resurrection["stale_source_adapter_commit_rejected"], "resurrected source adapter rejected generation one"),
        gate("newer_incarnation_fences_old_routing", bool(checks.get("newer_incarnation_fences_old_routing")), "external route rejected generation one"),
        gate("newer_incarnation_fences_old_object_publication", bool(checks.get("newer_incarnation_fences_old_object_publication")), "external publisher rejected generation one"),
        gate("destination_incarnation_routes_commits_and_publishes", bool(checks.get("destination_incarnation_routes_commits_and_publishes")) and activation["fresh_commit_succeeded"], "destination remained authorized and writable"),
    ]
    failures = [item for item in gates if not item["passed"]]
    receipt = {
        "schema_version": 1,
        "kind": KIND,
        "provider": PROVIDER_REVISION,
        "run_id": arguments.run_id,
        "source": source_before,
        "destination": destination,
        "object_closure": source["object_closure"],
        "authority_trace_sha256": authority["trace_sha256"],
        "source_fence": {
            item: fence[item]
            for item in [
                "started_at",
                "finished_at",
                "fence_value",
                "fence_provider_stamp",
                "concurrent_stale_commit_error_code",
                "concurrent_stale_commit_rejected",
                "post_fence_adapter_commit_rejected",
            ]
        },
        "activation": {
            item: activation[item]
            for item in [
                "started_at",
                "finished_at",
                "authority_trace_sha256",
                "source_fence_receipt_sha256",
                "source_fence_provider_stamp",
                "state_digest",
                "fresh_commit_succeeded",
                "fresh_commit_provider_stamp",
            ]
        },
        "resurrection": {
            item: resurrection[item]
            for item in [
                "probed_at",
                "activation_receipt_sha256",
                "restart_observation_sha256",
                "fence_value",
                "fence_provider_stamp",
                "fence_persisted",
                "stale_source_adapter_commit_rejected",
            ]
        },
        "restart": {
            item: restart[item]
            for item in [
                "stopped_at",
                "started_at",
                "stop_succeeded",
                "start_succeeded",
                "identities_retained",
            ]
        },
        "correctness_anomalies": len(failures),
        "incarnation_fencing_verified": not failures,
        "negative_control": None,
        "gates": gates,
        "scope": "R0 same-media FoundationDB source resurrection with external real-process authority; not cross-zone HA or pre-fence snapshot protection",
    }
    write_json(arguments.output, receipt)
    return 0 if not failures else 1


def assemble_poison(arguments: argparse.Namespace) -> int:
    source = read_json(arguments.source_receipt)
    authority = read_json(arguments.authority_report)
    source_identity = topology_identity(read_json(arguments.source_identity))
    checks = authority.get("checks", {})
    gates = [
        gate("provider_identities_distinct", True, "poison uses the real source provider"),
        gate("newer_incarnation_fences_old_commit_authority", False, f"unfenced provider stamp={source['unfenced_probe_provider_stamp']}"),
        gate("newer_incarnation_fences_old_routing", bool(checks.get("newer_incarnation_fences_old_routing")), "poison bypassed stale routing fence"),
        gate("newer_incarnation_fences_old_object_publication", bool(checks.get("newer_incarnation_fences_old_object_publication")), "poison bypassed stale publication fence"),
    ]
    failures = [item for item in gates if not item["passed"]]
    receipt = {
        "schema_version": 1,
        "kind": KIND,
        "provider": PROVIDER_REVISION,
        "run_id": arguments.run_id,
        "source": source_identity,
        "destination": source_identity,
        "object_closure": source["object_closure"],
        "authority_trace_sha256": authority["trace_sha256"],
        "source_fence": None,
        "activation": None,
        "resurrection": None,
        "restart": None,
        "correctness_anomalies": len(failures),
        "incarnation_fencing_verified": False,
        "negative_control": NEGATIVE_CONTROL,
        "gates": gates,
        "scope": "executed unfenced source commit plus stale-route and stale-publication authority poison",
    }
    write_json(arguments.output, receipt)
    return 1


def add_common_provider_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--cluster-file")
    command.add_argument("--run-id", required=True)
    command.add_argument("--bucket", default="doss-objectkv-dev-okv-evals")
    command.add_argument(
        "--object-prefix", default="results/provider-r0/incarnation"
    )
    command.add_argument("--restore-chunk-records", type=int, default=200)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    source = commands.add_parser("source")
    add_common_provider_arguments(source)
    source.add_argument("--record-count", type=int, default=1000)
    source.add_argument("--output", required=True)
    source.set_defaults(handler=source_phase)

    restore = commands.add_parser("restore")
    add_common_provider_arguments(restore)
    restore.add_argument("--source-receipt", required=True)
    restore.add_argument("--output", required=True)
    restore.set_defaults(handler=restore_phase)

    fence = commands.add_parser("fence")
    add_common_provider_arguments(fence)
    fence.add_argument("--source-receipt", required=True)
    fence.add_argument("--output", required=True)
    fence.set_defaults(handler=fence_phase)

    activate = commands.add_parser("activate")
    add_common_provider_arguments(activate)
    activate.add_argument("--source-receipt", required=True)
    activate.add_argument("--restore-receipt", required=True)
    activate.add_argument("--fence-receipt", required=True)
    activate.add_argument("--authority-report", required=True)
    activate.add_argument("--output", required=True)
    activate.set_defaults(handler=activate_phase)

    resurrect = commands.add_parser("resurrect")
    add_common_provider_arguments(resurrect)
    resurrect.add_argument("--source-receipt", required=True)
    resurrect.add_argument("--fence-receipt", required=True)
    resurrect.add_argument("--activation-receipt", required=True)
    resurrect.add_argument("--restart-observation", required=True)
    resurrect.add_argument("--output", required=True)
    resurrect.set_defaults(handler=resurrect_phase)

    positive = commands.add_parser("assemble-positive")
    positive.add_argument("--run-id", required=True)
    positive.add_argument("--source-receipt", required=True)
    positive.add_argument("--restore-receipt", required=True)
    positive.add_argument("--fence-receipt", required=True)
    positive.add_argument("--activation-receipt", required=True)
    positive.add_argument("--resurrection-receipt", required=True)
    positive.add_argument("--authority-report", required=True)
    positive.add_argument("--source-identity-before", required=True)
    positive.add_argument("--source-identity-after", required=True)
    positive.add_argument("--destination-identity", required=True)
    positive.add_argument("--restart-observation", required=True)
    positive.add_argument("--output", required=True)
    positive.set_defaults(handler=assemble_positive)

    poison = commands.add_parser("assemble-poison")
    poison.add_argument("--run-id", required=True)
    poison.add_argument("--source-receipt", required=True)
    poison.add_argument("--source-identity", required=True)
    poison.add_argument("--authority-report", required=True)
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
