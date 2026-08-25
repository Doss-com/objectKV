#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${OKV_NVME_PROJECT:-doss-objectkv-dev}"
ZONE="${OKV_NVME_ZONE:-us-central1-a}"
BUCKET="${OKV_NVME_BUCKET:-doss-objectkv-dev-okv-evals}"
MACHINE_TYPE="${OKV_NVME_MACHINE_TYPE:-n2-standard-8}"
SERVICE_ACCOUNT="${OKV_NVME_SERVICE_ACCOUNT:-objectkv-eval-runner@doss-objectkv-dev.iam.gserviceaccount.com}"
LOCAL_SSD_BY_ID="/dev/disk/by-id/google-local-nvme-ssd-0"
LOCAL_SSD_BYTES="402653184000"
ROCKSDB_COMMIT="3b446089141659fad25328c5ea3e7ed283df46e4"
SEEDS="${OKV_NVME_SEEDS:-724851,724877,724901,724921,724939}"
PAYLOADS="${OKV_NVME_PAYLOADS:-8192,16384,32768,65536}"
STATES="${OKV_NVME_STATES:-direct,buffered}"
RAW_FIO_SECONDS="${OKV_NVME_RAW_FIO_SECONDS:-30}"
CONTROLLER_INSTANCE=""

controller_cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$CONTROLLER_INSTANCE" ]]; then
    gcloud compute instances delete "$CONTROLLER_INSTANCE" --project="$PROJECT_ID" \
      --zone="$ZONE" --quiet || true
  fi
  exit "$status"
}

metadata() {
  curl --fail --silent --show-error \
    -H 'Metadata-Flavor: Google' \
    "http://metadata.google.internal/computeMetadata/v1/$1"
}

worker_status() {
  local state="$1"
  local detail="$2"
  jq -n \
    --arg state "$state" \
    --arg detail "$detail" \
    --arg runner "$RUNNER_ID" \
    --arg candidate "$CANDIDATE_COMMIT" \
    --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    '{state:$state,detail:$detail,runner:$runner,candidate_commit:$candidate,at:$at}' \
    >"$STATUS_FILE"
  gcloud storage cp --quiet "$STATUS_FILE" "$RESULT_PREFIX/status.json"
}

guard_scratch_path() {
  local path="$1"
  case "$path" in
    /mnt/objectkv/cases/*) ;;
    *) echo "refusing cleanup outside /mnt/objectkv/cases" >&2; return 1 ;;
  esac
}

worker_cleanup() {
  local status=$?
  set +e
  if mountpoint -q /mnt/objectkv; then
    sync
    umount /mnt/objectkv
  fi
  if [[ $status -ne 0 ]]; then
    worker_status failed "worker exited with status $status" || true
    gcloud storage cp --quiet /var/log/objectkv-rfc0071-worker.log \
      "$RESULT_PREFIX/worker.log" || true
  fi
  exit "$status"
}

device_guard() {
  [[ "$LOCAL_SSD_BY_ID" == "/dev/disk/by-id/google-local-nvme-ssd-0" ]]
  [[ -L "$LOCAL_SSD_BY_ID" ]]
  DEVICE="$(readlink -f "$LOCAL_SSD_BY_ID")"
  [[ "$DEVICE" =~ ^/dev/nvme[0-9]+n[0-9]+$ ]]

  local boot_source boot_device boot_parent local_name local_count size instance_name
  boot_source="$(findmnt -n -o SOURCE /)"
  boot_device="$(readlink -f "$boot_source")"
  boot_parent="$(lsblk -n -o PKNAME "$boot_device" | head -n 1)"
  local_name="$(basename "$DEVICE")"
  [[ -n "$boot_parent" ]]
  [[ "$boot_parent" != "$local_name" ]]
  size="$(blockdev --getsize64 "$DEVICE")"
  [[ "$size" == "$LOCAL_SSD_BYTES" ]]
  shopt -s nullglob
  local local_devices=(/dev/disk/by-id/google-local-nvme-ssd-*)
  shopt -u nullglob
  local_count="${#local_devices[@]}"
  [[ "$local_count" == "1" ]]
  instance_name="$(metadata instance/name)"
  [[ "$instance_name" == "$RUNNER_ID" ]]

  jq -n \
    --arg configured "$LOCAL_SSD_BY_ID" \
    --arg resolved "$DEVICE" \
    --arg boot "$boot_device" \
    --arg size "$size" \
    --arg instance "$instance_name" \
    --arg logical_sector "$(blockdev --getss "$DEVICE")" \
    --arg physical_sector "$(blockdev --getpbsz "$DEVICE")" \
    '{configured_path:$configured,resolved_device:$resolved,boot_device:$boot,
      device_bytes:($size|tonumber),instance:$instance,
      logical_sector_bytes:($logical_sector|tonumber),
      physical_sector_bytes:($physical_sector|tonumber),guard_passed:true}' \
    >"$RESULTS/device-guard.json"
}

run_fio() {
  worker_status running "raw Local SSD calibration"
  blkdiscard "$DEVICE"
  fio --name=full-write --filename="$DEVICE" --rw=write --bs=1M \
    --ioengine=libaio --iodepth=128 --direct=1 --numjobs=1 --size=100% \
    --group_reporting --output-format=json \
    --output="$RESULTS/fio-full-write.json"
  local block queue
  for block in 4096 8192 16384 32768 65536; do
    for queue in 1 8 32 128; do
      fio --name="randread-${block}-${queue}" --filename="$DEVICE" --rw=randread \
        --bs="$block" --ioengine=libaio --iodepth="$queue" --direct=1 \
        --numjobs=1 --runtime="$RAW_FIO_SECONDS" --time_based=1 --readonly=1 \
        --group_reporting --output-format=json \
        --output="$RESULTS/fio-randread-${block}-${queue}.json"
    done
  done
  fio --name=seqread-1m --filename="$DEVICE" --rw=read --bs=1M \
    --ioengine=libaio --iodepth=32 --direct=1 --numjobs=1 \
    --runtime="$RAW_FIO_SECONDS" --time_based=1 --readonly=1 --group_reporting \
    --output-format=json --output="$RESULTS/fio-seqread-1048576-32.json"
}

build_probes() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq build-essential ca-certificates cmake curl fio git jq \
    libbz2-dev liblz4-dev libsnappy-dev libssl-dev libzstd-dev pkg-config zlib1g-dev >/dev/null
  worker_status running "building pinned candidate and RocksDB incumbent"
  curl --proto '=https' --tlsv1.2 --silent --show-error --fail \
    https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.88.0 >/dev/null
  export PATH="/root/.cargo/bin:$PATH"
  export CARGO_TARGET_DIR=/opt/objectkv-target
  mkdir -p /opt/objectkv /opt/rocksdb
  git -C /opt/objectkv init -q
  git -C /opt/objectkv remote add origin https://github.com/Doss-com/objectKV.git
  git -C /opt/objectkv fetch -q --depth=1 origin "$CANDIDATE_COMMIT"
  git -C /opt/objectkv checkout -q --detach FETCH_HEAD
  [[ "$(git -C /opt/objectkv rev-parse HEAD)" == "$CANDIDATE_COMMIT" ]]
  (
    cd /opt/objectkv
    cargo build --locked --release -p okv-object --bin range-image-nvme-probe
  )

  git -C /opt/rocksdb init -q
  git -C /opt/rocksdb remote add origin https://github.com/facebook/rocksdb.git
  git -C /opt/rocksdb fetch -q --depth=1 origin "$ROCKSDB_COMMIT"
  git -C /opt/rocksdb checkout -q --detach FETCH_HEAD
  [[ "$(git -C /opt/rocksdb rev-parse HEAD)" == "$ROCKSDB_COMMIT" ]]
  c++ -std=c++20 -fsyntax-only -I/opt/rocksdb -I/opt/rocksdb/include \
    /opt/objectkv/experiments/range-image-nvme/rocksdb_probe.cc
  make -C /opt/rocksdb -j"$(nproc)" static_lib \
    PORTABLE=1 DEBUG_LEVEL=0 USE_RTTI=1 USE_GFLAGS=0 DISABLE_WARNING_AS_ERROR=1
  c++ -std=c++20 -O3 -DNDEBUG \
    -I/opt/rocksdb -I/opt/rocksdb/include \
    /opt/objectkv/experiments/range-image-nvme/rocksdb_probe.cc \
    /opt/rocksdb/librocksdb.a \
    -lssl -lcrypto -lsnappy -lz -lbz2 -llz4 -lzstd -lpthread -ldl -lrt \
    -o /opt/rocksdb-probe
  sha256sum /opt/objectkv-target/release/range-image-nvme-probe \
    /opt/rocksdb-probe >"$RESULTS/executable-sha256.txt"
}

prepare_filesystem() {
  mkfs.ext4 -q -F -b 4096 "$DEVICE"
  mkdir -p /mnt/objectkv
  mount -o noatime "$DEVICE" /mnt/objectkv
  mkdir -p /mnt/objectkv/traces /mnt/objectkv/cases
  findmnt -n -o SOURCE,FSTYPE,OPTIONS /mnt/objectkv >"$RESULTS/filesystem.txt"
  lsblk -b -J -o NAME,KNAME,PATH,SIZE,LOG-SEC,PHY-SEC,TYPE,MOUNTPOINTS \
    >"$RESULTS/lsblk.json"
}

run_matrix() {
  worker_status running "running exact objectKV and RocksDB curves"
  IFS=',' read -r -a seed_values <<<"$SEEDS"
  IFS=',' read -r -a payload_values <<<"$PAYLOADS"
  IFS=',' read -r -a state_values <<<"$STATES"
  local seed payload state trace_path case_id case_root object_root rocks_root direct_flag io_mode
  for seed in "${seed_values[@]}"; do
    trace_path="/mnt/objectkv/traces/trace-${seed}.bin"
    /opt/objectkv-target/release/range-image-nvme-probe trace \
      --path "$trace_path" --seed "$seed" --key-count 131072 \
      --warmup-points 16384 --measured-points 131072 \
      >"$RESULTS/trace-${seed}.sha256"
    for payload in "${payload_values[@]}"; do
      for state in "${state_values[@]}"; do
        case_id="${seed}-${payload}-${state}"
        case_root="/mnt/objectkv/cases/$case_id"
        object_root="$case_root/objectkv"
        rocks_root="$case_root/rocksdb"
        guard_scratch_path "$case_root"
        mkdir -p "$object_root" "$rocks_root"
        if [[ "$state" == "direct" ]]; then
          direct_flag=true
          io_mode=direct
        elif [[ "$state" == "buffered" ]]; then
          direct_flag=false
          io_mode=buffered
        else
          echo "unknown cache state: $state" >&2
          return 1
        fi
        /opt/objectkv-target/release/range-image-nvme-probe run \
          --root "$object_root" --trace "$trace_path" \
          --block-payload-bytes "$payload" --value-bytes 8192 \
          --reader-memory-budget-bytes 67108864 --concurrencies 1,8,32 \
          --io-mode "$io_mode" >"$RESULTS/objectkv-${case_id}.json"
        /opt/rocksdb-probe \
          --db="$rocks_root/db" --trace="$trace_path" --block-bytes="$payload" \
          --value-bytes=8192 --cache-bytes=67108864 --concurrencies=1,8,32 \
          --direct="$direct_flag" >"$RESULTS/rocksdb-${case_id}.json"
        jq -e '.point_curves | all(.exact == true)' "$RESULTS/objectkv-${case_id}.json" >/dev/null
        jq -e '.point_curves | all(.exact == true)' "$RESULTS/rocksdb-${case_id}.json" >/dev/null
        jq -e '.scan.exact == true' "$RESULTS/objectkv-${case_id}.json" >/dev/null
        jq -e '.scan.exact == true' "$RESULTS/rocksdb-${case_id}.json" >/dev/null
        jq -e --slurp \
          '.[0].trace_sha256 == .[1].trace_sha256 and
           .[0].fixture_sha256 == .[1].fixture_sha256 and
           .[0].scan.digest_sha256 == .[1].scan.digest_sha256' \
          "$RESULTS/objectkv-${case_id}.json" "$RESULTS/rocksdb-${case_id}.json" >/dev/null
        jq -n --slurpfile objectkv "$RESULTS/objectkv-${case_id}.json" \
          --slurpfile rocksdb "$RESULTS/rocksdb-${case_id}.json" \
          '{seed:$objectkv[0].seed,payload_bytes:$objectkv[0].block_payload_bytes,
            io_mode:$objectkv[0].io_mode,
            image_amplification:$objectkv[0].image_amplification,
            objectkv_accounted_bytes:$objectkv[0].accounted_resident_bytes,
            objectkv_peak_rss_bytes:$objectkv[0].peak_worker_rss_bytes,
            objectkv_point:$objectkv[0].point_curves,
            rocksdb_point:$rocksdb[0].point_curves,
            iops_ratio_c32:($objectkv[0].point_curves[2].iops/$rocksdb[0].point_curves[2].iops),
            p99_ratios:[range(0;3) as $i |
              ($objectkv[0].point_curves[$i].latency_p99_seconds /
               $rocksdb[0].point_curves[$i].latency_p99_seconds)],
            scan_ratio:($objectkv[0].scan.logical_bytes_per_second /
              $rocksdb[0].scan.logical_bytes_per_second),
            exact:true}' >"$RESULTS/summary-${case_id}.json"
        rm -rf -- "$case_root"
      done
    done
  done
}

worker_main() {
  RUNNER_ID="$(metadata instance/attributes/okv-runner-id)"
  CANDIDATE_COMMIT="$(metadata instance/attributes/okv-candidate-commit)"
  RUN_ID="$(metadata instance/attributes/okv-run-id)"
  SEEDS="$(metadata instance/attributes/okv-seeds)"
  PAYLOADS="$(metadata instance/attributes/okv-payloads)"
  STATES="$(metadata instance/attributes/okv-states)"
  RAW_FIO_SECONDS="$(metadata instance/attributes/okv-fio-seconds)"
  RESULT_PREFIX="gs://$BUCKET/results/rfc0071/$RUN_ID"
  RESULTS="/var/lib/objectkv-rfc0071-results"
  STATUS_FILE="/var/lib/objectkv-rfc0071-status.json"
  mkdir -p "$RESULTS"
  exec > >(tee -a /var/log/objectkv-rfc0071-worker.log) 2>&1
  trap worker_cleanup EXIT
  local started_epoch
  started_epoch="$(date +%s)"
  build_probes
  device_guard
  run_fio
  prepare_filesystem
  run_matrix
  local stopped_epoch
  stopped_epoch="$(date +%s)"
  jq -n \
    --arg run_id "$RUN_ID" --arg runner "$RUNNER_ID" --arg candidate "$CANDIDATE_COMMIT" \
    --arg rocksdb "$ROCKSDB_COMMIT" --arg seeds "$SEEDS" --arg payloads "$PAYLOADS" \
    --arg states "$STATES" --argjson started "$started_epoch" --argjson stopped "$stopped_epoch" \
    '{run_id:$run_id,runner:$runner,candidate_commit:$candidate,rocksdb_commit:$rocksdb,
      seeds:$seeds,payloads:$payloads,states:$states,started_epoch:$started,
      stopped_epoch:$stopped,provisioned_seconds:($stopped-$started),complete:true}' \
    >"$RESULTS/run.json"
  gcloud storage cp --quiet --recursive "$RESULTS" "$RESULT_PREFIX/"
  worker_status complete "results uploaded"
  trap - EXIT
  sync
  umount /mnt/objectkv
  shutdown -h now
}

controller_main() {
  local repo_root candidate run_id instance result_prefix
  repo_root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
  candidate="${OKV_NVME_CANDIDATE_COMMIT:-$(git -C "$repo_root" rev-parse HEAD)}"
  git -C "$repo_root" diff --quiet
  git -C "$repo_root" diff --cached --quiet
  git -C "$repo_root" branch -r --contains "$candidate" | grep -q 'origin/'
  run_id="$(date -u +%Y%m%dT%H%M%SZ)-${candidate:0:12}"
  instance="okv-nvme-$(printf '%s' "$run_id" | tr '[:upper:]' '[:lower:]')"
  instance="${instance//:/-}"
  result_prefix="gs://$BUCKET/results/rfc0071/$run_id"
  trap controller_cleanup EXIT INT TERM

  gcloud compute instances create "$instance" \
    --project="$PROJECT_ID" --zone="$ZONE" --machine-type="$MACHINE_TYPE" \
    --network=objectkv-eval --subnet=objectkv-eval-us-central1 \
    --service-account="$SERVICE_ACCOUNT" --scopes=cloud-platform \
    --image-family=debian-12 --image-project=debian-cloud --boot-disk-size=100GB \
    --boot-disk-type=pd-balanced --local-ssd=interface=NVME \
    --metadata="^:^okv-runner-id=$instance:okv-run-id=$run_id:okv-candidate-commit=$candidate:okv-seeds=$SEEDS:okv-payloads=$PAYLOADS:okv-states=$STATES:okv-fio-seconds=$RAW_FIO_SECONDS" \
    --metadata-from-file="startup-script=$repo_root/infra/gcp/run-range-image-nvme-incumbent.sh" \
    --labels=project=objectkv,purpose=rfc0071-eval >/dev/null
  CONTROLLER_INSTANCE="$instance"
  echo "runner=$instance run_id=$run_id candidate=$candidate"
  local state detail attempts=0
  while (( attempts < 720 )); do
    attempts=$((attempts + 1))
    if status_json="$(gcloud storage cat "$result_prefix/status.json" 2>/dev/null)"; then
      state="$(jq -r '.state' <<<"$status_json")"
      detail="$(jq -r '.detail' <<<"$status_json")"
      echo "state=$state detail=$detail"
      if [[ "$state" == "complete" ]]; then
        gcloud storage cat "$result_prefix/objectkv-rfc0071-results/run.json"
        return 0
      fi
      if [[ "$state" == "failed" ]]; then
        gcloud compute instances get-serial-port-output "$instance" \
          --project="$PROJECT_ID" --zone="$ZONE" --port=1 --start=-200 || true
        return 1
      fi
    fi
    sleep 30
  done
  echo "runner timed out" >&2
  return 1
}

if curl --connect-timeout 1 --fail --silent \
  -H 'Metadata-Flavor: Google' \
  http://metadata.google.internal/computeMetadata/v1/instance/attributes/okv-runner-id \
  >/dev/null 2>&1; then
  worker_main
else
  controller_main
fi
