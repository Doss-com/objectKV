#!/usr/bin/env bash
set -euo pipefail

stable_device=/dev/disk/by-id/google-objectkv-data
stable_mount=/var/lib/objectkv
hot_mount="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/objectkv-hot-mount)"

for _ in $(seq 1 60); do
  if [[ -b "${stable_device}" ]]; then
    break
  fi
  sleep 1
done

if [[ ! -b "${stable_device}" ]]; then
  echo "objectKV data disk did not appear" >&2
  exit 1
fi

hot_device=""
for _ in $(seq 1 60); do
  for candidate in /dev/disk/by-id/google-local-nvme-ssd-0 /dev/disk/by-id/google-local-ssd-0; do
    if [[ -b "${candidate}" ]]; then
      hot_device="${candidate}"
      break 2
    fi
  done
  hot_device="$(lsblk -dpno NAME,MODEL | awk '$2 ~ /Local_SSD|nvme_card|NVMe_Card/ {print $1; exit}')"
  if [[ -n "${hot_device}" && -b "${hot_device}" ]]; then
    break
  fi
  hot_device=""
  sleep 1
done

if [[ -z "${hot_device}" || ! -b "${hot_device}" ]]; then
  echo "objectKV local NVMe scratch did not appear" >&2
  exit 1
fi

mount_device() {
  local device="$1"
  local mount_point="$2"
  if ! blkid "${device}" >/dev/null 2>&1; then
    mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "${device}"
  fi
  mkdir -p "${mount_point}"
  local uuid
  uuid="$(blkid -s UUID -o value "${device}")"
  if ! grep -q "UUID=${uuid}" /etc/fstab; then
    printf 'UUID=%s %s ext4 defaults,noatime,nofail 0 2\n' "${uuid}" "${mount_point}" >> /etc/fstab
  fi
  mountpoint -q "${mount_point}" || mount "${mount_point}"
}

mount_device "${stable_device}" "${stable_mount}"
mount_device "${hot_device}" "${hot_mount}"

id objectkv >/dev/null 2>&1 || useradd --create-home --shell /bin/bash objectkv
install -d -m 0755 /opt/objectkv/bin
install -d -m 0755 -o objectkv -g objectkv \
  "${stable_mount}/evals" \
  "${stable_mount}/receipts" \
  "${stable_mount}/scratch" \
  "${hot_mount}/serving"

operator_key="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/objectkv-operator-ssh-key 2>/dev/null || true)"
if [[ -n "${operator_key}" ]]; then
  passwd --delete objectkv
  install -d -m 0700 -o objectkv -g objectkv /home/objectkv/.ssh
  printf '%s\n' "${operator_key}" >/home/objectkv/.ssh/authorized_keys
  chown objectkv:objectkv /home/objectkv/.ssh/authorized_keys
  chmod 0600 /home/objectkv/.ssh/authorized_keys
  printf 'objectkv ALL=(ALL) NOPASSWD:ALL\n' >/etc/sudoers.d/objectkv
  chmod 0440 /etc/sudoers.d/objectkv
  cat >/etc/ssh/sshd_config.d/00-objectkv-break-glass.conf <<'EOF'
AuthorizedKeysFile .ssh/authorized_keys
AuthorizedKeysCommand none
EOF
  sshd -t
  systemctl restart ssh
fi

cat >"${stable_mount}/runner-ready.json" <<EOF
{"schema_version":1,"stable_device":"${stable_device}","stable_filesystem":"ext4","stable_mount":"${stable_mount}","hot_device":"${hot_device}","hot_filesystem":"ext4","hot_mount":"${hot_mount}","hot_interface":"$(lsblk -ndo TRAN "${hot_device}" | head -1)","hot_bytes":$(blockdev --getsize64 "${hot_device}"),"status":"ready"}
EOF
