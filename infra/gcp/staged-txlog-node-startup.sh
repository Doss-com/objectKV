#!/usr/bin/env bash
set -euo pipefail

device=""
for _ in $(seq 1 60); do
  for candidate in /dev/disk/by-id/google-local-nvme-ssd-0 /dev/disk/by-id/google-local-ssd-0; do
    if [[ -b "${candidate}" ]]; then
      device="${candidate}"
      break 2
    fi
  done
  sleep 1
done

if [[ -z "${device}" ]]; then
  echo "objectKV local SSD did not appear" >&2
  exit 1
fi

mount_point=/var/lib/objectkv-hot
if ! blkid "${device}" >/dev/null 2>&1; then
  mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "${device}"
fi
mkdir -p "${mount_point}"
uuid="$(blkid -s UUID -o value "${device}")"
if ! grep -q "UUID=${uuid}" /etc/fstab; then
  printf 'UUID=%s %s ext4 defaults,noatime,nofail 0 2\n' "${uuid}" "${mount_point}" >> /etc/fstab
fi
mountpoint -q "${mount_point}" || mount "${mount_point}"
install -d -m 0777 "${mount_point}/txlog"

cat >"${mount_point}/node-ready.json" <<EOF
{"schema_version":1,"device":"${device}","filesystem":"ext4","mount":"${mount_point}","status":"ready"}
EOF
