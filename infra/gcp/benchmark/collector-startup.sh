#!/usr/bin/env bash
set -euo pipefail

device=/dev/disk/by-id/google-objectkv-otel
mount_point=/var/lib/objectkv

for _ in $(seq 1 60); do
  if [[ -b "${device}" ]]; then
    break
  fi
  sleep 1
done

if [[ ! -b "${device}" ]]; then
  echo "objectKV telemetry disk did not appear" >&2
  exit 1
fi

if ! blkid "${device}" >/dev/null 2>&1; then
  mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0 "${device}"
fi

mkdir -p "${mount_point}"
uuid="$(blkid -s UUID -o value "${device}")"
if ! grep -q "UUID=${uuid}" /etc/fstab; then
  printf 'UUID=%s %s ext4 defaults,noatime,nofail 0 2\n' "${uuid}" "${mount_point}" >> /etc/fstab
fi
mountpoint -q "${mount_point}" || mount "${mount_point}"
install -d -m 0750 -o 10001 -g 10001 "${mount_point}/otel"

runner_cidr="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/objectkv-runner-cidr)"
if ! iptables -C INPUT -s "${runner_cidr}" -p tcp -m multiport --dports 4317,4318,13133 -j ACCEPT 2>/dev/null; then
  iptables -I INPUT 1 -s "${runner_cidr}" -p tcp -m multiport --dports 4317,4318,13133 -j ACCEPT
fi

operator_key="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/objectkv-operator-ssh-key 2>/dev/null || true)"
if [[ -n "${operator_key}" ]]; then
  id objectkv >/dev/null 2>&1 || useradd --create-home --shell /bin/bash objectkv
  passwd --delete objectkv
  install -d -m 0700 -o objectkv -g objectkv /home/objectkv/.ssh
  printf '%s\n' "${operator_key}" >/home/objectkv/.ssh/authorized_keys
  chown objectkv:objectkv /home/objectkv/.ssh/authorized_keys
  chmod 0600 /home/objectkv/.ssh/authorized_keys
  printf 'objectkv ALL=(ALL) NOPASSWD:ALL\n' >/etc/sudoers.d/objectkv
  chmod 0440 /etc/sudoers.d/objectkv
fi

config_b64="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/objectkv-otel-config)"
collector_image="$(curl -fsS -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/attributes/objectkv-collector-image)"
printf '%s' "${config_b64}" | base64 -d >"${mount_point}/otel/collector.yaml"

image_ready=false
for _ in $(seq 1 12); do
  if docker image inspect "${collector_image}" >/dev/null 2>&1 || docker pull "${collector_image}"; then
    image_ready=true
    break
  fi
  sleep 5
done
if [[ "${image_ready}" != "true" ]]; then
  echo "OpenTelemetry collector image was not available after bounded retries" >&2
  exit 1
fi

docker rm -f objectkv-otel >/dev/null 2>&1 || true
docker run -d \
  --pull never \
  --name objectkv-otel \
  --restart always \
  --network host \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --mount "type=bind,src=${mount_point}/otel,dst=/var/lib/objectkv/otel" \
  "${collector_image}" \
  --config=/var/lib/objectkv/otel/collector.yaml
