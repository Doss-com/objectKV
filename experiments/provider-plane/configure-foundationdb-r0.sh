#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <run-label> <source|restore>" >&2
  exit 64
fi
if [[ "$(id -u)" -ne 0 ]]; then
  echo "configure-foundationdb-r0.sh must run as root" >&2
  exit 77
fi

run_label="$1"
phase="$2"
if [[ ! "${run_label}" =~ ^[a-z][a-z0-9-]{0,19}$ ]]; then
  echo "run label must be 1 to 20 lowercase letters, digits, or hyphens" >&2
  exit 64
fi
if [[ "${phase}" != "source" && "${phase}" != "restore" ]]; then
  echo "phase must be source or restore" >&2
  exit 64
fi

provider_root=/var/lib/objectkv/foundationdb
provider_log_root=/var/lib/objectkv/foundationdb-logs
cluster_file=/etc/foundationdb/fdb.cluster
config_file=/etc/foundationdb/foundationdb.conf
venv=/opt/objectkv/provider-venv
package_scratch="$(mktemp -d /tmp/objectkv-fdb-package.XXXXXX)"
trap 'find "${package_scratch}" -depth -delete' EXIT

clients_name=foundationdb-clients_7.4.6-1_amd64.deb
server_name=foundationdb-server_7.4.6-1_amd64.deb
clients_sha=7e29df033c3d1d27701d094651cf87a36fa5b0afc2896c8e4aaf47f549e68365
server_sha=78694510c1e99f36a51cc32c84bed45e214899771e42ad2b604b254665d5d9cf
release_root=https://github.com/apple/foundationdb/releases/download/7.4.6

apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ca-certificates curl git jq python3-venv
curl -fsSL "${release_root}/${clients_name}" -o "${package_scratch}/${clients_name}"
curl -fsSL "${release_root}/${server_name}" -o "${package_scratch}/${server_name}"
printf '%s  %s\n' "${clients_sha}" "${package_scratch}/${clients_name}" | sha256sum --check --status
printf '%s  %s\n' "${server_sha}" "${package_scratch}/${server_name}" | sha256sum --check --status
DEBIAN_FRONTEND=noninteractive dpkg -i \
  "${package_scratch}/${clients_name}" \
  "${package_scratch}/${server_name}"

systemctl stop foundationdb || true
install -d -m 0750 -o foundationdb -g foundationdb "${provider_root}" "${provider_log_root}"
install -d -m 0755 /etc/foundationdb
cluster_id="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
description="objectkv_${run_label//-/_}_${phase}"
if [[ ! "${description}" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "FoundationDB cluster description contains a disallowed character" >&2
  exit 64
fi
printf '%s:%s@127.0.0.1:4500\n' "${description}" "${cluster_id}" >"${cluster_file}"
chown foundationdb:foundationdb "${cluster_file}"
chmod 0644 "${cluster_file}"

cat >"${config_file}" <<EOF
[fdbmonitor]
user = foundationdb
group = foundationdb

[general]
cluster-file = ${cluster_file}
restart-delay = 60

[fdbserver]
command = /usr/sbin/fdbserver
public-address = 127.0.0.1:\$ID
listen-address = public
datadir = ${provider_root}/\$ID
logdir = ${provider_log_root}

[fdbserver.4500]
EOF
chown foundationdb:foundationdb "${config_file}"
chmod 0640 "${config_file}"
systemctl restart foundationdb

ready=false
for _ in $(seq 1 60); do
  if fdbcli -C "${cluster_file}" --exec 'configure new single ssd' >/tmp/objectkv-fdb-configure.log 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != "true" ]]; then
  cat /tmp/objectkv-fdb-configure.log >&2
  exit 1
fi
rm -f /tmp/objectkv-fdb-configure.log
fdbcli -C "${cluster_file}" --exec 'status minimal'

python3 -m venv "${venv}"
"${venv}/bin/pip" install --disable-pip-version-check --quiet \
  foundationdb==7.4.6 \
  google-cloud-storage==3.9.0
"${venv}/bin/pip" freeze --all >"${provider_root}/python-packages.txt"

cluster_file_sha256="$(sha256sum "${cluster_file}" | cut -d ' ' -f 1)"
jq -n \
  --arg configured_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg run_label "${run_label}" \
  --arg phase "${phase}" \
  --arg cluster_id "${cluster_id}" \
  --arg cluster_file_sha256 "${cluster_file_sha256}" \
  --arg cluster_file "${cluster_file}" \
  --arg datadir "${provider_root}" \
  --arg provider "foundationdb-7.4.6@e77b64d4c5d01d240931c08c5384a834cae27337" \
  '{
    schema_version: 1,
    kind: "objectkv_foundationdb_r0_configuration",
    configured_at: $configured_at,
    run_label: $run_label,
    phase: $phase,
    provider: $provider,
    cluster_id: $cluster_id,
    cluster_file_sha256: $cluster_file_sha256,
    cluster_file: $cluster_file,
    datadir: $datadir
  }' >"${provider_root}/configuration.json"
