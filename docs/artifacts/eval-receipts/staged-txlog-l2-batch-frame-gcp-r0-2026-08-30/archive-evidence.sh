#!/usr/bin/env bash
set -euo pipefail

evidence_dir=/tmp/staged-txlog-l2-batch-frame-2a159c3-r0
archive=/tmp/staged-txlog-l2-batch-frame-2a159c3-r0.tar.gz

if [[ -d "${evidence_dir}" ]]; then
  find "${evidence_dir}" -depth -delete
fi
install -d "${evidence_dir}/inputs" "${evidence_dir}/reports" "${evidence_dir}/source"
cp /tmp/node-0.json /tmp/node-1.json /tmp/node-2.json "${evidence_dir}/inputs/"
cp /tmp/curve-40000.json /tmp/curve-60000.json /tmp/curve-100000.json \
  /tmp/curve-150000.json /tmp/curve-200000.json "${evidence_dir}/inputs/"
cp /tmp/l2b-40000.json /tmp/l2b-60000.json /tmp/l2b-100000.json \
  /tmp/l2b-150000.json /tmp/l2b-200000.json "${evidence_dir}/reports/"
cp /tmp/objectkv-2a159c3.tar.gz "${evidence_dir}/source/"
sha256sum /home/wileyjones/bin-2a159c3/okv-eval > "${evidence_dir}/binary.sha256"
find "${evidence_dir}/inputs" "${evidence_dir}/reports" "${evidence_dir}/source" \
  -type f -print0 | sort -z | xargs -0 sha256sum > "${evidence_dir}/files.sha256"
tar -C /tmp -czf "${archive}" "$(basename "${evidence_dir}")"
sha256sum "${archive}"
du -h "${archive}"
du -sh "${evidence_dir}"
