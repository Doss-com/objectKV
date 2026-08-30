#!/usr/bin/env node

// Independent physical-format generator for RFC-0049. This file imports no
// objectKV package. It emits the full T28 format summary or small positive and
// corrupt compatibility fixtures without writing files.

import { createHash } from "node:crypto";

const MASK = (1n << 64n) - 1n;
const U64_MAX = MASK;
const FORMAT_VERSION = 2;
const GENERATION = 1n;
const GROUP_TARGET_ROWS = 32;
const MAX_VERSIONS_PER_KEY = 32;
const C0_MAX_POINT_BYTES = 65524;
const MAX_FRAME_PAIR_BYTES = Math.floor(C0_MAX_POINT_BYTES / 2);
const INDEX_MAGIC = Buffer.from("OKI2");
const PROJECTION_MAGIC = Buffer.from("OKP2");
const PAYLOAD_MAGIC = Buffer.from("OKV2");
const MANIFEST_MAGIC = Buffer.from("OKVCM2");
const PROJECTION_LEAF_DOMAIN = Buffer.from("okv-c5v2-projection-leaf-v1\0");
const PAYLOAD_LEAF_DOMAIN = Buffer.from("okv-c5v2-payload-leaf-v1\0");
const NODE_DOMAIN = Buffer.from("okv-c5v2-merkle-node-v1\0");
const INDEX_ENTRY_BYTES = 24;
const PROJECTION_RECORD_BYTES = 57;
const FRAME_HEADER_BYTES = 28;
const FULL_DIGEST_BYTES = 32;
const FULL_CONFIG = {
  fixtureSeed: 5699n,
  keyCount: 16384n,
  opaquePayloadBytes: 480,
  baseVersion: 1n,
  deltaCycles: 4n,
  updateFraction: 0.125,
  deleteFraction: 0.01,
};

function sha256(...parts) {
  const hash = createHash("sha256");
  for (const part of parts) hash.update(part);
  return hash.digest();
}

function hex(bytes) {
  return bytes.toString("hex");
}

function u64(value) {
  const bytes = Buffer.alloc(8);
  bytes.writeBigUInt64BE(value & MASK);
  return bytes;
}

function i64(value) {
  const bytes = Buffer.alloc(8);
  bytes.writeBigInt64BE(value);
  return bytes;
}

function u32(value) {
  const bytes = Buffer.alloc(4);
  bytes.writeUInt32BE(value);
  return bytes;
}

function u16(value) {
  const bytes = Buffer.alloc(2);
  bytes.writeUInt16BE(value);
  return bytes;
}

function rotateLeft(value, bits) {
  const count = BigInt(bits);
  return ((value << count) | (value >> (64n - count))) & MASK;
}

function splitmix64(input) {
  let value = (input + 0x9e3779b97f4a7c15n) & MASK;
  value = ((value ^ (value >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK;
  value = ((value ^ (value >> 27n)) * 0x94d049bb133111ebn) & MASK;
  return (value ^ (value >> 31n)) & MASK;
}

function mix(seed, key, version, salt) {
  return splitmix64(seed ^ rotateLeft(key, 17) ^ rotateLeft(version, 37) ^ salt);
}

function unitInterval(value) {
  return Number(value) / Number(U64_MAX);
}

function valueFields(seed, key, version, opaquePayloadBytes) {
  const tenant = Number(key % 32n);
  const category = Number(key % 64n);
  const flags = Number((key ^ version) & 0xffffn);
  const baseQuantity = key % 10000n;
  const quantity = (mix(seed, key, version, 0x51n) & 1n) === 0n
    ? baseQuantity + version
    : baseQuantity - version;
  const checksum = mix(seed, key, version, 0xc5n);
  const chunks = [u32(tenant), u16(category), u16(flags), i64(quantity), u64(version), u64(checksum)];
  let state = checksum;
  let remaining = opaquePayloadBytes;
  while (remaining > 0) {
    state = splitmix64(state);
    const bytes = u64(state);
    const take = Math.min(8, remaining);
    chunks.push(bytes.subarray(0, take));
    remaining -= take;
  }
  return {
    tenant,
    category,
    flags,
    quantity,
    updatedVersion: version,
    checksum,
    value: Buffer.concat(chunks),
  };
}

function buildFullHistory() {
  const recordsByKey = [];
  const live = [];
  for (let key = 0n; key < FULL_CONFIG.keyCount; key += 1n) {
    const fields = valueFields(
      FULL_CONFIG.fixtureSeed,
      key,
      FULL_CONFIG.baseVersion,
      FULL_CONFIG.opaquePayloadBytes,
    );
    recordsByKey.push([{ key, version: FULL_CONFIG.baseVersion, fields }]);
    live.push(true);
  }
  for (let cycle = 1n; cycle <= FULL_CONFIG.deltaCycles; cycle += 1n) {
    const version = FULL_CONFIG.baseVersion + cycle;
    for (let key = 0n; key < FULL_CONFIG.keyCount; key += 1n) {
      const index = Number(key);
      if (!live[index]) continue;
      if (unitInterval(mix(FULL_CONFIG.fixtureSeed, key, version, 0xd3n)) < FULL_CONFIG.deleteFraction) {
        recordsByKey[index].push({ key, version, fields: null });
        live[index] = false;
        continue;
      }
      if (unitInterval(mix(FULL_CONFIG.fixtureSeed, key, version, 0xa7n)) < FULL_CONFIG.updateFraction) {
        recordsByKey[index].push({
          key,
          version,
          fields: valueFields(
            FULL_CONFIG.fixtureSeed,
            key,
            version,
            FULL_CONFIG.opaquePayloadBytes,
          ),
        });
      }
    }
  }
  for (const versions of recordsByKey) {
    versions.sort((left, right) => Number(right.version - left.version));
  }
  return recordsByKey;
}

function buildCompatibilityHistory() {
  const recordsByKey = [];
  for (let key = 0n; key < 32n; key += 1n) {
    recordsByKey.push([{
      key,
      version: 1n,
      fields: valueFields(47n, key, 1n, 4),
    }]);
  }
  recordsByKey[0].push({
    key: 0n,
    version: 3n,
    fields: valueFields(47n, 0n, 3n, 4),
  });
  recordsByKey[1].push({ key: 1n, version: 2n, fields: null });
  for (const versions of recordsByKey) {
    versions.sort((left, right) => Number(right.version - left.version));
  }
  return recordsByKey;
}

function groupKeyChains(recordsByKey, targetRows, maxVersionsPerKey) {
  const groups = [];
  let current = [];
  for (const versions of recordsByKey) {
    if (versions.length === 0 || versions.length > maxVersionsPerKey) {
      throw new Error("version chain exceeds the physical format bound");
    }
    if (current.length > 0 && current.length + versions.length > targetRows) {
      groups.push(current);
      current = [];
    }
    current.push(...versions);
  }
  if (current.length > 0) groups.push(current);
  if (groups.length === 0) throw new Error("format requires at least one group");
  return groups;
}

function proofDepth(leafCount) {
  let depth = 0;
  let width = leafCount;
  while (width > 1) {
    width = Math.ceil(width / 2);
    depth += 1;
  }
  return depth;
}

function frameBody(magic, ordinal, groupCount, recordCount, content, proofCount) {
  return Buffer.concat([
    magic,
    u16(FORMAT_VERSION),
    u16(0),
    u32(ordinal),
    u32(groupCount),
    u32(recordCount),
    u32(content.length),
    u16(proofCount),
    u16(0),
    content,
  ]);
}

function merkleLevels(leaves) {
  const levels = [leaves];
  while (levels.at(-1).length > 1) {
    const current = levels.at(-1);
    const next = [];
    for (let index = 0; index < current.length; index += 2) {
      const left = current[index];
      const right = current[index + 1] ?? left;
      next.push(sha256(NODE_DOMAIN, left, right));
    }
    levels.push(next);
  }
  return levels;
}

function merkleProof(levels, ordinal) {
  const proof = [];
  let index = ordinal;
  for (let level = 0; level < levels.length - 1; level += 1) {
    const nodes = levels[level];
    const siblingIndex = index % 2 === 0 ? index + 1 : index - 1;
    proof.push(nodes[siblingIndex] ?? nodes[index]);
    index = Math.floor(index / 2);
  }
  return proof;
}

function encodeProjectionRecord(record, payloadOffset, payloadLength) {
  const fields = record.fields ?? {
    tenant: 0,
    category: 0,
    flags: 0,
    quantity: 0n,
    updatedVersion: 0n,
    checksum: 0n,
  };
  const encoded = Buffer.concat([
    u64(record.key),
    u64(record.version),
    Buffer.from([record.fields === null ? 0 : 1]),
    u32(fields.tenant),
    u16(fields.category),
    u16(fields.flags),
    i64(fields.quantity),
    u64(fields.updatedVersion),
    u64(fields.checksum),
    u32(payloadOffset),
    u32(payloadLength),
  ]);
  if (encoded.length !== PROJECTION_RECORD_BYTES) {
    throw new Error(`projection record length ${encoded.length}`);
  }
  return encoded;
}

function groupContents(group) {
  const payloadParts = [];
  const projectionParts = [];
  let payloadOffset = 0;
  for (const record of group) {
    const payload = record.fields === null ? Buffer.alloc(0) : record.fields.value.subarray(32);
    const recordPayloadOffset = record.fields === null ? 0 : payloadOffset;
    projectionParts.push(encodeProjectionRecord(record, recordPayloadOffset, payload.length));
    payloadParts.push(payload);
    payloadOffset += payload.length;
  }
  return {
    projection: Buffer.concat(projectionParts),
    payload: Buffer.concat(payloadParts),
  };
}

function encodeFramedObject(groups, kind) {
  const magic = kind === "projection" ? PROJECTION_MAGIC : PAYLOAD_MAGIC;
  const leafDomain = kind === "projection" ? PROJECTION_LEAF_DOMAIN : PAYLOAD_LEAF_DOMAIN;
  const depth = proofDepth(groups.length);
  const contents = groups.map((group) => groupContents(group)[kind]);
  const bodies = contents.map((content, ordinal) => frameBody(
    magic,
    ordinal,
    groups.length,
    groups[ordinal].length,
    content,
    depth,
  ));
  const leaves = bodies.map((body) => sha256(leafDomain, body));
  const levels = merkleLevels(leaves);
  const frames = bodies.map((body, ordinal) => Buffer.concat([
    body,
    ...merkleProof(levels, ordinal),
  ]));
  return {
    frames,
    root: levels.at(-1)[0],
    object: Buffer.concat(frames),
  };
}

function encodeIndex(groups, projection, payload) {
  const entries = [];
  let projectionOffset = 0n;
  let payloadOffset = 0n;
  for (let ordinal = 0; ordinal < groups.length; ordinal += 1) {
    entries.push(Buffer.concat([
      u64(groups[ordinal][0].key),
      u64(projectionOffset),
      u64(payloadOffset),
    ]));
    projectionOffset += BigInt(projection.frames[ordinal].length);
    payloadOffset += BigInt(payload.frames[ordinal].length);
  }
  const body = Buffer.concat([
    INDEX_MAGIC,
    u16(FORMAT_VERSION),
    u16(0),
    u64(GENERATION),
    u32(GROUP_TARGET_ROWS),
    u32(MAX_VERSIONS_PER_KEY),
    u32(groups.length),
    u64(BigInt(projection.object.length)),
    u64(BigInt(payload.object.length)),
    projection.root,
    payload.root,
    ...entries,
  ]);
  return Buffer.concat([body, sha256(body)]);
}

function encodeManifest(media, coveredThroughVersion, opaquePayloadBytes) {
  const manifest = {
    format_version: FORMAT_VERSION,
    generation: Number(GENERATION),
    covered_through: coveredThroughVersion,
    layout: "c5_columnar_main_aligned",
    projection_key: "layout/columnar-v2/projection.okp2",
    projection_bytes: media.projection.object.length,
    projection_sha256: hex(sha256(media.projection.object)),
    projection_merkle_root: hex(media.projection.root),
    payload_key: "layout/columnar-v2/payload.okv2",
    payload_bytes: media.payload.object.length,
    payload_sha256: hex(sha256(media.payload.object)),
    payload_merkle_root: hex(media.payload.root),
    index_key: "layout/columnar-v2/index.oki2",
    index_bytes: media.index.length,
    index_sha256: hex(sha256(media.index)),
    group_target_rows: GROUP_TARGET_ROWS,
    max_versions_per_key: MAX_VERSIONS_PER_KEY,
    opaque_payload_bytes: opaquePayloadBytes,
    max_frame_pair_bytes: MAX_FRAME_PAIR_BYTES,
    capabilities: [
      "indexed_mvcc_point",
      "concurrent_aligned_gather",
      "projection_only_scan",
      "merkle_range_proof",
      "disposable_range_engine_cache",
    ],
  };
  const payload = Buffer.from(JSON.stringify(manifest));
  const body = Buffer.concat([MANIFEST_MAGIC, payload]);
  return { manifest, encoded: Buffer.concat([body, sha256(body)]) };
}

function encodeLayout(recordsByKey, coveredThroughVersion, opaquePayloadBytes) {
  const groups = groupKeyChains(recordsByKey, GROUP_TARGET_ROWS, MAX_VERSIONS_PER_KEY);
  const projection = encodeFramedObject(groups, "projection");
  const payload = encodeFramedObject(groups, "payload");
  const index = encodeIndex(groups, projection, payload);
  const media = { projection, payload, index };
  const manifest = encodeManifest(media, coveredThroughVersion, opaquePayloadBytes);
  const pairBytes = projection.frames.map((frame, ordinal) => frame.length + payload.frames[ordinal].length);
  if (Math.max(...pairBytes) > MAX_FRAME_PAIR_BYTES) {
    throw new Error("frame pair exceeds the absolute point byte ceiling");
  }
  return { groups, projection, payload, index, manifest, pairBytes };
}

function visible(recordsByKey, key, readVersion) {
  const versions = recordsByKey[Number(key)];
  if (versions === undefined) return null;
  return versions.find((record) => record.version <= readVersion) ?? null;
}

function expectedOutcome(recordsByKey, key, readVersion) {
  const record = visible(recordsByKey, key, readVersion);
  if (record === null) return { kind: "absent" };
  if (record.fields === null) return { kind: "tombstone" };
  return { kind: "value", value_hex: hex(record.fields.value) };
}

function mediaSummary(layout) {
  const recordCounts = layout.groups.map((group) => group.length);
  return {
    group_count: layout.groups.length,
    record_count: recordCounts.reduce((total, count) => total + count, 0),
    minimum_group_records: Math.min(...recordCounts),
    maximum_group_records: Math.max(...recordCounts),
    proof_depth: proofDepth(layout.groups.length),
    index_entry_bytes: INDEX_ENTRY_BYTES,
    projection_record_bytes: PROJECTION_RECORD_BYTES,
    frame_header_bytes: FRAME_HEADER_BYTES,
    index_bytes: layout.index.length,
    index_sha256: hex(sha256(layout.index)),
    projection_bytes: layout.projection.object.length,
    projection_sha256: hex(sha256(layout.projection.object)),
    projection_merkle_root: hex(layout.projection.root),
    payload_bytes: layout.payload.object.length,
    payload_sha256: hex(sha256(layout.payload.object)),
    payload_merkle_root: hex(layout.payload.root),
    manifest_bytes: layout.manifest.encoded.length,
    manifest_sha256: hex(sha256(layout.manifest.encoded)),
    total_media_bytes: layout.index.length
      + layout.projection.object.length
      + layout.payload.object.length
      + layout.manifest.encoded.length,
    maximum_frame_pair_bytes: Math.max(...layout.pairBytes),
    c0_maximum_point_bytes: C0_MAX_POINT_BYTES,
    frame_pair_byte_ratio_millionths: Math.ceil(
      Math.max(...layout.pairBytes) * 1_000_000 / C0_MAX_POINT_BYTES,
    ),
    absolute_frame_pair_ceiling_bytes: MAX_FRAME_PAIR_BYTES,
  };
}

function planArtifact() {
  const recordsByKey = buildFullHistory();
  const layout = encodeLayout(recordsByKey, 5, FULL_CONFIG.opaquePayloadBytes);
  return {
    schema_version: 1,
    generator: "evals/oracles/t28-aligned-columnar-v2.mjs",
    format_id: "okv.columnar-overlay.v2",
    format_version: FORMAT_VERSION,
    logical_oracle_sha256: "b09eeeb482509b24ccb5e7f0c4a4d905983a612b0dbac2253519d9d82a98df86",
    canonical_history_sha256: "d4be64434f6b69990a2787876f514c6036727b41dcf1c5e120f91b6ce968ecd4",
    reused_c0: {
      fixture_id: "5d933648e3190b3bd6768c36c1d9022596c69c621c2347fa648a0754dc5431b0",
      typed_root_generation: "1788079307536563",
      maximum_point_bytes: C0_MAX_POINT_BYTES,
      resident_metadata_bytes: 19229,
      total_media_bytes: 13125073,
    },
    grouping: {
      algorithm: "ordered-key-chain-greedy-v1",
      target_records: GROUP_TARGET_ROWS,
      maximum_versions_per_key: MAX_VERSIONS_PER_KEY,
      key_chain_split_allowed: false,
    },
    merkle: {
      hash: "sha256",
      projection_leaf_domain_hex: hex(PROJECTION_LEAF_DOMAIN),
      payload_leaf_domain_hex: hex(PAYLOAD_LEAF_DOMAIN),
      node_domain_hex: hex(NODE_DOMAIN),
      proof_order: "leaf-to-root",
      direction: "derived-from-zero-based-group-ordinal",
      odd_leaf: "duplicate-self",
      root_for_one_leaf: "leaf-hash",
    },
    wire: {
      byte_order: "big-endian",
      index_magic_hex: hex(INDEX_MAGIC),
      projection_magic_hex: hex(PROJECTION_MAGIC),
      payload_magic_hex: hex(PAYLOAD_MAGIC),
      manifest_magic_hex: hex(MANIFEST_MAGIC),
      index_entry_bytes: INDEX_ENTRY_BYTES,
      projection_record_bytes: PROJECTION_RECORD_BYTES,
      frame_header_bytes: FRAME_HEADER_BYTES,
      full_digest_bytes: FULL_DIGEST_BYTES,
    },
    expected_media: mediaSummary(layout),
  };
}

function positiveFixture() {
  const recordsByKey = buildCompatibilityHistory();
  const layout = encodeLayout(recordsByKey, 3, 4);
  const fixture = {
    schema_version: 1,
    format_id: "okv.columnar-overlay.v2",
    generator: "evals/oracles/t28-aligned-columnar-v2.mjs",
    media: {
      index_hex: hex(layout.index),
      projection_hex: hex(layout.projection.object),
      payload_hex: hex(layout.payload.object),
      manifest_hex: hex(layout.manifest.encoded),
    },
    expected: {
      summary: mediaSummary(layout),
      points: [
        { key: 0, read_version: 2, outcome: expectedOutcome(recordsByKey, 0n, 2n) },
        { key: 0, read_version: 3, outcome: expectedOutcome(recordsByKey, 0n, 3n) },
        { key: 1, read_version: 2, outcome: expectedOutcome(recordsByKey, 1n, 2n) },
        { key: 31, read_version: 1, outcome: expectedOutcome(recordsByKey, 31n, 1n) },
        { key: 32, read_version: 3, outcome: expectedOutcome(recordsByKey, 32n, 3n) },
      ],
    },
  };
  return fixture;
}

function corruptFixture() {
  const positive = positiveFixture();
  const projection = Buffer.from(positive.media.projection_hex, "hex");
  const mutationOffset = FRAME_HEADER_BYTES + 7;
  projection[mutationOffset] ^= 0x01;
  return {
    schema_version: 1,
    format_id: "okv.columnar-overlay.v2",
    positive_fixture_sha256: hex(sha256(Buffer.from(`${JSON.stringify(positive, null, 2)}\n`))),
    mutation: {
      object: "projection",
      byte_offset: mutationOffset,
      xor: 1,
    },
    mutated_projection_hex: hex(projection),
    expected_error: "columnar_v2_projection_merkle_proof_mismatch",
  };
}

const artifact = process.argv[2] ?? "plan";
let output;
if (artifact === "plan") output = planArtifact();
else if (artifact === "positive") output = positiveFixture();
else if (artifact === "corrupt") output = corruptFixture();
else throw new Error(`unknown artifact ${artifact}`);

process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
