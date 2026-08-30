#!/usr/bin/env node

// Independent reference oracle for RFC-0048. This file imports no objectKV
// package and emits the frozen logical fixture and workload digests to stdout.

import { createHash } from "node:crypto";

const MASK = (1n << 64n) - 1n;
const U64_MAX = MASK;
const config = {
  fixture_seed: 5699n,
  trace_seeds: [5701n, 5702n, 5703n],
  key_count: 16384n,
  canonical_live_row_bytes: 512,
  opaque_payload_bytes: 480,
  base_version: 1n,
  delta_cycles: 4n,
  update_fraction: 0.125,
  delete_fraction: 0.01,
  point_operations: 1024,
};

const pointOrders = new Map([
  [5701, ["ABBA", "BAAB", "ABBA", "BAAB", "ABBA"]],
  [5702, ["BAAB", "ABBA", "BAAB", "ABBA", "BAAB"]],
  [5703, ["ABBA", "BAAB", "ABBA", "BAAB", "ABBA"]],
]);
const scanOrders = new Map([
  [5701, ["AB", "BA", "AB", "BA", "AB"]],
  [5702, ["BA", "AB", "BA", "AB", "BA"]],
  [5703, ["AB", "BA", "AB", "BA", "AB"]],
]);
const scanQuery = "SELECT key, tenant, category, quantity, COUNT(*) OVER () AS row_count, SUM(quantity) OVER () AS quantity_sum FROM okv_layout ORDER BY key";

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
  const n = BigInt(bits);
  return ((value << n) | (value >> (64n - n))) & MASK;
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

function canonicalValue(seed, key, version) {
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
  let remaining = config.opaque_payload_bytes;
  while (remaining > 0) {
    state = splitmix64(state);
    const bytes = u64(state);
    chunks.push(bytes.subarray(0, Math.min(8, remaining)));
    remaining -= Math.min(8, remaining);
  }
  const value = Buffer.concat(chunks);
  if (value.length !== config.canonical_live_row_bytes) {
    throw new Error(`canonical value length ${value.length}`);
  }
  return { value, tenant, category, quantity };
}

function buildHistory() {
  const recordsByKey = [];
  const live = [];
  for (let key = 0n; key < config.key_count; key += 1n) {
    const fields = canonicalValue(config.fixture_seed, key, config.base_version);
    recordsByKey.push([{ key, version: config.base_version, value: fields.value, fields }]);
    live.push(true);
  }
  for (let cycle = 1n; cycle <= config.delta_cycles; cycle += 1n) {
    const version = config.base_version + cycle;
    for (let key = 0n; key < config.key_count; key += 1n) {
      const index = Number(key);
      if (!live[index]) continue;
      if (unitInterval(mix(config.fixture_seed, key, version, 0xd3n)) < config.delete_fraction) {
        recordsByKey[index].push({ key, version, value: null, fields: null });
        live[index] = false;
        continue;
      }
      if (unitInterval(mix(config.fixture_seed, key, version, 0xa7n)) < config.update_fraction) {
        const fields = canonicalValue(config.fixture_seed, key, version);
        recordsByKey[index].push({ key, version, value: fields.value, fields });
      }
    }
  }
  for (const versions of recordsByKey) {
    versions.sort((left, right) => Number(right.version - left.version));
  }
  return recordsByKey;
}

function historyDigest(recordsByKey) {
  const hash = createHash("sha256");
  let recordCount = 0;
  for (const versions of recordsByKey) {
    for (const record of versions) {
      recordCount += 1;
      hash.update(u64(8n));
      hash.update(u64(record.key));
      hash.update(u64(record.version));
      if (record.value === null) {
        hash.update(Buffer.from([0]));
      } else {
        hash.update(Buffer.from([1]));
        hash.update(u64(BigInt(record.value.length)));
        hash.update(record.value);
      }
    }
  }
  return { sha256: hash.digest("hex"), recordCount };
}

function finalProjection(recordsByKey) {
  const rows = [];
  const version = config.base_version + config.delta_cycles;
  for (const versions of recordsByKey) {
    const record = versions.find((item) => item.version <= version);
    if (record && record.value !== null) {
      rows.push({
        key: record.key,
        tenant: record.fields.tenant,
        category: record.fields.category,
        quantity: record.fields.quantity,
      });
    }
  }
  return rows;
}

function projectionDigest(rows) {
  const hash = createHash("sha256");
  hash.update("okv-t28-ordered-projection-v1\0");
  hash.update(u64(BigInt(rows.length)));
  for (const row of rows) {
    hash.update(u64(row.key));
    hash.update(u32(row.tenant));
    hash.update(u16(row.category));
    hash.update(i64(row.quantity));
  }
  return hash.digest("hex");
}

function operations(traceSeed) {
  const values = [];
  let state = traceSeed;
  for (let ordinal = 0; ordinal < config.point_operations; ordinal += 1) {
    state = splitmix64(state ^ BigInt(ordinal));
    const key = state % config.key_count;
    state = splitmix64(state);
    const readVersion = 1n + (state % 5n);
    values.push({ ordinal, key, readVersion });
  }
  return values;
}

function visible(recordsByKey, key, readVersion) {
  return recordsByKey[Number(key)].find((record) => record.version <= readVersion) ?? null;
}

function traceDigests(recordsByKey, traceSeed) {
  const operationHash = createHash("sha256");
  const outcomeHash = createHash("sha256");
  operationHash.update("okv-t28-point-operations-v1\0");
  outcomeHash.update("okv-t28-point-outcomes-v1\0");
  operationHash.update(u64(traceSeed));
  outcomeHash.update(u64(traceSeed));
  operationHash.update(u64(BigInt(config.point_operations)));
  outcomeHash.update(u64(BigInt(config.point_operations)));
  for (const operation of operations(traceSeed)) {
    const prefix = Buffer.concat([
      u64(BigInt(operation.ordinal)),
      u64(operation.key),
      u64(operation.readVersion),
    ]);
    operationHash.update(prefix);
    outcomeHash.update(prefix);
    const record = visible(recordsByKey, operation.key, operation.readVersion);
    if (record === null) {
      outcomeHash.update(Buffer.from([0]));
    } else if (record.value === null) {
      outcomeHash.update(Buffer.from([1]));
    } else {
      outcomeHash.update(Buffer.from([2]));
      outcomeHash.update(u64(BigInt(record.value.length)));
      outcomeHash.update(record.value);
    }
  }
  return {
    seed: Number(traceSeed),
    operation_sequence_sha256: operationHash.digest("hex"),
    expected_outcomes_sha256: outcomeHash.digest("hex"),
  };
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

const recordsByKey = buildHistory();
const history = historyDigest(recordsByKey);
const rows = finalProjection(recordsByKey);
const traces = config.trace_seeds.map((seed) => traceDigests(recordsByKey, seed));
const workloadPlan = {
  fixture_seed: Number(config.fixture_seed),
  point: {
    blocks_per_seed: 5,
    concurrent_tasks: 8,
    orders: [...pointOrders].map(([seed, orders]) => ({ seed, orders })),
    reads_per_position: config.point_operations,
    traces: traces.map(({ seed, operation_sequence_sha256 }) => ({ seed, operation_sequence_sha256 })),
  },
  read_version: 5,
  scan: {
    concurrent_range_fetches: 1,
    orders: [...scanOrders].map(([seed, orders]) => ({ seed, orders })),
    query: scanQuery,
    scans_per_position: 1,
  },
};
const workloadPlanSha256 = createHash("sha256").update(canonicalJson(workloadPlan)).digest("hex");
const schema = {
  id: "objectkv.t28.typed-row.v1",
  columns: [
    { name: "key", type: "u64", nullable: false },
    { name: "tenant", type: "u32", nullable: false },
    { name: "category", type: "u16", nullable: false },
    { name: "quantity", type: "i64", nullable: false },
    { name: "opaque_payload", type: "bytes[480]", nullable: false },
  ],
};
const quantitySum = rows.reduce((total, row) => total + row.quantity, 0n);
const artifact = {
  schema_version: 1,
  generator: "evals/oracles/t28-layout-geometry-v1.mjs",
  fixture: {
    seed: Number(config.fixture_seed),
    key_count: Number(config.key_count),
    record_count: history.recordCount,
    live_row_count: rows.length,
    covered_through_version: Number(config.base_version + config.delta_cycles),
    canonical_history_sha256: history.sha256,
    ordered_projection_sha256: projectionDigest(rows),
    aggregate: {
      row_count: rows.length,
      quantity_sum: quantitySum.toString(),
    },
  },
  schema,
  schema_sha256: createHash("sha256").update(canonicalJson(schema)).digest("hex"),
  traces,
  workload_plan_sha256: workloadPlanSha256,
};

process.stdout.write(`${JSON.stringify(artifact, null, 2)}\n`);
