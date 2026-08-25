#include <openssl/evp.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/resource.h>
#include <thread>
#include <utility>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

namespace {

constexpr std::string_view kTraceMagic = "OKVTRC01";
constexpr std::size_t kTraceHeaderBytes = 40;

struct Args {
  std::filesystem::path db;
  std::filesystem::path trace;
  std::size_t block_bytes = 0;
  std::size_t value_bytes = 0;
  std::size_t cache_bytes = 0;
  bool direct = true;
  std::vector<std::size_t> concurrencies;
};

struct Trace {
  std::uint64_t seed = 0;
  std::size_t key_count = 0;
  std::vector<std::uint32_t> warmup;
  std::vector<std::uint32_t> measured;
  std::string sha256;
};

struct Usage {
  double cpu_seconds = 0.0;
  std::uint64_t peak_rss_bytes = 0;
  std::uint64_t minor_faults = 0;
  std::uint64_t major_faults = 0;
  std::uint64_t voluntary_context_switches = 0;
  std::uint64_t involuntary_context_switches = 0;
};

struct PointCurve {
  std::size_t concurrency = 0;
  std::size_t samples = 0;
  double duration_seconds = 0.0;
  double iops = 0.0;
  double p50_seconds = 0.0;
  double p95_seconds = 0.0;
  double p99_seconds = 0.0;
  double p999_seconds = 0.0;
  std::uint64_t physical_bytes = 0;
  double physical_bytes_per_second = 0.0;
  double logical_bytes_per_second = 0.0;
  double cache_hit_ratio = 0.0;
  double cpu_seconds_per_million_points = 0.0;
  std::uint64_t minor_faults = 0;
  std::uint64_t major_faults = 0;
  std::uint64_t voluntary_context_switches = 0;
  std::uint64_t involuntary_context_switches = 0;
  bool exact = false;
};

struct ScanCurve {
  std::size_t rows = 0;
  std::uint64_t logical_bytes = 0;
  std::uint64_t physical_bytes = 0;
  double duration_seconds = 0.0;
  double logical_bytes_per_second = 0.0;
  double rows_per_second = 0.0;
  std::string digest_sha256;
  bool exact = false;
};

class Sha256 {
 public:
  Sha256() : context_(EVP_MD_CTX_new()) {
    if (context_ == nullptr || EVP_DigestInit_ex(context_, EVP_sha256(), nullptr) != 1) {
      throw std::runtime_error("initialize SHA256");
    }
  }

  Sha256(const Sha256&) = delete;
  Sha256& operator=(const Sha256&) = delete;
  ~Sha256() { EVP_MD_CTX_free(context_); }

  void Update(const void* data, std::size_t bytes) {
    if (EVP_DigestUpdate(context_, data, bytes) != 1) {
      throw std::runtime_error("update SHA256");
    }
  }

  void Update(std::string_view value) { Update(value.data(), value.size()); }

  std::string Finish() {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int bytes = 0;
    if (EVP_DigestFinal_ex(context_, digest, &bytes) != 1 || bytes != 32) {
      throw std::runtime_error("finish SHA256");
    }
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (unsigned int index = 0; index < bytes; ++index) {
      output << std::setw(2) << static_cast<unsigned int>(digest[index]);
    }
    return output.str();
  }

 private:
  EVP_MD_CTX* context_;
};

std::uint64_t ParseU64(std::string_view value) {
  std::size_t consumed = 0;
  const auto parsed = std::stoull(std::string(value), &consumed);
  if (consumed != value.size()) {
    throw std::runtime_error("invalid unsigned integer argument");
  }
  return parsed;
}

std::vector<std::size_t> ParseConcurrencies(std::string_view value) {
  std::vector<std::size_t> output;
  std::size_t start = 0;
  while (start <= value.size()) {
    const auto end = value.find(',', start);
    const auto part = value.substr(start, end == std::string_view::npos ? value.size() - start
                                                                       : end - start);
    output.push_back(static_cast<std::size_t>(ParseU64(part)));
    if (end == std::string_view::npos) break;
    start = end + 1;
  }
  return output;
}

Args ParseArgs(int argc, char** argv) {
  Args args;
  for (int index = 1; index < argc; ++index) {
    const std::string_view argument(argv[index]);
    const auto separator = argument.find('=');
    if (separator == std::string_view::npos) {
      throw std::runtime_error("arguments must use --name=value");
    }
    const auto name = argument.substr(0, separator);
    const auto value = argument.substr(separator + 1);
    if (name == "--db") args.db = std::string(value);
    else if (name == "--trace") args.trace = std::string(value);
    else if (name == "--block-bytes") args.block_bytes = ParseU64(value);
    else if (name == "--value-bytes") args.value_bytes = ParseU64(value);
    else if (name == "--cache-bytes") args.cache_bytes = ParseU64(value);
    else if (name == "--concurrencies") args.concurrencies = ParseConcurrencies(value);
    else if (name == "--direct") args.direct = value == "true";
    else throw std::runtime_error("unknown argument: " + std::string(name));
  }
  if (args.db.empty() || args.trace.empty() || args.block_bytes == 0 || args.value_bytes == 0 ||
      args.cache_bytes == 0 || args.concurrencies.empty() ||
      std::any_of(args.concurrencies.begin(), args.concurrencies.end(),
                  [](std::size_t value) { return value == 0; })) {
    throw std::runtime_error("required benchmark argument is absent or zero");
  }
  return args;
}

std::uint64_t ReadBe64(const std::vector<unsigned char>& bytes, std::size_t offset) {
  if (offset + 8 > bytes.size()) throw std::runtime_error("trace integer is truncated");
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value = (value << 8) | bytes[offset + index];
  }
  return value;
}

std::uint32_t ReadBe32(const unsigned char* bytes) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) value = (value << 8) | bytes[index];
  return value;
}

void UpdateBe64(Sha256& digest, std::uint64_t value) {
  unsigned char bytes[8];
  for (int index = 7; index >= 0; --index) {
    bytes[index] = static_cast<unsigned char>(value & 0xff);
    value >>= 8;
  }
  digest.Update(bytes, sizeof(bytes));
}

std::string Sha256Bytes(const std::vector<unsigned char>& bytes) {
  Sha256 digest;
  digest.Update(bytes.data(), bytes.size());
  return digest.Finish();
}

Trace LoadTrace(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("open trace");
  const std::vector<unsigned char> bytes{std::istreambuf_iterator<char>(input),
                                         std::istreambuf_iterator<char>()};
  if (bytes.size() < kTraceHeaderBytes ||
      std::string_view(reinterpret_cast<const char*>(bytes.data()), 8) != kTraceMagic) {
    throw std::runtime_error("trace header is invalid");
  }
  Trace trace;
  trace.seed = ReadBe64(bytes, 8);
  trace.key_count = static_cast<std::size_t>(ReadBe64(bytes, 16));
  const auto warmup_count = static_cast<std::size_t>(ReadBe64(bytes, 24));
  const auto measured_count = static_cast<std::size_t>(ReadBe64(bytes, 32));
  if (trace.key_count == 0 ||
      bytes.size() != kTraceHeaderBytes + (warmup_count + measured_count) * sizeof(std::uint32_t)) {
    throw std::runtime_error("trace size is invalid");
  }
  trace.warmup.reserve(warmup_count);
  trace.measured.reserve(measured_count);
  for (std::size_t index = 0; index < warmup_count + measured_count; ++index) {
    const auto ordinal = ReadBe32(bytes.data() + kTraceHeaderBytes + index * 4);
    if (ordinal >= trace.key_count) throw std::runtime_error("trace ordinal is invalid");
    (index < warmup_count ? trace.warmup : trace.measured).push_back(ordinal);
  }
  trace.sha256 = Sha256Bytes(bytes);
  return trace;
}

std::string KeyFor(std::size_t ordinal) {
  std::ostringstream key;
  key << "k/" << std::hex << std::setfill('0') << std::setw(16) << ordinal;
  return key.str();
}

std::uint64_t InitialValueState(std::uint64_t seed, std::size_t ordinal) {
  return seed ^ (static_cast<std::uint64_t>(ordinal) * 0x9e3779b97f4a7c15ULL);
}

std::uint64_t NextValueWord(std::uint64_t& state) {
  state += 0x9e3779b97f4a7c15ULL;
  auto mixed = state;
  mixed = (mixed ^ (mixed >> 30)) * 0xbf58476d1ce4e5b9ULL;
  mixed = (mixed ^ (mixed >> 27)) * 0x94d049bb133111ebULL;
  return mixed ^ (mixed >> 31);
}

std::string ValueFor(std::uint64_t seed, std::size_t ordinal, std::size_t bytes) {
  std::string value;
  value.reserve(bytes);
  auto state = InitialValueState(seed, ordinal);
  while (value.size() < bytes) {
    const auto word = NextValueWord(state);
    for (int shift = 56; shift >= 0 && value.size() < bytes; shift -= 8) {
      value.push_back(static_cast<char>((word >> shift) & 0xff));
    }
  }
  return value;
}

bool ValueMatches(std::uint64_t seed, std::size_t ordinal, std::size_t bytes,
                  std::string_view value) {
  if (value.size() != bytes) return false;
  auto state = InitialValueState(seed, ordinal);
  std::size_t offset = 0;
  while (offset < value.size()) {
    const auto word = NextValueWord(state);
    for (int shift = 56; shift >= 0 && offset < value.size(); shift -= 8, ++offset) {
      if (static_cast<unsigned char>(value[offset]) != ((word >> shift) & 0xff)) return false;
    }
  }
  return true;
}

void UpdateFixtureDigest(Sha256& digest, std::string_view key, std::string_view value) {
  UpdateBe64(digest, key.size());
  digest.Update(key);
  UpdateBe64(digest, value.size());
  digest.Update(value);
}

void Check(const rocksdb::Status& status, std::string_view operation) {
  if (!status.ok()) throw std::runtime_error(std::string(operation) + ": " + status.ToString());
}

rocksdb::Options OptionsFor(const Args& args, bool create,
                            std::shared_ptr<rocksdb::Statistics> statistics) {
  rocksdb::BlockBasedTableOptions table;
  table.block_size = args.block_bytes;
  table.block_cache = rocksdb::NewLRUCache(args.cache_bytes);
  table.cache_index_and_filter_blocks = true;
  table.pin_l0_filter_and_index_blocks_in_cache = false;
  table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  rocksdb::Options options;
  options.create_if_missing = create;
  options.compression = rocksdb::kNoCompression;
  options.allow_mmap_reads = false;
  options.use_direct_reads = !create && args.direct;
  options.paranoid_checks = true;
  options.statistics = std::move(statistics);
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
  return options;
}

std::string Populate(const Args& args, const Trace& trace) {
  if (std::filesystem::exists(args.db)) throw std::runtime_error("database path already exists");
  std::filesystem::create_directories(args.db.parent_path());
  auto statistics = rocksdb::CreateDBStatistics();
  auto options = OptionsFor(args, true, statistics);
  std::unique_ptr<rocksdb::DB> db;
  Check(rocksdb::DB::Open(options, args.db.string(), &db), "open database for population");
  rocksdb::WriteOptions write_options;
  write_options.disableWAL = true;
  Sha256 fixture;
  rocksdb::WriteBatch batch;
  for (std::size_t ordinal = 0; ordinal < trace.key_count; ++ordinal) {
    const auto key = KeyFor(ordinal);
    const auto value = ValueFor(trace.seed, ordinal, args.value_bytes);
    UpdateFixtureDigest(fixture, key, value);
    Check(batch.Put(key, value), "append population batch");
    if ((ordinal + 1) % 256 == 0) {
      Check(db->Write(write_options, &batch), "write population batch");
      batch.Clear();
    }
  }
  if (batch.Count() > 0) Check(db->Write(write_options, &batch), "write final population batch");
  rocksdb::FlushOptions flush;
  flush.wait = true;
  Check(db->Flush(flush), "flush population");
  rocksdb::CompactRangeOptions compact;
  compact.exclusive_manual_compaction = true;
  Check(db->CompactRange(compact, nullptr, nullptr), "compact population");
  db.reset();
  return fixture.Finish();
}

struct OpenedDb {
  std::unique_ptr<rocksdb::DB> db;
  std::shared_ptr<rocksdb::Statistics> statistics;
};

OpenedDb OpenReadOnly(const Args& args) {
  auto statistics = rocksdb::CreateDBStatistics();
  auto options = OptionsFor(args, false, statistics);
  std::unique_ptr<rocksdb::DB> db;
  Check(rocksdb::DB::OpenForReadOnly(options, args.db.string(), &db), "open read-only database");
  return {std::move(db), statistics};
}

Usage CurrentUsage() {
  struct rusage usage {};
  if (getrusage(RUSAGE_SELF, &usage) != 0) throw std::runtime_error("getrusage");
  return {
      static_cast<double>(usage.ru_utime.tv_sec) + usage.ru_utime.tv_usec / 1e6 +
          static_cast<double>(usage.ru_stime.tv_sec) + usage.ru_stime.tv_usec / 1e6,
      static_cast<std::uint64_t>(usage.ru_maxrss) * 1024,
      static_cast<std::uint64_t>(usage.ru_minflt),
      static_cast<std::uint64_t>(usage.ru_majflt),
      static_cast<std::uint64_t>(usage.ru_nvcsw),
      static_cast<std::uint64_t>(usage.ru_nivcsw),
  };
}

std::uint64_t Ticker(const std::shared_ptr<rocksdb::Statistics>& statistics,
                     rocksdb::Tickers ticker) {
  return statistics->getTickerCount(ticker);
}

void RunWarmup(rocksdb::DB& db, const Trace& trace, const Args& args, std::size_t concurrency) {
  std::atomic<bool> exact(true);
  std::vector<std::thread> workers;
  workers.reserve(concurrency);
  for (std::size_t worker = 0; worker < concurrency; ++worker) {
    const auto start = trace.warmup.size() * worker / concurrency;
    const auto end = trace.warmup.size() * (worker + 1) / concurrency;
    workers.emplace_back([&, start, end] {
      rocksdb::ReadOptions read_options;
      read_options.verify_checksums = true;
      for (auto index = start; index < end; ++index) {
        const auto ordinal = trace.warmup[index];
        std::string value;
        const auto status = db.Get(read_options, KeyFor(ordinal), &value);
        if (!status.ok() || !ValueMatches(trace.seed, ordinal, args.value_bytes, value)) exact = false;
      }
    });
  }
  for (auto& worker : workers) worker.join();
  if (!exact) throw std::runtime_error("warmup oracle mismatch");
}

std::uint64_t Percentile(const std::vector<std::uint64_t>& sorted, std::size_t per_thousand) {
  if (sorted.empty()) return 0;
  const auto index = std::min(sorted.size() - 1,
                              (sorted.size() * per_thousand + 999) / 1000 - 1);
  return sorted[index];
}

PointCurve RunPointCurve(const Args& args, const Trace& trace, std::size_t concurrency) {
  auto opened = OpenReadOnly(args);
  RunWarmup(*opened.db, trace, args, concurrency);
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  std::size_t ready = 0;
  bool go = false;
  std::atomic<bool> exact(true);
  std::vector<std::vector<std::uint64_t>> worker_latencies(concurrency);
  std::vector<std::thread> workers;
  workers.reserve(concurrency);
  for (std::size_t worker = 0; worker < concurrency; ++worker) {
    const auto start = trace.measured.size() * worker / concurrency;
    const auto end = trace.measured.size() * (worker + 1) / concurrency;
    worker_latencies[worker].reserve(end - start);
    workers.emplace_back([&, worker, start, end] {
      {
        std::unique_lock lock(gate_mutex);
        ++ready;
        gate_cv.notify_all();
        gate_cv.wait(lock, [&] { return go; });
      }
      rocksdb::ReadOptions read_options;
      read_options.verify_checksums = true;
      for (auto index = start; index < end; ++index) {
        const auto ordinal = trace.measured[index];
        std::string value;
        const auto started = std::chrono::steady_clock::now();
        const auto status = opened.db->Get(read_options, KeyFor(ordinal), &value);
        const auto elapsed = std::chrono::steady_clock::now() - started;
        worker_latencies[worker].push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
        if (!status.ok() || !ValueMatches(trace.seed, ordinal, args.value_bytes, value)) exact = false;
      }
    });
  }
  {
    std::unique_lock lock(gate_mutex);
    gate_cv.wait(lock, [&] { return ready == concurrency; });
  }
  const auto bytes_before = Ticker(opened.statistics, rocksdb::BYTES_READ);
  const auto hits_before = Ticker(opened.statistics, rocksdb::BLOCK_CACHE_DATA_HIT);
  const auto misses_before = Ticker(opened.statistics, rocksdb::BLOCK_CACHE_DATA_MISS);
  const auto usage_before = CurrentUsage();
  const auto started = std::chrono::steady_clock::now();
  {
    std::lock_guard lock(gate_mutex);
    go = true;
  }
  gate_cv.notify_all();
  for (auto& worker : workers) worker.join();
  const auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  const auto usage_after = CurrentUsage();
  std::vector<std::uint64_t> latencies;
  latencies.reserve(trace.measured.size());
  for (auto& worker : worker_latencies) {
    latencies.insert(latencies.end(), worker.begin(), worker.end());
  }
  std::sort(latencies.begin(), latencies.end());
  const auto physical_bytes = Ticker(opened.statistics, rocksdb::BYTES_READ) - bytes_before;
  const auto hits = Ticker(opened.statistics, rocksdb::BLOCK_CACHE_DATA_HIT) - hits_before;
  const auto misses = Ticker(opened.statistics, rocksdb::BLOCK_CACHE_DATA_MISS) - misses_before;
  const auto cache_accesses = hits + misses;
  const auto samples = trace.measured.size();
  return {
      concurrency,
      samples,
      duration,
      samples / duration,
      Percentile(latencies, 500) / 1e9,
      Percentile(latencies, 950) / 1e9,
      Percentile(latencies, 990) / 1e9,
      Percentile(latencies, 999) / 1e9,
      physical_bytes,
      physical_bytes / duration,
      static_cast<double>(samples * args.value_bytes) / duration,
      cache_accesses == 0 ? 0.0 : static_cast<double>(hits) / cache_accesses,
      (usage_after.cpu_seconds - usage_before.cpu_seconds) /
          (static_cast<double>(samples) / 1e6),
      usage_after.minor_faults - usage_before.minor_faults,
      usage_after.major_faults - usage_before.major_faults,
      usage_after.voluntary_context_switches - usage_before.voluntary_context_switches,
      usage_after.involuntary_context_switches - usage_before.involuntary_context_switches,
      exact,
  };
}

ScanCurve RunScan(const Args& args, const Trace& trace) {
  auto opened = OpenReadOnly(args);
  rocksdb::ReadOptions read_options;
  read_options.verify_checksums = true;
  const auto bytes_before = Ticker(opened.statistics, rocksdb::BYTES_READ);
  Sha256 digest;
  std::size_t ordinal = 0;
  bool exact = true;
  const auto started = std::chrono::steady_clock::now();
  std::unique_ptr<rocksdb::Iterator> iterator(opened.db->NewIterator(read_options));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next(), ++ordinal) {
    const auto key = iterator->key().ToStringView();
    const auto value = iterator->value().ToStringView();
    exact = exact && key == KeyFor(ordinal) &&
            ValueMatches(trace.seed, ordinal, args.value_bytes, value);
    UpdateFixtureDigest(digest, key, value);
  }
  Check(iterator->status(), "ordered scan");
  const auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  exact = exact && ordinal == trace.key_count;
  const auto logical_bytes = static_cast<std::uint64_t>(trace.key_count) * args.value_bytes;
  const auto physical_bytes = Ticker(opened.statistics, rocksdb::BYTES_READ) - bytes_before;
  return {ordinal,
          logical_bytes,
          physical_bytes,
          duration,
          logical_bytes / duration,
          ordinal / duration,
          digest.Finish(),
          exact};
}

std::uint64_t DirectoryBytes(const std::filesystem::path& root) {
  std::uint64_t bytes = 0;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
    if (entry.is_regular_file()) bytes += entry.file_size();
  }
  return bytes;
}

std::uint64_t Property(rocksdb::DB& db, std::string_view name) {
  std::uint64_t value = 0;
  db.GetIntProperty(std::string(name), &value);
  return value;
}

void PrintPoint(const PointCurve& curve, bool comma) {
  std::cout << "{";
  std::cout << "\"concurrency\":" << curve.concurrency;
  std::cout << ",\"samples\":" << curve.samples;
  std::cout << ",\"duration_seconds\":" << curve.duration_seconds;
  std::cout << ",\"iops\":" << curve.iops;
  std::cout << ",\"latency_p50_seconds\":" << curve.p50_seconds;
  std::cout << ",\"latency_p95_seconds\":" << curve.p95_seconds;
  std::cout << ",\"latency_p99_seconds\":" << curve.p99_seconds;
  std::cout << ",\"latency_p999_seconds\":" << curve.p999_seconds;
  std::cout << ",\"physical_bytes\":" << curve.physical_bytes;
  std::cout << ",\"physical_bytes_per_second\":" << curve.physical_bytes_per_second;
  std::cout << ",\"logical_bytes_per_second\":" << curve.logical_bytes_per_second;
  std::cout << ",\"cache_hit_ratio\":" << curve.cache_hit_ratio;
  std::cout << ",\"cpu_seconds_per_million_points\":" << curve.cpu_seconds_per_million_points;
  std::cout << ",\"minor_faults\":" << curve.minor_faults;
  std::cout << ",\"major_faults\":" << curve.major_faults;
  std::cout << ",\"voluntary_context_switches\":" << curve.voluntary_context_switches;
  std::cout << ",\"involuntary_context_switches\":" << curve.involuntary_context_switches;
  std::cout << ",\"exact\":" << (curve.exact ? "true" : "false") << "}";
  if (comma) std::cout << ",";
}

void PrintReceipt(const Args& args, const Trace& trace, const std::string& fixture_sha256,
                  const std::vector<PointCurve>& points, const ScanCurve& scan) {
  auto opened = OpenReadOnly(args);
  const auto database_bytes = DirectoryBytes(args.db);
  const auto usage = CurrentUsage();
  std::cout << std::setprecision(12);
  std::cout << "{\"schema_version\":1,\"engine\":\"rocksdb-v11.1.2\"";
  std::cout << ",\"io_mode\":\"" << (args.direct ? "direct" : "buffered") << "\"";
  std::cout << ",\"seed\":" << trace.seed << ",\"key_count\":" << trace.key_count;
  std::cout << ",\"value_bytes\":" << args.value_bytes;
  std::cout << ",\"logical_value_bytes\":" << trace.key_count * args.value_bytes;
  std::cout << ",\"trace_sha256\":\"" << trace.sha256 << "\"";
  std::cout << ",\"fixture_sha256\":\"" << fixture_sha256 << "\"";
  std::cout << ",\"block_payload_bytes\":" << args.block_bytes;
  std::cout << ",\"database_bytes\":" << database_bytes;
  std::cout << ",\"database_amplification\":"
            << static_cast<double>(database_bytes) / (trace.key_count * args.value_bytes);
  std::cout << ",\"block_cache_bytes\":" << args.cache_bytes;
  std::cout << ",\"block_cache_usage_bytes\":" << Property(*opened.db, "rocksdb.block-cache-usage");
  std::cout << ",\"table_reader_memory_bytes\":"
            << Property(*opened.db, "rocksdb.estimate-table-readers-mem");
  std::cout << ",\"open_files\":" << Ticker(opened.statistics, rocksdb::NO_FILE_OPENS);
  std::cout << ",\"peak_worker_rss_bytes\":" << usage.peak_rss_bytes;
  std::cout << ",\"point_curves\":[";
  for (std::size_t index = 0; index < points.size(); ++index) {
    PrintPoint(points[index], index + 1 < points.size());
  }
  std::cout << "],\"scan\":{";
  std::cout << "\"rows\":" << scan.rows << ",\"logical_bytes\":" << scan.logical_bytes;
  std::cout << ",\"physical_bytes\":" << scan.physical_bytes;
  std::cout << ",\"duration_seconds\":" << scan.duration_seconds;
  std::cout << ",\"logical_bytes_per_second\":" << scan.logical_bytes_per_second;
  std::cout << ",\"rows_per_second\":" << scan.rows_per_second;
  std::cout << ",\"digest_sha256\":\"" << scan.digest_sha256 << "\"";
  std::cout << ",\"exact\":" << (scan.exact ? "true" : "false") << "}}\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const auto args = ParseArgs(argc, argv);
    const auto trace = LoadTrace(args.trace);
    const auto fixture_sha256 = Populate(args, trace);
    std::vector<PointCurve> points;
    points.reserve(args.concurrencies.size());
    for (const auto concurrency : args.concurrencies) {
      points.push_back(RunPointCurve(args, trace, concurrency));
    }
    const auto scan = RunScan(args, trace);
    PrintReceipt(args, trace, fixture_sha256, points, scan);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "rocksdb_probe: " << error.what() << "\n";
    return 1;
  }
}
