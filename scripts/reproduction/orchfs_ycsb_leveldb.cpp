#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <unistd.h>

namespace {

struct Config {
  std::string db;
  std::string phase = "load";
  std::uint64_t records = 1000;
  std::uint64_t operations = 1000;
  std::uint64_t value_size = 1024;
  std::uint64_t write_buffer_size = 64ULL << 20;
  std::uint64_t seed = 20260722;
  std::uint64_t scan_length = 10;
  bool sync = true;
  bool prepare = false;
};

[[noreturn]] void Usage(const char* program, std::string_view error = {}) {
  if (!error.empty()) {
    std::cerr << error << '\n';
  }
  std::cerr
      << "usage: " << program
      << " --db PATH --phase load|a|b|c|d|e|f [options]\n"
      << "  --records N --operations N --value-size N\n"
      << "  --write-buffer-size N --seed N --scan-length N --sync 0|1 "
         "--prepare 0|1\n";
  std::exit(2);
}

std::uint64_t ParseUnsigned(std::string_view text, const char* option) {
  std::uint64_t value = 0;
  const auto result = std::from_chars(text.data(), text.data() + text.size(),
                                      value);
  if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
    Usage(option, "invalid unsigned integer");
  }
  return value;
}

Config ParseArguments(int argc, char** argv) {
  Config config;
  for (int index = 1; index < argc; ++index) {
    const std::string_view option(argv[index]);
    if (index + 1 >= argc) {
      Usage(argv[0], "missing option value");
    }
    const std::string_view value(argv[++index]);
    if (option == "--db") {
      config.db = value;
    } else if (option == "--phase") {
      config.phase = value;
    } else if (option == "--records") {
      config.records = ParseUnsigned(value, "--records");
    } else if (option == "--operations") {
      config.operations = ParseUnsigned(value, "--operations");
    } else if (option == "--value-size") {
      config.value_size = ParseUnsigned(value, "--value-size");
    } else if (option == "--write-buffer-size") {
      config.write_buffer_size = ParseUnsigned(value, "--write-buffer-size");
    } else if (option == "--seed") {
      config.seed = ParseUnsigned(value, "--seed");
    } else if (option == "--scan-length") {
      config.scan_length = ParseUnsigned(value, "--scan-length");
    } else if (option == "--sync") {
      const std::uint64_t parsed = ParseUnsigned(value, "--sync");
      if (parsed > 1) {
        Usage(argv[0], "--sync accepts 0 or 1");
      }
      config.sync = parsed != 0;
    } else if (option == "--prepare") {
      const std::uint64_t parsed = ParseUnsigned(value, "--prepare");
      if (parsed > 1) {
        Usage(argv[0], "--prepare accepts 0 or 1");
      }
      config.prepare = parsed != 0;
    } else {
      Usage(argv[0], "unknown option");
    }
  }
  if (config.db.empty() || config.records == 0 || config.operations == 0 ||
      config.value_size == 0 || config.scan_length == 0 ||
      (config.phase != "load" && config.phase != "a" &&
       config.phase != "b" && config.phase != "c" &&
       config.phase != "d" && config.phase != "e" &&
       config.phase != "f")) {
    Usage(argv[0], "invalid or missing required option");
  }
  return config;
}

std::string Key(std::uint64_t record) {
  std::string key = "user";
  const std::string digits = std::to_string(record);
  key.append(16 - std::min<std::size_t>(16, digits.size()), '0');
  key += digits;
  return key;
}

std::string Value(std::uint64_t record, std::size_t size) {
  std::string value(size, static_cast<char>('a' + (record % 26)));
  const std::string prefix = std::to_string(record);
  std::copy_n(prefix.begin(), std::min(prefix.size(), value.size()),
              value.begin());
  return value;
}

std::uint64_t Mix(std::uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

class Zipfian {
 public:
  explicit Zipfian(std::uint64_t items) : items_(items) {
    constexpr double theta = 0.99;
    double zeta = 0.0;
    for (std::uint64_t index = 1; index <= items_; ++index) {
      zeta += 1.0 / std::pow(static_cast<double>(index), theta);
    }
    const double zeta_two = 1.0 + 1.0 / std::pow(2.0, theta);
    alpha_ = 1.0 / (1.0 - theta);
    eta_ = (1.0 - std::pow(2.0 / static_cast<double>(items_),
                           1.0 - theta)) /
           (1.0 - zeta_two / zeta);
    zeta_ = zeta;
  }

  std::uint64_t Next(std::mt19937_64& random) const {
    constexpr double theta = 0.99;
    const double unit = std::generate_canonical<double, 64>(random);
    const double uz = unit * zeta_;
    std::uint64_t rank = 0;
    if (uz < 1.0) {
      rank = 0;
    } else if (uz < 1.0 + std::pow(0.5, theta)) {
      rank = 1;
    } else {
      rank = static_cast<std::uint64_t>(
          static_cast<double>(items_) *
          std::pow(eta_ * unit - eta_ + 1.0, alpha_));
      rank = std::min(rank, items_ - 1);
    }
    return Mix(rank) % items_;
  }

 private:
  std::uint64_t items_;
  double alpha_ = 0.0;
  double eta_ = 0.0;
  double zeta_ = 0.0;
};

int Fail(const Config& config, std::string_view stage,
         const leveldb::Status& status) {
  std::cerr << "orchfs_ycsb_error phase=" << config.phase
            << " stage=" << stage << " status=" << std::quoted(status.ToString())
            << '\n';
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  const Config config = ParseArguments(argc, argv);
  if (config.phase != "load" && !config.prepare) {
    // Both compared LibFS implementations inherit the research prototype's
    // no-op advisory-lock semantics.  Remove the prior process's lock inode
    // so the legacy O_CREAT path can reopen a loaded database.  The runner is
    // single-process and never shares a database between clients.
    (void)::unlink((config.db + "/LOCK").c_str());
  }
  leveldb::Options options;
  options.create_if_missing = config.phase == "load" || config.prepare;
  options.error_if_exists = config.phase == "load" || config.prepare;
  options.write_buffer_size = config.write_buffer_size;
  leveldb::DB* raw_db = nullptr;
  const leveldb::Status open_status =
      leveldb::DB::Open(options, config.db, &raw_db);
  if (!open_status.ok()) {
    return Fail(config, "open", open_status);
  }
  std::unique_ptr<leveldb::DB> db(raw_db);
  leveldb::WriteOptions write_options;
  write_options.sync = config.sync;
  leveldb::ReadOptions read_options;
  std::mt19937_64 random(config.seed);
  Zipfian zipf(config.records);
  std::uint64_t reads = 0;
  std::uint64_t writes = 0;
  std::uint64_t scans = 0;
  std::uint64_t checksum = 0;
  std::uint64_t current_records = config.records;
  if (config.prepare && config.phase != "load") {
    for (std::uint64_t record = 0; record < config.records; ++record) {
      const leveldb::Status status = db->Put(
          write_options, Key(record), Value(record, config.value_size));
      if (!status.ok()) {
        return Fail(config, "prepare", status);
      }
    }
  }
  const auto started = std::chrono::steady_clock::now();

  for (std::uint64_t operation = 0; operation < config.operations; ++operation) {
    leveldb::Status status;
    if (config.phase == "load") {
      const std::uint64_t record = operation % config.records;
      status = db->Put(write_options, Key(record),
                       Value(record, config.value_size));
      ++writes;
    } else {
      const double choice = std::generate_canonical<double, 64>(random);
      std::uint64_t record = zipf.Next(random);
      if (config.phase == "d") {
        const std::uint64_t distance = std::min<std::uint64_t>(
            current_records - 1,
            static_cast<std::uint64_t>(std::geometric_distribution<int>(0.2)(random)));
        record = current_records - 1 - distance;
      }
      const bool insert = (config.phase == "d" || config.phase == "e") &&
                          choice >= 0.95;
      const bool update = (config.phase == "a" && choice >= 0.50) ||
                          (config.phase == "b" && choice >= 0.95);
      const bool read_modify_write = config.phase == "f" && choice >= 0.50;
      if (insert) {
        record = current_records++;
        status = db->Put(write_options, Key(record),
                         Value(record, config.value_size));
        ++writes;
      } else if (update) {
        status = db->Put(write_options, Key(record),
                         Value(operation + config.seed, config.value_size));
        ++writes;
      } else if (config.phase == "e") {
        std::unique_ptr<leveldb::Iterator> iterator(db->NewIterator(read_options));
        iterator->Seek(Key(record));
        std::uint64_t visited = 0;
        while (iterator->Valid() && visited < config.scan_length) {
          checksum += iterator->value().size();
          iterator->Next();
          ++visited;
        }
        status = iterator->status();
        ++scans;
      } else {
        std::string value;
        status = db->Get(read_options, Key(record), &value);
        ++reads;
        checksum += value.size();
        if (status.ok() && read_modify_write) {
          status = db->Put(write_options, Key(record),
                           Value(operation + checksum, config.value_size));
          ++writes;
        }
      }
    }
    if (!status.ok()) {
      return Fail(config, "operation", status);
    }
  }

  const double seconds = std::chrono::duration<double>(
      std::chrono::steady_clock::now() - started).count();
  std::cout << std::fixed << std::setprecision(6)
            << "orchfs_ycsb_result phase=" << config.phase
            << " operations=" << config.operations
            << " records=" << config.records
            << " value_size=" << config.value_size
            << " write_buffer_size=" << config.write_buffer_size
            << " sync=" << static_cast<int>(config.sync)
            << " prepare=" << static_cast<int>(config.prepare)
            << " reads=" << reads << " writes=" << writes
            << " scans=" << scans << " checksum=" << checksum
            << " seconds=" << seconds
            << " ops_per_s=" << static_cast<double>(config.operations) / seconds
            << " seed=" << config.seed << '\n';
  return 0;
}
