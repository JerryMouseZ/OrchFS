#include "orchfs/async/server.hpp"

#include "orchfs/async/ipc_transport.hpp"
#include "orchfs/async/range_arbiter.hpp"
#include "orchfs/async/rpc_protocol.hpp"
#include "orchfs/async/runtime.hpp"

#include "../LibFS/lib_dir.h"
#include "../LibFS/orchfs.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <limits>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <system_error>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/vfs.h>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sys/eventfd.h>
#include <unistd.h>

extern "C" {
void read_data_from_devs(void* destination, int64_t length, int64_t offset);
void write_data_to_devs(void* source, int64_t length, int64_t offset);
int device_sync(void);
// Older test doubles expose the historical raw DIR* encoding. Production
// LibFS provides orchfs_dirfd() for the fd+1 encoding that keeps fd 0 non-null.
int orchfs_dirfd(DIR* directory) __attribute__((weak));
}

namespace orchfs::async {
namespace {

int legacy_directory_fd(DIR* directory) noexcept {
  if (orchfs_dirfd != nullptr) {
    return orchfs_dirfd(directory);
  }
  return static_cast<int>(reinterpret_cast<std::uintptr_t>(directory));
}

struct ParsedRequest {
  RpcRequest request;
  std::string path1;
  std::string path2;
  std::span<const std::byte> data;
};

Result<ParsedRequest> parse_request(const std::vector<std::byte>& payload) {
  if (payload.size() < sizeof(RpcRequest)) {
    return Result<ParsedRequest>::failure(
        std::make_error_code(std::errc::protocol_error));
  }
  ParsedRequest parsed;
  std::memcpy(&parsed.request, payload.data(), sizeof(parsed.request));
  if (parsed.request.schema_version != kRpcSchemaVersion) {
    return Result<ParsedRequest>::failure(
        std::make_error_code(std::errc::protocol_not_supported));
  }

  const std::uint64_t variable_size =
      static_cast<std::uint64_t>(parsed.request.path1_length) +
      parsed.request.path2_length + parsed.request.data_length;
  if (variable_size != payload.size() - sizeof(RpcRequest)) {
    return Result<ParsedRequest>::failure(
        std::make_error_code(std::errc::protocol_error));
  }

  const std::byte* cursor = payload.data() + sizeof(RpcRequest);
  parsed.path1.assign(reinterpret_cast<const char*>(cursor),
                      parsed.request.path1_length);
  cursor += parsed.request.path1_length;
  parsed.path2.assign(reinterpret_cast<const char*>(cursor),
                      parsed.request.path2_length);
  cursor += parsed.request.path2_length;
  parsed.data = std::span<const std::byte>(cursor, parsed.request.data_length);
  if (parsed.path1.find('\0') != std::string::npos ||
      parsed.path2.find('\0') != std::string::npos) {
    return Result<ParsedRequest>::failure(
        std::make_error_code(std::errc::invalid_argument));
  }
  return Result<ParsedRequest>::success(std::move(parsed));
}

std::uint64_t hash_path(std::string_view path) noexcept {
  std::uint64_t value = 1469598103934665603ULL;
  for (const char character : path) {
    const auto byte = static_cast<unsigned char>(character);
    value ^= byte;
    value *= 1099511628211ULL;
  }
  return value;
}

std::string_view rename_parent(std::string_view path) noexcept {
  const auto slash = path.find_last_of('/');
  return slash == std::string_view::npos ? std::string_view{}
                                         : path.substr(0, slash);
}

std::string_view rename_basename(std::string_view path) noexcept {
  const auto slash = path.find_last_of('/');
  return slash == std::string_view::npos ? path : path.substr(slash + 1);
}

int normalize_legacy_rename_path(std::string& path) {
  if (path.empty()) {
    return ENOENT;
  }

  const auto original_slash = path.find_last_of('/');
  if (original_slash == std::string::npos) {
    // Preserve current-directory semantics while satisfying the legacy
    // parser's requirement that a directory component be present.
    path.insert(0, "./");
  } else if (path.front() == '/' &&
             path.substr(0, original_slash).find_first_not_of('/') ==
                 std::string::npos) {
    // The legacy parser gives "/name" an empty directory, while path_to_inode
    // cannot resolve a directory made only of slashes.  Route every lexical
    // root-level spelling through the mount root's "." entry instead.
    path = "/./" + path.substr(original_slash + 1);
  }

  constexpr std::size_t kLegacyDirectoryCapacity = 4096;
  const auto parent = rename_parent(path);
  const auto basename = rename_basename(path);
  if (parent.empty() || basename.empty() || basename == "." ||
      basename == "..") {
    return EINVAL;
  }
  if (parent.size() >= kLegacyDirectoryCapacity ||
      basename.size() > ORCH_DIRENT_NAME_MAX) {
    return ENAMETOOLONG;
  }
  return 0;
}

int validate_open_flags(int flags) noexcept {
  const int access_mode = flags & O_ACCMODE;
  if (access_mode != O_RDONLY && access_mode != O_WRONLY &&
      access_mode != O_RDWR) {
    return EINVAL;
  }
#ifdef O_PATH
  if ((flags & O_PATH) != 0) {
    return EOPNOTSUPP;
  }
#endif
#ifdef O_TMPFILE
  if ((flags & O_TMPFILE) == O_TMPFILE) {
    return EOPNOTSUPP;
  }
#endif
#ifdef O_SYNC
  if ((flags & O_SYNC) != 0) {
    return EOPNOTSUPP;
  }
#endif
#ifdef O_DSYNC
  if ((flags & O_DSYNC) != 0) {
    return EOPNOTSUPP;
  }
#endif

  int supported = O_ACCMODE | O_CREAT | O_EXCL | O_TRUNC | O_APPEND |
                  O_NONBLOCK;
#ifdef O_CLOEXEC
  supported |= O_CLOEXEC;
#endif
#ifdef O_NOCTTY
  supported |= O_NOCTTY;
#endif
#ifdef O_LARGEFILE
  supported |= O_LARGEFILE;
#endif
#ifdef O_DIRECTORY
  if ((flags & O_DIRECTORY) != 0 && (flags & O_CREAT) != 0) {
    // Linux rejects this combination. More importantly, the legacy create
    // path always creates a regular inode, so it must be rejected before the
    // legacy call to avoid leaving a file behind after an ENOTDIR result.
    return EINVAL;
  }
  supported |= O_DIRECTORY;
#endif
  return (flags & ~supported) == 0 ? 0 : EOPNOTSUPP;
}

int stored_open_flags(int flags) noexcept {
  int transient = O_CREAT | O_EXCL | O_TRUNC;
#ifdef O_CLOEXEC
  transient |= O_CLOEXEC;
#endif
#ifdef O_NOCTTY
  transient |= O_NOCTTY;
#endif
  return flags & ~transient;
}

int validate_legacy_io_range(std::int64_t offset,
                             std::uint64_t length) noexcept {
  if (offset < 0) {
    return EINVAL;
  }
  const auto remaining = static_cast<std::uint64_t>(
      std::numeric_limits<std::int64_t>::max() - offset);
  return length > remaining ? EOVERFLOW : 0;
}

RpcFileStat to_wire(const struct stat& value) noexcept {
  return RpcFileStat{
      .device = static_cast<std::uint64_t>(value.st_dev),
      .inode = static_cast<std::uint64_t>(value.st_ino),
      .mode = static_cast<std::uint64_t>(value.st_mode),
      .link_count = static_cast<std::uint64_t>(value.st_nlink),
      .uid = static_cast<std::uint64_t>(value.st_uid),
      .gid = static_cast<std::uint64_t>(value.st_gid),
      .rdev = static_cast<std::uint64_t>(value.st_rdev),
      .size = static_cast<std::int64_t>(value.st_size),
      .block_size = static_cast<std::int64_t>(value.st_blksize),
      .blocks = static_cast<std::int64_t>(value.st_blocks),
      .atime_seconds = value.st_atim.tv_sec,
      .atime_nanoseconds = value.st_atim.tv_nsec,
      .mtime_seconds = value.st_mtim.tv_sec,
      .mtime_nanoseconds = value.st_mtim.tv_nsec,
      .ctime_seconds = value.st_ctim.tv_sec,
      .ctime_nanoseconds = value.st_ctim.tv_nsec,
  };
}

RpcStatFs to_wire(const struct statfs& value) noexcept {
  return RpcStatFs{
      .type = static_cast<std::uint64_t>(value.f_type),
      .block_size = static_cast<std::uint64_t>(value.f_bsize),
      .blocks = static_cast<std::uint64_t>(value.f_blocks),
      .blocks_free = static_cast<std::uint64_t>(value.f_bfree),
      .blocks_available = static_cast<std::uint64_t>(value.f_bavail),
      .files = static_cast<std::uint64_t>(value.f_files),
      .files_free = static_cast<std::uint64_t>(value.f_ffree),
      .name_length = static_cast<std::uint64_t>(value.f_namelen),
      .fragment_size = static_cast<std::uint64_t>(value.f_frsize),
      .flags = static_cast<std::uint64_t>(value.f_flags),
  };
}

template <typename T>
std::vector<std::byte> encode_object(const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  std::vector<std::byte> bytes(sizeof(T));
  std::memcpy(bytes.data(), &value, sizeof(T));
  return bytes;
}

struct Completion {
  std::uint32_t lane{};
  IpcDescriptor descriptor;
  std::vector<std::byte> payload;
};

struct Incoming {
  std::uint32_t lane{};
  IpcDescriptor descriptor;
  std::vector<std::byte> payload;
};

inline constexpr std::uint64_t kWholeFileRangeLength =
    std::numeric_limits<std::uint64_t>::max();

// The legacy core can block on its internal worker pools and storage. Keep all
// of those calls off Runtime workers while preserving the coroutine's owner
// affinity when it resumes.
class BlockingExecutor final {
 private:
  struct Job {
    virtual ~Job() = default;
    virtual void execute() noexcept = 0;
  };

 public:
  explicit BlockingExecutor(std::size_t worker_count) {
    try {
      workers_.reserve(worker_count);
      for (std::size_t index = 0; index < worker_count; ++index) {
        workers_.emplace_back([this] { worker_loop(); });
      }
    } catch (...) {
      stop_and_join();
      throw;
    }
  }

  BlockingExecutor(const BlockingExecutor&) = delete;
  BlockingExecutor& operator=(const BlockingExecutor&) = delete;

  ~BlockingExecutor() { stop_and_join(); }

  template <typename Function>
  class Awaiter final : private Job {
   public:
    using Value = std::invoke_result_t<Function&>;
    static_assert(!std::is_void_v<Value>);

    Awaiter(BlockingExecutor& executor, Function function)
        : executor_(&executor), function_(std::move(function)) {}

    Awaiter(const Awaiter&) = delete;
    Awaiter& operator=(const Awaiter&) = delete;
    Awaiter(Awaiter&&) = delete;
    Awaiter& operator=(Awaiter&&) = delete;

    [[nodiscard]] bool await_ready() const noexcept { return false; }

    [[nodiscard]] bool await_suspend(
        std::coroutine_handle<> continuation) noexcept {
      runtime_ = Runtime::current();
      worker_ = Runtime::current_worker();
      continuation_ = continuation;
      if (runtime_ == nullptr || worker_ == detail::no_worker) {
        error_ = std::make_error_code(std::errc::operation_not_permitted);
        return false;
      }
      if (!executor_->submit(this)) {
        error_ = std::make_error_code(std::errc::resource_unavailable_try_again);
        return false;
      }
      return true;
    }

    [[nodiscard]] Result<Value> await_resume() noexcept(
        std::is_nothrow_move_constructible_v<Value>) {
      if (error_) {
        return Result<Value>::failure(error_);
      }
      return Result<Value>::success(std::move(*value_));
    }

   private:
    void execute() noexcept override {
      try {
        value_.emplace(std::invoke(function_));
      } catch (const std::bad_alloc&) {
        error_ = std::make_error_code(std::errc::not_enough_memory);
      } catch (...) {
        std::terminate();
      }
      // Runtime drains submitted roots before stopping its workers. Therefore
      // failure here would otherwise strand a live root with no safe fallback.
      if (!runtime_->schedule(continuation_, worker_)) {
        std::terminate();
      }
    }

    BlockingExecutor* executor_;
    Function function_;
    Runtime* runtime_{nullptr};
    std::size_t worker_{detail::no_worker};
    std::coroutine_handle<> continuation_{};
    std::optional<Value> value_;
    std::error_code error_;
  };

  template <typename Function>
  [[nodiscard]] auto run(Function&& function) {
    return Awaiter<std::decay_t<Function>>(
        *this, std::forward<Function>(function));
  }

 private:
  bool submit(Job* job) noexcept {
    {
      std::lock_guard lock(mutex_);
      if (stopping_) {
        return false;
      }
      try {
        jobs_.push_back(job);
      } catch (...) {
        return false;
      }
    }
    ready_.notify_one();
    return true;
  }

  void worker_loop() noexcept {
    for (;;) {
      Job* job = nullptr;
      {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [this] { return stopping_ || !jobs_.empty(); });
        if (jobs_.empty()) {
          if (stopping_) {
            return;
          }
          continue;
        }
        job = jobs_.front();
        jobs_.pop_front();
      }
      job->execute();
    }
  }

  void stop_and_join() noexcept {
    {
      std::lock_guard lock(mutex_);
      stopping_ = true;
    }
    ready_.notify_all();
    for (auto& worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    workers_.clear();
  }

  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<Job*> jobs_;
  bool stopping_{false};
  std::vector<std::thread> workers_;
};

template <typename T>
struct LegacyCallResult {
  T value{};
  int error{};
};

template <typename Function>
auto call_legacy(Function&& function) noexcept
    -> LegacyCallResult<std::invoke_result_t<Function&>> {
  using Value = std::invoke_result_t<Function&>;
  static_assert(!std::is_void_v<Value>);
  errno = 0;
  const Value value = std::invoke(function);
  return LegacyCallResult<Value>{.value = value, .error = errno};
}

struct OpenLegacyResult {
  int fd{-1};
  std::int64_t inode{-1};
  DIR* directory{};
  int error{};
};

struct DirectoryBatchResult {
  std::vector<std::byte> payload;
  std::uint64_t count{};
  bool end_of_stream{false};
  int error{};
};

struct StatLegacyResult {
  struct stat value {};
  int result{-1};
  int error{};
};

struct StatFsLegacyResult {
  struct statfs value {};
  int result{-1};
  int error{};
};

struct TruncateLegacyResult {
  int truncate_result{-1};
  int truncate_error{};
  int close_result{-1};
  int close_error{};
};

struct HandleState {
  enum class Kind { file, directory };

  int fd{-1};
  std::int64_t inode{-1};
  Kind kind{Kind::file};
  DIR* directory{};
  std::atomic<bool> closing{false};
  std::atomic<int> open_flags{0};
  RangeArbiter lifecycle;
  RangeArbiter offset_gate;
};

class CoreCoordinator {
 public:
  std::shared_ptr<RangeArbiter> range_for(std::int64_t inode) {
    std::lock_guard lock(mutex_);
    auto found = data_ranges_.find(inode);
    if (found != data_ranges_.end()) {
      return found->second;
    }
    auto range = std::make_shared<RangeArbiter>();
    data_ranges_.emplace(inode, range);
    return range;
  }

  RangeArbiter& namespace_gate() noexcept { return namespace_gate_; }

 private:
  std::mutex mutex_;
  std::unordered_map<std::int64_t, std::shared_ptr<RangeArbiter>> data_ranges_;
  RangeArbiter namespace_gate_;
};

class ServerSession;

Task<void> run_request(std::shared_ptr<ServerSession> session,
                       Incoming incoming);
Task<void> run_session_cleanup(std::shared_ptr<ServerSession> session);

class ServerSession final : public std::enable_shared_from_this<ServerSession> {
 public:
  ServerSession(Runtime& runtime, ServerTransport transport,
                std::shared_ptr<CoreCoordinator> core,
                std::shared_ptr<BlockingExecutor> blocking)
      : runtime_(&runtime), transport_(std::move(transport)),
        config_(transport_.config()), core_(std::move(core)),
        blocking_(std::move(blocking)) {
    result_event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (result_event_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "eventfd");
    }
  }

  ~ServerSession() {
    request_stop();
    if (thread_.joinable()) {
      if (thread_.get_id() == std::this_thread::get_id()) {
        thread_.detach();
      } else {
        thread_.join();
      }
    }
    if (result_event_fd_ >= 0) {
      ::close(result_event_fd_);
    }
  }

  void start() {
    auto self = shared_from_this();
    thread_ = std::thread([self = std::move(self)] { self->io_loop(); });
  }

  void request_stop() noexcept {
    closing_.store(true, std::memory_order_release);
    stop_requested_.store(true, std::memory_order_release);
    signal_result();
  }

  void join() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  [[nodiscard]] bool finished() const noexcept {
    return finished_.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::error_code cleanup_error() const noexcept {
    const int value = cleanup_error_.load(std::memory_order_acquire);
    return value == 0 ? std::error_code{}
                      : std::error_code(value, std::generic_category());
  }

  Task<Completion> dispatch(Incoming incoming) {
    Completion completion;
    completion.lane = incoming.lane;
    completion.descriptor = incoming.descriptor;
    completion.descriptor.flags = DescriptorFlag::response;
    completion.descriptor.payload_length = 0;
    completion.descriptor.result_length = 0;
    completion.descriptor.status = 0;

    auto parsed_result = parse_request(incoming.payload);
    if (!parsed_result) {
      completion.descriptor.status = -parsed_result.error().value();
      co_return completion;
    }
    auto parsed = std::move(parsed_result).value();

    std::shared_ptr<HandleState> handle;
    if (parsed.request.handle != kInvalidRemoteHandle) {
      handle = find_handle(parsed.request.handle);
    }

    std::uint64_t owner_key = hash_path(parsed.path1);
    if (handle && handle->inode >= 0) {
      owner_key = static_cast<std::uint64_t>(handle->inode);
    }
    auto scheduled = co_await runtime_->schedule_on(runtime_->owner_for(owner_key));
    if (!scheduled) {
      completion.descriptor.status = -scheduled.error().value();
      co_return completion;
    }

    switch (incoming.descriptor.opcode) {
      case Opcode::open:
        if (parsed.path1.empty()) {
          fail(completion, ENOENT);
          co_return completion;
        }
        co_return co_await do_open(std::move(completion), parsed, nullptr);
      case Opcode::open_at:
        if (parsed.path1.empty()) {
          fail(completion, ENOENT);
          co_return completion;
        }
        // POSIX openat ignores dirfd for an absolute path. The legacy
        // orchfs_openat instead exits the process, so route this case through
        // ordinary open before validating the remote directory handle.
        if (parsed.path1.front() == '/') {
          co_return co_await do_open(std::move(completion), parsed, nullptr);
        }
        if (parsed.request.handle == kInvalidRemoteHandle || !handle) {
          fail(completion, EBADF);
          co_return completion;
        }
        if (handle->kind != HandleState::Kind::directory) {
          fail(completion, ENOTDIR);
          co_return completion;
        }
        co_return co_await do_open(std::move(completion), parsed, handle);
      case Opcode::close:
        co_return co_await do_close(std::move(completion), parsed.request.handle,
                                    HandleState::Kind::file);
      case Opcode::read:
      case Opcode::read_at:
        co_return co_await do_read(std::move(completion), parsed, handle,
                                   incoming.descriptor.opcode == Opcode::read);
      case Opcode::write:
      case Opcode::write_at:
        co_return co_await do_write(std::move(completion), parsed, handle,
                                    incoming.descriptor.opcode == Opcode::write);
      case Opcode::seek:
        co_return co_await do_seek(std::move(completion), parsed, handle);
      case Opcode::stat_path:
        co_return co_await do_stat_path(std::move(completion), parsed.path1);
      case Opcode::stat_handle:
        co_return co_await do_stat_handle(std::move(completion), handle);
      case Opcode::statfs:
        co_return co_await do_statfs(std::move(completion), handle);
      case Opcode::mkdir:
      case Opcode::rmdir:
      case Opcode::unlink:
      case Opcode::rename:
        co_return co_await do_path_mutation(std::move(completion), parsed,
                                            incoming.descriptor.opcode);
      case Opcode::truncate_path:
        co_return co_await do_truncate_path(std::move(completion), parsed);
      case Opcode::truncate_handle:
        co_return co_await do_truncate_handle(std::move(completion), parsed,
                                              handle);
      case Opcode::open_directory:
        co_return co_await do_open_directory(std::move(completion), parsed.path1);
      case Opcode::open_directory_handle:
        co_return co_await do_open_directory_handle(
            std::move(completion), handle);
      case Opcode::read_directory_batch:
        co_return co_await do_read_directory(std::move(completion), parsed,
                                             handle);
      case Opcode::close_directory:
        co_return co_await do_close(std::move(completion), parsed.request.handle,
                                    HandleState::Kind::directory);
      case Opcode::sync:
        co_return co_await do_sync(std::move(completion), handle);
      case Opcode::set_flags:
        co_return co_await do_flags(std::move(completion), parsed, handle);
      case Opcode::shutdown_session:
        co_return co_await do_shutdown(std::move(completion));
      case Opcode::raw_device_read:
      case Opcode::raw_device_write:
      case Opcode::raw_device_flush:
        co_return co_await do_raw_device(std::move(completion), parsed,
                                         incoming.descriptor.opcode);
      case Opcode::connect:
      case Opcode::ping:
        co_return completion;
      case Opcode::invalid:
        completion.descriptor.status = -EINVAL;
        co_return completion;
    }
    completion.descriptor.status = -ENOTSUP;
    co_return completion;
  }

  void enqueue_completion(Completion completion) noexcept {
    {
      std::lock_guard lock(completion_mutex_);
      completions_.push_back(std::move(completion));
    }
    inflight_.fetch_sub(1, std::memory_order_acq_rel);
    signal_result();
  }

 private:
  static void fail(Completion& completion, int error) noexcept {
    completion.descriptor.status = -(error > 0 ? error : EIO);
    completion.payload.clear();
    completion.descriptor.payload_length = 0;
    completion.descriptor.result_length = 0;
  }

  std::shared_ptr<HandleState> find_handle(RemoteHandle id) {
    std::lock_guard lock(handles_mutex_);
    auto found = handles_.find(id);
    if (found == handles_.end() ||
        found->second->closing.load(std::memory_order_acquire)) {
      return {};
    }
    return found->second;
  }

  RemoteHandle insert_handle(int fd, std::int64_t inode,
                             HandleState::Kind kind, DIR* directory,
                             int open_flags = 0) {
    auto state = std::make_shared<HandleState>();
    state->fd = fd;
    state->inode = inode;
    state->kind = kind;
    state->directory = directory;
    state->open_flags.store(open_flags, std::memory_order_relaxed);
    const RemoteHandle id = next_handle_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard lock(handles_mutex_);
    handles_.emplace(id, std::move(state));
    return id;
  }

  std::shared_ptr<HandleState> mark_closing(RemoteHandle id,
                                            HandleState::Kind expected) {
    std::lock_guard lock(handles_mutex_);
    auto found = handles_.find(id);
    if (found == handles_.end() || found->second->kind != expected) {
      return {};
    }
    bool was_closing = false;
    if (!found->second->closing.compare_exchange_strong(
            was_closing, true, std::memory_order_acq_rel)) {
      return {};
    }
    return found->second;
  }

  void erase_handle(RemoteHandle id) {
    std::lock_guard lock(handles_mutex_);
    handles_.erase(id);
  }

  static void rollback_closing(
      const std::shared_ptr<HandleState>& handle) noexcept {
    handle->closing.store(false, std::memory_order_release);
  }

  Task<Completion> do_open(Completion completion, const ParsedRequest& parsed,
                           const std::shared_ptr<HandleState>& directory) {
    if (const int error = validate_open_flags(parsed.request.open_flags);
        error != 0) {
      fail(completion, error);
      co_return completion;
    }
    const bool truncating = (parsed.request.open_flags & O_TRUNC) != 0;
    const RangeMode mode =
        (parsed.request.open_flags & (O_CREAT | O_TRUNC)) != 0
                               ? RangeMode::write
                               : RangeMode::read;
    auto gate_result = co_await core_->namespace_gate().acquire(0, 1, mode);
    if (!gate_result) {
      fail(completion, gate_result.error().value());
      co_return completion;
    }
    auto gate = std::move(gate_result).value();

    std::optional<RangePermit> directory_life;
    if (directory) {
      auto life_result =
          co_await directory->lifecycle.acquire(0, 1, RangeMode::read);
      if (!life_result) {
        fail(completion, life_result.error().value());
      } else {
        auto life = std::move(life_result).value();
        if (directory->closing.load(std::memory_order_acquire)) {
          auto released = co_await life.release();
          fail(completion,
               released ? EBADF : released.error().value());
        } else {
          directory_life.emplace(std::move(life));
        }
      }
    }

    if (completion.descriptor.status == 0 && directory &&
        directory->kind != HandleState::Kind::directory) {
      fail(completion, ENOTDIR);
    }
    if (completion.descriptor.status == 0) {
      const std::string path = parsed.path1;
      const int flags = parsed.request.open_flags;
      // The legacy core currently ignores O_TRUNC. Strip it even if a future
      // implementation starts honoring it, because truncation must happen
      // only after the inode-wide reservation below has been acquired.
      const int legacy_flags = flags & ~O_TRUNC;
      const auto create_mode = parsed.request.mode;
      const int directory_fd = directory ? directory->fd : -1;
#ifdef O_DIRECTORY
      const bool require_directory = (flags & O_DIRECTORY) != 0;
#else
      constexpr bool require_directory = false;
#endif
      auto blocked = co_await blocking_->run(
          [path, legacy_flags, create_mode, directory_fd,
           has_directory = !!directory, require_directory]
          () noexcept {
            errno = 0;
            const int fd = has_directory
                               ? orchfs_openat(directory_fd, path.c_str(),
                                               legacy_flags, create_mode)
                               : orchfs_open(path.c_str(), legacy_flags,
                                             create_mode);
            int saved_error = errno;
            std::int64_t inode = -1;
            if (fd >= 0) {
              errno = 0;
              inode = orchfs_fd_inode_id(fd);
              if (inode < 0) {
                saved_error = errno != 0 ? errno : EIO;
                (void)orchfs_close(fd);
              } else if (require_directory) {
                errno = 0;
                const int file_type = orchfs_fd_file_type(fd);
                if (file_type != ORCHFS_FILE_TYPE_DIRECTORY) {
                  saved_error =
                      file_type == ORCHFS_FILE_TYPE_ERROR && errno != 0
                          ? errno
                          : ENOTDIR;
                  (void)orchfs_close(fd);
                  inode = -1;
                }
              }
            }
            return OpenLegacyResult{.fd = inode < 0 ? -1 : fd,
                                    .inode = inode,
                                    .directory = nullptr,
                                    .error = saved_error};
          });
      if (!blocked) {
        fail(completion, blocked.error().value());
      } else {
        auto opened = std::move(blocked).value();
        if (opened.fd < 0) {
          const int fallback = (flags & O_CREAT) != 0 ? EIO : ENOENT;
          fail(completion, opened.error != 0 ? opened.error : fallback);
        } else {
          if (truncating) {
            auto range = core_->range_for(opened.inode);
            auto range_result = co_await range->acquire(
                0, kWholeFileRangeLength, RangeMode::write);
            if (!range_result) {
              fail(completion, range_result.error().value());
            } else {
              auto permit = std::move(range_result).value();
              auto truncated = co_await blocking_->run(
                  [fd = opened.fd]() noexcept {
                    return call_legacy(
                        [&]() noexcept { return orchfs_ftruncate(fd, 0); });
                  });
              if (!truncated) {
                fail(completion, truncated.error().value());
              } else if (const auto result = std::move(truncated).value();
                         result.value != 0) {
                fail(completion, result.error != 0 ? result.error : EIO);
              }
              auto range_released = co_await permit.release();
              if (!range_released && completion.descriptor.status == 0) {
                fail(completion, range_released.error().value());
              }
            }
          }
          if (completion.descriptor.status == 0) {
            completion.descriptor.result_length = insert_handle(
                opened.fd, opened.inode, HandleState::Kind::file, nullptr,
                stored_open_flags(flags));
          } else {
            // No remote handle was published, so the temporary local fd must
            // be consumed before returning the open error.
            auto ignored = co_await blocking_->run([fd = opened.fd]() noexcept {
              return call_legacy(
                  [&]() noexcept { return orchfs_close(fd); });
            });
            (void)ignored;
          }
        }
      }
    }
    if (directory_life) {
      auto released = co_await directory_life->release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
    }
    auto released = co_await gate.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_close(Completion completion, RemoteHandle id,
                            HandleState::Kind kind) {
    auto handle = mark_closing(id, kind);
    if (!handle) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto permit_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::write);
    if (!permit_result) {
      rollback_closing(handle);
      fail(completion, permit_result.error().value());
      co_return completion;
    }
    auto permit = std::move(permit_result).value();
    auto blocked = co_await blocking_->run(
        [kind, directory = handle->directory, fd = handle->fd]() noexcept {
          return call_legacy([&]() noexcept {
            return kind == HandleState::Kind::directory
                       ? orchfs_closedir(directory)
                       : orchfs_close(fd);
          });
        });
    if (!blocked) {
      rollback_closing(handle);
      fail(completion, blocked.error().value());
    } else if (const auto closed = std::move(blocked).value();
               closed.value != 0) {
      rollback_closing(handle);
      fail(completion, closed.error != 0 ? closed.error : EIO);
    } else {
      erase_handle(id);
    }
    auto released = co_await permit.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_read(Completion completion, const ParsedRequest& parsed,
                           const std::shared_ptr<HandleState>& handle,
                           bool implicit) {
    if (!handle || handle->kind != HandleState::Kind::file ||
        parsed.request.length > config_.data_slot_size) {
      fail(completion, handle ? EMSGSIZE : EBADF);
      co_return completion;
    }
    if ((handle->open_flags.load(std::memory_order_acquire) & O_ACCMODE) ==
        O_WRONLY) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }

    std::optional<RangePermit> offset_permit;
    std::int64_t offset = parsed.request.offset;
    if (implicit) {
      auto offset_result =
          co_await handle->offset_gate.acquire(0, 1, RangeMode::write);
      if (!offset_result) {
        fail(completion, offset_result.error().value());
      } else {
        offset_permit.emplace(std::move(offset_result).value());
        auto blocked = co_await blocking_->run([fd = handle->fd]() noexcept {
          return call_legacy([&]() noexcept { return orchfs_tell(fd); });
        });
        if (!blocked) {
          fail(completion, blocked.error().value());
        } else {
          const auto position = std::move(blocked).value();
          offset = position.value;
          if (offset < 0) {
            fail(completion, position.error != 0 ? position.error : EIO);
          }
        }
      }
    }

    if (completion.descriptor.status == 0) {
      if (const int error =
              validate_legacy_io_range(offset, parsed.request.length);
          error != 0) {
        fail(completion, error);
      }
    }

    std::optional<RangePermit> data_permit;
    if (completion.descriptor.status == 0 && parsed.request.length != 0 &&
        offset >= 0) {
      auto range = core_->range_for(handle->inode);
      auto data_result = co_await range->acquire(
          static_cast<std::uint64_t>(offset), parsed.request.length,
          RangeMode::read);
      if (!data_result) {
        fail(completion, data_result.error().value());
      } else {
        data_permit.emplace(std::move(data_result).value());
      }
    }

    if (completion.descriptor.status == 0) {
      try {
        completion.payload.resize(parsed.request.length);
      } catch (...) {
        fail(completion, ENOMEM);
      }
    }
    if (completion.descriptor.status == 0) {
      auto blocked = co_await blocking_->run(
          [implicit, fd = handle->fd, buffer = completion.payload.data(),
           length = parsed.request.length, offset]() noexcept {
            return call_legacy([&]() noexcept {
              const auto legacy_length = static_cast<std::int64_t>(length);
              return implicit ? orchfs_read(fd, buffer, legacy_length)
                              : orchfs_pread(fd, buffer, legacy_length, offset);
            });
          });
      if (!blocked) {
        fail(completion, blocked.error().value());
      } else {
        const auto read = std::move(blocked).value();
        if (read.value < 0) {
          fail(completion, read.error != 0 ? read.error : EIO);
        } else if (static_cast<std::uint64_t>(read.value) >
                   parsed.request.length) {
          fail(completion, EIO);
        } else {
          completion.payload.resize(static_cast<std::size_t>(read.value));
          completion.descriptor.result_length =
              static_cast<std::uint64_t>(read.value);
          completion.descriptor.payload_length =
              static_cast<std::uint32_t>(read.value);
          if (read.value != 0) {
            completion.descriptor.flags |= DescriptorFlag::has_payload;
          }
        }
      }
    }

    if (data_permit) {
      auto released = co_await data_permit->release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
    }
    if (offset_permit) {
      auto released = co_await offset_permit->release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
    }
    auto life_released = co_await life.release();
    if (!life_released && completion.descriptor.status == 0) {
      fail(completion, life_released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_write(Completion completion, const ParsedRequest& parsed,
                            const std::shared_ptr<HandleState>& handle,
                            bool implicit) {
    if (!handle || handle->kind != HandleState::Kind::file ||
        parsed.request.length != parsed.data.size()) {
      fail(completion, handle ? EINVAL : EBADF);
      co_return completion;
    }
    if ((handle->open_flags.load(std::memory_order_acquire) & O_ACCMODE) ==
        O_RDONLY) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }

    const bool append =
        implicit &&
        (handle->open_flags.load(std::memory_order_acquire) & O_APPEND) != 0;
    std::optional<RangePermit> offset_permit;
    std::int64_t offset = parsed.request.offset;
    if (implicit) {
      auto offset_result =
          co_await handle->offset_gate.acquire(0, 1, RangeMode::write);
      if (!offset_result) {
        fail(completion, offset_result.error().value());
      } else {
        offset_permit.emplace(std::move(offset_result).value());
        if (!append) {
          auto blocked = co_await blocking_->run([fd = handle->fd]() noexcept {
            return call_legacy([&]() noexcept { return orchfs_tell(fd); });
          });
          if (!blocked) {
            fail(completion, blocked.error().value());
          } else {
            const auto position = std::move(blocked).value();
            offset = position.value;
            if (offset < 0) {
              fail(completion, position.error != 0 ? position.error : EIO);
            }
          }
        }
      }
    }

    if (completion.descriptor.status == 0 && !append) {
      if (const int error =
              validate_legacy_io_range(offset, parsed.request.length);
          error != 0) {
        fail(completion, error);
      }
    }

    std::optional<RangePermit> data_permit;
    if (completion.descriptor.status == 0 && parsed.request.length != 0 &&
        (append || offset >= 0)) {
      auto range = core_->range_for(handle->inode);
      auto data_result = append
                             ? co_await range->acquire(
                                   0, kWholeFileRangeLength, RangeMode::write)
                             : co_await range->acquire(
                                   static_cast<std::uint64_t>(offset),
                                   parsed.request.length, RangeMode::write);
      if (!data_result) {
        fail(completion, data_result.error().value());
      } else {
        data_permit.emplace(std::move(data_result).value());
      }
    }

    if (completion.descriptor.status == 0) {
      auto blocked = append
                         ? co_await blocking_->run(
                               [fd = handle->fd,
                                data = parsed.data]() noexcept {
                                 struct stat value {};
                                 errno = 0;
                                 if (orchfs_fstat(fd, &value) != 0) {
                                   return LegacyCallResult<std::int64_t>{
                                       .value = -1,
                                       .error = errno != 0 ? errno : EIO};
                                 }
                                 if (value.st_size < 0) {
                                   return LegacyCallResult<std::int64_t>{
                                       .value = -1, .error = EIO};
                                 }
                                 if (const int error = validate_legacy_io_range(
                                         value.st_size, data.size());
                                     error != 0) {
                                   return LegacyCallResult<std::int64_t>{
                                       .value = -1, .error = error};
                                 }
                                 errno = 0;
                                 if (orchfs_lseek(fd, value.st_size, SEEK_SET) !=
                                     0) {
                                   return LegacyCallResult<std::int64_t>{
                                       .value = -1,
                                       .error = errno != 0 ? errno : EIO};
                                 }
                                 return call_legacy([&]() noexcept {
                                   return orchfs_write(fd, data.data(),
                                                       data.size());
                                 });
                               })
                         : co_await blocking_->run(
                               [implicit, fd = handle->fd, data = parsed.data,
                                offset]() noexcept {
                                 return call_legacy([&]() noexcept {
                                   return implicit
                                              ? orchfs_write(fd, data.data(),
                                                             data.size())
                                              : orchfs_pwrite(
                                                    fd, data.data(),
                                                    static_cast<std::int64_t>(
                                                        data.size()),
                                                    offset);
                                 });
                               });
      if (!blocked) {
        fail(completion, blocked.error().value());
      } else {
        const auto written = std::move(blocked).value();
        if (written.value < 0) {
          fail(completion, written.error != 0 ? written.error : EIO);
        } else if (static_cast<std::uint64_t>(written.value) >
                   parsed.request.length) {
          fail(completion, EIO);
        } else {
          completion.descriptor.result_length =
              static_cast<std::uint64_t>(written.value);
        }
      }
    }

    if (data_permit) {
      auto released = co_await data_permit->release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
    }
    if (offset_permit) {
      auto released = co_await offset_permit->release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
    }
    auto life_released = co_await life.release();
    if (!life_released && completion.descriptor.status == 0) {
      fail(completion, life_released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_seek(Completion completion, const ParsedRequest& parsed,
                           const std::shared_ptr<HandleState>& handle) {
    if (!handle || handle->kind != HandleState::Kind::file) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }
    auto permit_result =
        co_await handle->offset_gate.acquire(0, 1, RangeMode::write);
    if (!permit_result) {
      fail(completion, permit_result.error().value());
      auto released = co_await life.release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
      co_return completion;
    }
    auto permit = std::move(permit_result).value();
    auto blocked = co_await blocking_->run(
        [fd = handle->fd, offset = parsed.request.offset,
         whence = parsed.request.whence]() noexcept {
          errno = 0;
          if (orchfs_lseek(fd, offset, whence) != 0) {
            return LegacyCallResult<std::int64_t>{
                .value = -1, .error = errno != 0 ? errno : EINVAL};
          }
          const auto position = orchfs_tell(fd);
          return LegacyCallResult<std::int64_t>{
              .value = position,
              .error = position < 0 ? (errno != 0 ? errno : EIO) : 0};
        });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else {
      const auto seeked = std::move(blocked).value();
      if (seeked.value < 0) {
        fail(completion, seeked.error != 0 ? seeked.error : EIO);
      } else {
        completion.descriptor.result_length =
            static_cast<std::uint64_t>(seeked.value);
      }
    }
    auto released = co_await permit.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    auto life_released = co_await life.release();
    if (!life_released && completion.descriptor.status == 0) {
      fail(completion, life_released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_stat_path(Completion completion, const std::string& path) {
    auto gate_result =
        co_await core_->namespace_gate().acquire(0, 1, RangeMode::read);
    if (!gate_result) {
      fail(completion, gate_result.error().value());
      co_return completion;
    }
    auto gate = std::move(gate_result).value();
    auto blocked = co_await blocking_->run([path]() noexcept {
      StatLegacyResult result;
      errno = 0;
      result.result = orchfs_stat(path.c_str(), &result.value);
      result.error = errno;
      return result;
    });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else {
      const auto stated = std::move(blocked).value();
      if (stated.result != 0) {
        fail(completion, stated.error != 0 ? stated.error : ENOENT);
      } else {
        completion.payload = encode_object(to_wire(stated.value));
        completion.descriptor.payload_length =
            static_cast<std::uint32_t>(completion.payload.size());
        completion.descriptor.flags |= DescriptorFlag::has_payload;
      }
    }
    auto released = co_await gate.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_stat_handle(
      Completion completion, const std::shared_ptr<HandleState>& handle) {
    if (!handle) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }
    auto blocked = co_await blocking_->run([fd = handle->fd]() noexcept {
      StatLegacyResult result;
      errno = 0;
      result.result = orchfs_fstat(fd, &result.value);
      result.error = errno;
      return result;
    });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else {
      const auto stated = std::move(blocked).value();
      if (stated.result != 0) {
        fail(completion, stated.error != 0 ? stated.error : EIO);
      } else {
        completion.payload = encode_object(to_wire(stated.value));
        completion.descriptor.payload_length =
            static_cast<std::uint32_t>(completion.payload.size());
        completion.descriptor.flags |= DescriptorFlag::has_payload;
      }
    }
    auto released = co_await life.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_statfs(Completion completion,
                             const std::shared_ptr<HandleState>& handle) {
    if (!handle) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }
    auto blocked = co_await blocking_->run([fd = handle->fd]() noexcept {
      StatFsLegacyResult result;
      errno = 0;
      result.result = orchfs_fstatfs(fd, &result.value);
      result.error = errno;
      return result;
    });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else {
      const auto stated = std::move(blocked).value();
      if (stated.result != 0) {
        fail(completion, stated.error != 0 ? stated.error : EIO);
      } else {
        completion.payload = encode_object(to_wire(stated.value));
        completion.descriptor.payload_length =
            static_cast<std::uint32_t>(completion.payload.size());
        completion.descriptor.flags |= DescriptorFlag::has_payload;
      }
    }
    auto released = co_await life.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_path_mutation(Completion completion,
                                    const ParsedRequest& parsed,
                                    Opcode opcode) {
    if (opcode == Opcode::mkdir &&
        parsed.request.mode > std::numeric_limits<std::uint16_t>::max()) {
      fail(completion, EINVAL);
      co_return completion;
    }
    std::string path1 = parsed.path1;
    std::string path2 = parsed.path2;
    if (opcode == Opcode::rename) {
      if (const int error = normalize_legacy_rename_path(path1); error != 0) {
        fail(completion, error);
        co_return completion;
      }
      if (const int error = normalize_legacy_rename_path(path2); error != 0) {
        fail(completion, error);
        co_return completion;
      }
      if (rename_parent(path1) != rename_parent(path2)) {
        fail(completion, EXDEV);
        co_return completion;
      }
    }
    auto gate_result =
        co_await core_->namespace_gate().acquire(0, 1, RangeMode::write);
    if (!gate_result) {
      fail(completion, gate_result.error().value());
      co_return completion;
    }
    auto gate = std::move(gate_result).value();
    const auto mode = static_cast<std::uint16_t>(parsed.request.mode);
    auto blocked = co_await blocking_->run(
        [opcode, path1, path2, mode]() noexcept {
          errno = 0;
          int result = -1;
          switch (opcode) {
            case Opcode::mkdir:
              result = orchfs_mkdir(path1.c_str(), mode);
              break;
            case Opcode::rmdir:
              result = orchfs_rmdir(path1.c_str());
              break;
            case Opcode::unlink:
              result = orchfs_unlink(path1.c_str());
              break;
            case Opcode::rename:
              result = orchfs_rename(path1.c_str(), path2.c_str());
              break;
            default:
              errno = EINVAL;
              break;
          }
          return LegacyCallResult<int>{.value = result, .error = errno};
        });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else if (const auto mutated = std::move(blocked).value();
               mutated.value != 0) {
      int fallback = EIO;
      if (opcode == Opcode::unlink || opcode == Opcode::rmdir ||
          opcode == Opcode::rename) {
        fallback = ENOENT;
      }
      fail(completion, mutated.error != 0 ? mutated.error : fallback);
    }
    auto released = co_await gate.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_truncate_path(Completion completion,
                                    const ParsedRequest& parsed) {
    if (parsed.request.length >
        static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
      fail(completion, EFBIG);
      co_return completion;
    }
    auto gate_result =
        co_await core_->namespace_gate().acquire(0, 1, RangeMode::write);
    if (!gate_result) {
      fail(completion, gate_result.error().value());
      co_return completion;
    }
    auto gate = std::move(gate_result).value();

    const std::string path = parsed.path1;
    auto opened_result = co_await blocking_->run([path]() noexcept {
      errno = 0;
      const int fd = orchfs_open(path.c_str(), O_RDWR, 0);
      int saved_error = errno;
      std::int64_t inode = -1;
      if (fd >= 0) {
        errno = 0;
        inode = orchfs_fd_inode_id(fd);
        if (inode < 0) {
          saved_error = errno != 0 ? errno : EIO;
          (void)orchfs_close(fd);
        }
      }
      return OpenLegacyResult{.fd = inode < 0 ? -1 : fd,
                              .inode = inode,
                              .directory = nullptr,
                              .error = saved_error};
    });

    int temporary_fd = -1;
    if (!opened_result) {
      fail(completion, opened_result.error().value());
    } else {
      auto opened = std::move(opened_result).value();
      temporary_fd = opened.fd;
      if (temporary_fd < 0) {
        fail(completion, opened.error != 0 ? opened.error : ENOENT);
      } else {
        auto range = core_->range_for(opened.inode);
        auto range_result = co_await range->acquire(
            0, kWholeFileRangeLength, RangeMode::write);
        if (!range_result) {
          fail(completion, range_result.error().value());
        } else {
          auto permit = std::move(range_result).value();
          auto truncated = co_await blocking_->run(
              [fd = temporary_fd, length = parsed.request.length]() noexcept {
                TruncateLegacyResult result;
                errno = 0;
                result.truncate_result = orchfs_ftruncate(fd, length);
                result.truncate_error = errno;
                errno = 0;
                result.close_result = orchfs_close(fd);
                result.close_error = errno;
                return result;
              });
          if (!truncated) {
            fail(completion, truncated.error().value());
          } else {
            const auto result = std::move(truncated).value();
            if (result.close_result == 0) {
              temporary_fd = -1;
            }
            if (result.truncate_result != 0) {
              fail(completion, result.truncate_error != 0
                                   ? result.truncate_error
                                   : EIO);
            } else if (result.close_result != 0) {
              fail(completion,
                   result.close_error != 0 ? result.close_error : EIO);
            }
          }
          auto range_released = co_await permit.release();
          if (!range_released && completion.descriptor.status == 0) {
            fail(completion, range_released.error().value());
          }
        }
      }
    }

    if (temporary_fd >= 0) {
      auto closed = co_await blocking_->run([fd = temporary_fd]() noexcept {
        return call_legacy([&]() noexcept { return orchfs_close(fd); });
      });
      if (!closed && completion.descriptor.status == 0) {
        fail(completion, closed.error().value());
      } else if (closed && closed.value().value != 0 &&
                 completion.descriptor.status == 0) {
        fail(completion, closed.value().error != 0 ? closed.value().error
                                                   : EIO);
      }
    }
    auto released = co_await gate.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_truncate_handle(
      Completion completion, const ParsedRequest& parsed,
      const std::shared_ptr<HandleState>& handle) {
    if (!handle || handle->kind != HandleState::Kind::file) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }
    auto range = core_->range_for(handle->inode);
    auto range_result =
        co_await range->acquire(0, kWholeFileRangeLength, RangeMode::write);
    if (!range_result) {
      fail(completion, range_result.error().value());
    } else {
      auto permit = std::move(range_result).value();
      auto blocked = co_await blocking_->run(
          [fd = handle->fd, length = parsed.request.length]() noexcept {
            return call_legacy(
                [&]() noexcept { return orchfs_ftruncate(fd, length); });
          });
      if (!blocked) {
        fail(completion, blocked.error().value());
      } else if (const auto truncated = std::move(blocked).value();
                 truncated.value != 0) {
        fail(completion, truncated.error != 0 ? truncated.error : EIO);
      }
      auto released = co_await permit.release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
    }
    auto life_released = co_await life.release();
    if (!life_released && completion.descriptor.status == 0) {
      fail(completion, life_released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_open_directory_handle(
      Completion completion, const std::shared_ptr<HandleState>& source) {
    if (!source || source->kind != HandleState::Kind::file) {
      fail(completion, EBADF);
      co_return completion;
    }

    auto gate_result =
        co_await core_->namespace_gate().acquire(0, 1, RangeMode::read);
    if (!gate_result) {
      fail(completion, gate_result.error().value());
      co_return completion;
    }
    auto gate = std::move(gate_result).value();

    auto life_result =
        co_await source->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
    } else {
      auto life = std::move(life_result).value();
      if (source->closing.load(std::memory_order_acquire)) {
        fail(completion, EBADF);
      } else {
        auto blocked = co_await blocking_->run(
            [fd = source->fd, expected_inode = source->inode]() noexcept {
              errno = 0;
              DIR* directory = orchfs_fdopendir(fd);
              int saved_error = errno;
              if (directory == nullptr) {
                return OpenLegacyResult{.error = saved_error};
              }
              const int directory_fd = legacy_directory_fd(directory);
              if (directory_fd < 0) {
                saved_error = errno != 0 ? errno : EBADF;
                (void)orchfs_closedir(directory);
                return OpenLegacyResult{.error = saved_error};
              }
              errno = 0;
              const auto inode = orchfs_fd_inode_id(directory_fd);
              if (inode < 0 || inode != expected_inode) {
                saved_error = inode < 0 && errno != 0 ? errno : EIO;
                (void)orchfs_closedir(directory);
                return OpenLegacyResult{.error = saved_error};
              }
              return OpenLegacyResult{.fd = directory_fd,
                                      .inode = inode,
                                      .directory = directory,
                                      .error = 0};
            });
        if (!blocked) {
          fail(completion, blocked.error().value());
        } else {
          auto opened = std::move(blocked).value();
          if (opened.directory == nullptr) {
            fail(completion, opened.error != 0 ? opened.error : ENOTDIR);
          } else {
            completion.descriptor.result_length = insert_handle(
                opened.fd, opened.inode, HandleState::Kind::directory,
                opened.directory);
          }
        }
      }
      auto life_released = co_await life.release();
      if (!life_released && completion.descriptor.status == 0) {
        fail(completion, life_released.error().value());
      }
    }

    auto gate_released = co_await gate.release();
    if (!gate_released && completion.descriptor.status == 0) {
      fail(completion, gate_released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_open_directory(Completion completion,
                                     const std::string& path) {
    auto gate_result =
        co_await core_->namespace_gate().acquire(0, 1, RangeMode::read);
    if (!gate_result) {
      fail(completion, gate_result.error().value());
      co_return completion;
    }
    auto gate = std::move(gate_result).value();
    auto blocked = co_await blocking_->run([path]() noexcept {
      errno = 0;
      DIR* directory = orchfs_opendir(path.c_str());
      int saved_error = errno;
      if (directory == nullptr) {
        return OpenLegacyResult{.error = saved_error};
      }
      const int fd = legacy_directory_fd(directory);
      if (fd < 0) {
        saved_error = errno != 0 ? errno : EBADF;
        (void)orchfs_closedir(directory);
        return OpenLegacyResult{.error = saved_error};
      }
      errno = 0;
      const auto inode = orchfs_fd_inode_id(fd);
      if (inode < 0) {
        saved_error = errno != 0 ? errno : EIO;
        (void)orchfs_closedir(directory);
        return OpenLegacyResult{.error = saved_error};
      }
      return OpenLegacyResult{.fd = fd,
                              .inode = inode,
                              .directory = directory,
                              .error = 0};
    });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else {
      auto opened = std::move(blocked).value();
      if (opened.directory == nullptr) {
        fail(completion, opened.error != 0 ? opened.error : ENOENT);
      } else {
        completion.descriptor.result_length =
            insert_handle(opened.fd, opened.inode,
                          HandleState::Kind::directory, opened.directory);
      }
    }
    auto released = co_await gate.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_read_directory(
      Completion completion, const ParsedRequest& parsed,
      const std::shared_ptr<HandleState>& handle) {
    if (!handle || handle->kind != HandleState::Kind::directory ||
        parsed.request.length > config_.data_slot_size / sizeof(RpcDirEntry)) {
      fail(completion, handle ? EMSGSIZE : EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }
    auto offset_result =
        co_await handle->offset_gate.acquire(0, 1, RangeMode::write);
    if (!offset_result) {
      fail(completion, offset_result.error().value());
      auto released = co_await life.release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, released.error().value());
      }
      co_return completion;
    }
    auto offset = std::move(offset_result).value();
    auto blocked = co_await blocking_->run(
        [directory = handle->directory,
         count = parsed.request.length]() -> DirectoryBatchResult {
          DirectoryBatchResult batch;
          batch.payload.reserve(count * sizeof(RpcDirEntry));
          for (std::uint64_t index = 0; index < count; ++index) {
            errno = 0;
            struct dirent* entry = orchfs_readdir(directory);
            if (entry == nullptr) {
              batch.error = errno;
              batch.end_of_stream = batch.error == 0;
              break;
            }
            RpcDirEntry wire{};
            wire.inode = entry->d_ino;
            wire.offset = entry->d_off;
            wire.record_length = entry->d_reclen;
            wire.type = entry->d_type;
            const auto name_length = std::min<std::size_t>(
                std::strlen(entry->d_name), wire.name.size() - 1);
            wire.name_length = static_cast<std::uint8_t>(name_length);
            std::memcpy(wire.name.data(), entry->d_name, name_length);
            const auto old_size = batch.payload.size();
            batch.payload.resize(old_size + sizeof(wire));
            std::memcpy(batch.payload.data() + old_size, &wire, sizeof(wire));
            ++batch.count;
          }
          return batch;
        });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else {
      auto batch = std::move(blocked).value();
      if (batch.error != 0) {
        fail(completion, batch.error);
      } else {
        completion.payload = std::move(batch.payload);
        completion.descriptor.result_length = batch.count;
        completion.descriptor.payload_length =
            static_cast<std::uint32_t>(completion.payload.size());
        if (batch.end_of_stream) {
          completion.descriptor.flags |= DescriptorFlag::end_of_stream;
        }
        if (!completion.payload.empty()) {
          completion.descriptor.flags |= DescriptorFlag::has_payload;
        }
      }
    }
    auto offset_released = co_await offset.release();
    if (!offset_released && completion.descriptor.status == 0) {
      fail(completion, offset_released.error().value());
    }
    auto life_released = co_await life.release();
    if (!life_released && completion.descriptor.status == 0) {
      fail(completion, life_released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_sync(Completion completion,
                           const std::shared_ptr<HandleState>& handle) {
    if (!handle) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }
    auto blocked = co_await blocking_->run([fd = handle->fd]() noexcept {
      return call_legacy([&]() noexcept { return orchfs_fsync(fd); });
    });
    if (!blocked) {
      fail(completion, blocked.error().value());
    } else if (const auto synced = std::move(blocked).value();
               synced.value != 0) {
      fail(completion, synced.error != 0 ? synced.error : EIO);
    }
    auto released = co_await life.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<Completion> do_flags(Completion completion, const ParsedRequest& parsed,
                            const std::shared_ptr<HandleState>& handle) {
    if (!handle) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto life_result =
        co_await handle->lifecycle.acquire(0, 1, RangeMode::read);
    if (!life_result) {
      fail(completion, life_result.error().value());
      co_return completion;
    }
    auto life = std::move(life_result).value();
    if (handle->closing.load(std::memory_order_acquire)) {
      auto released = co_await life.release();
      fail(completion, released ? EBADF : released.error().value());
      co_return completion;
    }
    const auto requested = parsed.request.value;
    if (requested == -1) {
      completion.descriptor.result_length = static_cast<std::uint64_t>(
          handle->open_flags.load(std::memory_order_acquire));
    } else if (requested < std::numeric_limits<int>::min() ||
               requested > std::numeric_limits<int>::max()) {
      fail(completion, EINVAL);
    } else {
      // O_NONBLOCK is a valid F_SETFL bit but has no effect for regular files.
      // O_APPEND is the only mutable status flag with server-side behavior.
      constexpr int mutable_mask = O_APPEND | O_NONBLOCK;
      const int requested_flags = static_cast<int>(requested);
      int current = handle->open_flags.load(std::memory_order_acquire);
      int desired;
      do {
        desired = (current & ~mutable_mask) | (requested_flags & mutable_mask);
      } while (!handle->open_flags.compare_exchange_weak(
          current, desired, std::memory_order_acq_rel,
          std::memory_order_acquire));
    }
    auto released = co_await life.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, released.error().value());
    }
    co_return completion;
  }

  Task<int> cleanup_all_handles() {
    int first_error = 0;
    std::vector<std::pair<RemoteHandle, HandleState::Kind>> handles;
    {
      std::lock_guard lock(handles_mutex_);
      handles.reserve(handles_.size());
      for (const auto& [id, handle] : handles_) {
        handles.emplace_back(id, handle->kind);
      }
    }
    for (const auto& [id, kind] : handles) {
      Completion ignored;
      ignored = co_await do_close(std::move(ignored), id, kind);
      if (ignored.descriptor.status != 0 && first_error == 0) {
        first_error = -ignored.descriptor.status;
      }
    }
    co_return first_error;
  }

  Task<Completion> do_shutdown(Completion completion) {
    closing_.store(true, std::memory_order_release);
    cleanup_started_.store(true, std::memory_order_release);

    // shutdown_session is itself included in inflight_. No new requests are
    // accepted after closing_ is published, so reaching one gives cleanup a
    // stable and complete handle table without blocking a Runtime worker.
    while (inflight_.load(std::memory_order_acquire) > 1) {
      auto yielded = co_await Runtime::yield();
      if (!yielded) {
        fail(completion, yielded.error().value());
        break;
      }
    }
    if (completion.descriptor.status == 0) {
      const int error = co_await cleanup_all_handles();
      if (error != 0) {
        cleanup_error_.store(error, std::memory_order_release);
        fail(completion, error);
      }
    }
    cleanup_done_.store(true, std::memory_order_release);
    stop_after_flush_.store(true, std::memory_order_release);
    signal_result();
    co_return completion;
  }

  Task<Completion> do_raw_device(Completion completion,
                                 const ParsedRequest& parsed, Opcode opcode) {
    if (parsed.request.offset < 0 ||
        parsed.request.length > config_.data_slot_size) {
      fail(completion, EINVAL);
      co_return completion;
    }
    if (opcode == Opcode::raw_device_read) {
      try {
        completion.payload.resize(parsed.request.length);
      } catch (...) {
        fail(completion, ENOMEM);
      }
      if (completion.descriptor.status == 0) {
        auto blocked = co_await blocking_->run(
            [buffer = completion.payload.data(), length = parsed.request.length,
             offset = parsed.request.offset]() noexcept {
              read_data_from_devs(buffer,
                                  static_cast<std::int64_t>(length), offset);
              return 0;
            });
        if (!blocked) {
          fail(completion, blocked.error().value());
        } else {
          completion.descriptor.result_length = parsed.request.length;
          completion.descriptor.payload_length =
              static_cast<std::uint32_t>(completion.payload.size());
          completion.descriptor.flags |= DescriptorFlag::has_payload;
        }
      }
    } else if (opcode == Opcode::raw_device_write) {
      if (parsed.data.size() != parsed.request.length) {
        fail(completion, EINVAL);
      } else {
        auto blocked = co_await blocking_->run(
            [data = parsed.data, offset = parsed.request.offset]() noexcept {
              write_data_to_devs(const_cast<std::byte*>(data.data()),
                                 static_cast<std::int64_t>(data.size()), offset);
              return 0;
            });
        if (!blocked) {
          fail(completion, blocked.error().value());
        } else {
          completion.descriptor.result_length =
              static_cast<std::uint64_t>(parsed.data.size());
        }
      }
    } else {
      auto blocked = co_await blocking_->run([]() noexcept {
        return call_legacy([]() noexcept { return device_sync(); });
      });
      if (!blocked) {
        fail(completion, blocked.error().value());
      } else if (const auto flushed = std::move(blocked).value();
                 flushed.value != 0) {
        fail(completion, flushed.error != 0 ? flushed.error : EIO);
      }
    }
    co_return completion;
  }

  void signal_result() noexcept {
    const std::uint64_t one = 1;
    const auto result = ::write(result_event_fd_, &one, sizeof(one));
    (void)result;
  }

  void drain_result_event() noexcept {
    std::uint64_t value;
    while (::read(result_event_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
  }

  void start_background_cleanup() noexcept {
    bool expected = false;
    if (!cleanup_started_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
      return;
    }
    inflight_.fetch_add(1, std::memory_order_acq_rel);
    auto submitted =
        runtime_->submit(run_session_cleanup(shared_from_this()));
    if (!submitted) {
      inflight_.fetch_sub(1, std::memory_order_acq_rel);
      cleanup_error_.store(submitted.error().value(),
                           std::memory_order_release);
      cleanup_done_.store(true, std::memory_order_release);
      signal_result();
    }
  }

  void finish_background_cleanup(int error) noexcept {
    if (error != 0) {
      cleanup_error_.store(error, std::memory_order_release);
    }
    cleanup_done_.store(true, std::memory_order_release);
    inflight_.fetch_sub(1, std::memory_order_acq_rel);
    signal_result();
  }

  void submit_incoming(Incoming incoming) noexcept {
    bool reject = false;
    if (incoming.descriptor.opcode == Opcode::shutdown_session) {
      bool expected = false;
      if (stop_requested_.load(std::memory_order_acquire) ||
          !shutdown_requested_.compare_exchange_strong(
              expected, true, std::memory_order_acq_rel)) {
        reject = true;
      } else {
        closing_.store(true, std::memory_order_release);
      }
    } else if (closing_.load(std::memory_order_acquire)) {
      reject = true;
    }
    if (reject) {
      Completion completion{
          .lane = incoming.lane,
          .descriptor = incoming.descriptor,
          .payload = {},
      };
      completion.descriptor.flags = DescriptorFlag::response;
      fail(completion, ESHUTDOWN);
      {
        std::lock_guard lock(completion_mutex_);
        completions_.push_back(std::move(completion));
      }
      signal_result();
      return;
    }
    const auto lane = incoming.lane;
    const auto descriptor = incoming.descriptor;
    inflight_.fetch_add(1, std::memory_order_acq_rel);
    auto submitted = runtime_->submit(run_request(shared_from_this(),
                                                  std::move(incoming)));
    if (!submitted) {
      inflight_.fetch_sub(1, std::memory_order_acq_rel);
      Completion completion;
      completion.lane = lane;
      completion.descriptor = descriptor;
      completion.descriptor.flags = DescriptorFlag::response;
      fail(completion, ESHUTDOWN);
      std::lock_guard lock(completion_mutex_);
      completions_.push_back(std::move(completion));
      signal_result();
    }
  }

  void pump_submissions(std::uint32_t lane,
                        std::vector<std::byte>& buffer) noexcept {
    std::uint64_t notifications{};
    auto notify_error =
        transport_.drain_submission_notifications(lane, notifications);
    if (notify_error &&
        notify_error != make_error_code(TransportErrc::would_block)) {
      request_stop();
      return;
    }
    for (;;) {
      IpcDescriptor descriptor;
      std::size_t payload_size{};
      auto error = transport_.try_receive_submission(
          lane, descriptor, buffer, payload_size);
      if (error == make_error_code(TransportErrc::would_block)) {
        return;
      }
      if (error == make_error_code(TransportErrc::buffer_too_small)) {
        try {
          buffer.resize(payload_size);
        } catch (...) {
          request_stop();
          return;
        }
        continue;
      }
      if (error) {
        request_stop();
        return;
      }
      if (descriptor.client_id != transport_.client_id() ||
          descriptor.session_generation != transport_.session_generation() ||
          !has_flag(descriptor.flags, DescriptorFlag::request)) {
        Completion completion{
            .lane = lane, .descriptor = descriptor, .payload = {}};
        completion.descriptor.flags = DescriptorFlag::response;
        fail(completion, EPROTO);
        {
          std::lock_guard lock(completion_mutex_);
          completions_.push_back(std::move(completion));
        }
        signal_result();
        continue;
      }
      Incoming incoming;
      incoming.lane = lane;
      incoming.descriptor = descriptor;
      try {
        incoming.payload.resize(payload_size);
        if (payload_size != 0) {
          std::memcpy(incoming.payload.data(), buffer.data(), payload_size);
        }
      } catch (...) {
        Completion completion{
            .lane = lane, .descriptor = descriptor, .payload = {}};
        completion.descriptor.flags = DescriptorFlag::response;
        fail(completion, ENOMEM);
        {
          std::lock_guard lock(completion_mutex_);
          completions_.push_back(std::move(completion));
        }
        signal_result();
        continue;
      }
      submit_incoming(std::move(incoming));
    }
  }

  void pump_completions() noexcept {
    for (;;) {
      std::error_code error;
      {
        std::lock_guard lock(completion_mutex_);
        if (completions_.empty()) {
          return;
        }
        Completion& completion = completions_.front();
        completion.descriptor.payload_length =
            static_cast<std::uint32_t>(completion.payload.size());
        error = transport_.try_complete(
            completion.lane, completion.descriptor, completion.payload);
        if (error == make_error_code(TransportErrc::would_block)) {
          return;
        }
        completions_.pop_front();
      }
      if (error) {
        request_stop();
        return;
      }
    }
  }

  bool completions_empty() {
    std::lock_guard lock(completion_mutex_);
    return completions_.empty();
  }

  void discard_completions() noexcept {
    std::lock_guard lock(completion_mutex_);
    completions_.clear();
  }

  void io_loop() noexcept {
    std::vector<pollfd> poll_fds;
    std::vector<std::byte> buffer;
    try {
      poll_fds.reserve(config_.lane_count + 1);
      poll_fds.push_back({result_event_fd_, POLLIN, 0});
      for (std::uint32_t lane = 0; lane < config_.lane_count; ++lane) {
        poll_fds.push_back({transport_.submission_event_fd(lane), POLLIN, 0});
      }
      buffer.resize(config_.data_slot_size);
    } catch (...) {
      request_stop();
      cleanup_done_.store(true, std::memory_order_release);
      finished_.store(true, std::memory_order_release);
      return;
    }

    while (true) {
      bool forced_stop = stop_requested_.load(std::memory_order_acquire);
      if (!forced_stop) {
        for (std::uint32_t lane = 0; lane < config_.lane_count; ++lane) {
          pump_submissions(lane, buffer);
        }
        pump_completions();
      } else {
        discard_completions();
      }
      if (!transport_.peer_alive()) {
        closing_.store(true, std::memory_order_release);
        stop_requested_.store(true, std::memory_order_release);
      }
      forced_stop = stop_requested_.load(std::memory_order_acquire);
      if (forced_stop) {
        // An external stop or dead peer cannot consume a full CQ. Only the
        // graceful shutdown_session path waits for its response to be drained.
        discard_completions();
      }
      const bool stopping = forced_stop ||
                            stop_after_flush_.load(std::memory_order_acquire);
      if (stopping && inflight_.load(std::memory_order_acquire) == 0 &&
          !cleanup_done_.load(std::memory_order_acquire)) {
        start_background_cleanup();
      }
      if (stopping && cleanup_done_.load(std::memory_order_acquire) &&
          inflight_.load(std::memory_order_acquire) == 0) {
        if (forced_stop) {
          discard_completions();
          break;
        }
        if (completions_empty()) {
          break;
        }
      }
      const int result = ::poll(poll_fds.data(), poll_fds.size(), 5);
      if (result < 0 && errno != EINTR) {
        request_stop();
      }
      if (poll_fds[0].revents & POLLIN) {
        drain_result_event();
      }
      for (auto& fd : poll_fds) {
        fd.revents = 0;
      }
    }
    finished_.store(true, std::memory_order_release);
  }

  Runtime* runtime_;
  ServerTransport transport_;
  TransportConfig config_;
  std::shared_ptr<CoreCoordinator> core_;
  std::shared_ptr<BlockingExecutor> blocking_;
  int result_event_fd_{-1};
  std::thread thread_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> stop_after_flush_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> shutdown_requested_{false};
  std::atomic<bool> cleanup_started_{false};
  std::atomic<bool> cleanup_done_{false};
  std::atomic<bool> finished_{false};
  std::atomic<int> cleanup_error_{0};
  std::atomic<std::size_t> inflight_{0};
  std::atomic<RemoteHandle> next_handle_{1};

  std::mutex handles_mutex_;
  std::unordered_map<RemoteHandle, std::shared_ptr<HandleState>> handles_;

  std::mutex completion_mutex_;
  std::deque<Completion> completions_;

  friend Task<void> run_request(std::shared_ptr<ServerSession>, Incoming);
  friend Task<void> run_session_cleanup(std::shared_ptr<ServerSession>);
};

Task<void> run_request(std::shared_ptr<ServerSession> session,
                       Incoming incoming) {
  auto completion = co_await session->dispatch(std::move(incoming));
  session->enqueue_completion(std::move(completion));
  co_return;
}

Task<void> run_session_cleanup(std::shared_ptr<ServerSession> session) {
  const int error = co_await session->cleanup_all_handles();
  session->finish_background_cleanup(error);
  co_return;
}

}  // namespace

class Server::Impl {
 public:
  Impl(Runtime& runtime, ServerOptions options, ControlServer listener)
      : runtime_(&runtime), options_(std::move(options)),
        listener_(std::move(listener)), core_(std::make_shared<CoreCoordinator>()),
        blocking_(std::make_shared<BlockingExecutor>(
            options_.blocking_worker_count)),
        stop_event_fd_(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) {
    if (stop_event_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "eventfd");
    }
  }

  ~Impl() {
    if (accept_thread_.joinable()) {
      (void)join();
    }
    ::close(stop_event_fd_);
  }

  void start() {
    accept_thread_ = std::thread([this] { accept_loop(); });
  }

  void request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    constexpr std::uint64_t one = 1;
    ssize_t written = -1;
    do {
      written = ::write(stop_event_fd_, &one, sizeof(one));
    } while (written < 0 && errno == EINTR);
    std::vector<std::shared_ptr<ServerSession>> sessions;
    {
      std::lock_guard lock(sessions_mutex_);
      sessions = sessions_;
    }
    for (auto& session : sessions) {
      session->request_stop();
    }
  }

  Result<void> join() {
    request_stop();
    if (accept_thread_.joinable()) {
      accept_thread_.join();
    }
    std::vector<std::shared_ptr<ServerSession>> sessions;
    {
      std::lock_guard lock(sessions_mutex_);
      sessions.swap(sessions_);
    }
    for (auto& session : sessions) {
      session->join();
      record_cleanup_error(session->cleanup_error());
    }
    const int cleanup_error = cleanup_error_.load(std::memory_order_acquire);
    if (cleanup_error != 0) {
      return Result<void>::failure(
          std::error_code(cleanup_error, std::generic_category()));
    }
    return Result<void>::success();
  }

 private:
  void record_cleanup_error(std::error_code error) noexcept {
    if (!error) {
      return;
    }
    int expected = 0;
    (void)cleanup_error_.compare_exchange_strong(
        expected, error.value(), std::memory_order_acq_rel);
  }

  void reap_finished_sessions() noexcept {
    for (;;) {
      std::shared_ptr<ServerSession> retired;
      {
        std::lock_guard lock(sessions_mutex_);
        auto session = std::find_if(
            sessions_.begin(), sessions_.end(),
            [](const auto& candidate) { return candidate->finished(); });
        if (session == sessions_.end()) {
          return;
        }
        retired = std::move(*session);
        sessions_.erase(session);
      }
      retired->join();
      record_cleanup_error(retired->cleanup_error());
    }
  }

  void accept_loop() noexcept {
    while (!stop_requested_.load(std::memory_order_acquire)) {
      reap_finished_sessions();
      std::error_code error;
      auto transport = listener_.accept(error, stop_event_fd_);
      if (error || !transport) {
        if (stop_requested_.load(std::memory_order_acquire)) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }
      try {
        auto session = std::make_shared<ServerSession>(
            *runtime_, std::move(transport), core_, blocking_);
        session->start();
        bool published = false;
        {
          std::lock_guard lock(sessions_mutex_);
          if (!stop_requested_.load(std::memory_order_acquire)) {
            sessions_.push_back(session);
            published = true;
          }
        }
        if (!published) {
          session->request_stop();
          session->join();
          record_cleanup_error(session->cleanup_error());
          break;
        }
      } catch (...) {
        // The listener remains available for other clients. Allocation and
        // thread creation failures are reflected by dropping this session.
      }
    }
  }

  Runtime* runtime_;
  ServerOptions options_;
  ControlServer listener_;
  std::shared_ptr<CoreCoordinator> core_;
  std::shared_ptr<BlockingExecutor> blocking_;
  int stop_event_fd_{-1};
  std::atomic<bool> stop_requested_{false};
  std::atomic<int> cleanup_error_{0};
  std::thread accept_thread_;
  std::mutex sessions_mutex_;
  std::vector<std::shared_ptr<ServerSession>> sessions_;
};

Server::Server(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

Server::Server(Server&&) noexcept = default;
Server& Server::operator=(Server&&) noexcept = default;

Server::~Server() {
  if (impl_) {
    (void)impl_->join();
  }
}

Result<std::unique_ptr<Server>> Server::start(Runtime& runtime,
                                              ServerOptions options) {
  if (options.lane_count == 0) {
    options.lane_count = runtime.worker_count();
  }
  if (options.blocking_worker_count == 0) {
    options.blocking_worker_count = runtime.worker_count();
  }
  if (options.lane_count == 0 || options.lane_count > kMaxIpcWorkerLanes ||
      options.blocking_worker_count == 0 ||
      options.ring_capacity == 0 || options.data_slot_size <= sizeof(RpcRequest) ||
      options.lane_count > std::numeric_limits<std::uint32_t>::max() ||
      options.ring_capacity > std::numeric_limits<std::uint32_t>::max() ||
      options.data_slot_size > std::numeric_limits<std::uint32_t>::max()) {
    return Result<std::unique_ptr<Server>>::failure(
        std::make_error_code(std::errc::invalid_argument));
  }
  TransportConfig config{
      .lane_count = static_cast<std::uint32_t>(options.lane_count),
      .ring_capacity = static_cast<std::uint32_t>(options.ring_capacity),
      .data_slot_size = static_cast<std::uint32_t>(options.data_slot_size),
  };
  std::error_code error;
  auto listener = ControlServer::listen(options.endpoint, config, error);
  if (error) {
    return Result<std::unique_ptr<Server>>::failure(error);
  }
  try {
    auto impl = std::make_unique<Impl>(runtime, std::move(options),
                                       std::move(listener));
    impl->start();
    return Result<std::unique_ptr<Server>>::success(
        std::unique_ptr<Server>(new Server(std::move(impl))));
  } catch (const std::bad_alloc&) {
    return Result<std::unique_ptr<Server>>::failure(
        std::make_error_code(std::errc::not_enough_memory));
  } catch (const std::system_error& thread_error) {
    return Result<std::unique_ptr<Server>>::failure(thread_error.code());
  } catch (...) {
    std::terminate();
  }
}

void Server::request_stop() noexcept {
  if (impl_) {
    impl_->request_stop();
  }
}

Result<void> Server::join() {
  return impl_ ? impl_->join() : Result<void>::success();
}

}  // namespace orchfs::async
