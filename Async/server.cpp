#include "orchfs/async/server.hpp"

#include "orchfs/async/block_device.hpp"
#include "orchfs/async/detail/concurrency.hpp"
#include "orchfs/async/detail/range_lock.hpp"
#include "orchfs/async/detail/stat_conversion.hpp"
#include "orchfs/async/filesystem.hpp"
#include "orchfs/async/ipc_transport.hpp"
#include "orchfs/async/range_arbiter.hpp"
#include "orchfs/async/rpc_protocol.hpp"
#include "orchfs/async/runtime.hpp"

#include "../KernelFS/async_device.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

namespace orchfs::async {
namespace {

constexpr std::uint64_t kDirectoryCursorInitial = 2U * 256U;
constexpr std::uint64_t kNotDirectoryCursor =
    std::numeric_limits<std::uint64_t>::max();
constexpr std::uint64_t kWholeHandleRange =
    std::numeric_limits<std::uint64_t>::max();

constexpr std::uint64_t opcode_bit(Opcode opcode) noexcept {
  return std::uint64_t{1} << static_cast<unsigned>(opcode);
}

constexpr std::uint64_t kHandleRequiredOpcodes =
    opcode_bit(Opcode::read) | opcode_bit(Opcode::write) |
    opcode_bit(Opcode::read_at) | opcode_bit(Opcode::write_at) |
    opcode_bit(Opcode::seek) | opcode_bit(Opcode::stat_handle) |
    opcode_bit(Opcode::statfs) | opcode_bit(Opcode::truncate_handle) |
    opcode_bit(Opcode::open_directory_handle) |
    opcode_bit(Opcode::read_directory_batch) | opcode_bit(Opcode::sync) |
    opcode_bit(Opcode::set_flags);

static_assert(static_cast<unsigned>(Opcode::open_directory_handle) <
              std::numeric_limits<std::uint64_t>::digits);

constexpr bool opcode_requires_handle(Opcode opcode) noexcept {
  const auto value = static_cast<unsigned>(opcode);
  return value < std::numeric_limits<std::uint64_t>::digits &&
         (kHandleRequiredOpcodes & (std::uint64_t{1} << value)) != 0;
}

struct ParsedRequest {
  RpcRequest request;
  std::string path1;
  std::string path2;
  std::span<const std::byte> data;
};

Result<ParsedRequest> parse_request(std::span<const std::byte> payload) {
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
  parsed.data = std::span<const std::byte>(cursor,
                                           parsed.request.data_length);
  if (parsed.path1.find('\0') != std::string::npos ||
      parsed.path2.find('\0') != std::string::npos) {
    return Result<ParsedRequest>::failure(
        std::make_error_code(std::errc::invalid_argument));
  }
  return Result<ParsedRequest>::success(std::move(parsed));
}

std::string_view path_parent(std::string_view path) noexcept {
  const auto slash = path.find_last_of('/');
  if (slash == std::string_view::npos) {
    return ".";
  }
  if (slash == 0) {
    return "/";
  }
  return path.substr(0, slash);
}

int validate_path(std::string_view path) noexcept {
  if (path.empty()) {
    return ENOENT;
  }
  if (path.size() > std::numeric_limits<std::uint32_t>::max()) {
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

int validate_io_range(std::uint64_t offset, std::uint64_t length) noexcept {
  constexpr auto maximum =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  return offset > maximum || length > maximum - offset ? EOVERFLOW : 0;
}

RpcFileStat to_wire(const FileStat& value) noexcept {
  return detail::copy_file_stat_fields<RpcFileStat>(value);
}

RpcStatFs to_wire(const FileSystemStat& value) noexcept {
  return detail::copy_filesystem_stat_fields<RpcStatFs>(value);
}

struct Completion {
  std::uint32_t lane{};
  IpcDescriptor descriptor;
  std::vector<std::byte> payload;
  CompletionReservation reservation;
  std::size_t filled_payload_size{};
};

struct Incoming {
  std::uint32_t lane{};
  IpcDescriptor descriptor;
  std::span<const std::byte> payload;
  ReceivedIpcSlot submission;
  CompletionReservation completion;
};

class DetachedTask final {
 public:
  struct promise_type {
    static void* operator new(std::size_t size) {
      return detail::allocate_coroutine_frame(size);
    }
    static void operator delete(void* frame) noexcept {
      detail::deallocate_coroutine_frame(frame);
    }
    static void operator delete(void* frame, std::size_t) noexcept {
      detail::deallocate_coroutine_frame(frame);
    }

    DetachedTask get_return_object() noexcept {
      return DetachedTask(
          std::coroutine_handle<promise_type>::from_promise(*this));
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }
    std::suspend_never final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    [[noreturn]] void unhandled_exception() const noexcept {
      std::terminate();
    }
  };

  DetachedTask(const DetachedTask&) = delete;
  DetachedTask& operator=(const DetachedTask&) = delete;
  DetachedTask(DetachedTask&& other) noexcept
      : coroutine_(std::exchange(other.coroutine_, {})) {}
  ~DetachedTask() {
    if (coroutine_) {
      coroutine_.destroy();
    }
  }

  std::coroutine_handle<> release() noexcept {
    return std::exchange(coroutine_, {});
  }

 private:
  explicit DetachedTask(
      std::coroutine_handle<promise_type> coroutine) noexcept
      : coroutine_(coroutine) {}

  std::coroutine_handle<promise_type> coroutine_{};
};

struct HandleState {
  InodeNumber inode{-1};
  NodeType type{NodeType::unknown};
  std::uint64_t offset{};
  std::uint64_t directory_cursor{kNotDirectoryCursor};
  std::atomic<int> open_flags{O_RDONLY};
  std::atomic<bool> closing{false};
  RangeArbiter lifecycle_gate;
  RangeArbiter offset_gate;

  [[nodiscard]] bool is_directory_cursor() const noexcept {
    return directory_cursor != kNotDirectoryCursor;
  }
};

class ServerSession;
DetachedTask run_request(std::shared_ptr<ServerSession> session,
                         Incoming incoming);
Task<void> run_session_cleanup(std::shared_ptr<ServerSession> session);

class ServerSession final : public std::enable_shared_from_this<ServerSession> {
  using HandleMap =
      std::unordered_map<RemoteHandle, std::shared_ptr<HandleState>>;

  struct CompletionNode {
    Completion completion;
    CompletionNode* next{};
  };

  class CompletionQueue final {
   public:
    explicit CompletionQueue(std::size_t capacity) : slots_(capacity) {
      if (capacity == 0) {
        throw std::invalid_argument("completion queue capacity");
      }
    }

    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

    Completion& front() noexcept {
      if (empty() || !slots_[head_]) {
        std::terminate();
      }
      return *slots_[head_];
    }

    void push_back(Completion completion) noexcept {
      if (size_ == slots_.size() || slots_[tail_]) {
        std::terminate();
      }
      slots_[tail_].emplace(std::move(completion));
      tail_ = (tail_ + 1U) % slots_.size();
      ++size_;
    }

    void pop_front() noexcept {
      if (empty() || !slots_[head_]) {
        std::terminate();
      }
      slots_[head_].reset();
      head_ = (head_ + 1U) % slots_.size();
      --size_;
    }

    void clear() noexcept {
      while (!empty()) {
        pop_front();
      }
    }

   private:
    std::vector<std::optional<Completion>> slots_;
    std::size_t head_{};
    std::size_t tail_{};
    std::size_t size_{};
  };

  struct LaneState {
    LaneState(ServerSession& session_value, std::uint32_t lane_value,
              std::size_t owner_value, std::size_t ring_capacity)
        : session(&session_value), lane(lane_value), owner(owner_value),
          completions(ring_capacity) {}

    ServerSession* session;
    std::uint32_t lane;
    std::size_t owner;
    Runtime::PollRegistration registration;
    detail::MpscInbox<CompletionNode> completion_inbox;
    CompletionQueue completions;
    std::atomic<std::size_t> inflight{0};
    std::atomic<bool> drained{false};
  };

 public:
  using RetirementCallback = void (*)(void*, ServerSession*) noexcept;

  ServerSession(Runtime& runtime, ServerTransport transport,
                std::shared_ptr<AsyncFilesystem> filesystem,
                void* retirement_context,
                RetirementCallback retirement_callback,
                std::atomic<std::size_t>& active_dma_regions,
                std::atomic<std::size_t>& dma_registrations,
                std::atomic<std::size_t>& dma_unregistrations)
      : runtime_(&runtime),
        transport_(std::move(transport)),
        config_(transport_.config()),
        filesystem_(std::move(filesystem)),
        control_owner_(runtime.owner_for(transport_.client_id())),
        retirement_context_(retirement_context),
        retirement_callback_(retirement_callback),
        active_dma_regions_(&active_dma_regions),
        dma_registrations_(&dma_registrations),
        dma_unregistrations_(&dma_unregistrations) {
    lanes_.reserve(config_.lane_count);
    for (std::uint32_t lane = 0; lane < config_.lane_count; ++lane) {
      lanes_.push_back(std::make_unique<LaneState>(
          *this, lane, lane % runtime.worker_count(), config_.ring_capacity));
    }
  }

  ~ServerSession() {
    request_stop();
    join();
  }

  std::error_code start() noexcept {
    dma_region_ = transport_.shared_memory_region();
    if (dma_region_.hugepage_backed) {
      const int error = orchfs_device_register_dma_region(
          dma_region_.address, dma_region_.size);
      if (error != 0) {
        return {error, std::generic_category()};
      }
      dma_registered_.store(true, std::memory_order_release);
      active_dma_regions_->fetch_add(1, std::memory_order_relaxed);
      dma_registrations_->fetch_add(1, std::memory_order_relaxed);
    }
    for (auto& lane : lanes_) {
      auto registration = runtime_->register_poller(
          lane->owner, &ServerSession::poll_lane, lane.get());
      if (!registration) {
        const auto error = registration.error();
        for (auto& registered_lane : lanes_) {
          registered_lane->registration.reset();
        }
        return error;
      }
      lane->registration = std::move(registration).value();
    }
    auto registration = runtime_->register_poller(
        control_owner_, &ServerSession::poll_control, this);
    if (!registration) {
      const auto error = registration.error();
      for (auto& lane : lanes_) {
        lane->registration.reset();
      }
      return error;
    }
    control_registration_ = std::move(registration).value();
    started_.store(true, std::memory_order_release);
    return {};
  }

  void request_stop() noexcept {
    closing_.store(true, std::memory_order_release);
    stop_requested_.store(true, std::memory_order_release);
    (void)runtime_->notify(control_owner_);
    for (const auto& lane : lanes_) {
      (void)runtime_->notify(lane->owner);
    }
  }

  void join() noexcept {
    if (!started_.load(std::memory_order_acquire)) {
      control_registration_.reset();
      for (auto& lane : lanes_) {
        lane->registration.reset();
      }
      unregister_dma_region();
      return;
    }
    while (!finished_.load(std::memory_order_acquire)) {
      finished_.wait(false, std::memory_order_acquire);
    }
    control_registration_.reset();
    for (auto& lane : lanes_) {
      lane->registration.reset();
    }
    unregister_dma_region();
  }

  [[nodiscard]] bool finished() const noexcept {
    return finished_.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::error_code cleanup_error() const noexcept {
    const int error = cleanup_error_.load(std::memory_order_acquire);
    return error == 0 ? std::error_code{}
                      : std::error_code(error, std::generic_category());
  }

  void set_retirement_next(ServerSession* next) noexcept {
    retirement_next_.store(next, std::memory_order_relaxed);
  }

  [[nodiscard]] ServerSession* retirement_next() const noexcept {
    return retirement_next_.load(std::memory_order_relaxed);
  }

  void unregister_dma_region() noexcept {
    if (!dma_registered_.exchange(false, std::memory_order_acq_rel)) {
      return;
    }
    const int error = orchfs_device_unregister_dma_region(
        dma_region_.address, dma_region_.size);
    if (error != 0) {
      int expected = 0;
      (void)cleanup_error_.compare_exchange_strong(
          expected, error, std::memory_order_acq_rel);
      return;
    }
    active_dma_regions_->fetch_sub(1, std::memory_order_relaxed);
    dma_unregistrations_->fetch_add(1, std::memory_order_relaxed);
  }

  Task<Completion> dispatch(Incoming incoming) {
    Completion completion{
        .lane = incoming.lane,
        .descriptor = incoming.descriptor,
        .reservation = std::move(incoming.completion),
    };
    completion.descriptor.flags = DescriptorFlag::response;
    completion.descriptor.status = 0;

    auto parsed_result = parse_request(incoming.payload);
    if (!parsed_result) {
      fail(completion, parsed_result.error().value());
      co_return completion;
    }
    ParsedRequest parsed = std::move(parsed_result).value();

    std::shared_ptr<HandleState> handle;
    if (parsed.request.handle != kInvalidRemoteHandle) {
      handle = find_handle(parsed.request.handle);
    }

    std::size_t owner = 0;
    const bool worker_zero_operation =
        incoming.descriptor.opcode == Opcode::open_at ||
        incoming.descriptor.opcode == Opcode::open_directory_handle ||
        incoming.descriptor.opcode == Opcode::close ||
        incoming.descriptor.opcode == Opcode::close_directory;
    if (handle && !worker_zero_operation) {
      owner = runtime_->owner_for(static_cast<std::uint64_t>(handle->inode));
    } else if (incoming.descriptor.opcode == Opcode::raw_device_read ||
               incoming.descriptor.opcode == Opcode::raw_device_write) {
      owner = runtime_->owner_for(parsed.request.offset / (32U * 1024U));
    }
    if (Runtime::current_worker() != owner) {
      auto scheduled = co_await runtime_->schedule_on(owner);
      if (!scheduled) {
        fail(completion, scheduled.error().value());
        co_return completion;
      }
    }
    if (!handle && opcode_requires_handle(incoming.descriptor.opcode)) {
      fail(completion, EBADF);
      co_return completion;
    }

    switch (incoming.descriptor.opcode) {
      case Opcode::connect:
      case Opcode::ping:
        co_return completion;
      case Opcode::open:
        co_return co_await do_open(std::move(completion), parsed, nullptr);
      case Opcode::open_at:
        if (!handle && !parsed.path1.empty() && parsed.path1.front() != '/') {
          fail(completion, EBADF);
          co_return completion;
        }
        co_return co_await do_open(
            std::move(completion), parsed,
            !parsed.path1.empty() && parsed.path1.front() == '/'
                ? std::shared_ptr<HandleState>{}
                : handle);
      case Opcode::close:
        co_return co_await do_close(std::move(completion),
                                    parsed.request.handle, false);
      case Opcode::read:
      case Opcode::read_at:
        co_return co_await do_read(
            std::move(completion), parsed, handle,
            incoming.descriptor.opcode == Opcode::read);
      case Opcode::write:
      case Opcode::write_at:
        co_return co_await do_write(
            std::move(completion), parsed, handle,
            incoming.descriptor.opcode == Opcode::write);
      case Opcode::seek:
        co_return co_await do_seek(std::move(completion), parsed, handle);
      case Opcode::stat_path:
        co_return co_await do_stat_path(std::move(completion), parsed.path1);
      case Opcode::stat_handle:
      case Opcode::statfs:
        co_return co_await do_handle_stat(
            std::move(completion), handle, incoming.descriptor.opcode);
      case Opcode::mkdir:
      case Opcode::rmdir:
      case Opcode::unlink:
      case Opcode::rename:
        co_return co_await do_path_mutation(
            std::move(completion), parsed, incoming.descriptor.opcode);
      case Opcode::truncate_path:
        co_return co_await do_truncate_path(std::move(completion), parsed);
      case Opcode::truncate_handle:
        co_return co_await do_truncate_handle(std::move(completion), parsed,
                                              handle);
      case Opcode::open_directory:
        co_return co_await do_open_directory(std::move(completion),
                                             parsed.path1);
      case Opcode::open_directory_handle:
        co_return co_await do_open_directory_handle(std::move(completion),
                                                    handle);
      case Opcode::read_directory_batch:
        co_return co_await do_read_directory(std::move(completion), parsed,
                                             handle);
      case Opcode::close_directory:
        co_return co_await do_close(std::move(completion),
                                    parsed.request.handle, true);
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
      default:
        fail(completion, ENOTSUP);
        co_return completion;
    }
  }

  void enqueue_completion(Completion completion) noexcept {
    const auto lane = completion.lane;
    queue_completion(std::move(completion));
    lanes_[lane]->inflight.fetch_sub(1, std::memory_order_acq_rel);
    inflight_.fetch_sub(1, std::memory_order_acq_rel);
    (void)runtime_->notify(lanes_[lane]->owner);
    (void)runtime_->notify(control_owner_);
  }

 private:
  static void fail(Completion& completion, int error) noexcept {
    completion.descriptor.status = -detail::errno_error(error).value();
    completion.descriptor.result_length = 0;
    completion.payload.clear();
    completion.filled_payload_size = 0;
  }

  static int error_number(std::error_code error) noexcept {
    return detail::errno_error(error.value()).value();
  }

  struct CompletionRangeLockPolicy {
    [[nodiscard, gnu::always_inline]] static Completion early_failure(
        Completion&& completion, std::error_code error) noexcept {
      ServerSession::fail(completion, ServerSession::error_number(error));
      return std::move(completion);
    }

    [[nodiscard, gnu::always_inline]] static bool succeeded(
        const Completion& completion) noexcept {
      return completion.descriptor.status == 0;
    }

    [[gnu::always_inline]] static void apply_failure(
        Completion& completion, std::error_code error) noexcept {
      ServerSession::fail(completion, ServerSession::error_number(error));
    }
  };

  template <typename Schedule, typename Operation, typename Finish>
  [[gnu::always_inline]] Task<Completion> with_completion_range_lock(
      Completion completion, Schedule schedule, RangeArbiter& gate,
      std::uint64_t offset, std::uint64_t length, RangeMode mode,
      Operation operation, Finish finish) {
    return detail::with_range_lock(
        std::move(schedule), gate, offset, length, mode,
        std::move(completion), std::move(operation),
        [finish = std::move(finish)](
            Completion&& locked_completion, auto&& result) mutable {
          std::invoke(finish, locked_completion,
                      std::forward<decltype(result)>(result));
          return std::move(locked_completion);
        },
        CompletionRangeLockPolicy{});
  }

  template <typename Schedule, typename Operation>
  [[gnu::always_inline]] Task<Completion> with_completion_range_lock(
      Completion completion, Schedule schedule, RangeArbiter& gate,
      std::uint64_t offset, std::uint64_t length, RangeMode mode,
      Operation operation) {
    return with_completion_range_lock(
        std::move(completion), std::move(schedule), gate, offset, length,
        mode, std::move(operation),
        [](Completion& locked_completion, auto&& result) {
          if (!result) {
            ServerSession::fail(
                locked_completion,
                ServerSession::error_number(result.error()));
          }
        });
  }

  Runtime::ScheduleOnAwaiter schedule_handle_owner(
      const HandleState& handle) noexcept {
    // schedule_on also marks a frame that is already on this worker as owned.
    // That prevents a contended RangeArbiter handoff from relocating the
    // operation to the worker that happened to release the preceding permit.
    return runtime_->schedule_on(
        runtime_->owner_for(static_cast<std::uint64_t>(handle.inode)));
  }

  void queue_completion(Completion completion) noexcept {
    if (completion.lane >= lanes_.size()) {
      std::terminate();
    }
    auto& lane = *lanes_[completion.lane];
    if (Runtime::current() == runtime_ &&
        Runtime::current_worker() == lane.owner) {
      lane.completions.push_back(std::move(completion));
      return;
    }
    auto* node = new (std::nothrow) CompletionNode{
        .completion = std::move(completion),
    };
    if (node == nullptr) {
      std::terminate();
    }
    lane.completion_inbox.push(*node);
    (void)runtime_->notify(lane.owner);
  }

  bool drain_completion_inbox(LaneState& lane) noexcept {
    if (lane.completion_inbox.empty()) {
      return false;
    }
    CompletionNode* fifo = lane.completion_inbox.drain();
    const bool progress = fifo != nullptr;
    while (fifo != nullptr) {
      CompletionNode* next = fifo->next;
      lane.completions.push_back(std::move(fifo->completion));
      delete fifo;
      fifo = next;
    }
    return progress;
  }

  std::shared_ptr<HandleState> find_handle(RemoteHandle id) const noexcept {
    const auto handles = handles_.load(std::memory_order_acquire);
    const auto found = handles->find(id);
    if (found == handles->end() ||
        found->second->closing.load(std::memory_order_acquire)) {
      return {};
    }
    return found->second;
  }

  RemoteHandle add_handle(OpenedNode node, int flags,
                          bool directory_cursor) {
    auto state = std::make_shared<HandleState>();
    state->inode = node.inode;
    state->type = node.type;
    state->open_flags.store(flags, std::memory_order_relaxed);
    state->directory_cursor = directory_cursor ? kDirectoryCursorInitial
                                               : kNotDirectoryCursor;
    const RemoteHandle id =
        next_handle_.fetch_add(1, std::memory_order_relaxed);
    auto current = handles_.load(std::memory_order_acquire);
    for (;;) {
      auto updated = std::make_shared<HandleMap>(*current);
      updated->emplace(id, state);
      std::shared_ptr<const HandleMap> published = std::move(updated);
      if (handles_.compare_exchange_weak(
              current, std::move(published), std::memory_order_release,
              std::memory_order_acquire)) {
        return id;
      }
    }
  }

  std::shared_ptr<HandleState> mark_closing(RemoteHandle id,
                                            bool directory_cursor) noexcept {
    const auto handles = handles_.load(std::memory_order_acquire);
    const auto found = handles->find(id);
    if (found == handles->end() ||
        found->second->is_directory_cursor() != directory_cursor) {
      return {};
    }
    bool expected = false;
    if (!found->second->closing.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
      return {};
    }
    return found->second;
  }

  void erase_handle(RemoteHandle id) {
    auto current = handles_.load(std::memory_order_acquire);
    for (;;) {
      if (!current->contains(id)) {
        return;
      }
      auto updated = std::make_shared<HandleMap>(*current);
      updated->erase(id);
      std::shared_ptr<const HandleMap> published = std::move(updated);
      if (handles_.compare_exchange_weak(
              current, std::move(published), std::memory_order_release,
              std::memory_order_acquire)) {
        return;
      }
    }
  }

  Task<Completion> do_open(Completion completion, const ParsedRequest& parsed,
                           const std::shared_ptr<HandleState>& directory) {
    if (const int error = validate_path(parsed.path1); error != 0) {
      fail(completion, error);
      co_return completion;
    }
    if (const int error = validate_open_flags(parsed.request.open_flags);
        error != 0) {
      fail(completion, error);
      co_return completion;
    }
    if (directory && directory->type != NodeType::directory) {
      fail(completion, ENOTDIR);
      co_return completion;
    }
    if (directory) {
      auto scheduled = co_await runtime_->schedule_on(0);
      if (!scheduled) {
        fail(completion, error_number(scheduled.error()));
        co_return completion;
      }
    }

    std::optional<RangePermit> life;
    if (directory) {
      auto acquired = co_await directory->lifecycle_gate.acquire(
          0, kWholeHandleRange, RangeMode::read);
      if (!acquired) {
        fail(completion, error_number(acquired.error()));
        co_return completion;
      }
      life.emplace(std::move(acquired).value());
    }

    auto opened = directory
                      ? co_await filesystem_->open_at(
                            directory->inode, parsed.path1,
                            parsed.request.open_flags, parsed.request.mode)
                      : co_await filesystem_->open(
                            parsed.path1, parsed.request.open_flags,
                            parsed.request.mode);
    if (!opened) {
      fail(completion, error_number(opened.error()));
    } else if (opened.value().inode < 0 ||
               opened.value().type == NodeType::unknown) {
      fail(completion, EIO);
#ifdef O_DIRECTORY
    } else if ((parsed.request.open_flags & O_DIRECTORY) != 0 &&
               opened.value().type != NodeType::directory) {
      auto ignored = co_await filesystem_->close(opened.value());
      (void)ignored;
      fail(completion, ENOTDIR);
#endif
    } else {
      completion.descriptor.result_length = add_handle(
          opened.value(), stored_open_flags(parsed.request.open_flags), false);
    }

    if (life) {
      auto released = co_await life->release();
      if (!released && completion.descriptor.status == 0) {
        fail(completion, error_number(released.error()));
      }
    }
    co_return completion;
  }

  Task<Completion> do_close(Completion completion, RemoteHandle id,
                            bool directory_cursor) {
    auto scheduled = co_await runtime_->schedule_on(0);
    if (!scheduled) {
      fail(completion, error_number(scheduled.error()));
      co_return completion;
    }
    auto handle = mark_closing(id, directory_cursor);
    if (!handle) {
      fail(completion, EBADF);
      co_return completion;
    }
    auto acquired = co_await handle->lifecycle_gate.acquire(
        0, kWholeHandleRange, RangeMode::write);
    if (!acquired) {
      handle->closing.store(false, std::memory_order_release);
      fail(completion, error_number(acquired.error()));
      co_return completion;
    }
    auto permit = std::move(acquired).value();
    const OpenedNode node{.inode = handle->inode, .type = handle->type};
    auto closed = directory_cursor
                      ? co_await filesystem_->close_directory(node)
                      : co_await filesystem_->close(node);
    if (!closed) {
      handle->closing.store(false, std::memory_order_release);
      fail(completion, error_number(closed.error()));
    } else {
      erase_handle(id);
    }
    auto released = co_await permit.release();
    if (!released && completion.descriptor.status == 0) {
      fail(completion, error_number(released.error()));
    }
    co_return completion;
  }

  Task<Completion> do_read(Completion completion, const ParsedRequest& parsed,
                           const std::shared_ptr<HandleState>& handle,
                           bool implicit) {
    if (handle->type != NodeType::regular || handle->is_directory_cursor()) {
      fail(completion, EISDIR);
      co_return completion;
    }
    if ((handle->open_flags.load(std::memory_order_acquire) & O_ACCMODE) ==
        O_WRONLY) {
      fail(completion, EBADF);
      co_return completion;
    }
    if (parsed.request.length > config_.data_slot_size ||
        parsed.request.length > std::numeric_limits<std::size_t>::max()) {
      fail(completion, EMSGSIZE);
      co_return completion;
    }
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, &parsed, handle, implicit](
            Completion& locked_completion) -> Task<Result<void>> {
          const auto read_at_offset =
              [this, &parsed, handle, implicit, &locked_completion](
                  std::uint64_t offset) -> Task<Result<void>> {
            if (!implicit && parsed.request.offset < 0) {
              co_return detail::errno_failure<void>(EINVAL);
            }
            if (const int error =
                    validate_io_range(offset, parsed.request.length);
                error != 0) {
              co_return detail::errno_failure<void>(error);
            }
            auto destination = locked_completion.reservation.payload().first(
                static_cast<std::size_t>(parsed.request.length));
            auto read = co_await filesystem_->read(
                handle->inode, offset, destination);
            if (!read) {
              co_return Result<void>::failure(read.error());
            }
            if (read.value() > destination.size()) {
              co_return detail::errno_failure<void>(EIO);
            }
            locked_completion.filled_payload_size = read.value();
            locked_completion.descriptor.result_length = read.value();
            if (implicit) {
              handle->offset = offset + read.value();
            }
            co_return Result<void>::success();
          };

          if (implicit) {
            co_return co_await detail::with_range_lock(
                handle->offset_gate, 0, kWholeHandleRange,
                RangeMode::write,
                [&] { return read_at_offset(handle->offset); });
          }
          co_return co_await read_at_offset(
              static_cast<std::uint64_t>(parsed.request.offset));
        });
  }

  Task<Completion> do_write(Completion completion,
                            const ParsedRequest& parsed,
                            const std::shared_ptr<HandleState>& handle,
                            bool implicit) {
    if (handle->type != NodeType::regular || handle->is_directory_cursor()) {
      fail(completion, EISDIR);
      co_return completion;
    }
    if ((handle->open_flags.load(std::memory_order_acquire) & O_ACCMODE) ==
        O_RDONLY) {
      fail(completion, EBADF);
      co_return completion;
    }
    if (parsed.request.length != parsed.data.size()) {
      fail(completion, EPROTO);
      co_return completion;
    }
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, &parsed, handle, implicit](
            Completion& locked_completion) -> Task<Result<void>> {
          const bool append =
              (handle->open_flags.load(std::memory_order_acquire) &
               O_APPEND) != 0;
          const auto write_at_offset =
              [this, &parsed, handle, implicit, append,
               &locked_completion](std::uint64_t offset)
                  -> Task<Result<void>> {
            if (!implicit && parsed.request.offset < 0) {
              co_return detail::errno_failure<void>(EINVAL);
            }
            if (!append) {
              if (const int error =
                      validate_io_range(offset, parsed.request.length);
                  error != 0) {
                co_return detail::errno_failure<void>(error);
              }
            }
            auto written = co_await filesystem_->write(
                handle->inode, offset, parsed.data, append);
            if (!written) {
              co_return Result<void>::failure(written.error());
            }
            if (written.value().bytes > parsed.data.size() ||
                validate_io_range(written.value().offset,
                                  written.value().bytes) != 0) {
              co_return detail::errno_failure<void>(EIO);
            }
            locked_completion.descriptor.result_length =
                written.value().bytes;
            locked_completion.descriptor.offset = written.value().offset;
            if (implicit || append) {
              handle->offset =
                  written.value().offset + written.value().bytes;
            }
            co_return Result<void>::success();
          };

          if (implicit || append) {
            co_return co_await detail::with_range_lock(
                handle->offset_gate, 0, kWholeHandleRange,
                RangeMode::write,
                [&] {
                  const std::uint64_t offset = implicit
                      ? handle->offset
                      : static_cast<std::uint64_t>(parsed.request.offset);
                  return write_at_offset(offset);
                });
          }
          co_return co_await write_at_offset(
              static_cast<std::uint64_t>(parsed.request.offset));
        });
  }

  Task<Completion> do_seek(Completion completion, const ParsedRequest& parsed,
                           const std::shared_ptr<HandleState>& handle) {
    if (handle->type != NodeType::regular || handle->is_directory_cursor()) {
      fail(completion, ESPIPE);
      co_return completion;
    }
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, &parsed, handle](Completion& locked_completion) {
          return detail::with_range_lock(
              handle->offset_gate, 0, kWholeHandleRange, RangeMode::write,
              [this, &parsed, handle, &locked_completion]()
                  -> Task<Result<void>> {
                auto seeked = co_await filesystem_->seek(
                    handle->inode, handle->offset, parsed.request.offset,
                    parsed.request.whence);
                if (!seeked) {
                  co_return Result<void>::failure(seeked.error());
                }
                handle->offset = seeked.value();
                locked_completion.descriptor.result_length = seeked.value();
                co_return Result<void>::success();
              });
        });
  }

  Task<Completion> do_stat_path(Completion completion, std::string path) {
    if (const int error = validate_path(path); error != 0) {
      fail(completion, error);
      co_return completion;
    }
    auto stated = co_await filesystem_->stat(std::move(path));
    if (!stated) {
      fail(completion, error_number(stated.error()));
    } else {
      completion.payload = detail::encode_object(to_wire(stated.value()));
      completion.descriptor.result_length = completion.payload.size();
    }
    co_return completion;
  }

  Task<Completion> do_handle_stat(
      Completion completion, const std::shared_ptr<HandleState>& handle,
      Opcode opcode) {
    const auto record_stat = [](Completion& locked_completion,
                                auto&& stated) {
      if (!stated) {
        fail(locked_completion, error_number(stated.error()));
      } else {
        locked_completion.payload =
            detail::encode_object(to_wire(stated.value()));
        locked_completion.descriptor.result_length =
            locked_completion.payload.size();
      }
    };
    if (opcode == Opcode::statfs) {
      co_return co_await with_completion_range_lock(
          std::move(completion),
          [this, handle] { return schedule_handle_owner(*handle); },
          handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
          [this, handle](Completion&) {
            return filesystem_->statfs(handle->inode);
          },
          record_stat);
    }
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, handle](Completion&) {
          return filesystem_->stat(handle->inode);
        },
        record_stat);
  }

  Task<Completion> do_path_mutation(Completion completion,
                                    const ParsedRequest& parsed,
                                    Opcode opcode) {
    if (const int error = validate_path(parsed.path1); error != 0) {
      fail(completion, error);
      co_return completion;
    }
    if (opcode == Opcode::rename) {
      if (const int error = validate_path(parsed.path2); error != 0) {
        fail(completion, error);
        co_return completion;
      }
      if (path_parent(parsed.path1) != path_parent(parsed.path2)) {
        fail(completion, EXDEV);
        co_return completion;
      }
    }

    Result<void> result = Result<void>::failure(
        std::make_error_code(std::errc::operation_not_supported));
    switch (opcode) {
      case Opcode::mkdir:
        result = co_await filesystem_->make_directory(
            parsed.path1, parsed.request.mode);
        break;
      case Opcode::rmdir:
        result = co_await filesystem_->remove_directory(parsed.path1);
        break;
      case Opcode::unlink:
        result = co_await filesystem_->unlink(parsed.path1);
        break;
      case Opcode::rename:
        result = co_await filesystem_->rename(parsed.path1, parsed.path2);
        break;
      default:
        break;
    }
    if (!result) {
      fail(completion, error_number(result.error()));
    }
    co_return completion;
  }

  Task<Completion> do_truncate_path(Completion completion,
                                    const ParsedRequest& parsed) {
    if (const int error = validate_path(parsed.path1); error != 0) {
      fail(completion, error);
      co_return completion;
    }
    auto truncated = co_await filesystem_->truncate(
        parsed.path1, parsed.request.length);
    if (!truncated) {
      fail(completion, error_number(truncated.error()));
    }
    co_return completion;
  }

  Task<Completion> do_truncate_handle(
      Completion completion, const ParsedRequest& parsed,
      const std::shared_ptr<HandleState>& handle) {
    if (handle->type != NodeType::regular || handle->is_directory_cursor()) {
      fail(completion, EISDIR);
      co_return completion;
    }
    if ((handle->open_flags.load(std::memory_order_acquire) & O_ACCMODE) ==
        O_RDONLY) {
      fail(completion, EBADF);
      co_return completion;
    }
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, &parsed, handle](Completion&) {
          return filesystem_->truncate(handle->inode,
                                       parsed.request.length);
        });
  }

  Task<Completion> do_open_directory(Completion completion,
                                     std::string path) {
    if (const int error = validate_path(path); error != 0) {
      fail(completion, error);
      co_return completion;
    }
    auto opened = co_await filesystem_->open_directory(std::move(path));
    if (!opened) {
      fail(completion, error_number(opened.error()));
    } else if (opened.value().type != NodeType::directory ||
               opened.value().inode < 0) {
      fail(completion, ENOTDIR);
    } else {
      completion.descriptor.result_length =
          add_handle(opened.value(), O_RDONLY, true);
    }
    co_return completion;
  }

  Task<Completion> do_open_directory_handle(
      Completion completion, const std::shared_ptr<HandleState>& source) {
    if (source->type != NodeType::directory) {
      fail(completion, ENOTDIR);
      co_return completion;
    }
    co_return co_await with_completion_range_lock(
        std::move(completion), [this] { return runtime_->schedule_on(0); },
        source->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, source](Completion&) {
          return filesystem_->open_directory(source->inode);
        },
        [this](Completion& locked_completion, auto&& opened) {
          if (!opened) {
            fail(locked_completion, error_number(opened.error()));
          } else if (opened.value().type != NodeType::directory ||
                     opened.value().inode < 0) {
            fail(locked_completion, ENOTDIR);
          } else {
            locked_completion.descriptor.result_length =
                add_handle(opened.value(), O_RDONLY, true);
          }
        });
  }

  Task<Completion> do_read_directory(
      Completion completion, const ParsedRequest& parsed,
      const std::shared_ptr<HandleState>& handle) {
    if (handle->type != NodeType::directory ||
        !handle->is_directory_cursor()) {
      fail(completion, ENOTDIR);
      co_return completion;
    }
    const std::size_t maximum = config_.data_slot_size / sizeof(RpcDirEntry);
    if (parsed.request.length > maximum) {
      fail(completion, EMSGSIZE);
      co_return completion;
    }
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, &parsed, handle](Completion& locked_completion) {
          return detail::with_range_lock(
              handle->offset_gate, 0, kWholeHandleRange, RangeMode::write,
              [this, &parsed, handle, &locked_completion]()
                  -> Task<Result<void>> {
                std::vector<DirEntry> entries;
                try {
                  entries.resize(
                      static_cast<std::size_t>(parsed.request.length));
                } catch (const std::bad_alloc&) {
                  co_return detail::errno_failure<void>(ENOMEM);
                }
                auto read = co_await filesystem_->read_directory(
                    handle->inode, handle->directory_cursor, entries);
                if (!read) {
                  co_return Result<void>::failure(read.error());
                }
                if (read.value().count > entries.size()) {
                  co_return detail::errno_failure<void>(EIO);
                }
                entries.resize(read.value().count);
                try {
                  locked_completion.payload.resize(
                      entries.size() * sizeof(RpcDirEntry));
                  auto* output = reinterpret_cast<RpcDirEntry*>(
                      locked_completion.payload.data());
                  for (std::size_t index = 0; index < entries.size();
                       ++index) {
                    if (entries[index].name.size() > kRpcNameCapacity) {
                      co_return detail::errno_failure<void>(ENAMETOOLONG);
                    }
                    RpcDirEntry wire{};
                    wire.inode = entries[index].inode;
                    wire.offset = entries[index].offset;
                    wire.record_length = sizeof(RpcDirEntry);
                    wire.type = entries[index].type;
                    wire.name_length = static_cast<std::uint8_t>(
                        entries[index].name.size());
                    std::memcpy(wire.name.data(), entries[index].name.data(),
                                entries[index].name.size());
                    output[index] = wire;
                  }
                } catch (const std::bad_alloc&) {
                  co_return detail::errno_failure<void>(ENOMEM);
                }
                handle->directory_cursor = read.value().next_cursor;
                locked_completion.descriptor.result_length = entries.size();
                if (entries.empty()) {
                  locked_completion.descriptor.flags |=
                      DescriptorFlag::end_of_stream;
                }
                co_return Result<void>::success();
              });
        });
  }

  Task<Completion> do_sync(Completion completion,
                           const std::shared_ptr<HandleState>& handle) {
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [this, handle](Completion&) {
          return filesystem_->sync(handle->inode);
        });
  }

  Task<Completion> do_flags(Completion completion, const ParsedRequest& parsed,
                            const std::shared_ptr<HandleState>& handle) {
    co_return co_await with_completion_range_lock(
        std::move(completion),
        [this, handle] { return schedule_handle_owner(*handle); },
        handle->lifecycle_gate, 0, kWholeHandleRange, RangeMode::read,
        [&parsed, handle](
            Completion& locked_completion) -> Task<Result<void>> {
          if (parsed.request.value == F_GETFL) {
            locked_completion.descriptor.result_length =
                static_cast<std::uint64_t>(
                    handle->open_flags.load(std::memory_order_acquire));
          } else if (parsed.request.value == F_SETFL) {
            constexpr int mutable_flags = O_APPEND | O_NONBLOCK;
            const int requested = parsed.request.open_flags;
            int current =
                handle->open_flags.load(std::memory_order_acquire);
            handle->open_flags.store((current & ~mutable_flags) |
                                         (requested & mutable_flags),
                                     std::memory_order_release);
          } else {
            co_return detail::errno_failure<void>(EINVAL);
          }
          co_return Result<void>::success();
        });
  }

  Task<Completion> do_raw_device(Completion completion,
                                 const ParsedRequest& parsed,
                                 Opcode opcode) {
    AsyncBlockDevice device(*runtime_);
    if (opcode == Opcode::raw_device_flush) {
      auto flushed = co_await device.flush();
      if (!flushed) {
        fail(completion, error_number(flushed.error()));
      }
      co_return completion;
    }
    if (const int error = validate_io_range(parsed.request.offset,
                                            parsed.request.length);
        error != 0) {
      fail(completion, error);
      co_return completion;
    }
    if (opcode == Opcode::raw_device_read) {
      if (parsed.request.length > config_.data_slot_size ||
          parsed.request.length > std::numeric_limits<std::size_t>::max()) {
        fail(completion, EMSGSIZE);
        co_return completion;
      }
      auto destination = completion.reservation.payload().first(
          static_cast<std::size_t>(parsed.request.length));
      auto read = co_await device.read(parsed.request.offset,
                                       destination);
      if (!read) {
        fail(completion, error_number(read.error()));
      } else if (read.value() > destination.size()) {
        fail(completion, EIO);
      } else {
        completion.filled_payload_size = read.value();
        completion.descriptor.result_length = read.value();
      }
      co_return completion;
    }
    if (parsed.request.length != parsed.data.size()) {
      fail(completion, EPROTO);
      co_return completion;
    }
    auto written = co_await device.write(parsed.request.offset, parsed.data);
    if (!written) {
      fail(completion, error_number(written.error()));
    } else if (written.value() != parsed.data.size()) {
      fail(completion, EIO);
    } else {
      completion.descriptor.result_length = written.value();
    }
    co_return completion;
  }

  Task<Completion> do_shutdown(Completion completion) {
    bool expected = false;
    if (!shutdown_requested_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
      fail(completion, EALREADY);
      co_return completion;
    }
    closing_.store(true, std::memory_order_release);
    stop_after_flush_.store(true, std::memory_order_release);
    co_return completion;
  }

  Task<int> cleanup_all_handles() {
    auto scheduled = co_await runtime_->schedule_on(0);
    if (!scheduled) {
      co_return error_number(scheduled.error());
    }
    int first_error = 0;
    const auto handles = handles_.load(std::memory_order_acquire);
    for (const auto& [id, handle] : *handles) {
      bool expected = false;
      if (!handle->closing.compare_exchange_strong(
              expected, true, std::memory_order_acq_rel) && !expected) {
        continue;
      }
      auto acquired = co_await handle->lifecycle_gate.acquire(
          0, kWholeHandleRange, RangeMode::write);
      if (!acquired) {
        if (first_error == 0) {
          first_error = error_number(acquired.error());
        }
        continue;
      }
      auto permit = std::move(acquired).value();
      const OpenedNode node{.inode = handle->inode, .type = handle->type};
      auto closed = handle->is_directory_cursor()
                        ? co_await filesystem_->close_directory(node)
                        : co_await filesystem_->close(node);
      if (!closed && first_error == 0) {
        first_error = error_number(closed.error());
      }
      erase_handle(id);
      auto released = co_await permit.release();
      if (!released && first_error == 0) {
        first_error = error_number(released.error());
      }
    }
    co_return first_error;
  }

  void start_background_cleanup() noexcept {
    bool expected = false;
    if (!cleanup_started_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
      return;
    }
    inflight_.fetch_add(1, std::memory_order_acq_rel);
    auto submitted = runtime_->submit(run_session_cleanup(shared_from_this()));
    if (!submitted) {
      inflight_.fetch_sub(1, std::memory_order_acq_rel);
      cleanup_error_.store(error_number(submitted.error()),
                           std::memory_order_release);
      cleanup_done_.store(true, std::memory_order_release);
    }
  }

  void finish_background_cleanup(int error) noexcept {
    if (error != 0) {
      cleanup_error_.store(error, std::memory_order_release);
    }
    cleanup_done_.store(true, std::memory_order_release);
    inflight_.fetch_sub(1, std::memory_order_acq_rel);
    (void)runtime_->notify(control_owner_);
  }

  void submit_incoming(Incoming incoming) noexcept {
    bool reject = false;
    if (incoming.descriptor.opcode == Opcode::shutdown_session) {
      if (stop_requested_.load(std::memory_order_acquire) ||
          shutdown_requested_.load(std::memory_order_acquire)) {
        reject = true;
      }
    } else if (closing_.load(std::memory_order_acquire)) {
      reject = true;
    }
    if (reject) {
      Completion completion{
          .lane = incoming.lane,
          .descriptor = incoming.descriptor,
          .reservation = std::move(incoming.completion),
      };
      completion.descriptor.flags = DescriptorFlag::response;
      fail(completion, ESHUTDOWN);
      queue_completion(std::move(completion));
      return;
    }

    const auto lane = incoming.lane;
    const auto descriptor = incoming.descriptor;
    lanes_[lane]->inflight.fetch_add(1, std::memory_order_acq_rel);
    inflight_.fetch_add(1, std::memory_order_acq_rel);
    try {
      auto task = run_request(shared_from_this(), std::move(incoming));
      auto coroutine = task.release();
      coroutine.resume();
    } catch (const std::bad_alloc&) {
      lanes_[lane]->inflight.fetch_sub(1, std::memory_order_acq_rel);
      inflight_.fetch_sub(1, std::memory_order_acq_rel);
      (void)descriptor;
      request_stop();
    } catch (...) {
      std::terminate();
    }
  }

  bool pump_submissions(LaneState& lane) noexcept {
    bool progress = false;
    for (;;) {
      if (!transport_.submission_ready(lane.lane)) {
        return progress;
      }
      CompletionReservation response;
      auto error = transport_.try_reserve_completion(lane.lane, response);
      if (error == make_error_code(TransportErrc::would_block)) {
        return progress;
      }
      if (error) {
        request_stop();
        return true;
      }
      ReceivedIpcSlot submission;
      error = transport_.try_acquire_submission(lane.lane, submission);
      if (error) {
        response.abandon();
        request_stop();
        return true;
      }
      progress = true;
      const IpcDescriptor descriptor = submission.descriptor();
      if (descriptor.client_id != transport_.client_id() ||
          descriptor.session_generation != transport_.session_generation() ||
          !has_flag(descriptor.flags, DescriptorFlag::request)) {
        Completion completion{
            .lane = lane.lane,
            .descriptor = descriptor,
            .reservation = std::move(response),
        };
        completion.descriptor.flags = DescriptorFlag::response;
        fail(completion, EPROTO);
        queue_completion(std::move(completion));
        continue;
      }
      Incoming incoming{
          .lane = lane.lane,
          .descriptor = descriptor,
          .payload = submission.payload(),
          .submission = std::move(submission),
          .completion = std::move(response),
      };
      submit_incoming(std::move(incoming));
    }
  }

  bool pump_completions(LaneState& lane) noexcept {
    bool progress = drain_completion_inbox(lane);
    while (!lane.completions.empty()) {
      Completion& completion = lane.completions.front();
      const auto error = completion.filled_payload_size != 0
          ? completion.reservation.publish(
                completion.descriptor, completion.filled_payload_size)
          : completion.reservation.publish_copy(
                completion.descriptor, completion.payload);
      progress = true;
      lane.completions.pop_front();
      if (error) {
        request_stop();
        return true;
      }
    }
    return progress;
  }

  bool completions_empty(LaneState& lane) noexcept {
    (void)drain_completion_inbox(lane);
    return lane.completions.empty() &&
           lane.completion_inbox.empty();
  }

  bool discard_completions(LaneState& lane) noexcept {
    const bool progress = drain_completion_inbox(lane) ||
                          !lane.completions.empty();
    lane.completions.clear();
    return progress;
  }

  static Runtime::PollState poll_lane(void* context) noexcept {
    auto& lane = *static_cast<LaneState*>(context);
    return lane.session->poll_lane_once(lane);
  }

  Runtime::PollState poll_lane_once(LaneState& lane) noexcept {
    if (lane.drained.load(std::memory_order_acquire)) {
      return Runtime::PollState::idle;
    }
    bool progress = false;
    bool forced_stop = stop_requested_.load(std::memory_order_acquire);
    if (!forced_stop) {
      progress |= pump_submissions(lane);
      progress |= pump_completions(lane);
    } else {
      progress |= discard_completions(lane);
    }
    forced_stop = forced_stop ||
                  stop_requested_.load(std::memory_order_acquire);
    if (forced_stop) {
      progress |= discard_completions(lane);
    }
    const bool stopping = forced_stop ||
                          stop_after_flush_.load(std::memory_order_acquire);
    if (stopping && lane.inflight.load(std::memory_order_acquire) == 0) {
      if (forced_stop) {
        progress |= discard_completions(lane);
      } else {
        progress |= pump_completions(lane);
      }
      if (forced_stop || completions_empty(lane)) {
        // Retirement can remove the registry owner as soon as control sees
        // drained. Pin only this one terminal callback; steady polling remains
        // free of shared_ptr traffic.
        auto self = shared_from_this();
        lane.registration.reset();
        // Publish drained only after the owner has cleared the registration
        // member. join() may run on worker 0 as soon as control observes this
        // flag, so the release/acquire edge also protects that member.
        (void)runtime_->notify(control_owner_);
        lane.drained.store(true, std::memory_order_release);
        return Runtime::PollState::progress;
      }
    }
    if (progress) {
      return Runtime::PollState::progress;
    }
    // The peer eventfd is distinct from the Runtime wake word. Every active
    // lane therefore stays hot until control asks it to drain.
    return Runtime::PollState::busy;
  }

  static Runtime::PollState poll_control(void* context) noexcept {
    return static_cast<ServerSession*>(context)->poll_control_once();
  }

  Runtime::PollState poll_control_once() noexcept {
    if (finished_.load(std::memory_order_acquire)) {
      return Runtime::PollState::idle;
    }
    bool progress = false;
    constexpr std::uint32_t kHealthPollMask = 1023;
    if ((++health_poll_ticks_ & kHealthPollMask) == 0) {
      if (!transport_.peer_alive()) {
        request_stop();
        progress = true;
      }
    }
    const bool forced_stop = stop_requested_.load(std::memory_order_acquire);
    const bool stopping = forced_stop ||
                          stop_after_flush_.load(std::memory_order_acquire);
    if (!stopping) {
      return progress ? Runtime::PollState::progress
                      : Runtime::PollState::busy;
    }
    const bool all_drained = std::all_of(
        lanes_.begin(), lanes_.end(), [](const auto& lane) {
          return lane->drained.load(std::memory_order_acquire);
        });
    if (!all_drained) {
      return progress ? Runtime::PollState::progress
                      : Runtime::PollState::busy;
    }
    if (!cleanup_done_.load(std::memory_order_acquire)) {
      start_background_cleanup();
      return Runtime::PollState::progress;
    }
    if (inflight_.load(std::memory_order_acquire) != 0) {
      return Runtime::PollState::busy;
    }
    transport_.mark_dead();
    // The retirement event may immediately release the registry's last owner.
    // Keep the object alive until this terminal poll callback has returned.
    auto self = shared_from_this();
    control_registration_.reset();
    if (retirement_callback_ != nullptr) {
      retirement_callback_(retirement_context_, this);
    }
    // Publish completion only after the callback stops using its raw Impl
    // context. Server::Impl::join() may otherwise return and destroy that
    // context while this worker is still publishing the retirement event.
    finished_.store(true, std::memory_order_release);
    finished_.notify_all();
    return Runtime::PollState::progress;
  }

  Runtime* runtime_;
  ServerTransport transport_;
  TransportConfig config_;
  std::shared_ptr<AsyncFilesystem> filesystem_;
  std::size_t control_owner_{};
  void* retirement_context_{};
  RetirementCallback retirement_callback_{};
  std::atomic<std::size_t>* active_dma_regions_{};
  std::atomic<std::size_t>* dma_registrations_{};
  std::atomic<std::size_t>* dma_unregistrations_{};
  std::atomic<ServerSession*> retirement_next_{nullptr};
  Runtime::PollRegistration control_registration_;
  std::vector<std::unique_ptr<LaneState>> lanes_;
  SharedMemoryRegion dma_region_{};
  std::atomic<bool> dma_registered_{false};

  std::atomic<bool> started_{false};
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
  std::atomic<std::shared_ptr<const HandleMap>> handles_{
      std::make_shared<const HandleMap>()};
  std::uint32_t health_poll_ticks_{};

  friend DetachedTask run_request(std::shared_ptr<ServerSession>, Incoming);
  friend Task<void> run_session_cleanup(std::shared_ptr<ServerSession>);
};

DetachedTask run_request(std::shared_ptr<ServerSession> session,
                         Incoming incoming) {
  // Poller callbacks run outside Runtime's per-work-item resume scope.  Enter
  // that scope before dispatch can acquire a pin: otherwise a request that
  // suspends while holding a RangePermit leaves its pin depth in worker TLS
  // and contaminates the next request drained by the same lane poller.
  auto entered = co_await Runtime::yield();
  if (!entered) {
    std::terminate();
  }
  auto completion = co_await session->dispatch(std::move(incoming));
  const auto lane = completion.lane;
  if (lane >= session->lanes_.size()) {
    std::terminate();
  }
  auto scheduled = co_await session->runtime_->schedule_on(
      session->lanes_[lane]->owner);
  if (!scheduled) {
    std::terminate();
  }
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
  using SessionList = std::vector<std::shared_ptr<ServerSession>>;

  struct RetirementLink {
    [[nodiscard, gnu::always_inline]] static ServerSession* next(
        const ServerSession& session) noexcept {
      return session.retirement_next();
    }

    [[gnu::always_inline]] static void set_next(
        ServerSession& session, ServerSession* next) noexcept {
      session.set_retirement_next(next);
    }
  };

 public:
  Impl(Runtime& runtime, ServerOptions options, ControlServer listener)
      : runtime_(&runtime),
        options_(std::move(options)),
        listener_(std::move(listener)) {}

  ~Impl() { (void)join(); }

  void start() {
    auto registered = runtime_->register_poller(
        0, &Impl::poll_accept_entry, this);
    if (!registered) {
      throw std::system_error(registered.error(), "accept poller");
    }
    accept_poll_registration_ = std::move(registered).value();
  }

  void request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    (void)runtime_->notify(0);
  }

  void request_drain() noexcept {
    drain_requested_.store(true, std::memory_order_release);
    stop_requested_.store(true, std::memory_order_release);
    (void)runtime_->notify(0);
  }

  Result<void> join() noexcept {
    const bool drain = drain_requested_.load(std::memory_order_acquire);
    if (drain) {
      request_drain();
    } else {
      request_stop();
    }
    accept_poll_registration_.reset();
    SessionList sessions = std::move(sessions_);
    sessions_.clear();
    active_sessions_.store(0, std::memory_order_release);
    if (!drain) {
      for (const auto& session : sessions) {
        session->request_stop();
      }
    }
    for (const auto& session : sessions) {
      session->join();
      record_cleanup_error(session->cleanup_error());
    }
    // Completion callbacks may publish while join() drains the stable owner
    // snapshot. No accept poller remains to consume those intrusive links.
    retired_sessions_.clear();
    const int cleanup_error = cleanup_error_.load(std::memory_order_acquire);
    if (cleanup_error != 0) {
      return Result<void>::failure(
          std::error_code(cleanup_error, std::generic_category()));
    }
    return Result<void>::success();
  }

  [[nodiscard]] ServerSessionStats session_stats() const noexcept {
    return {
        .active = active_sessions_.load(std::memory_order_acquire),
        .retired = retired_sessions_count_.load(std::memory_order_acquire),
        .dma_regions = active_dma_regions_.load(std::memory_order_acquire),
        .dma_registrations =
            dma_registrations_.load(std::memory_order_acquire),
        .dma_unregistrations =
            dma_unregistrations_.load(std::memory_order_acquire),
    };
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

  static void publish_retirement_entry(void* context,
                                       ServerSession* session) noexcept {
    auto& self = *static_cast<Impl*>(context);
    self.retired_sessions_.push(*session);
    (void)self.runtime_->notify(0);
  }

  bool drain_retired_sessions() noexcept {
    ServerSession* ordered = retired_sessions_.drain();
    if (ordered == nullptr) {
      return false;
    }

    while (ordered != nullptr) {
      ServerSession* current = ordered;
      ordered = current->retirement_next();
      current->set_retirement_next(nullptr);
      const auto found = std::find_if(
          sessions_.begin(), sessions_.end(),
          [current](const auto& candidate) {
            return candidate.get() == current;
          });
      if (found == sessions_.end()) {
        continue;
      }
      auto session = std::move(*found);
      sessions_.erase(found);
      active_sessions_.fetch_sub(1, std::memory_order_release);
      retired_sessions_count_.fetch_add(1, std::memory_order_relaxed);
      session->join();
      record_cleanup_error(session->cleanup_error());
    }
    return true;
  }

  static Runtime::PollState poll_accept_entry(void* context) noexcept {
    return static_cast<Impl*>(context)->poll_accept();
  }

  Runtime::PollState poll_accept() noexcept {
    const bool retired = drain_retired_sessions();
    if (stop_requested_.load(std::memory_order_acquire)) {
      bool progress = retired;
      if (!drain_requested_.load(std::memory_order_acquire) &&
          !session_stop_propagated_) {
        for (const auto& session : sessions_) {
          session->request_stop();
        }
        session_stop_propagated_ = true;
        progress = true;
      }
      return progress ? Runtime::PollState::progress
                      : Runtime::PollState::idle;
    }
    constexpr std::uint32_t kAcceptPollMask = 1023;
    if ((++accept_poll_ticks_ & kAcceptPollMask) != 0) {
      return retired ? Runtime::PollState::progress
                     : Runtime::PollState::busy;
    }
    bool progress = retired;
    constexpr std::size_t kAcceptBudget = 8;
    for (std::size_t accepted = 0; accepted < kAcceptBudget; ++accepted) {
      std::error_code error;
      auto transport = listener_.try_accept(error);
      if (error || !transport) {
        if (error == std::errc::resource_unavailable_try_again) {
          break;
        }
        progress = true;
        continue;
      }
      progress = true;
      try {
        auto session = std::make_shared<ServerSession>(
            *runtime_, std::move(transport), options_.filesystem, this,
            &Impl::publish_retirement_entry, active_dma_regions_,
            dma_registrations_, dma_unregistrations_);
        sessions_.push_back(session);
        if (const auto start_error = session->start(); start_error) {
          session->request_stop();
          sessions_.pop_back();
          continue;
        }
        active_sessions_.fetch_add(1, std::memory_order_release);
        if (stop_requested_.load(std::memory_order_acquire)) {
          session->request_stop();
          return Runtime::PollState::progress;
        }
      } catch (...) {
        // Keep accepting independent clients after an allocation failure.
      }
    }
    if (progress) {
      return Runtime::PollState::progress;
    }
    return Runtime::PollState::busy;
  }

  Runtime* runtime_;
  ServerOptions options_;
  ControlServer listener_;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> drain_requested_{false};
  std::atomic<int> cleanup_error_{0};
  std::uint32_t accept_poll_ticks_{1023};
  bool session_stop_propagated_{false};
  Runtime::PollRegistration accept_poll_registration_;
  SessionList sessions_;
  detail::MpscInbox<ServerSession, RetirementLink> retired_sessions_;
  std::atomic<std::size_t> active_sessions_{0};
  std::atomic<std::size_t> retired_sessions_count_{0};
  std::atomic<std::size_t> active_dma_regions_{0};
  std::atomic<std::size_t> dma_registrations_{0};
  std::atomic<std::size_t> dma_unregistrations_{0};
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
  if (!options.filesystem || options.lane_count == 0 ||
      options.lane_count > kMaxIpcWorkerLanes ||
      options.ring_capacity == 0 ||
      options.data_slot_size <= sizeof(RpcRequest) ||
      options.lane_count > std::numeric_limits<std::uint32_t>::max() ||
      options.ring_capacity > std::numeric_limits<std::uint32_t>::max() ||
      options.data_slot_size > std::numeric_limits<std::uint32_t>::max()) {
    return Result<std::unique_ptr<Server>>::failure(
        std::make_error_code(std::errc::invalid_argument));
  }

  const TransportConfig config{
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
  } catch (const std::system_error& error) {
    return Result<std::unique_ptr<Server>>::failure(error.code());
  } catch (...) {
    std::terminate();
  }
}

void Server::request_stop() noexcept {
  if (impl_) {
    impl_->request_stop();
  }
}

void Server::request_drain() noexcept {
  if (impl_) {
    impl_->request_drain();
  }
}

Result<void> Server::join() {
  return impl_ ? impl_->join() : Result<void>::success();
}

ServerSessionStats Server::session_stats() const noexcept {
  return impl_ ? impl_->session_stats() : ServerSessionStats{};
}

}  // namespace orchfs::async
