#include "orchfs/async/client.hpp"

#include "orchfs/async/ipc_transport.hpp"
#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <poll.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sys/eventfd.h>
#include <unistd.h>

namespace orchfs::async {
namespace {

struct RpcReply {
  IpcDescriptor descriptor;
  std::vector<std::byte> payload;
};

template <typename T>
void append_object(std::vector<std::byte>& bytes, const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  const auto old_size = bytes.size();
  bytes.resize(old_size + sizeof(T));
  std::memcpy(bytes.data() + old_size, &value, sizeof(T));
}

void append_bytes(std::vector<std::byte>& bytes, const void* data,
                  std::size_t size) {
  if (size == 0) {
    return;
  }
  const auto old_size = bytes.size();
  bytes.resize(old_size + size);
  std::memcpy(bytes.data() + old_size, data, size);
}

Result<std::vector<std::byte>> make_request(
    std::size_t wire_size_limit, const RpcRequest& request,
    std::string_view path1 = {}, std::string_view path2 = {},
    std::span<const std::byte> data = {}) {
  constexpr auto wire_length_max =
      static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max());
  wire_size_limit = std::min(wire_size_limit, wire_length_max);
  if (wire_size_limit < sizeof(RpcRequest)) {
    return Result<std::vector<std::byte>>::failure(
        std::make_error_code(std::errc::message_size));
  }
  std::size_t wire_size = sizeof(RpcRequest);
  for (const auto field_size : {path1.size(), path2.size(), data.size()}) {
    if (field_size > wire_size_limit - wire_size) {
      return Result<std::vector<std::byte>>::failure(
          std::make_error_code(std::errc::message_size));
    }
    wire_size += field_size;
  }

  RpcRequest wire = request;
  wire.schema_version = kRpcSchemaVersion;
  wire.path1_length = static_cast<std::uint32_t>(path1.size());
  wire.path2_length = static_cast<std::uint32_t>(path2.size());
  wire.data_length = static_cast<std::uint32_t>(data.size());

  std::vector<std::byte> bytes;
  bytes.reserve(wire_size);
  append_object(bytes, wire);
  append_bytes(bytes, path1.data(), path1.size());
  append_bytes(bytes, path2.data(), path2.size());
  append_bytes(bytes, data.data(), data.size());
  return Result<std::vector<std::byte>>::success(std::move(bytes));
}

std::error_code status_error(std::int32_t status) noexcept {
  if (status == 0) {
    return {};
  }
  const std::int64_t magnitude =
      status < 0 ? -static_cast<std::int64_t>(status) : status;
  if (magnitude > std::numeric_limits<int>::max()) {
    return std::make_error_code(std::errc::protocol_error);
  }
  return {static_cast<int>(magnitude), std::generic_category()};
}

template <typename T>
Result<T> decode_object(const RpcReply& reply) {
  if (auto error = status_error(reply.descriptor.status)) {
    return Result<T>::failure(error);
  }
  if (reply.payload.size() != sizeof(T)) {
    return Result<T>::failure(std::make_error_code(std::errc::protocol_error));
  }
  T value{};
  std::memcpy(&value, reply.payload.data(), sizeof(T));
  return Result<T>::success(std::move(value));
}

FileStat from_wire(const RpcFileStat& value) noexcept {
  return FileStat{
      .device = value.device,
      .inode = value.inode,
      .mode = value.mode,
      .link_count = value.link_count,
      .uid = value.uid,
      .gid = value.gid,
      .rdev = value.rdev,
      .size = value.size,
      .block_size = value.block_size,
      .blocks = value.blocks,
      .atime_seconds = value.atime_seconds,
      .atime_nanoseconds = value.atime_nanoseconds,
      .mtime_seconds = value.mtime_seconds,
      .mtime_nanoseconds = value.mtime_nanoseconds,
      .ctime_seconds = value.ctime_seconds,
      .ctime_nanoseconds = value.ctime_nanoseconds,
  };
}

FileSystemStat from_wire(const RpcStatFs& value) noexcept {
  return FileSystemStat{
      .type = value.type,
      .block_size = value.block_size,
      .blocks = value.blocks,
      .blocks_free = value.blocks_free,
      .blocks_available = value.blocks_available,
      .files = value.files,
      .files_free = value.files_free,
      .name_length = value.name_length,
      .fragment_size = value.fragment_size,
      .flags = value.flags,
  };
}

class ConnectAwaiter {
 public:
  ConnectAwaiter(Runtime& runtime, std::string endpoint,
                 TransportConfig config) noexcept
      : runtime_(&runtime), endpoint_(std::move(endpoint)), config_(config) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    if (Runtime::current() != runtime_) {
      error_ = make_error_code(Errc::wrong_runtime);
      return false;
    }
    worker_ = Runtime::current_worker();
    try {
      thread_ = std::thread([this, continuation] {
        transport_ = ClientTransport::connect(endpoint_, config_, error_);
        if (!runtime_->schedule(continuation, worker_)) {
          std::terminate();
        }
      });
    } catch (const std::system_error& error) {
      error_ = error.code();
      return false;
    } catch (...) {
      std::terminate();
    }
    return true;
  }

  Result<ClientTransport> await_resume() noexcept {
    if (thread_.joinable()) {
      thread_.join();
    }
    if (error_) {
      return Result<ClientTransport>::failure(error_);
    }
    if (!transport_) {
      return Result<ClientTransport>::failure(
          std::make_error_code(std::errc::not_connected));
    }
    return Result<ClientTransport>::success(std::move(transport_));
  }

 private:
  Runtime* runtime_;
  std::string endpoint_;
  TransportConfig config_;
  std::size_t worker_{};
  std::thread thread_;
  ClientTransport transport_;
  std::error_code error_;
};

}  // namespace

class Session final : public std::enable_shared_from_this<Session> {
 public:
  struct Pending {
    IpcDescriptor descriptor;
    std::vector<std::byte> request_payload;
    RpcReply reply;
    std::error_code error;
    std::coroutine_handle<> continuation{};
    std::size_t resume_worker{};
    std::uint32_t lane{};
    bool detached{};
    bool stop_after_completion{};
  };

  static Result<std::shared_ptr<Session>> create(Runtime& runtime,
                                                 ClientTransport transport) {
    std::shared_ptr<Session> session;
    try {
      session = std::shared_ptr<Session>(
          new Session(runtime, std::move(transport)));
      session->start();
    } catch (const std::bad_alloc&) {
      return Result<std::shared_ptr<Session>>::failure(
          std::make_error_code(std::errc::not_enough_memory));
    } catch (const std::system_error& error) {
      if (session) {
        session->request_stop();
      }
      return Result<std::shared_ptr<Session>>::failure(error.code());
    } catch (...) {
      std::terminate();
    }
    return Result<std::shared_ptr<Session>>::success(std::move(session));
  }

  ~Session() {
    request_stop();
    if (io_thread_.joinable()) {
      if (io_thread_.get_id() == std::this_thread::get_id()) {
        io_thread_.detach();
      } else {
        io_thread_.join();
      }
    }
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
    }
  }

  Session(const Session&) = delete;
  Session& operator=(const Session&) = delete;

  class PendingAwaiter {
   public:
    PendingAwaiter(std::shared_ptr<Session> session,
                   std::shared_ptr<Pending> pending) noexcept
        : session_(std::move(session)), pending_(std::move(pending)) {}

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      if (Runtime::current() != session_->runtime_) {
        pending_->error = make_error_code(Errc::wrong_runtime);
        return false;
      }
      pending_->continuation = continuation;
      pending_->resume_worker = Runtime::current_worker();
      pending_->lane = static_cast<std::uint32_t>(
          pending_->resume_worker % session_->config_.lane_count);
      return session_->enqueue(pending_);
    }

    Result<RpcReply> await_resume() noexcept {
      if (pending_->error) {
        return Result<RpcReply>::failure(pending_->error);
      }
      return Result<RpcReply>::success(std::move(pending_->reply));
    }

   private:
    std::shared_ptr<Session> session_;
    std::shared_ptr<Pending> pending_;
  };

  Task<Result<RpcReply>> rpc(
      Opcode opcode, Result<std::vector<std::byte>> encoded_request,
      bool stop_after_completion = false) {
    if (!encoded_request) {
      co_return Result<RpcReply>::failure(encoded_request.error());
    }

    auto payload = std::move(encoded_request).value();
    std::shared_ptr<Pending> pending;
    try {
      pending = std::make_shared<Pending>();
      pending->descriptor.opcode = opcode;
      pending->descriptor.flags = DescriptorFlag::request |
                                  DescriptorFlag::has_payload;
      pending->descriptor.payload_length =
          static_cast<std::uint32_t>(payload.size());
      pending->request_payload = std::move(payload);
      pending->stop_after_completion = stop_after_completion;
    } catch (const std::bad_alloc&) {
      co_return Result<RpcReply>::failure(
          std::make_error_code(std::errc::not_enough_memory));
    }

    auto reply = co_await PendingAwaiter(shared_from_this(), pending);
    co_return reply;
  }

  [[nodiscard]] std::size_t max_payload() const noexcept {
    return config_.data_slot_size;
  }

  [[nodiscard]] bool available() const noexcept {
    return accepting_.load(std::memory_order_acquire) &&
           !stop_requested_.load(std::memory_order_acquire);
  }

  void add_handle() noexcept {
    open_handles_.fetch_add(1, std::memory_order_relaxed);
  }

  void release_handle(RemoteHandle handle, Opcode opcode) noexcept {
    if (handle != kInvalidRemoteHandle) {
      (void)enqueue_detached(opcode, handle, false);
    }
    const auto previous = open_handles_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 1) {
      maybe_shutdown();
    }
  }

  void release_handle_after_close() noexcept {
    const auto previous = open_handles_.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 1) {
      maybe_shutdown();
    }
  }

  void release_client() noexcept {
    client_alive_.store(false, std::memory_order_release);
    maybe_shutdown();
  }

  void request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    accepting_.store(false, std::memory_order_release);
    wake_io_thread();
  }

 private:
  Session(Runtime& runtime, ClientTransport transport)
      : runtime_(&runtime), transport_(std::move(transport)),
        config_(transport_.config()) {
    wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) {
      throw std::system_error(errno, std::generic_category(), "eventfd");
    }
    outbound_.resize(config_.lane_count);
  }

  void start() {
    auto self = shared_from_this();
    io_thread_ = std::thread([self = std::move(self)] { self->io_loop(); });
  }

  bool enqueue(const std::shared_ptr<Pending>& pending) noexcept {
    if (pending->request_payload.size() > config_.data_slot_size) {
      pending->error = std::make_error_code(std::errc::message_size);
      return false;
    }

    pending->descriptor.client_id = transport_.client_id();
    pending->descriptor.session_generation = transport_.session_generation();
    pending->descriptor.request_id =
        next_request_id_.fetch_add(1, std::memory_order_relaxed);
    pending->descriptor.resume_worker =
        static_cast<std::uint32_t>(pending->resume_worker);

    bool inserted = false;
    try {
      std::lock_guard lock(queue_mutex_);
      // Recheck while holding the same mutex used by fail_all().  Otherwise a
      // request can observe accepting=true, race the I/O thread's terminal
      // cleanup, and be inserted after that thread has exited.
      if (!accepting_.load(std::memory_order_acquire)) {
        pending->error = std::make_error_code(std::errc::not_connected);
        return false;
      }
      inserted = pending_.emplace(pending->descriptor.request_id, pending).second;
      if (!inserted) {
        pending->error = std::make_error_code(std::errc::device_or_resource_busy);
        return false;
      }
      outbound_[pending->lane].push_back(pending);
    } catch (const std::bad_alloc&) {
      if (inserted) {
        std::lock_guard lock(queue_mutex_);
        pending_.erase(pending->descriptor.request_id);
      }
      pending->error = std::make_error_code(std::errc::not_enough_memory);
      return false;
    } catch (...) {
      std::terminate();
    }
    wake_io_thread();
    return true;
  }

  [[nodiscard]] bool enqueue_detached(Opcode opcode, RemoteHandle handle,
                                      bool stop_after_completion) noexcept {
    try {
      auto pending = std::make_shared<Pending>();
      RpcRequest request;
      request.handle = handle;
      auto encoded_request = make_request(config_.data_slot_size, request);
      if (!encoded_request) {
        return false;
      }
      pending->request_payload = std::move(encoded_request).value();
      pending->descriptor.opcode = opcode;
      pending->descriptor.flags = DescriptorFlag::request |
                                  DescriptorFlag::has_payload;
      pending->descriptor.payload_length =
          static_cast<std::uint32_t>(pending->request_payload.size());
      pending->detached = true;
      pending->stop_after_completion = stop_after_completion;
      pending->resume_worker = 0;
      pending->lane = 0;
      return enqueue(pending);
    } catch (...) {
      // Destructors are noexcept. Allocation failure follows the process-wide
      // fatal allocation policy used by Runtime queues.
      std::terminate();
    }
  }

  void maybe_shutdown() noexcept {
    if (client_alive_.load(std::memory_order_acquire) ||
        open_handles_.load(std::memory_order_acquire) != 0) {
      return;
    }
    bool expected = false;
    if (shutdown_enqueued_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
      if (!enqueue_detached(Opcode::shutdown_session, kInvalidRemoteHandle,
                            true)) {
        // There is no remaining Client or File owner to retry this request.
        // Break the I/O thread's self-ownership cycle and let transport teardown
        // make the server clean up the abandoned session.
        request_stop();
      }
    }
  }

  void wake_io_thread() noexcept {
    if (wake_fd_ < 0) {
      return;
    }
    const std::uint64_t one = 1;
    const auto result = ::write(wake_fd_, &one, sizeof(one));
    (void)result;
  }

  void drain_wake_fd() noexcept {
    std::uint64_t value;
    while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
  }

  void finish_pending(const std::shared_ptr<Pending>& pending,
                      std::error_code error, IpcDescriptor descriptor = {},
                      std::vector<std::byte> payload = {}) noexcept {
    pending->error = error;
    pending->reply.descriptor = descriptor;
    pending->reply.payload = std::move(payload);
    if (pending->continuation) {
      if (!runtime_->schedule(pending->continuation, pending->resume_worker)) {
        std::terminate();
      }
    }
    if (pending->stop_after_completion) {
      request_stop();
    }
  }

  void fail_all(std::error_code error) noexcept {
    std::vector<std::shared_ptr<Pending>> failed;
    {
      std::lock_guard lock(queue_mutex_);
      failed.reserve(pending_.size());
      for (auto& [id, pending] : pending_) {
        (void)id;
        failed.push_back(std::move(pending));
      }
      pending_.clear();
      for (auto& queue : outbound_) {
        queue.clear();
      }
    }
    for (auto& pending : failed) {
      finish_pending(pending, error);
    }
  }

  void pump_submissions() noexcept {
    for (std::uint32_t lane = 0; lane < config_.lane_count; ++lane) {
      for (;;) {
        std::shared_ptr<Pending> pending;
        {
          std::lock_guard lock(queue_mutex_);
          if (outbound_[lane].empty()) {
            break;
          }
          pending = outbound_[lane].front();
        }

        auto error = transport_.try_submit(
            lane, pending->descriptor, pending->request_payload,
            nullptr);
        if (error == make_error_code(TransportErrc::would_block)) {
          break;
        }
        {
          std::lock_guard lock(queue_mutex_);
          if (!outbound_[lane].empty() &&
              outbound_[lane].front() == pending) {
            outbound_[lane].pop_front();
          }
          if (error) {
            pending_.erase(pending->descriptor.request_id);
          }
        }
        if (error) {
          finish_pending(pending, error);
        }
      }
    }
  }

  void pump_completions(std::uint32_t lane,
                        std::vector<std::byte>& buffer) noexcept {
    std::uint64_t notifications{};
    const auto notify_error =
        transport_.drain_completion_notifications(lane, notifications);
    if (notify_error &&
        notify_error != make_error_code(TransportErrc::would_block)) {
      fail_all(notify_error);
      request_stop();
      return;
    }

    for (;;) {
      IpcDescriptor descriptor;
      std::size_t payload_size{};
      auto error = transport_.try_receive_completion(
          lane, descriptor, buffer, payload_size);
      if (error == make_error_code(TransportErrc::would_block)) {
        return;
      }
      if (error == make_error_code(TransportErrc::buffer_too_small)) {
        try {
          buffer.resize(payload_size);
        } catch (...) {
          fail_all(std::make_error_code(std::errc::not_enough_memory));
          request_stop();
          return;
        }
        continue;
      }
      if (error) {
        fail_all(error);
        request_stop();
        return;
      }

      std::shared_ptr<Pending> pending;
      {
        std::lock_guard lock(queue_mutex_);
        auto found = pending_.find(descriptor.request_id);
        if (found == pending_.end()) {
          continue;
        }
        pending = std::move(found->second);
        pending_.erase(found);
      }
      std::vector<std::byte> payload;
      try {
        payload.resize(payload_size);
        if (payload_size != 0) {
          std::memcpy(payload.data(), buffer.data(), payload_size);
        }
      } catch (...) {
        finish_pending(pending,
                       std::make_error_code(std::errc::not_enough_memory));
        continue;
      }
      finish_pending(pending, {}, descriptor, std::move(payload));
    }
  }

  bool empty() noexcept {
    std::lock_guard lock(queue_mutex_);
    return pending_.empty();
  }

  void io_loop() noexcept {
    std::vector<pollfd> poll_fds;
    std::vector<std::byte> payload_buffer;
    try {
      poll_fds.reserve(config_.lane_count + 1);
      poll_fds.push_back({wake_fd_, POLLIN, 0});
      for (std::uint32_t lane = 0; lane < config_.lane_count; ++lane) {
        poll_fds.push_back(
            {transport_.completion_event_fd(lane), POLLIN, 0});
      }
      payload_buffer.resize(config_.data_slot_size);
    } catch (...) {
      fail_all(std::make_error_code(std::errc::not_enough_memory));
      request_stop();
      return;
    }

    for (;;) {
      pump_submissions();
      for (std::uint32_t lane = 0; lane < config_.lane_count; ++lane) {
        pump_completions(lane, payload_buffer);
      }

      if (!transport_.peer_alive()) {
        request_stop();
        fail_all(std::make_error_code(std::errc::connection_reset));
        break;
      }
      if (stop_requested_.load(std::memory_order_acquire) && empty()) {
        break;
      }

      const int result = ::poll(poll_fds.data(), poll_fds.size(), 2);
      if (result < 0 && errno != EINTR) {
        request_stop();
        fail_all(std::error_code(errno, std::generic_category()));
        break;
      }
      if (poll_fds[0].revents & POLLIN) {
        drain_wake_fd();
      }
      for (auto& fd : poll_fds) {
        fd.revents = 0;
      }
      transport_.heartbeat();
    }

    // Make every terminal path reject future RPCs. request_stop() is
    // idempotent and closes the enqueue-vs-fail_all race via enqueue's locked
    // accepting check above.
    request_stop();
    fail_all(std::make_error_code(std::errc::not_connected));
  }

  Runtime* runtime_;
  ClientTransport transport_;
  TransportConfig config_;
  int wake_fd_{-1};
  std::thread io_thread_;

  std::atomic<bool> accepting_{true};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> client_alive_{true};
  std::atomic<bool> shutdown_enqueued_{false};
  std::atomic<std::size_t> open_handles_{0};
  std::atomic<std::uint64_t> next_request_id_{1};

  std::mutex queue_mutex_;
  std::unordered_map<std::uint64_t, std::shared_ptr<Pending>> pending_;
  std::vector<std::deque<std::shared_ptr<Pending>>> outbound_;
};

Client::Client(std::shared_ptr<Session> session) noexcept
    : session_(std::move(session)) {}

Client::Client(Client&& other) noexcept
    : session_(std::move(other.session_)) {}

Client& Client::operator=(Client&& other) noexcept {
  if (this != &other) {
    if (session_) {
      session_->release_client();
    }
    session_ = std::move(other.session_);
  }
  return *this;
}

Client::~Client() {
  if (session_) {
    session_->release_client();
  }
}

Task<Result<Client>> Client::connect(Runtime& runtime,
                                     ClientOptions options) {
  if (options.ring_capacity == 0 || options.data_slot_size <= sizeof(RpcRequest) ||
      runtime.worker_count() == 0 ||
      options.ring_capacity > std::numeric_limits<std::uint32_t>::max() ||
      options.data_slot_size > std::numeric_limits<std::uint32_t>::max() ||
      runtime.worker_count() > kMaxIpcWorkerLanes) {
    co_return Result<Client>::failure(
        std::make_error_code(std::errc::invalid_argument));
  }

  TransportConfig config{
      .lane_count = static_cast<std::uint32_t>(runtime.worker_count()),
      .ring_capacity = static_cast<std::uint32_t>(options.ring_capacity),
      .data_slot_size = static_cast<std::uint32_t>(options.data_slot_size),
  };
  auto connected =
      co_await ConnectAwaiter(runtime, std::move(options.endpoint), config);
  if (!connected) {
    co_return Result<Client>::failure(connected.error());
  }
  auto session = Session::create(runtime, std::move(connected).value());
  if (!session) {
    co_return Result<Client>::failure(session.error());
  }
  co_return Result<Client>::success(Client(std::move(session).value()));
}

Task<Result<File>> Client::open(std::string path, int flags,
                                std::uint32_t mode) {
  if (!session_) {
    co_return Result<File>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.open_flags = flags;
  request.mode = mode;
  auto reply = co_await session_->rpc(
      Opcode::open, make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<File>::failure(reply.error());
  }
  if (auto error = status_error(reply.value().descriptor.status)) {
    co_return Result<File>::failure(error);
  }
  const RemoteHandle handle = reply.value().descriptor.result_length;
  if (handle == kInvalidRemoteHandle) {
    co_return Result<File>::failure(std::make_error_code(std::errc::io_error));
  }
  session_->add_handle();
  co_return Result<File>::success(File(session_, handle));
}

Task<Result<File>> Client::open_at(const Directory& directory,
                                   std::string path, int flags,
                                   std::uint32_t mode) {
  if (!session_ || directory.session_ != session_ || !directory.valid()) {
    co_return Result<File>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = directory.handle_;
  request.open_flags = flags;
  request.mode = mode;
  auto reply = co_await session_->rpc(
      Opcode::open_at, make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<File>::failure(reply.error());
  }
  if (auto error = status_error(reply.value().descriptor.status)) {
    co_return Result<File>::failure(error);
  }
  const RemoteHandle handle = reply.value().descriptor.result_length;
  if (handle == kInvalidRemoteHandle) {
    co_return Result<File>::failure(std::make_error_code(std::errc::protocol_error));
  }
  session_->add_handle();
  co_return Result<File>::success(File(session_, handle));
}

Task<Result<Directory>> Client::open_directory(std::string path) {
  if (!session_) {
    co_return Result<Directory>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  auto reply = co_await session_->rpc(
      Opcode::open_directory,
      make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<Directory>::failure(reply.error());
  }
  if (auto error = status_error(reply.value().descriptor.status)) {
    co_return Result<Directory>::failure(error);
  }
  const RemoteHandle handle = reply.value().descriptor.result_length;
  if (handle == kInvalidRemoteHandle) {
    co_return Result<Directory>::failure(
        std::make_error_code(std::errc::protocol_error));
  }
  session_->add_handle();
  co_return Result<Directory>::success(Directory(session_, handle));
}

Task<Result<Directory>> Client::open_directory(const File& file) {
  if (!session_ || file.session_ != session_ || !file.valid()) {
    co_return Result<Directory>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = file.handle_;
  auto reply = co_await session_->rpc(
      Opcode::open_directory_handle,
      make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<Directory>::failure(reply.error());
  }
  if (auto error = status_error(reply.value().descriptor.status)) {
    co_return Result<Directory>::failure(error);
  }
  const RemoteHandle handle = reply.value().descriptor.result_length;
  if (handle == kInvalidRemoteHandle) {
    co_return Result<Directory>::failure(
        std::make_error_code(std::errc::protocol_error));
  }
  session_->add_handle();
  co_return Result<Directory>::success(Directory(session_, handle));
}

Task<Result<FileStat>> Client::stat(std::string path) {
  if (!session_) {
    co_return Result<FileStat>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  auto reply = co_await session_->rpc(
      Opcode::stat_path,
      make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<FileStat>::failure(reply.error());
  }
  auto decoded = decode_object<RpcFileStat>(reply.value());
  if (!decoded) {
    co_return Result<FileStat>::failure(decoded.error());
  }
  co_return Result<FileStat>::success(from_wire(decoded.value()));
}

Task<Result<void>> Client::make_directory(std::string path,
                                          std::uint32_t mode) {
  if (!session_) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.mode = mode;
  auto reply = co_await session_->rpc(
      Opcode::mkdir, make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> Client::remove_directory(std::string path) {
  if (!session_) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  auto reply = co_await session_->rpc(
      Opcode::rmdir, make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> Client::unlink(std::string path) {
  if (!session_) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  auto reply = co_await session_->rpc(
      Opcode::unlink, make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> Client::rename(std::string old_path,
                                  std::string new_path) {
  if (!session_) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  auto reply = co_await session_->rpc(
      Opcode::rename,
      make_request(session_->max_payload(), request, old_path, new_path));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> Client::truncate(std::string path,
                                    std::uint64_t size) {
  if (!session_) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.length = size;
  auto reply = co_await session_->rpc(
      Opcode::truncate_path,
      make_request(session_->max_payload(), request, path));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> Client::shutdown() {
  if (!session_) {
    co_return Result<void>::success();
  }
  RpcRequest request;
  auto session = session_;
  auto reply = co_await session->rpc(
      Opcode::shutdown_session,
      make_request(session->max_payload(), request), true);
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  session_.reset();
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

bool Client::valid() const noexcept {
  return session_ && session_->available();
}

File::File(std::shared_ptr<Session> session, RemoteHandle handle) noexcept
    : session_(std::move(session)), handle_(handle) {}

File::File(File&& other) noexcept
    : session_(std::move(other.session_)),
      handle_(std::exchange(other.handle_, kInvalidRemoteHandle)),
      offset_gate_(std::move(other.offset_gate_)) {}

File& File::operator=(File&& other) noexcept {
  if (this != &other) {
    if (session_ && handle_ != kInvalidRemoteHandle) {
      session_->release_handle(handle_, Opcode::close);
    }
    session_ = std::move(other.session_);
    handle_ = std::exchange(other.handle_, kInvalidRemoteHandle);
    offset_gate_ = std::move(other.offset_gate_);
  }
  return *this;
}

File::~File() {
  if (session_ && handle_ != kInvalidRemoteHandle) {
    session_->release_handle(handle_, Opcode::close);
  }
}

Task<Result<std::size_t>> File::read(std::span<std::byte> buffer) {
  if (!valid()) {
    co_return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  auto acquired = co_await offset_gate_.acquire(0, 1, RangeMode::write);
  if (!acquired) {
    co_return Result<std::size_t>::failure(acquired.error());
  }
  auto permit = std::move(acquired).value();
  auto result = co_await read_unlocked(buffer);
  auto released = co_await permit.release();
  if (!released) {
    co_return Result<std::size_t>::failure(released.error());
  }
  co_return result;
}

Task<Result<std::size_t>> File::read_unlocked(std::span<std::byte> buffer) {
  std::size_t completed = 0;
  const std::size_t max_chunk = session_->max_payload() - sizeof(RpcRequest);
  while (completed < buffer.size()) {
    const std::size_t chunk = std::min(max_chunk, buffer.size() - completed);
    RpcRequest request;
    request.handle = handle_;
    request.length = chunk;
    request.flags = static_cast<std::uint32_t>(RpcRequestFlag::implicit_offset);
    auto reply = co_await session_->rpc(
        Opcode::read, make_request(session_->max_payload(), request));
    if (!reply) {
      co_return Result<std::size_t>::failure(reply.error());
    }
    if (auto error = status_error(reply.value().descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.value().descriptor.result_length;
    if (bytes > chunk || reply.value().payload.size() != bytes) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    std::memcpy(buffer.data() + completed, reply.value().payload.data(), bytes);
    completed += bytes;
    if (bytes != chunk) {
      break;
    }
  }
  co_return Result<std::size_t>::success(completed);
}

Task<Result<std::size_t>> File::write(std::span<const std::byte> buffer) {
  if (!valid()) {
    co_return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  auto acquired = co_await offset_gate_.acquire(0, 1, RangeMode::write);
  if (!acquired) {
    co_return Result<std::size_t>::failure(acquired.error());
  }
  auto permit = std::move(acquired).value();
  auto result = co_await write_unlocked(buffer);
  auto released = co_await permit.release();
  if (!released) {
    co_return Result<std::size_t>::failure(released.error());
  }
  co_return result;
}

Task<Result<std::size_t>> File::write_unlocked(
    std::span<const std::byte> buffer) {
  std::size_t completed = 0;
  const std::size_t max_chunk = session_->max_payload() - sizeof(RpcRequest);
  while (completed < buffer.size()) {
    const std::size_t chunk = std::min(max_chunk, buffer.size() - completed);
    RpcRequest request;
    request.handle = handle_;
    request.length = chunk;
    request.flags = static_cast<std::uint32_t>(RpcRequestFlag::implicit_offset) |
                    static_cast<std::uint32_t>(RpcRequestFlag::data_follows);
    auto reply = co_await session_->rpc(
        Opcode::write,
        make_request(session_->max_payload(), request, {}, {},
                     buffer.subspan(completed, chunk)));
    if (!reply) {
      co_return Result<std::size_t>::failure(reply.error());
    }
    if (auto error = status_error(reply.value().descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.value().descriptor.result_length;
    if (bytes > chunk) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    completed += bytes;
    if (bytes != chunk) {
      break;
    }
  }
  co_return Result<std::size_t>::success(completed);
}

Task<Result<std::size_t>> File::read_at(std::uint64_t offset,
                                        std::span<std::byte> buffer) {
  if (!valid()) {
    co_return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  if (offset > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
      buffer.size() > static_cast<std::uint64_t>(
                          std::numeric_limits<std::int64_t>::max()) - offset) {
    co_return Result<std::size_t>::failure(
        std::make_error_code(std::errc::value_too_large));
  }
  std::size_t completed = 0;
  const std::size_t max_chunk = session_->max_payload() - sizeof(RpcRequest);
  while (completed < buffer.size()) {
    const std::size_t chunk = std::min(max_chunk, buffer.size() - completed);
    RpcRequest request;
    request.handle = handle_;
    request.offset = static_cast<std::int64_t>(offset + completed);
    request.length = chunk;
    auto reply = co_await session_->rpc(
        Opcode::read_at, make_request(session_->max_payload(), request));
    if (!reply) {
      co_return Result<std::size_t>::failure(reply.error());
    }
    if (auto error = status_error(reply.value().descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.value().descriptor.result_length;
    if (bytes > chunk || reply.value().payload.size() != bytes) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    std::memcpy(buffer.data() + completed, reply.value().payload.data(), bytes);
    completed += bytes;
    if (bytes != chunk) {
      break;
    }
  }
  co_return Result<std::size_t>::success(completed);
}

Task<Result<std::size_t>> File::write_at(
    std::uint64_t offset, std::span<const std::byte> buffer) {
  if (!valid()) {
    co_return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  if (offset > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
      buffer.size() > static_cast<std::uint64_t>(
                          std::numeric_limits<std::int64_t>::max()) - offset) {
    co_return Result<std::size_t>::failure(
        std::make_error_code(std::errc::value_too_large));
  }
  std::size_t completed = 0;
  const std::size_t max_chunk = session_->max_payload() - sizeof(RpcRequest);
  while (completed < buffer.size()) {
    const std::size_t chunk = std::min(max_chunk, buffer.size() - completed);
    RpcRequest request;
    request.handle = handle_;
    request.offset = static_cast<std::int64_t>(offset + completed);
    request.length = chunk;
    request.flags = static_cast<std::uint32_t>(RpcRequestFlag::data_follows);
    auto reply = co_await session_->rpc(
        Opcode::write_at,
        make_request(session_->max_payload(), request, {}, {},
                     buffer.subspan(completed, chunk)));
    if (!reply) {
      co_return Result<std::size_t>::failure(reply.error());
    }
    if (auto error = status_error(reply.value().descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.value().descriptor.result_length;
    if (bytes > chunk) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    completed += bytes;
    if (bytes != chunk) {
      break;
    }
  }
  co_return Result<std::size_t>::success(completed);
}

Task<Result<std::uint64_t>> File::seek(std::int64_t offset, int whence) {
  if (!valid()) {
    co_return Result<std::uint64_t>::failure(Errc::invalid_handle);
  }
  auto acquired = co_await offset_gate_.acquire(0, 1, RangeMode::write);
  if (!acquired) {
    co_return Result<std::uint64_t>::failure(acquired.error());
  }
  auto permit = std::move(acquired).value();
  auto result = co_await seek_unlocked(offset, whence);
  auto released = co_await permit.release();
  if (!released) {
    co_return Result<std::uint64_t>::failure(released.error());
  }
  co_return result;
}

Task<Result<std::uint64_t>> File::seek_unlocked(std::int64_t offset,
                                                int whence) {
  RpcRequest request;
  request.handle = handle_;
  request.offset = offset;
  request.whence = whence;
  auto reply = co_await session_->rpc(
      Opcode::seek, make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<std::uint64_t>::failure(reply.error());
  }
  if (auto error = status_error(reply.value().descriptor.status)) {
    co_return Result<std::uint64_t>::failure(error);
  }
  co_return Result<std::uint64_t>::success(reply.value().descriptor.result_length);
}

Task<Result<FileStat>> File::stat() {
  if (!valid()) {
    co_return Result<FileStat>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  auto reply = co_await session_->rpc(
      Opcode::stat_handle, make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<FileStat>::failure(reply.error());
  }
  auto decoded = decode_object<RpcFileStat>(reply.value());
  if (!decoded) {
    co_return Result<FileStat>::failure(decoded.error());
  }
  co_return Result<FileStat>::success(from_wire(decoded.value()));
}

Task<Result<FileSystemStat>> File::statfs() {
  if (!valid()) {
    co_return Result<FileSystemStat>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  auto reply = co_await session_->rpc(
      Opcode::statfs, make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<FileSystemStat>::failure(reply.error());
  }
  auto decoded = decode_object<RpcStatFs>(reply.value());
  if (!decoded) {
    co_return Result<FileSystemStat>::failure(decoded.error());
  }
  co_return Result<FileSystemStat>::success(from_wire(decoded.value()));
}

Task<Result<void>> File::truncate(std::uint64_t size) {
  if (!valid()) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  request.length = size;
  auto reply = co_await session_->rpc(
      Opcode::truncate_handle,
      make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> File::sync() {
  if (!valid()) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  auto reply = co_await session_->rpc(
      Opcode::sync, make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<int>> File::get_flags() {
  if (!valid()) {
    co_return Result<int>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  request.value = -1;
  auto reply = co_await session_->rpc(
      Opcode::set_flags, make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<int>::failure(reply.error());
  }
  if (auto error = status_error(reply.value().descriptor.status)) {
    co_return Result<int>::failure(error);
  }
  co_return Result<int>::success(
      static_cast<int>(reply.value().descriptor.result_length));
}

Task<Result<void>> File::set_flags(int flags) {
  if (!valid()) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  request.value = flags;
  auto reply = co_await session_->rpc(
      Opcode::set_flags, make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> File::close() {
  if (!valid()) {
    co_return Result<void>::success();
  }
  RpcRequest request;
  request.handle = handle_;
  auto reply = co_await session_->rpc(
      Opcode::close, make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  if (error) {
    co_return Result<void>::failure(error);
  }
  handle_ = kInvalidRemoteHandle;
  session_->release_handle_after_close();
  session_.reset();
  co_return Result<void>::success();
}

bool File::valid() const noexcept {
  return session_ && session_->available() &&
         handle_ != kInvalidRemoteHandle;
}

Directory::Directory(std::shared_ptr<Session> session,
                     RemoteHandle handle) noexcept
    : session_(std::move(session)), handle_(handle) {}

Directory::Directory(Directory&& other) noexcept
    : session_(std::move(other.session_)),
      handle_(std::exchange(other.handle_, kInvalidRemoteHandle)) {}

Directory& Directory::operator=(Directory&& other) noexcept {
  if (this != &other) {
    if (session_ && handle_ != kInvalidRemoteHandle) {
      session_->release_handle(handle_, Opcode::close_directory);
    }
    session_ = std::move(other.session_);
    handle_ = std::exchange(other.handle_, kInvalidRemoteHandle);
  }
  return *this;
}

Directory::~Directory() {
  if (session_ && handle_ != kInvalidRemoteHandle) {
    session_->release_handle(handle_, Opcode::close_directory);
  }
}

Task<Result<std::size_t>> Directory::next_batch(
    std::span<DirEntry> entries) {
  if (!valid()) {
    co_return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  const std::size_t max_entries =
      session_->max_payload() / sizeof(RpcDirEntry);
  const std::size_t requested = std::min(entries.size(), max_entries);
  RpcRequest request;
  request.handle = handle_;
  request.length = requested;
  auto reply = co_await session_->rpc(
      Opcode::read_directory_batch,
      make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<std::size_t>::failure(reply.error());
  }
  if (auto error = status_error(reply.value().descriptor.status)) {
    co_return Result<std::size_t>::failure(error);
  }
  const std::size_t count = reply.value().descriptor.result_length;
  if (count > requested ||
      reply.value().payload.size() != count * sizeof(RpcDirEntry)) {
    co_return Result<std::size_t>::failure(
        std::make_error_code(std::errc::protocol_error));
  }
  for (std::size_t i = 0; i < count; ++i) {
    RpcDirEntry wire{};
    std::memcpy(&wire,
                reply.value().payload.data() + i * sizeof(RpcDirEntry),
                sizeof(wire));
    if (wire.name_length >= wire.name.size()) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    entries[i] = DirEntry{
        .inode = wire.inode,
        .offset = wire.offset,
        .type = wire.type,
        .name = std::string(wire.name.data(), wire.name_length),
    };
  }
  co_return Result<std::size_t>::success(count);
}

Task<Result<void>> Directory::close() {
  if (!valid()) {
    co_return Result<void>::success();
  }
  RpcRequest request;
  request.handle = handle_;
  auto reply = co_await session_->rpc(
      Opcode::close_directory,
      make_request(session_->max_payload(), request));
  if (!reply) {
    co_return Result<void>::failure(reply.error());
  }
  const auto error = status_error(reply.value().descriptor.status);
  if (error) {
    co_return Result<void>::failure(error);
  }
  handle_ = kInvalidRemoteHandle;
  session_->release_handle_after_close();
  session_.reset();
  co_return Result<void>::success();
}

bool Directory::valid() const noexcept {
  return session_ && session_->available() &&
         handle_ != kInvalidRemoteHandle;
}

}  // namespace orchfs::async
