#include "orchfs/async/client.hpp"

#include "orchfs/async/ipc_transport.hpp"
#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <limits>
#include <new>
#include <memory_resource>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace orchfs::async {
namespace {

struct RpcReply {
  IpcDescriptor descriptor;
  std::vector<std::byte> payload;
  std::size_t payload_size{};
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
    continuation_ = continuation;
    auto started = ClientConnector::start(endpoint_, config_);
    if (!started) {
      error_ = started.error();
      return false;
    }
    connector_.emplace(std::move(started).value());
    auto registered = runtime_->register_poller(
        worker_, &ConnectAwaiter::poll, this);
    if (!registered) {
      error_ = registered.error();
      connector_.reset();
      return false;
    }
    registration_ = std::move(registered).value();
    return true;
  }

  Result<ClientTransport> await_resume() noexcept {
    registration_.reset();
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
  static Runtime::PollState poll(void* context) noexcept {
    return static_cast<ConnectAwaiter*>(context)->poll_once();
  }

  Runtime::PollState poll_once() noexcept {
    if (!connector_) {
      return Runtime::PollState::idle;
    }
    auto advanced = connector_->poll();
    if (!advanced) {
      error_ = advanced.error();
    } else {
      auto value = std::move(advanced).value();
      if (!value) {
        return Runtime::PollState::busy;
      }
      transport_ = std::move(*value);
    }
    connector_.reset();
    registration_.reset();
    const auto continuation =
        std::exchange(continuation_, std::coroutine_handle<>{});
    if (!continuation || !runtime_->schedule(continuation, worker_)) {
      std::terminate();
    }
    return Runtime::PollState::progress;
  }

  Runtime* runtime_;
  std::string endpoint_;
  TransportConfig config_;
  std::size_t worker_{};
  std::optional<ClientConnector> connector_;
  Runtime::PollRegistration registration_;
  std::coroutine_handle<> continuation_{};
  ClientTransport transport_;
  std::error_code error_;
};

}  // namespace

class Session final : public std::enable_shared_from_this<Session> {
 public:
  struct Pending {
    IpcDescriptor descriptor;
    std::vector<std::byte> request_payload;
    std::array<std::byte, sizeof(RpcRequest)> inline_request{};
    std::span<const std::byte> request_tail;
    std::span<std::byte> response_target;
    RpcReply reply;
    std::error_code error;
    std::coroutine_handle<> continuation{};
    std::size_t resume_worker{};
    std::uint32_t lane{};
    bool detached{};
    bool direct_response{};
    bool stop_after_completion{};
    bool uses_inline_request{};

    std::span<const std::byte> request_head() const noexcept {
      return uses_inline_request
                 ? std::span<const std::byte>(inline_request)
                 : std::span<const std::byte>(request_payload);
    }
  };

 private:
  struct InboxNode {
    std::shared_ptr<Pending> pending;
    InboxNode* next{};
  };

  struct LaneState {
    LaneState(Session& session_value, std::uint32_t lane_value,
              std::size_t owner_value)
        : session(&session_value), lane(lane_value), owner(owner_value) {}

    Session* session;
    std::uint32_t lane;
    std::size_t owner;
    Runtime::PollRegistration registration;
    std::atomic<InboxNode*> inbox{nullptr};
    std::atomic<std::size_t> active_submitters{0};
    std::atomic<bool> drained{false};
    std::pmr::unsynchronized_pool_resource pending_pool;
    std::pmr::unordered_map<std::uint64_t, std::shared_ptr<Pending>> pending{
        &pending_pool};
    std::deque<std::shared_ptr<Pending>> outbound;
  };

 public:

  static Result<std::shared_ptr<Session>> create(Runtime& runtime,
                                                 ClientTransport transport) {
    std::shared_ptr<Session> session;
    try {
      session = std::shared_ptr<Session>(
          new Session(runtime, std::move(transport)));
      const auto error = session->start();
      if (error) {
        return Result<std::shared_ptr<Session>>::failure(error);
      }
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
    control_registration_.reset();
    for (auto& lane : lanes_) {
      lane->registration.reset();
      InboxNode* node =
          lane->inbox.exchange(nullptr, std::memory_order_acquire);
      while (node != nullptr) {
        InboxNode* next = node->next;
        delete node;
        node = next;
      }
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
      bool stop_after_completion = false,
      std::span<const std::byte> request_tail = {},
      std::span<std::byte> response_target = {}) {
    if (!encoded_request) {
      co_return Result<RpcReply>::failure(encoded_request.error());
    }

    auto payload = std::move(encoded_request).value();
    std::shared_ptr<Pending> pending;
    try {
      pending = make_pending();
      pending->descriptor.opcode = opcode;
      pending->descriptor.flags = DescriptorFlag::request |
                                  DescriptorFlag::has_payload;
      pending->descriptor.payload_length =
          static_cast<std::uint32_t>(payload.size() + request_tail.size());
      pending->request_payload = std::move(payload);
      pending->request_tail = request_tail;
      pending->response_target = response_target;
      pending->direct_response = !response_target.empty();
      pending->stop_after_completion = stop_after_completion;
    } catch (const std::bad_alloc&) {
      co_return Result<RpcReply>::failure(
          std::make_error_code(std::errc::not_enough_memory));
    }

    auto reply = co_await PendingAwaiter(shared_from_this(), pending);
    co_return reply;
  }

  Task<Result<RpcReply>> rpc_header(
      Opcode opcode, RpcRequest request, std::size_t data_size = 0,
      std::span<const std::byte> request_tail = {},
      std::span<std::byte> response_target = {}) {
    if (sizeof(RpcRequest) > config_.data_slot_size ||
        data_size != request_tail.size() ||
        data_size > config_.data_slot_size - sizeof(RpcRequest) ||
        data_size > std::numeric_limits<std::uint32_t>::max()) {
      co_return Result<RpcReply>::failure(
          std::make_error_code(std::errc::message_size));
    }
    request.schema_version = kRpcSchemaVersion;
    request.path1_length = 0;
    request.path2_length = 0;
    request.data_length = static_cast<std::uint32_t>(data_size);

    std::shared_ptr<Pending> pending;
    try {
      pending = make_pending();
      std::memcpy(pending->inline_request.data(), &request, sizeof(request));
      pending->uses_inline_request = true;
      pending->descriptor.opcode = opcode;
      pending->descriptor.flags = DescriptorFlag::request |
                                  DescriptorFlag::has_payload;
      pending->descriptor.payload_length =
          static_cast<std::uint32_t>(sizeof(RpcRequest) + data_size);
      pending->request_tail = request_tail;
      pending->response_target = response_target;
      pending->direct_response = !response_target.empty();
    } catch (const std::bad_alloc&) {
      co_return Result<RpcReply>::failure(
          std::make_error_code(std::errc::not_enough_memory));
    }
    co_return co_await PendingAwaiter(shared_from_this(), pending);
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
    (void)runtime_->notify(control_owner_);
    for (const auto& lane : lanes_) {
      (void)runtime_->notify(lane->owner);
    }
  }

 private:
  std::shared_ptr<Pending> make_pending() {
    if (Runtime::current() == runtime_) {
      const std::size_t worker = Runtime::current_worker();
      if (worker != detail::no_worker && !lanes_.empty()) {
        auto& lane = *lanes_[worker % lanes_.size()];
        return std::allocate_shared<Pending>(
            std::pmr::polymorphic_allocator<Pending>(&lane.pending_pool));
      }
    }
    return std::make_shared<Pending>();
  }

  Session(Runtime& runtime, ClientTransport transport)
      : runtime_(&runtime), transport_(std::move(transport)),
        config_(transport_.config()),
        control_owner_(runtime.owner_for(transport_.client_id())) {
    lanes_.reserve(config_.lane_count);
    for (std::uint32_t lane = 0; lane < config_.lane_count; ++lane) {
      lanes_.push_back(std::make_unique<LaneState>(
          *this, lane, lane % runtime.worker_count()));
    }
  }

  std::error_code start() noexcept {
    keep_alive_ = shared_from_this();
    for (auto& lane : lanes_) {
      auto registered = runtime_->register_poller(
          lane->owner, &Session::poll_lane, lane.get());
      if (!registered) {
        const auto error = registered.error();
        request_stop();
        for (auto& registered_lane : lanes_) {
          registered_lane->registration.reset();
        }
        keep_alive_.reset();
        return error;
      }
      lane->registration = std::move(registered).value();
    }
    auto registered = runtime_->register_poller(
        control_owner_, &Session::poll_control, this);
    if (!registered) {
      const auto error = registered.error();
      request_stop();
      for (auto& lane : lanes_) {
        lane->registration.reset();
      }
      keep_alive_.reset();
      return error;
    }
    control_registration_ = std::move(registered).value();
    return {};
  }

  bool enqueue(const std::shared_ptr<Pending>& pending) noexcept {
    const auto request_head = pending->request_head();
    if (request_head.size() > config_.data_slot_size ||
        pending->request_tail.size() >
            config_.data_slot_size - request_head.size()) {
      pending->error = std::make_error_code(std::errc::message_size);
      return false;
    }
    if (pending->lane >= lanes_.size()) {
      pending->error = std::make_error_code(std::errc::invalid_argument);
      return false;
    }

    auto& lane = *lanes_[pending->lane];
    lane.active_submitters.fetch_add(1, std::memory_order_acq_rel);
    const auto finish_submit = [&lane] {
      if (lane.active_submitters.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        lane.active_submitters.notify_all();
      }
    };
    if (!accepting_.load(std::memory_order_acquire)) {
      pending->error = std::make_error_code(std::errc::not_connected);
      finish_submit();
      return false;
    }

    pending->descriptor.client_id = transport_.client_id();
    pending->descriptor.session_generation = transport_.session_generation();
    pending->descriptor.request_id =
        next_request_id_.fetch_add(1, std::memory_order_relaxed);
    if (pending->descriptor.request_id == 0) {
      pending->descriptor.request_id =
          next_request_id_.fetch_add(1, std::memory_order_relaxed);
    }
    pending->descriptor.resume_worker =
        static_cast<std::uint32_t>(pending->resume_worker);

    if (Runtime::current() == runtime_ &&
        Runtime::current_worker() == lane.owner) {
      outstanding_.fetch_add(1, std::memory_order_acq_rel);
      try {
        if (!lane.pending.emplace(
                 pending->descriptor.request_id, pending).second) {
          pending->error =
              std::make_error_code(std::errc::device_or_resource_busy);
          outstanding_.fetch_sub(1, std::memory_order_acq_rel);
          finish_submit();
          return false;
        }
        auto error = transport_.try_submit_scattered(
            lane.lane, pending->descriptor, request_head,
            pending->request_tail, nullptr);
        if (error == make_error_code(TransportErrc::would_block)) {
          lane.outbound.push_back(pending);
        } else if (error) {
          lane.pending.erase(pending->descriptor.request_id);
          pending->error = error;
          outstanding_.fetch_sub(1, std::memory_order_acq_rel);
          finish_submit();
          return false;
        }
      } catch (const std::bad_alloc&) {
        lane.pending.erase(pending->descriptor.request_id);
        pending->error = std::make_error_code(std::errc::not_enough_memory);
        outstanding_.fetch_sub(1, std::memory_order_acq_rel);
        finish_submit();
        return false;
      } catch (...) {
        std::terminate();
      }
      finish_submit();
      return true;
    }

    auto* node = new (std::nothrow) InboxNode{.pending = pending};
    if (node == nullptr) {
      pending->error = std::make_error_code(std::errc::not_enough_memory);
      finish_submit();
      return false;
    }
    if (!accepting_.load(std::memory_order_acquire)) {
      delete node;
      pending->error = std::make_error_code(std::errc::not_connected);
      finish_submit();
      return false;
    }
    outstanding_.fetch_add(1, std::memory_order_acq_rel);
    InboxNode* head = lane.inbox.load(std::memory_order_relaxed);
    do {
      node->next = head;
    } while (!lane.inbox.compare_exchange_weak(
        head, node, std::memory_order_release, std::memory_order_relaxed));
    finish_submit();
    if (!runtime_->notify(lane.owner)) {
      request_stop();
    }
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
        // Break the session's self-ownership cycle and let transport teardown
        // make the server clean up the abandoned session.
        request_stop();
      }
    }
  }

  void finish_pending(const std::shared_ptr<Pending>& pending,
                      std::error_code error, IpcDescriptor descriptor = {},
                      std::vector<std::byte> payload = {},
                      std::size_t direct_payload_size = 0) noexcept {
    pending->error = error;
    pending->reply.descriptor = descriptor;
    pending->reply.payload = std::move(payload);
    pending->reply.payload_size = pending->reply.payload.empty()
                                      ? direct_payload_size
                                      : pending->reply.payload.size();
    if (pending->continuation) {
      if (!runtime_->schedule(pending->continuation, pending->resume_worker)) {
        std::terminate();
      }
    }
    outstanding_.fetch_sub(1, std::memory_order_acq_rel);
    (void)runtime_->notify(control_owner_);
    if (pending->stop_after_completion) {
      request_stop();
    }
  }

  void fail_lane(LaneState& lane, std::error_code error) noexcept {
    std::vector<std::shared_ptr<Pending>> failed;
    try {
      failed.reserve(lane.pending.size());
      for (auto& [id, pending] : lane.pending) {
        (void)id;
        failed.push_back(std::move(pending));
      }
    } catch (...) {
      std::terminate();
    }
    lane.pending.clear();
    lane.outbound.clear();
    for (auto& pending : failed) {
      finish_pending(pending, error);
    }
  }

  bool drain_inbox(LaneState& lane) noexcept {
    InboxNode* stack =
        lane.inbox.exchange(nullptr, std::memory_order_acquire);
    const bool progress = stack != nullptr;
    InboxNode* fifo = nullptr;
    while (stack != nullptr) {
      InboxNode* next = stack->next;
      stack->next = fifo;
      fifo = stack;
      stack = next;
    }
    while (fifo != nullptr) {
      InboxNode* next = fifo->next;
      auto pending = std::move(fifo->pending);
      delete fifo;
      fifo = next;
      if (!accepting_.load(std::memory_order_acquire)) {
        finish_pending(pending,
                       std::make_error_code(std::errc::not_connected));
        continue;
      }
      try {
        const bool inserted = lane.pending.emplace(
            pending->descriptor.request_id, pending).second;
        if (!inserted) {
          finish_pending(
              pending,
              std::make_error_code(std::errc::device_or_resource_busy));
          continue;
        }
        try {
          lane.outbound.push_back(pending);
        } catch (...) {
          lane.pending.erase(pending->descriptor.request_id);
          throw;
        }
      } catch (const std::bad_alloc&) {
        finish_pending(pending,
                       std::make_error_code(std::errc::not_enough_memory));
      } catch (...) {
        std::terminate();
      }
    }
    return progress;
  }

  bool pump_submissions(LaneState& lane) noexcept {
    bool progress = false;
    for (;;) {
      if (lane.outbound.empty()) {
        break;
      }
      auto pending = lane.outbound.front();

      auto error = transport_.try_submit_scattered(
          lane.lane, pending->descriptor, pending->request_head(),
          pending->request_tail, nullptr);
      if (error == make_error_code(TransportErrc::would_block)) {
        break;
      }
      progress = true;
      if (!lane.outbound.empty() && lane.outbound.front() == pending) {
        lane.outbound.pop_front();
      }
      if (error) {
        lane.pending.erase(pending->descriptor.request_id);
        finish_pending(pending, error);
      }
    }
    return progress;
  }

  bool pump_completions(LaneState& lane) noexcept {
    bool progress = false;
    for (;;) {
      ReceivedIpcSlot completion;
      auto error = transport_.try_acquire_completion(
          lane.lane, completion);
      if (error == make_error_code(TransportErrc::would_block)) {
        return progress;
      }
      progress = true;
      if (error) {
        fail_lane(lane, error);
        request_stop();
        return true;
      }

      const auto descriptor = completion.descriptor();
      const auto received_payload = completion.payload();
      auto found = lane.pending.find(descriptor.request_id);
      if (found == lane.pending.end()) {
        continue;
      }
      auto pending = std::move(found->second);
      lane.pending.erase(found);
      if (pending->direct_response) {
        if (received_payload.size() > pending->response_target.size()) {
          finish_pending(
              pending, std::make_error_code(std::errc::protocol_error));
          continue;
        }
        if (!received_payload.empty()) {
          std::memcpy(pending->response_target.data(), received_payload.data(),
                      received_payload.size());
        }
        finish_pending(pending, {}, descriptor, {}, received_payload.size());
        continue;
      }
      std::vector<std::byte> payload;
      try {
        payload.resize(received_payload.size());
        if (!received_payload.empty()) {
          std::memcpy(payload.data(), received_payload.data(),
                      received_payload.size());
        }
      } catch (...) {
        finish_pending(pending,
                       std::make_error_code(std::errc::not_enough_memory));
        continue;
      }
      finish_pending(pending, {}, descriptor, std::move(payload));
    }
  }

  static bool lane_empty(const LaneState& lane) noexcept {
    return lane.pending.empty() && lane.outbound.empty() &&
           lane.inbox.load(std::memory_order_acquire) == nullptr &&
           lane.active_submitters.load(std::memory_order_acquire) == 0;
  }

  static Runtime::PollState poll_lane(void* context) noexcept {
    auto& lane = *static_cast<LaneState*>(context);
    return lane.session->poll_lane_once(lane);
  }

  Runtime::PollState poll_lane_once(LaneState& lane) noexcept {
    if (lane.drained.load(std::memory_order_acquire)) {
      return Runtime::PollState::idle;
    }
    bool progress = drain_inbox(lane);
    progress |= pump_submissions(lane);
    progress |= pump_completions(lane);
    if (stop_requested_.load(std::memory_order_acquire)) {
      accepting_.store(false, std::memory_order_release);
      if (lane.active_submitters.load(std::memory_order_acquire) == 0) {
        progress |= drain_inbox(lane);
        if (!lane.pending.empty()) {
          fail_lane(
              lane,
              peer_failed_.load(std::memory_order_acquire)
                  ? std::make_error_code(std::errc::connection_reset)
                  : std::make_error_code(std::errc::not_connected));
          progress = true;
        }
      }
      if (lane_empty(lane)) {
        lane.drained.store(true, std::memory_order_release);
        lane.registration.reset();
        (void)runtime_->notify(control_owner_);
        return Runtime::PollState::progress;
      }
    }
    if (progress) {
      return Runtime::PollState::progress;
    }
    return lane.pending.empty() ? Runtime::PollState::idle
                                : Runtime::PollState::busy;
  }

  static Runtime::PollState poll_control(void* context) noexcept {
    return static_cast<Session*>(context)->poll_control_once();
  }

  Runtime::PollState poll_control_once() noexcept {
    if (finished_.load(std::memory_order_acquire)) {
      return Runtime::PollState::idle;
    }
    constexpr std::uint32_t kHealthPollMask = 1023;
    const bool idle = outstanding_.load(std::memory_order_acquire) == 0;
    if (idle || (++health_poll_ticks_ & kHealthPollMask) == 0) {
      transport_.heartbeat();
      if (!transport_.peer_alive()) {
        peer_failed_.store(true, std::memory_order_release);
        request_stop();
      }
    }
    if (!stop_requested_.load(std::memory_order_acquire)) {
      return idle ? Runtime::PollState::idle : Runtime::PollState::busy;
    }
    const bool all_drained = std::all_of(
        lanes_.begin(), lanes_.end(), [](const auto& lane) {
          return lane->drained.load(std::memory_order_acquire);
        });
    if (!all_drained) {
      return Runtime::PollState::busy;
    }
    finished_.store(true, std::memory_order_release);
    finished_.notify_all();
    // keep_alive_ may be the last owner. Hold the session until this callback
    // returns, while keeping shared_ptr traffic off the steady polling path.
    auto self = shared_from_this();
    control_registration_.reset();
    keep_alive_.reset();
    return Runtime::PollState::progress;
  }

  Runtime* runtime_;
  ClientTransport transport_;
  TransportConfig config_;
  std::size_t control_owner_{};
  Runtime::PollRegistration control_registration_;
  std::shared_ptr<Session> keep_alive_;
  std::vector<std::unique_ptr<LaneState>> lanes_;

  std::atomic<bool> accepting_{true};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> peer_failed_{false};
  std::atomic<bool> finished_{false};
  std::atomic<bool> client_alive_{true};
  std::atomic<bool> shutdown_enqueued_{false};
  std::atomic<std::size_t> open_handles_{0};
  std::atomic<std::size_t> outstanding_{0};
  std::atomic<std::uint64_t> next_request_id_{1};
  std::uint32_t health_poll_ticks_{};
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
    auto reply = co_await session_->rpc_header(
        Opcode::read, request, 0, {}, buffer.subspan(completed, chunk));
    if (!reply) {
      co_return Result<std::size_t>::failure(reply.error());
    }
    if (auto error = status_error(reply.value().descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.value().descriptor.result_length;
    if (bytes > chunk || reply.value().payload_size != bytes) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    if (!reply.value().payload.empty()) {
      std::memcpy(buffer.data() + completed, reply.value().payload.data(),
                  bytes);
    }
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
    auto reply = co_await session_->rpc_header(
        Opcode::write, request, chunk, buffer.subspan(completed, chunk));
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
    auto reply = co_await session_->rpc_header(
        Opcode::read_at, request, 0, {}, buffer.subspan(completed, chunk));
    if (!reply) {
      co_return Result<std::size_t>::failure(reply.error());
    }
    if (auto error = status_error(reply.value().descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.value().descriptor.result_length;
    if (bytes > chunk || reply.value().payload_size != bytes) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    if (!reply.value().payload.empty()) {
      std::memcpy(buffer.data() + completed, reply.value().payload.data(),
                  bytes);
    }
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
    auto reply = co_await session_->rpc_header(
        Opcode::write_at, request, chunk, buffer.subspan(completed, chunk));
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
  request.value = F_GETFL;
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
  request.value = F_SETFL;
  request.open_flags = flags;
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
