#include "orchfs/async/client.hpp"

#include "orchfs/async/detail/concurrency.hpp"
#include "orchfs/async/detail/range_lock.hpp"
#include "orchfs/async/detail/stat_conversion.hpp"
#include "orchfs/async/ipc_transport.hpp"
#include "orchfs/async/repro_trace.hpp"
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
  detail::append_object(bytes, wire);
  detail::append_bytes(bytes, path1.data(), path1.size());
  detail::append_bytes(bytes, path2.data(), path2.size());
  detail::append_bytes(bytes, data.data(), data.size());
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

std::error_code positioned_range_error(std::uint64_t offset,
                                       std::size_t length) noexcept {
  constexpr auto max_offset =
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  if (offset > max_offset || length > max_offset - offset) {
    return std::make_error_code(std::errc::value_too_large);
  }
  return {};
}

template <typename T>
[[gnu::always_inline]] inline Result<T> decode_reply(const RpcReply& reply) {
  if (auto error = status_error(reply.descriptor.status)) {
    return Result<T>::failure(error);
  }
  return detail::decode_object<T>(reply.payload);
}

FileStat from_wire(const RpcFileStat& value) noexcept {
  return detail::copy_file_stat_fields<FileStat>(value);
}

FileSystemStat from_wire(const RpcStatFs& value) noexcept {
  return detail::copy_filesystem_stat_fields<FileSystemStat>(value);
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
  struct BlockingCall {
    std::atomic<bool> ready{false};
    std::error_code error;
    ReceivedIpcSlot completion;

    void wait(std::size_t spin_count) noexcept {
      bool completed = ready.load(std::memory_order_acquire);
      while (!completed && spin_count-- != 0) {
#if defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#else
        std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
        completed = ready.load(std::memory_order_acquire);
      }
      while (!completed) {
        ready.wait(false, std::memory_order_acquire);
        completed = ready.load(std::memory_order_acquire);
      }
    }
  };

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
    BlockingCall* blocking_call{};
    bool detached{};
    bool direct_response{};
    bool stop_after_completion{};
    bool uses_inline_request{};
    std::uint64_t trace_started_ns{};

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
    detail::MpscInbox<InboxNode> inbox;
    std::atomic<std::size_t> active_submitters{0};
    std::atomic<std::size_t> direct_waiters{0};
    std::atomic_flag transport_gate = ATOMIC_FLAG_INIT;
    std::atomic<bool> peer_failure_checked{false};
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
      InboxNode* node = lane->inbox.take_all();
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
      pending_->lane = session_->select_lane(pending_->resume_worker);
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
      Opcode opcode, RpcRequest request,
      std::span<const std::byte> request_tail = {},
      std::span<std::byte> response_target = {}) {
    auto created =
        make_header_pending(opcode, request, request_tail, response_target);
    if (!created) {
      co_return Result<RpcReply>::failure(created.error());
    }
    auto pending = std::move(created).value();
    co_return co_await PendingAwaiter(shared_from_this(), pending);
  }

  Result<ReceivedIpcSlot> rpc_header_blocking(
      Opcode opcode, RpcRequest request,
      std::span<const std::byte> request_tail = {}) {
    if (auto* current_runtime = Runtime::current(); current_runtime != nullptr) {
      return Result<ReceivedIpcSlot>::failure(
          current_runtime == runtime_ ? Errc::join_from_worker
                                      : Errc::wrong_runtime);
    }

    if (lanes_.size() > 1) {
      return rpc_header_direct_blocking(opcode, request, request_tail);
    }

    auto created = make_header_pending(opcode, request, request_tail, {});
    if (!created) {
      return Result<ReceivedIpcSlot>::failure(created.error());
    }
    auto pending = std::move(created).value();
    BlockingCall call;
    pending->blocking_call = &call;
    pending->lane = next_lane_.fetch_add(1, std::memory_order_relaxed) %
                    static_cast<std::uint32_t>(lanes_.size());
    pending->resume_worker = lanes_[pending->lane]->owner;
    if (!enqueue(pending)) {
      return Result<ReceivedIpcSlot>::failure(pending->error);
    }

    const std::size_t spin_count = runtime_->begin_blocking_wait();
    call.wait(spin_count);
    runtime_->end_blocking_wait();
    if (call.error) {
      return Result<ReceivedIpcSlot>::failure(call.error);
    }
    return Result<ReceivedIpcSlot>::success(std::move(call.completion));
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
  Result<ReceivedIpcSlot> rpc_header_direct_blocking(
      Opcode opcode, RpcRequest request,
      std::span<const std::byte> request_tail) noexcept {
    if (sizeof(RpcRequest) > config_.data_slot_size ||
        request_tail.size() > config_.data_slot_size - sizeof(RpcRequest)) {
      return Result<ReceivedIpcSlot>::failure(
          std::make_error_code(std::errc::message_size));
    }

    // Give each blocking caller a stable SHM lane.  Using one process-wide
    // blocking lane serialized otherwise independent POSIX pwrite/pread calls
    // at the transport gate and prevented KFS/SPDK from seeing concurrency.
    static thread_local const Session* direct_session = nullptr;
    static thread_local std::uint32_t direct_lane = 0;
    if (direct_session != this) {
      direct_lane = next_direct_lane_.fetch_add(1, std::memory_order_relaxed) %
                    static_cast<std::uint32_t>(lanes_.size());
      direct_session = this;
    }
    auto& lane = *lanes_[direct_lane];
    lane.direct_waiters.fetch_add(1, std::memory_order_acq_rel);
    while (lane.transport_gate.test_and_set(std::memory_order_acquire)) {
      if (!accepting_.load(std::memory_order_acquire)) {
        lane.direct_waiters.fetch_sub(1, std::memory_order_acq_rel);
        return Result<ReceivedIpcSlot>::failure(
            std::make_error_code(std::errc::not_connected));
      }
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#else
      std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
    }
    lane.direct_waiters.fetch_sub(1, std::memory_order_acq_rel);
    struct GateGuard final {
      std::atomic_flag* gate;
      ~GateGuard() { gate->clear(std::memory_order_release); }
    } gate_guard{&lane.transport_gate};

    if (!accepting_.load(std::memory_order_acquire)) {
      return Result<ReceivedIpcSlot>::failure(
          std::make_error_code(std::errc::not_connected));
    }
    lane.active_submitters.fetch_add(1, std::memory_order_acq_rel);
    outstanding_.fetch_add(1, std::memory_order_acq_rel);
    struct CounterGuard final {
      Session* session;
      LaneState* lane;
      ~CounterGuard() {
        session->outstanding_.fetch_sub(1, std::memory_order_acq_rel);
        if (lane->active_submitters.fetch_sub(
                1, std::memory_order_acq_rel) == 1) {
          lane->active_submitters.notify_all();
        }
      }
    } counter_guard{this, &lane};

    request.schema_version = kRpcSchemaVersion;
    request.path1_length = 0;
    request.path2_length = 0;
    request.data_length = static_cast<std::uint32_t>(request_tail.size());
    IpcDescriptor descriptor;
    descriptor.opcode = opcode;
    descriptor.flags = DescriptorFlag::request | DescriptorFlag::has_payload;
    descriptor.payload_length = static_cast<std::uint32_t>(
        sizeof(RpcRequest) + request_tail.size());
    descriptor.resume_worker = static_cast<std::uint32_t>(lane.owner);
    const auto request_head = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(&request), sizeof(request));
    std::uint64_t request_id = 0;
    const std::uint64_t trace_started_ns = orchfs_repro_trace_begin();
    std::error_code error;
    do {
      error = transport_.try_submit_scattered(
          lane.lane, descriptor, request_head, request_tail, &request_id);
      if (error == make_error_code(TransportErrc::would_block)) {
#if defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#else
        std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
      }
    } while (error == make_error_code(TransportErrc::would_block) &&
             accepting_.load(std::memory_order_acquire));
    if (error) {
      return Result<ReceivedIpcSlot>::failure(error);
    }

    std::uint32_t health_ticks = 0;
    for (;;) {
      ReceivedIpcSlot completion;
      error = transport_.try_acquire_completion(lane.lane, completion);
      if (!error) {
        const auto& incoming = completion.descriptor();
        if (incoming.request_id != request_id) {
          return Result<ReceivedIpcSlot>::failure(
              std::make_error_code(std::errc::protocol_error));
        }
        if (trace_started_ns != 0) {
          const int trace_error = incoming.status < 0
              ? -incoming.status : incoming.status;
          orchfs_repro_trace_end(
              ORCHFS_TRACE_CLIENT_ROUND_TRIP, request_id, trace_started_ns,
              incoming.result_length, 1, trace_error);
        }
        std::uint64_t notifications = 0;
        (void)transport_.drain_completion_notifications(
            lane.lane, notifications);
        return Result<ReceivedIpcSlot>::success(std::move(completion));
      }
      if (error != make_error_code(TransportErrc::would_block)) {
        return Result<ReceivedIpcSlot>::failure(error);
      }
      if ((++health_ticks & 1023U) == 0 && !transport_.peer_alive()) {
        return Result<ReceivedIpcSlot>::failure(
            std::make_error_code(std::errc::connection_reset));
      }
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#else
      std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
    }
  }

  Result<std::shared_ptr<Pending>> make_header_pending(
      Opcode opcode, RpcRequest request,
      std::span<const std::byte> request_tail,
      std::span<std::byte> response_target) {
    const std::size_t data_size = request_tail.size();
    if (sizeof(RpcRequest) > config_.data_slot_size ||
        data_size > config_.data_slot_size - sizeof(RpcRequest)) {
      return Result<std::shared_ptr<Pending>>::failure(
          std::make_error_code(std::errc::message_size));
    }
    request.schema_version = kRpcSchemaVersion;
    request.path1_length = 0;
    request.path2_length = 0;
    request.data_length = static_cast<std::uint32_t>(data_size);

    try {
      auto pending = make_pending();
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
      return Result<std::shared_ptr<Pending>>::success(std::move(pending));
    } catch (const std::bad_alloc&) {
      return Result<std::shared_ptr<Pending>>::failure(
          std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
      std::terminate();
    }
  }

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

  std::uint32_t select_lane(std::size_t resume_worker) noexcept {
    const std::uint32_t asynchronous_lanes = config_.lane_count > 1
        ? config_.lane_count - 1 : config_.lane_count;
    if (asynchronous_lanes <= runtime_->worker_count()) {
      return static_cast<std::uint32_t>(resume_worker % asynchronous_lanes);
    }
    return next_lane_.fetch_add(1, std::memory_order_relaxed) %
           asynchronous_lanes;
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
    pending->trace_started_ns = orchfs_repro_trace_begin();
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
    lane.inbox.push(*node);
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
    if (pending->blocking_call != nullptr) {
      finish_blocking_pending(pending, error, {});
      return;
    }
    if (pending->trace_started_ns != 0) {
      int trace_error = error ? error.value() : 0;
      if (trace_error == 0 && descriptor.status != 0) {
        trace_error = descriptor.status < 0 ? -descriptor.status
                                             : descriptor.status;
      }
      orchfs_repro_trace_end(
          ORCHFS_TRACE_CLIENT_ROUND_TRIP,
          pending->descriptor.request_id, pending->trace_started_ns,
          descriptor.result_length, 1, trace_error);
      pending->trace_started_ns = 0;
    }
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

  void finish_blocking_pending(const std::shared_ptr<Pending>& pending,
                               std::error_code error,
                               ReceivedIpcSlot completion) noexcept {
    BlockingCall* call = std::exchange(pending->blocking_call, nullptr);
    if (call == nullptr) {
      std::terminate();
    }
    if (pending->trace_started_ns != 0) {
      const auto descriptor = completion.descriptor();
      int trace_error = error ? error.value() : 0;
      if (trace_error == 0 && descriptor.status != 0) {
        trace_error = descriptor.status < 0 ? -descriptor.status
                                             : descriptor.status;
      }
      orchfs_repro_trace_end(
          ORCHFS_TRACE_CLIENT_ROUND_TRIP,
          pending->descriptor.request_id, pending->trace_started_ns,
          descriptor.result_length, 1, trace_error);
      pending->trace_started_ns = 0;
    }
    call->error = error;
    call->completion = std::move(completion);
    outstanding_.fetch_sub(1, std::memory_order_acq_rel);
    (void)runtime_->notify(control_owner_);
    if (pending->stop_after_completion) {
      request_stop();
    }
    // Publish last: after this store the external thread may destroy both its
    // stack call cell and the Pending owner immediately.
    call->ready.store(true, std::memory_order_release);
    call->ready.notify_one();
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
    if (lane.inbox.empty()) {
      return false;
    }
    InboxNode* fifo = lane.inbox.drain();
    const bool progress = fifo != nullptr;
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
      if (pending->blocking_call != nullptr) {
        finish_blocking_pending(pending, {}, std::move(completion));
        continue;
      }
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
           lane.inbox.empty() &&
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
    if (lane.direct_waiters.load(std::memory_order_acquire) != 0 ||
        lane.transport_gate.test_and_set(std::memory_order_acquire)) {
      return Runtime::PollState::busy;
    }
    struct GateGuard final {
      std::atomic_flag* gate;
      ~GateGuard() { gate->clear(std::memory_order_release); }
    } gate_guard{&lane.transport_gate};
    bool progress = drain_inbox(lane);
    progress |= pump_submissions(lane);
    progress |= pump_completions(lane);
    if (peer_failed_.load(std::memory_order_acquire) &&
        !lane.peer_failure_checked.load(std::memory_order_acquire)) {
      // The server publishes any terminal completion before marking the
      // transport dead. Give every lane one owner-local receive pass before
      // control fails requests that genuinely have no completion. Otherwise
      // a graceful shutdown reply can race with the control-socket close and
      // be reported as ECONNRESET.
      progress |= pump_completions(lane);
      lane.peer_failure_checked.store(true, std::memory_order_release);
      (void)runtime_->notify(control_owner_);
      progress = true;
    }
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
        // Control may release keep_alive_ as soon as every lane reports
        // drained. Pin only this terminal callback and publish drained after
        // clearing the owner-local registration member.
        auto self = shared_from_this();
        lane.registration.reset();
        (void)runtime_->notify(control_owner_);
        lane.drained.store(true, std::memory_order_release);
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
        const bool first_failure =
            !peer_failed_.exchange(true, std::memory_order_acq_rel);
        if (first_failure) {
          for (const auto& lane : lanes_) {
            (void)runtime_->notify(lane->owner);
          }
        }
      }
    }
    if (peer_failed_.load(std::memory_order_acquire)) {
      const bool lanes_checked = std::all_of(
          lanes_.begin(), lanes_.end(), [](const auto& lane) {
            return lane->peer_failure_checked.load(std::memory_order_acquire);
          });
      if (lanes_checked) {
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
    // keep_alive_ may be the last owner. Hold the session until this callback
    // returns, while keeping shared_ptr traffic off the steady polling path.
    auto self = shared_from_this();
    control_registration_.reset();
    finished_.store(true, std::memory_order_release);
    finished_.notify_all();
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
  std::atomic<std::uint32_t> next_lane_{0};
  std::atomic<std::uint32_t> next_direct_lane_{0};
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
  const std::size_t lane_count =
      options.lane_count == 0 ? runtime.worker_count() : options.lane_count;
  if (options.ring_capacity == 0 || options.data_slot_size <= sizeof(RpcRequest) ||
      runtime.worker_count() == 0 || lane_count == 0 ||
      options.ring_capacity > std::numeric_limits<std::uint32_t>::max() ||
      options.data_slot_size > std::numeric_limits<std::uint32_t>::max() ||
      lane_count > kMaxIpcWorkerLanes) {
    co_return Result<Client>::failure(
        std::make_error_code(std::errc::invalid_argument));
  }

  TransportConfig config{
      .lane_count = static_cast<std::uint32_t>(lane_count),
      .ring_capacity = static_cast<std::uint32_t>(options.ring_capacity),
      .data_slot_size = static_cast<std::uint32_t>(options.data_slot_size),
  };
  ORCHFS_TRY(transport, co_await ConnectAwaiter(
      runtime, std::move(options.endpoint), config));
  ORCHFS_TRY(session, Session::create(runtime, std::move(transport)));
  co_return Result<Client>::success(Client(std::move(session)));
}

Task<Result<File>> Client::open(std::string path, int flags,
                                std::uint32_t mode) {
  if (!session_) {
    co_return Result<File>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.open_flags = flags;
  request.mode = mode;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::open, make_request(session_->max_payload(), request, path)));
  if (auto error = status_error(reply.descriptor.status)) {
    co_return Result<File>::failure(error);
  }
  const RemoteHandle handle = reply.descriptor.result_length;
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
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::open_at, make_request(session_->max_payload(), request, path)));
  if (auto error = status_error(reply.descriptor.status)) {
    co_return Result<File>::failure(error);
  }
  const RemoteHandle handle = reply.descriptor.result_length;
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
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::open_directory,
      make_request(session_->max_payload(), request, path)));
  if (auto error = status_error(reply.descriptor.status)) {
    co_return Result<Directory>::failure(error);
  }
  const RemoteHandle handle = reply.descriptor.result_length;
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
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::open_directory_handle,
      make_request(session_->max_payload(), request)));
  if (auto error = status_error(reply.descriptor.status)) {
    co_return Result<Directory>::failure(error);
  }
  const RemoteHandle handle = reply.descriptor.result_length;
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
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::stat_path,
      make_request(session_->max_payload(), request, path)));
  ORCHFS_TRY(decoded, decode_reply<RpcFileStat>(reply));
  co_return Result<FileStat>::success(from_wire(decoded));
}

Task<Result<void>> Client::path_mutation(Opcode opcode, RpcRequest request,
                                         std::string path1,
                                         std::string path2) {
  if (!session_) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  ORCHFS_TRY(reply, co_await session_->rpc(
      opcode, make_request(session_->max_payload(), request, path1, path2)));
  const auto error = status_error(reply.descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> Client::make_directory(std::string path,
                                          std::uint32_t mode) {
  RpcRequest request;
  request.mode = mode;
  return path_mutation(Opcode::mkdir, request, std::move(path));
}

Task<Result<void>> Client::remove_directory(std::string path) {
  RpcRequest request;
  return path_mutation(Opcode::rmdir, request, std::move(path));
}

Task<Result<void>> Client::unlink(std::string path) {
  RpcRequest request;
  return path_mutation(Opcode::unlink, request, std::move(path));
}

Task<Result<void>> Client::rename(std::string old_path,
                                  std::string new_path) {
  RpcRequest request;
  return path_mutation(Opcode::rename, request, std::move(old_path),
                       std::move(new_path));
}

Task<Result<void>> Client::truncate(std::string path,
                                    std::uint64_t size) {
  RpcRequest request;
  request.length = size;
  return path_mutation(Opcode::truncate_path, request, std::move(path));
}

Task<Result<void>> Client::shutdown() {
  if (!session_) {
    co_return Result<void>::success();
  }
  RpcRequest request;
  auto session = session_;
  ORCHFS_TRY(reply, co_await session->rpc(
      Opcode::shutdown_session,
      make_request(session->max_payload(), request), true));
  const auto error = status_error(reply.descriptor.status);
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
  ORCHFS_WITH_RANGE_LOCK(result, offset_gate_, 0, 1, RangeMode::write,
                         read_unlocked(buffer));
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
    ORCHFS_TRY(reply, co_await session_->rpc_header(
        Opcode::read, request, {}, buffer.subspan(completed, chunk)));
    if (auto error = status_error(reply.descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.descriptor.result_length;
    if (bytes > chunk || reply.payload_size != bytes) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    if (!reply.payload.empty()) {
      std::memcpy(buffer.data() + completed, reply.payload.data(),
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
  ORCHFS_WITH_RANGE_LOCK(result, offset_gate_, 0, 1, RangeMode::write,
                         write_unlocked(buffer));
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
    ORCHFS_TRY(reply, co_await session_->rpc_header(
        Opcode::write, request, buffer.subspan(completed, chunk)));
    if (auto error = status_error(reply.descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.descriptor.result_length;
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
  if (const auto error = positioned_range_error(offset, buffer.size())) {
    co_return Result<std::size_t>::failure(error);
  }
  std::size_t completed = 0;
  const std::size_t max_chunk = session_->max_payload() - sizeof(RpcRequest);
  while (completed < buffer.size()) {
    const std::size_t chunk = std::min(max_chunk, buffer.size() - completed);
    RpcRequest request;
    request.handle = handle_;
    request.offset = static_cast<std::int64_t>(offset + completed);
    request.length = chunk;
    ORCHFS_TRY(reply, co_await session_->rpc_header(
        Opcode::read_at, request, {}, buffer.subspan(completed, chunk)));
    if (auto error = status_error(reply.descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.descriptor.result_length;
    if (bytes > chunk || reply.payload_size != bytes) {
      co_return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    if (!reply.payload.empty()) {
      std::memcpy(buffer.data() + completed, reply.payload.data(),
                  bytes);
    }
    completed += bytes;
    if (bytes != chunk) {
      break;
    }
  }
  co_return Result<std::size_t>::success(completed);
}

Result<std::size_t> File::read_at_blocking(
    std::uint64_t offset, std::span<std::byte> buffer) {
  if (!valid()) {
    return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  if (const auto error = positioned_range_error(offset, buffer.size())) {
    return Result<std::size_t>::failure(error);
  }

  std::size_t completed = 0;
  const std::size_t max_chunk = session_->max_payload() - sizeof(RpcRequest);
  while (completed < buffer.size()) {
    const std::size_t chunk = std::min(max_chunk, buffer.size() - completed);
    RpcRequest request;
    request.handle = handle_;
    request.offset = static_cast<std::int64_t>(offset + completed);
    request.length = chunk;
    auto received =
        session_->rpc_header_blocking(Opcode::read_at, request);
    if (!received) {
      return Result<std::size_t>::failure(received.error());
    }
    auto completion = std::move(received).value();
    const auto& descriptor = completion.descriptor();
    const auto payload = completion.payload();
    if (auto error = status_error(descriptor.status)) {
      return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = descriptor.result_length;
    if (bytes > chunk || payload.size() != bytes) {
      return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    if (!payload.empty()) {
      std::memcpy(buffer.data() + completed, payload.data(), payload.size());
    }
    completed += bytes;
    if (bytes != chunk) {
      break;
    }
  }
  return Result<std::size_t>::success(completed);
}

Task<Result<std::size_t>> File::write_at(
    std::uint64_t offset, std::span<const std::byte> buffer) {
  if (!valid()) {
    co_return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  if (const auto error = positioned_range_error(offset, buffer.size())) {
    co_return Result<std::size_t>::failure(error);
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
    ORCHFS_TRY(reply, co_await session_->rpc_header(
        Opcode::write_at, request, buffer.subspan(completed, chunk)));
    if (auto error = status_error(reply.descriptor.status)) {
      co_return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = reply.descriptor.result_length;
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

Result<std::size_t> File::write_at_blocking(
    std::uint64_t offset, std::span<const std::byte> buffer) {
  if (!valid()) {
    return Result<std::size_t>::failure(Errc::invalid_handle);
  }
  if (const auto error = positioned_range_error(offset, buffer.size())) {
    return Result<std::size_t>::failure(error);
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
    auto received = session_->rpc_header_blocking(
        Opcode::write_at, request, buffer.subspan(completed, chunk));
    if (!received) {
      return Result<std::size_t>::failure(received.error());
    }
    auto completion = std::move(received).value();
    const auto& descriptor = completion.descriptor();
    if (auto error = status_error(descriptor.status)) {
      return Result<std::size_t>::failure(error);
    }
    const std::size_t bytes = descriptor.result_length;
    if (bytes > chunk) {
      return Result<std::size_t>::failure(
          std::make_error_code(std::errc::protocol_error));
    }
    completed += bytes;
    if (bytes != chunk) {
      break;
    }
  }
  return Result<std::size_t>::success(completed);
}

Task<Result<std::uint64_t>> File::seek(std::int64_t offset, int whence) {
  if (!valid()) {
    co_return Result<std::uint64_t>::failure(Errc::invalid_handle);
  }
  ORCHFS_WITH_RANGE_LOCK(result, offset_gate_, 0, 1, RangeMode::write,
                         seek_unlocked(offset, whence));
  co_return result;
}

Task<Result<std::uint64_t>> File::seek_unlocked(std::int64_t offset,
                                                int whence) {
  RpcRequest request;
  request.handle = handle_;
  request.offset = offset;
  request.whence = whence;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::seek, make_request(session_->max_payload(), request)));
  if (auto error = status_error(reply.descriptor.status)) {
    co_return Result<std::uint64_t>::failure(error);
  }
  co_return Result<std::uint64_t>::success(reply.descriptor.result_length);
}

Task<Result<FileStat>> File::stat() {
  if (!valid()) {
    co_return Result<FileStat>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::stat_handle, make_request(session_->max_payload(), request)));
  ORCHFS_TRY(decoded, decode_reply<RpcFileStat>(reply));
  co_return Result<FileStat>::success(from_wire(decoded));
}

Task<Result<FileSystemStat>> File::statfs() {
  if (!valid()) {
    co_return Result<FileSystemStat>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::statfs, make_request(session_->max_payload(), request)));
  ORCHFS_TRY(decoded, decode_reply<RpcStatFs>(reply));
  co_return Result<FileSystemStat>::success(from_wire(decoded));
}

Task<Result<void>> File::truncate(std::uint64_t size) {
  if (!valid()) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  request.length = size;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::truncate_handle,
      make_request(session_->max_payload(), request)));
  const auto error = status_error(reply.descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> File::sync() {
  if (!valid()) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::sync, make_request(session_->max_payload(), request)));
  const auto error = status_error(reply.descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<int>> File::get_flags() {
  if (!valid()) {
    co_return Result<int>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  request.value = F_GETFL;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::set_flags, make_request(session_->max_payload(), request)));
  if (auto error = status_error(reply.descriptor.status)) {
    co_return Result<int>::failure(error);
  }
  co_return Result<int>::success(
      static_cast<int>(reply.descriptor.result_length));
}

Task<Result<void>> File::set_flags(int flags) {
  if (!valid()) {
    co_return Result<void>::failure(Errc::invalid_handle);
  }
  RpcRequest request;
  request.handle = handle_;
  request.value = F_SETFL;
  request.open_flags = flags;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::set_flags, make_request(session_->max_payload(), request)));
  const auto error = status_error(reply.descriptor.status);
  co_return error ? Result<void>::failure(error) : Result<void>::success();
}

Task<Result<void>> File::close() {
  if (!valid()) {
    co_return Result<void>::success();
  }
  RpcRequest request;
  request.handle = handle_;
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::close, make_request(session_->max_payload(), request)));
  const auto error = status_error(reply.descriptor.status);
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
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::read_directory_batch,
      make_request(session_->max_payload(), request)));
  if (auto error = status_error(reply.descriptor.status)) {
    co_return Result<std::size_t>::failure(error);
  }
  const std::size_t count = reply.descriptor.result_length;
  if (count > requested ||
      reply.payload.size() != count * sizeof(RpcDirEntry)) {
    co_return Result<std::size_t>::failure(
        std::make_error_code(std::errc::protocol_error));
  }
  for (std::size_t i = 0; i < count; ++i) {
    RpcDirEntry wire{};
    std::memcpy(&wire,
                reply.payload.data() + i * sizeof(RpcDirEntry),
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
  ORCHFS_TRY(reply, co_await session_->rpc(
      Opcode::close_directory,
      make_request(session_->max_payload(), request)));
  const auto error = status_error(reply.descriptor.status);
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
