#include "orchfs/async/client.hpp"
#include "orchfs/async/ipc_transport.hpp"
#include "orchfs/async/rpc_protocol.hpp"
#include "orchfs/async/runtime.hpp"
#include "orchfs/async/server.hpp"

#include "../LibFS/orchfs.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <poll.h>

namespace {

std::string root_path;
std::mutex directory_mutex;
std::unordered_map<int, DIR*> directories;
int next_directory_handle = 100000;
std::atomic<std::size_t> legacy_call_count{0};
std::atomic<bool> legacy_called_on_runtime{false};
std::mutex legacy_thread_mutex;
std::unordered_set<std::thread::id> legacy_threads;
std::mutex file_handle_mutex;
std::unordered_map<int, std::string> file_handle_paths;
std::unordered_set<int> close_failed_once;
std::atomic<std::size_t> active_legacy_handles{0};

void record_legacy_call() {
  legacy_call_count.fetch_add(1, std::memory_order_relaxed);
  if (orchfs::async::Runtime::current() != nullptr) {
    legacy_called_on_runtime.store(true, std::memory_order_relaxed);
  }
  std::lock_guard lock(legacy_thread_mutex);
  legacy_threads.insert(std::this_thread::get_id());
}

std::string host_path(const char* path) {
  std::string_view view(path == nullptr ? "" : path);
  while (!view.empty() && view.front() == '/') {
    view.remove_prefix(1);
  }
  return root_path + "/" + std::string(view);
}

int directory_fd(int handle) {
  std::lock_guard lock(directory_mutex);
  const auto found = directories.find(handle);
  return found == directories.end() ? -1 : ::dirfd(found->second);
}

void remember_file_handle(int fd, std::string path) {
  if (fd < 0) {
    return;
  }
  {
    std::lock_guard lock(file_handle_mutex);
    file_handle_paths.insert_or_assign(fd, std::move(path));
  }
  active_legacy_handles.fetch_add(1, std::memory_order_relaxed);
}

bool file_path_is(int fd, std::string_view expected) {
  std::lock_guard lock(file_handle_mutex);
  const auto found = file_handle_paths.find(fd);
  return found != file_handle_paths.end() && found->second == expected;
}

bool fail_this_close_once(int fd) {
  std::lock_guard lock(file_handle_mutex);
  const auto found = file_handle_paths.find(fd);
  return found != file_handle_paths.end() && found->second == "/close-retry" &&
         close_failed_once.insert(fd).second;
}

int legacy_open_flags(int flags) noexcept {
  int ignored = O_TRUNC | O_APPEND;
#ifdef O_DIRECTORY
  // The production legacy core ignores O_DIRECTORY. Keep the test double from
  // hiding the server-side fd type validation behind host open(2) behavior.
  ignored |= O_DIRECTORY;
#endif
  return flags & ~ignored;
}

void forget_file_handle(int fd) {
  bool erased = false;
  {
    std::lock_guard lock(file_handle_mutex);
    erased = file_handle_paths.erase(fd) != 0;
    close_failed_once.erase(fd);
  }
  if (erased) {
    active_legacy_handles.fetch_sub(1, std::memory_order_relaxed);
  }
}

template <typename T>
T run(orchfs::async::Runtime& runtime,
      orchfs::async::Task<orchfs::async::Result<T>> task) {
  auto submitted = runtime.submit(std::move(task));
  if (!submitted) {
    throw std::system_error(submitted.error());
  }
  auto handle = std::move(submitted).value();
  auto joined = std::move(handle).join();
  if (!joined) {
    throw std::system_error(joined.error());
  }
  auto result = std::move(joined).value();
  if (!result) {
    throw std::system_error(result.error());
  }
  return std::move(result).value();
}

void run(orchfs::async::Runtime& runtime,
         orchfs::async::Task<orchfs::async::Result<void>> task) {
  auto submitted = runtime.submit(std::move(task));
  if (!submitted) {
    throw std::system_error(submitted.error());
  }
  auto handle = std::move(submitted).value();
  auto joined = std::move(handle).join();
  if (!joined) {
    throw std::system_error(joined.error());
  }
  auto result = std::move(joined).value();
  if (!result) {
    throw std::system_error(result.error());
  }
}

template <typename T>
std::error_code run_error(
    orchfs::async::Runtime& runtime,
    orchfs::async::Task<orchfs::async::Result<T>> task) {
  auto submitted = runtime.submit(std::move(task));
  if (!submitted) {
    return submitted.error();
  }
  auto joined = std::move(submitted).value().join();
  if (!joined) {
    return joined.error();
  }
  auto result = std::move(joined).value();
  return result ? std::error_code{} : result.error();
}

void require(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error(std::string(message));
  }
}

struct RawReply {
  orchfs::async::IpcDescriptor descriptor;
  std::vector<std::byte> payload;
};

std::vector<std::byte> make_raw_request(
    orchfs::async::RpcRequest request, std::string_view path1 = {},
    std::string_view path2 = {}, std::span<const std::byte> data = {}) {
  request.schema_version = orchfs::async::kRpcSchemaVersion;
  request.path1_length = static_cast<std::uint32_t>(path1.size());
  request.path2_length = static_cast<std::uint32_t>(path2.size());
  request.data_length = static_cast<std::uint32_t>(data.size());
  std::vector<std::byte> payload(sizeof(request) + path1.size() + path2.size() +
                                 data.size());
  std::byte* cursor = payload.data();
  std::memcpy(cursor, &request, sizeof(request));
  cursor += sizeof(request);
  if (!path1.empty()) {
    std::memcpy(cursor, path1.data(), path1.size());
  }
  cursor += path1.size();
  if (!path2.empty()) {
    std::memcpy(cursor, path2.data(), path2.size());
  }
  cursor += path2.size();
  if (!data.empty()) {
    std::memcpy(cursor, data.data(), data.size());
  }
  return payload;
}

void raw_submit(orchfs::async::ClientTransport& transport, std::uint32_t lane,
                orchfs::async::Opcode opcode,
                std::vector<std::byte> payload) {
  orchfs::async::IpcDescriptor request;
  request.opcode = opcode;
  request.flags = orchfs::async::DescriptorFlag::request;
  if (!payload.empty()) {
    request.flags |= orchfs::async::DescriptorFlag::has_payload;
  }
  const auto error = transport.try_submit(lane, request, payload);
  if (error) {
    throw std::system_error(error);
  }
}

void raw_submit_retry(orchfs::async::ClientTransport& transport,
                      std::uint32_t lane, orchfs::async::Opcode opcode,
                      const std::vector<std::byte>& payload) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  for (;;) {
    orchfs::async::IpcDescriptor request;
    request.opcode = opcode;
    request.flags = orchfs::async::DescriptorFlag::request;
    if (!payload.empty()) {
      request.flags |= orchfs::async::DescriptorFlag::has_payload;
    }
    const auto error = transport.try_submit(lane, request, payload);
    if (!error) {
      return;
    }
    if (error != orchfs::async::make_error_code(
                     orchfs::async::TransportErrc::would_block)) {
      throw std::system_error(error);
    }
    require(std::chrono::steady_clock::now() < deadline,
            "raw RPC submission timeout");
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

RawReply raw_receive(orchfs::async::ClientTransport& transport,
                     std::uint32_t lane) {
  pollfd completion_fd{transport.completion_event_fd(lane), POLLIN, 0};
  int poll_result;
  do {
    poll_result = ::poll(&completion_fd, 1, 5000);
  } while (poll_result < 0 && errno == EINTR);
  require(poll_result == 1 && (completion_fd.revents & POLLIN) != 0,
          "raw RPC completion timeout");
  std::uint64_t notifications = 0;
  auto error = transport.drain_completion_notifications(lane, notifications);
  if (error) {
    throw std::system_error(error);
  }
  require(notifications != 0, "raw RPC completion notification missing");

  RawReply reply;
  reply.payload.resize(4096);
  std::size_t payload_size = 0;
  error = transport.try_receive_completion(lane, reply.descriptor,
                                           reply.payload, payload_size);
  if (error) {
    throw std::system_error(error);
  }
  reply.payload.resize(payload_size);
  return reply;
}

RawReply raw_receive_eventually(orchfs::async::ClientTransport& transport,
                                std::uint32_t lane) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  for (;;) {
    RawReply reply;
    reply.payload.resize(4096);
    std::size_t payload_size = 0;
    auto error = transport.try_receive_completion(
        lane, reply.descriptor, reply.payload, payload_size);
    if (!error) {
      reply.payload.resize(payload_size);
      return reply;
    }
    if (error != orchfs::async::make_error_code(
                     orchfs::async::TransportErrc::would_block)) {
      throw std::system_error(error);
    }
    require(std::chrono::steady_clock::now() < deadline,
            "raw RPC completion timeout");
    pollfd completion_fd{transport.completion_event_fd(lane), POLLIN, 0};
    int poll_result;
    do {
      poll_result = ::poll(&completion_fd, 1, 100);
    } while (poll_result < 0 && errno == EINTR);
    require(poll_result >= 0, "raw RPC completion poll failed");
    if (poll_result == 1 && (completion_fd.revents & POLLIN) != 0) {
      std::uint64_t notifications = 0;
      error =
          transport.drain_completion_notifications(lane, notifications);
      if (error) {
        throw std::system_error(error);
      }
    }
  }
}

void wait_for_completion_notifications(
    orchfs::async::ClientTransport& transport, std::uint32_t lane,
    std::uint64_t expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  std::uint64_t observed = 0;
  while (observed < expected) {
    require(std::chrono::steady_clock::now() < deadline,
            "completion ring did not fill");
    pollfd completion_fd{transport.completion_event_fd(lane), POLLIN, 0};
    int poll_result;
    do {
      poll_result = ::poll(&completion_fd, 1, 100);
    } while (poll_result < 0 && errno == EINTR);
    require(poll_result >= 0, "completion ring poll failed");
    if (poll_result == 0 || (completion_fd.revents & POLLIN) == 0) {
      continue;
    }
    std::uint64_t notifications = 0;
    const auto error =
        transport.drain_completion_notifications(lane, notifications);
    if (error) {
      throw std::system_error(error);
    }
    observed += notifications;
  }
}

RawReply raw_rpc(orchfs::async::ClientTransport& transport,
                 orchfs::async::Opcode opcode,
                 std::vector<std::byte> payload, std::uint32_t lane = 0) {
  raw_submit(transport, lane, opcode, std::move(payload));
  return raw_receive(transport, lane);
}

orchfs::async::ClientTransport connect_raw(
    const std::string& endpoint, orchfs::async::TransportConfig config) {
  std::error_code error;
  auto transport =
      orchfs::async::ClientTransport::connect(endpoint, config, error);
  if (error) {
    throw std::system_error(error);
  }
  require(static_cast<bool>(transport), "raw transport connect failed");
  return transport;
}

orchfs::async::ClientTransport connect_raw(const std::string& endpoint) {
  return connect_raw(
      endpoint,
      {.lane_count = 2, .ring_capacity = 8, .data_slot_size = 4096});
}

int connect_without_hello(const std::string& endpoint) {
  const int fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category());
  }
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  require(endpoint.size() < sizeof(address.sun_path),
          "control endpoint is too long");
  std::memcpy(address.sun_path, endpoint.data(), endpoint.size());
  address.sun_path[endpoint.size()] = '\0';
  const auto length = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + endpoint.size() + 1U);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), length) != 0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category());
  }
  return fd;
}

orchfs::async::RemoteHandle raw_open(
    orchfs::async::ClientTransport& transport, std::string_view path,
    int flags = O_CREAT | O_RDWR, std::uint32_t lane = 0) {
  orchfs::async::RpcRequest request;
  request.open_flags = flags;
  request.mode = 0644;
  const auto reply = raw_rpc(transport, orchfs::async::Opcode::open,
                             make_raw_request(request, path), lane);
  require(reply.descriptor.status == 0, "raw open failed");
  return reply.descriptor.result_length;
}

void raw_close(orchfs::async::ClientTransport& transport,
               orchfs::async::RemoteHandle handle) {
  orchfs::async::RpcRequest request;
  request.handle = handle;
  const auto reply = raw_rpc(transport, orchfs::async::Opcode::close,
                             make_raw_request(request));
  require(reply.descriptor.status == 0, "raw close failed");
}

orchfs::async::RpcFileStat raw_stat(
    orchfs::async::ClientTransport& transport,
    orchfs::async::RemoteHandle handle) {
  orchfs::async::RpcRequest request;
  request.handle = handle;
  const auto reply = raw_rpc(transport, orchfs::async::Opcode::stat_handle,
                             make_raw_request(request));
  require(reply.descriptor.status == 0 &&
              reply.payload.size() == sizeof(orchfs::async::RpcFileStat),
          "raw fstat failed");
  orchfs::async::RpcFileStat value;
  std::memcpy(&value, reply.payload.data(), sizeof(value));
  return value;
}

void submit_stat_burst(orchfs::async::ClientTransport& transport,
                       orchfs::async::RemoteHandle handle) {
  orchfs::async::RpcRequest request;
  request.handle = handle;
  const auto payload = make_raw_request(request);
  for (int index = 0; index < 3; ++index) {
    raw_submit_retry(transport, 0, orchfs::async::Opcode::stat_handle,
                     payload);
  }
  wait_for_completion_notifications(transport, 0, 2);
  // The first two replies now occupy the entire CQ. Give the server I/O loop
  // time to exercise the would_block retry path for the third non-empty reply.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

void exercise_completion_backpressure(const std::string& endpoint) {
  auto transport = connect_raw(
      endpoint,
      {.lane_count = 1, .ring_capacity = 2, .data_slot_size = 4096});
  const auto handle = raw_open(transport, "/completion-backpressure");
  submit_stat_burst(transport, handle);
  for (int index = 0; index < 3; ++index) {
    const auto reply = raw_receive_eventually(transport, 0);
    require(reply.descriptor.status == 0 &&
                reply.payload.size() == sizeof(orchfs::async::RpcFileStat),
            "CQ retry lost a completion payload");
  }
  raw_close(transport, handle);
}

orchfs::async::ClientTransport make_saturated_session(
    const std::string& endpoint) {
  auto transport = connect_raw(
      endpoint,
      {.lane_count = 1, .ring_capacity = 2, .data_slot_size = 4096});
  const auto handle = raw_open(transport, "/saturated-on-server-stop");
  submit_stat_burst(transport, handle);
  return transport;
}

void exercise_close_capture_race(
    orchfs::async::ClientTransport& transport) {
  for (int iteration = 0; iteration < 64; ++iteration) {
    const std::string original_path =
        "/capture-race-original-" + std::to_string(iteration);
    const auto original = raw_open(transport, original_path);
    const auto original_stat = raw_stat(transport, original);

    orchfs::async::RpcRequest close_request;
    close_request.handle = original;
    orchfs::async::RpcRequest stat_request;
    stat_request.handle = original;
    // Different lanes let both Runtime roots capture/schedule independently.
    // If stat captures first but close wins lifecycle-write, stat must return
    // EBADF rather than touching a subsequently reused legacy descriptor.
    raw_submit(transport, 0, orchfs::async::Opcode::stat_handle,
               make_raw_request(stat_request));
    raw_submit(transport, 1, orchfs::async::Opcode::close,
               make_raw_request(close_request));
    const auto close_reply = raw_receive(transport, 1);
    require(close_reply.descriptor.status == 0,
            "capture-race close failed");

    const std::string replacement_path =
        "/capture-race-replacement-" + std::to_string(iteration);
    const auto replacement =
        raw_open(transport, replacement_path, O_CREAT | O_RDWR, 1);
    const auto stat_reply = raw_receive(transport, 0);
    if (stat_reply.descriptor.status == 0) {
      require(stat_reply.payload.size() ==
                  sizeof(orchfs::async::RpcFileStat),
              "capture-race stat payload malformed");
      orchfs::async::RpcFileStat raced_stat;
      std::memcpy(&raced_stat, stat_reply.payload.data(), sizeof(raced_stat));
      require(raced_stat.inode == original_stat.inode,
              "captured handle observed a reused legacy fd");
    } else {
      require(stat_reply.descriptor.status == -EBADF,
              "capture-race stat returned an unexpected error");
    }
    raw_close(transport, replacement);
  }
}

void exercise_raw_openat_and_disconnect(const std::string& endpoint) {
  auto transport = connect_raw(endpoint);
  const auto file_handle = raw_open(transport, "/raw-file");

  orchfs::async::RpcRequest invalid_io;
  invalid_io.handle = file_handle;
  invalid_io.offset = -1;
  invalid_io.length = 1;
  auto reply = raw_rpc(transport, orchfs::async::Opcode::read_at,
                       make_raw_request(invalid_io));
  require(reply.descriptor.status == -EINVAL,
          "negative positioned read reached the legacy core");
  const std::array<std::byte, 1> invalid_write{std::byte{0x5a}};
  reply = raw_rpc(transport, orchfs::async::Opcode::write_at,
                  make_raw_request(invalid_io, {}, {}, invalid_write));
  require(reply.descriptor.status == -EINVAL,
          "negative positioned write reached the legacy core");
  invalid_io.offset = std::numeric_limits<std::int64_t>::max();
  invalid_io.length = 2;
  reply = raw_rpc(transport, orchfs::async::Opcode::read_at,
                  make_raw_request(invalid_io));
  require(reply.descriptor.status == -EOVERFLOW,
          "overflowing positioned I/O range was accepted");

  orchfs::async::RpcRequest open_at;
  open_at.handle = std::numeric_limits<orchfs::async::RemoteHandle>::max();
  open_at.open_flags = O_CREAT | O_RDWR;
  open_at.mode = 0644;
  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, "relative-invalid"));
  require(reply.descriptor.status == -EBADF,
          "relative open_at accepted an invalid handle");

  open_at.handle = file_handle;
  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, "relative-nondirectory"));
  require(reply.descriptor.status == -ENOTDIR,
          "relative open_at accepted a non-directory handle");

  open_at.handle = std::numeric_limits<orchfs::async::RemoteHandle>::max();
  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, "/absolute-openat"));
  require(reply.descriptor.status == 0,
          "absolute open_at incorrectly validated dirfd");
  raw_close(transport, reply.descriptor.result_length);

  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, ""));
  require(reply.descriptor.status == -ENOENT,
          "empty open_at path reached the legacy core");

  orchfs::async::RpcRequest missing;
  missing.open_flags = O_RDONLY;
  reply = raw_rpc(transport, orchfs::async::Opcode::open,
                  make_raw_request(missing, "/definitely-missing"));
  require(reply.descriptor.status == -ENOENT,
          "missing open did not return ENOENT");

  raw_close(transport, file_handle);
  exercise_close_capture_race(transport);
  (void)raw_open(transport, "/abandoned-on-disconnect");
  // Transport destruction is intentional: the server must run the same
  // once-only cleanup used by explicit shutdown.
}

struct SharedLibraryCloser {
  void operator()(void* library) const noexcept {
    if (library != nullptr) {
      (void)::dlclose(library);
    }
  }
};

template <typename Function>
Function wrapper_symbol(void* library, const char* name) {
  (void)::dlerror();
  void* address = ::dlsym(library, name);
  const char* error = ::dlerror();
  require(error == nullptr && address != nullptr,
          std::string("dlsym failed for ") + name +
              (error == nullptr ? "" : std::string(": ") + error));
  Function function{};
  static_assert(sizeof(function) == sizeof(address));
  std::memcpy(&function, &address, sizeof(function));
  return function;
}

struct RemoveTree {
  std::filesystem::path path;

  ~RemoveTree() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

struct RestoreWorkingDirectory {
  int fd{-1};

  ~RestoreWorkingDirectory() {
    if (fd >= 0) {
      (void)::fchdir(fd);
      (void)::close(fd);
    }
  }
};

int run_wrapper_client(const std::string& endpoint) {
  try {
    for (int retry = 0; retry < 200 && ::access(endpoint.c_str(), F_OK) != 0;
         ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    require(::access(endpoint.c_str(), F_OK) == 0,
            "wrapper client timed out waiting for server");
    require(::setenv("ORCHFS_ASYNC_ENDPOINT", endpoint.c_str(), 1) == 0,
            "setenv(ORCHFS_ASYNC_ENDPOINT) failed");
    require(::setenv("ORCHFS_CLIENT_WORKERS", "1", 1) == 0,
            "setenv(ORCHFS_CLIENT_WORKERS) failed");
    require(::setenv("ORCHFS_IPC_RING_CAPACITY", "8", 1) == 0,
            "setenv(ORCHFS_IPC_RING_CAPACITY) failed");
    require(::setenv("ORCHFS_IPC_DATA_SLOT_SIZE", "4096", 1) == 0,
            "setenv(ORCHFS_IPC_DATA_SLOT_SIZE) failed");

    void* loaded = ::dlopen(ORCHFS_WRAPPER_PATH, RTLD_NOW | RTLD_LOCAL);
    const char* load_error = loaded == nullptr ? ::dlerror() : nullptr;
    require(loaded != nullptr,
            std::string("dlopen wrapper failed") +
                (load_error == nullptr ? "" : std::string(": ") + load_error));
    std::unique_ptr<void, SharedLibraryCloser> library(loaded);

    using Open = int (*)(const char*, int, ...);
    using OpenAt = int (*)(int, const char*, int, ...);
    using Close = int (*)(int);
    using Mkdir = int (*)(const char*, mode_t);
    using Unlink = int (*)(const char*);
    using Rmdir = int (*)(const char*);
    using Rename = int (*)(const char*, const char*);
    const auto wrapped_open = wrapper_symbol<Open>(loaded, "open");
    const auto wrapped_openat = wrapper_symbol<OpenAt>(loaded, "openat");
    const auto wrapped_close = wrapper_symbol<Close>(loaded, "close");
    const auto wrapped_mkdir = wrapper_symbol<Mkdir>(loaded, "mkdir");
    const auto wrapped_unlink = wrapper_symbol<Unlink>(loaded, "unlink");
    const auto wrapped_rmdir = wrapper_symbol<Rmdir>(loaded, "rmdir");
    const auto wrapped_rename = wrapper_symbol<Rename>(loaded, "rename");

    require(wrapped_mkdir("\\/adapter-openat-directory", 0755) == 0,
            "wrapper mkdir fixture failed");
    const int directory =
        wrapped_open("\\/adapter-openat-directory", O_RDONLY | O_CLOEXEC);
    require(directory >= 1048576,
            "plain directory open did not return a virtual descriptor");
    const int child = wrapped_openat(
        directory, "child", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0644);
    require(child >= 1048576,
            "openat rejected a directory opened without O_DIRECTORY");
    require(wrapped_close(child) == 0 && wrapped_close(directory) == 0,
            "wrapper directory descriptor close failed");
    struct stat child_stat {};
    require(::stat((root_path + "/adapter-openat-directory/child").c_str(),
                   &child_stat) == 0 &&
                S_ISREG(child_stat.st_mode),
            "wrapper openat created no file under the anchored directory");

    const int regular = wrapped_open("\\/adapter-openat-regular",
                                     O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                                     0644);
    require(regular >= 1048576,
            "wrapper regular-file fixture open failed");
    errno = 0;
    const int rejected = wrapped_openat(
        regular, "child", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0644);
    const int rejected_error = errno;
    require(rejected == -1 && rejected_error == ENOTDIR,
            "regular virtual descriptor was accepted as an openat dirfd");
    require(wrapped_close(regular) == 0,
            "wrapper regular descriptor close failed");
    require(wrapped_unlink("\\/adapter-openat-directory/child") == 0 &&
                wrapped_rmdir("\\/adapter-openat-directory") == 0 &&
                wrapped_unlink("\\/adapter-openat-regular") == 0,
            "wrapper openat fixture cleanup failed");

    const auto host_directory =
        std::filesystem::temp_directory_path() /
        ("orchfs-wrapper-rename-" + std::to_string(::getpid()));
    std::error_code cleanup_error;
    std::filesystem::remove_all(host_directory, cleanup_error);
    require(std::filesystem::create_directory(host_directory),
            "host rename sandbox creation failed");
    RemoveTree remove_host_directory{host_directory};
    RestoreWorkingDirectory restore_cwd{
        ::open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
    require(restore_cwd.fd >= 0, "saving cwd failed");
    require(::chdir(host_directory.c_str()) == 0,
            "entering host rename sandbox failed");
    constexpr char source[] = "host-only-rename-source";
    constexpr char target[] = "host-only-rename-target";
    const int host_file =
        ::open(source, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0644);
    require(host_file >= 0 && ::close(host_file) == 0,
            "host-only rename fixture creation failed");
    errno = 0;
    const int rename_result = wrapped_rename(source, target);
    const int rename_error = errno;
    require(rename_result == -1 && rename_error == ENOENT,
            "missing relative OrchFS rename was retried in the host namespace");
    require(::access(source, F_OK) == 0 && ::access(target, F_OK) != 0 &&
                errno == ENOENT,
            "failed OrchFS rename mutated the host namespace");
    require(::unlink(source) == 0, "host rename fixture cleanup failed");

    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "wrapper client failure: %s\n", error.what());
    return 1;
  }
}

int run_client(const std::string& endpoint) {
  try {
    for (int retry = 0; retry < 200 && ::access(endpoint.c_str(), F_OK) != 0;
         ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    exercise_raw_openat_and_disconnect(endpoint);

    orchfs::async::RuntimeOptions runtime_options;
    runtime_options.worker_count = 2;
    auto created = orchfs::async::Runtime::create(std::move(runtime_options));
    require(static_cast<bool>(created), "client Runtime::create failed");
    auto runtime = std::move(created).value();

    orchfs::async::ClientOptions client_options;
    client_options.endpoint = endpoint;
    client_options.ring_capacity = 8;
    client_options.data_slot_size = 4096;
    auto client = run(*runtime,
                      orchfs::async::Client::connect(*runtime, client_options));

    std::string oversized_path(client_options.data_slot_size, 'x');
    oversized_path.front() = '/';
    require(run_error(*runtime,
                      client.open(std::move(oversized_path), O_RDONLY)) ==
                std::make_error_code(std::errc::message_size),
            "oversized request did not return message_size");

    auto file = run(*runtime, client.open("/alpha", O_CREAT | O_RDWR, 0644));
    const std::array<std::byte, 11> message{
        std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'},
        std::byte{'o'}, std::byte{' '}, std::byte{'o'}, std::byte{'r'},
        std::byte{'c'}, std::byte{'h'}, std::byte{'!'},
    };
    require(run(*runtime, file.write(message)) == message.size(),
            "short async write");
    require(run(*runtime, file.seek(0, SEEK_SET)) == 0, "seek failed");

    std::array<std::byte, message.size()> read_buffer{};
    require(run(*runtime, file.read(read_buffer)) == read_buffer.size(),
            "short async read");
    require(read_buffer == message, "read data mismatch");

    const std::array<std::byte, 5> patch{
        std::byte{'C'}, std::byte{'+'}, std::byte{'+'}, std::byte{'2'},
        std::byte{'0'},
    };
    require(run(*runtime, file.write_at(6, patch)) == patch.size(),
            "short positioned write");
    std::array<std::byte, patch.size()> patch_read{};
    require(run(*runtime, file.read_at(6, patch_read)) == patch_read.size(),
            "short positioned read");
    require(patch_read == patch, "positioned data mismatch");

    run(*runtime, file.truncate(0));
    require(run(*runtime, file.seek(0, SEEK_SET)) == 0,
            "seek before serialized writes failed");
    std::vector<std::byte> first_write(9000, std::byte{'A'});
    std::vector<std::byte> second_write(9000, std::byte{'B'});
    auto first_submitted = runtime->submit(file.write(first_write));
    auto second_submitted = runtime->submit(file.write(second_write));
    require(first_submitted && second_submitted,
            "concurrent implicit write submit failed");
    auto first_joined = std::move(first_submitted).value().join();
    auto second_joined = std::move(second_submitted).value().join();
    require(first_joined && second_joined,
            "concurrent implicit write join failed");
    auto first_result = std::move(first_joined).value();
    auto second_result = std::move(second_joined).value();
    require(first_result && first_result.value() == first_write.size() &&
                second_result && second_result.value() == second_write.size(),
            "concurrent implicit write failed");
    std::vector<std::byte> serialized(first_write.size() + second_write.size());
    require(run(*runtime, file.read_at(0, serialized)) == serialized.size(),
            "serialized write readback failed");
    const bool first_then_second =
        std::all_of(serialized.begin(),
                    serialized.begin() + static_cast<std::ptrdiff_t>(first_write.size()),
                    [](std::byte value) { return value == std::byte{'A'}; }) &&
        std::all_of(serialized.begin() +
                        static_cast<std::ptrdiff_t>(first_write.size()),
                    serialized.end(),
                    [](std::byte value) { return value == std::byte{'B'}; });
    const bool second_then_first =
        std::all_of(serialized.begin(),
                    serialized.begin() + static_cast<std::ptrdiff_t>(second_write.size()),
                    [](std::byte value) { return value == std::byte{'B'}; }) &&
        std::all_of(serialized.begin() +
                        static_cast<std::ptrdiff_t>(second_write.size()),
                    serialized.end(),
                    [](std::byte value) { return value == std::byte{'A'}; });
    require(first_then_second || second_then_first,
            "multi-chunk implicit writes interleaved");

    const auto file_stat = run(*runtime, file.stat());
    require(file_stat.size == static_cast<std::int64_t>(serialized.size()),
            "bad file size");
    constexpr std::int64_t large_offset = 3LL * 1024 * 1024 * 1024;
    require(run(*runtime, file.seek(large_offset, SEEK_SET)) ==
                static_cast<std::uint64_t>(large_offset),
            "64-bit seek was truncated by the RPC/legacy boundary");
    require(run(*runtime, file.seek(0, SEEK_CUR)) ==
                static_cast<std::uint64_t>(large_offset),
            "64-bit SEEK_CUR position mismatch");
    require(run(*runtime, file.seek(0, SEEK_END)) == serialized.size(),
            "SEEK_END(0) did not select the file size");
    run(*runtime, file.sync());
    run(*runtime, file.close());

    auto relative_file =
        run(*runtime, client.open("relative-old", O_CREAT | O_RDWR, 0644));
    require(run(*runtime, relative_file.write(message)) == message.size(),
            "relative rename fixture write failed");
    run(*runtime, relative_file.close());
    constexpr std::size_t legacy_name_max = 230;
    const std::string maximum_name(legacy_name_max, 'm');
    const std::string oversized_name(legacy_name_max + 1, 'x');
    run(*runtime, client.rename("relative-old", maximum_name));
    require(run_error(*runtime,
                      client.rename(maximum_name, oversized_name)) ==
                std::errc::filename_too_long,
            "oversized rename basename reached the legacy core");
    run(*runtime, client.rename(maximum_name, "relative-new"));
    require(run(*runtime, client.stat("relative-new")).size ==
                static_cast<std::int64_t>(message.size()),
            "bare relative rename did not preserve the file");
    require(run_error(*runtime, client.stat("relative-old")) ==
                std::errc::no_such_file_or_directory,
            "bare relative rename left the old name visible");
    run(*runtime, client.unlink("relative-new"));

    run(*runtime, client.rename("/alpha", "/renamed"));
    auto deferred_stat = client.stat(std::string("/renamed"));
    require(run(*runtime, std::move(deferred_stat)).size ==
                static_cast<std::int64_t>(serialized.size()),
            "path copy/stat failed");

#ifdef O_DIRECTORY
    require(run_error(
                *runtime,
                client.open("/renamed", O_RDONLY | O_DIRECTORY)) ==
                std::errc::not_a_directory,
            "O_DIRECTORY accepted a regular file");
    require(run_error(
                *runtime,
                client.open("/directory-create-side-effect",
                            O_RDONLY | O_CREAT | O_DIRECTORY, 0755)) ==
                std::errc::invalid_argument,
            "O_CREAT|O_DIRECTORY was not rejected");
    require(run_error(*runtime,
                      client.stat("/directory-create-side-effect")) ==
                std::errc::no_such_file_or_directory,
            "failed O_CREAT|O_DIRECTORY left a regular file behind");
#endif

    run(*runtime, client.make_directory("/directory", 0755));
    auto directory = run(*runtime, client.open_directory("/"));
    std::array<orchfs::async::DirEntry, 32> entries;
    const auto count = run(*runtime, directory.next_batch(entries));
    bool saw_renamed = false;
    bool saw_directory = false;
    for (std::size_t index = 0; index < count; ++index) {
      saw_renamed |= entries[index].name == "renamed";
      saw_directory |= entries[index].name == "directory";
    }
    require(saw_renamed && saw_directory, "directory batch missed entries");
    run(*runtime, directory.close());

    run(*runtime, client.make_directory("/directory-anchor", 0755));
    auto directory_file = run(
        *runtime,
        client.open("/directory-anchor", O_RDONLY | O_DIRECTORY));
    run(*runtime,
        client.rename("/directory-anchor", "/directory-anchor-moved"));
    run(*runtime, client.make_directory("/directory-anchor", 0755));
    auto anchored_directory =
        run(*runtime, client.open_directory(directory_file));
    auto anchored_child = run(
        *runtime,
        client.open_at(anchored_directory, "child", O_CREAT | O_RDWR, 0644));
    run(*runtime, anchored_child.close());
    require(run(*runtime, client.stat("/directory-anchor-moved/child")).size ==
                0,
            "directory handle did not follow the already-open inode");
    require(run_error(*runtime, client.stat("/directory-anchor/child")) ==
                std::errc::no_such_file_or_directory,
            "directory handle was re-resolved through the recreated path");
    run(*runtime, anchored_directory.close());
    run(*runtime, directory_file.close());
    run(*runtime, client.unlink("/directory-anchor-moved/child"));
    run(*runtime, client.remove_directory("/directory-anchor-moved"));
    run(*runtime, client.remove_directory("/directory-anchor"));

    run(*runtime, client.unlink("/renamed"));
    run(*runtime, client.remove_directory("/directory"));

    auto close_retry =
        run(*runtime, client.open("/close-retry", O_CREAT | O_RDWR, 0644));
    auto close_error = run_error(*runtime, close_retry.close());
    require(close_error == std::errc::io_error,
            "injected close failure was not returned");
    require(run(*runtime, close_retry.stat()).size == 0,
            "failed close made the remote handle unusable");
    run(*runtime, close_retry.close());

    auto over_write =
        run(*runtime, client.open("/over-write", O_CREAT | O_RDWR, 0644));
    const std::array<std::byte, 1> one_byte{std::byte{'W'}};
    require(run_error(*runtime, over_write.write(one_byte)) ==
                std::errc::io_error,
            "write over-return escaped server validation");
    run(*runtime, over_write.close());

    auto over_read =
        run(*runtime, client.open("/over-read", O_CREAT | O_RDWR, 0644));
    require(run(*runtime, over_read.write(one_byte)) == one_byte.size(),
            "over-read fixture write failed");
    require(run(*runtime, over_read.seek(0, SEEK_SET)) == 0,
            "over-read fixture seek failed");
    std::array<std::byte, 1> over_read_buffer{};
    require(run_error(*runtime, over_read.read(over_read_buffer)) ==
                std::errc::io_error,
            "read over-return escaped server validation");
    run(*runtime, over_read.close());

    auto truncate_file =
        run(*runtime, client.open("/truncate-file", O_CREAT | O_RDWR, 0644));
    require(run(*runtime, truncate_file.write(message)) == message.size(),
            "truncate fixture write failed");
    run(*runtime, truncate_file.truncate(7));
    require(run(*runtime, truncate_file.stat()).size == 7,
            "handle truncate failed");
    run(*runtime, truncate_file.close());
    run(*runtime, client.truncate("/truncate-file", 3));
    require(run(*runtime, client.stat("/truncate-file")).size == 3,
            "path truncate failed");
    auto truncate_open = run(
        *runtime,
        client.open("/truncate-file", O_RDWR | O_TRUNC, 0644));
    require(run(*runtime, truncate_open.stat()).size == 0,
            "O_TRUNC was not applied under the inode gate");
    run(*runtime, truncate_open.close());

    require(run_error(*runtime,
                      client.open("/bad-access-mode", O_ACCMODE, 0644)) ==
                std::errc::invalid_argument,
            "access mode 3 was accepted");
    require(run_error(*runtime,
                      client.open("/unsupported-sync", O_RDWR | O_SYNC,
                                  0644))
                    .value() == EOPNOTSUPP,
            "O_SYNC was accepted without synchronous-write semantics");
    require(run_error(*runtime,
                      client.open("/unknown-open-flag",
                                  O_RDWR | static_cast<int>(0x40000000U),
                                  0644))
                    .value() == EOPNOTSUPP,
            "unknown open flag was accepted");
#ifdef O_PATH
    require(run_error(*runtime,
                      client.open("/unsupported-path", O_PATH, 0644))
                    .value() == EOPNOTSUPP,
            "O_PATH was accepted");
#endif
#ifdef O_TMPFILE
    require(run_error(*runtime,
                      client.open("/", O_TMPFILE | O_RDWR, 0644))
                    .value() == EOPNOTSUPP,
            "O_TMPFILE was accepted");
#endif

    auto append_seed = run(
        *runtime, client.open("/append-file", O_CREAT | O_RDWR | O_TRUNC, 0644));
    const std::array<std::byte, 1> seed{std::byte{'A'}};
    require(run(*runtime, append_seed.write(seed)) == seed.size(),
            "append seed write failed");
    run(*runtime, append_seed.close());
    auto append_left =
        run(*runtime, client.open("/append-file",
                                  O_WRONLY | O_APPEND | O_CLOEXEC));
    auto append_right =
        run(*runtime, client.open("/append-file", O_WRONLY | O_APPEND));
    const int initial_flags = run(*runtime, append_left.get_flags());
    require((initial_flags & O_ACCMODE) == O_WRONLY &&
                (initial_flags & O_APPEND) != 0 &&
                (initial_flags & O_CLOEXEC) == 0,
            "F_GETFL lost status flags or exposed O_CLOEXEC");
    int requested_status = O_APPEND | O_NONBLOCK;
    int unsupported_status = 0;
#ifdef O_DIRECT
    requested_status |= O_DIRECT;
    unsupported_status |= O_DIRECT;
#endif
#ifdef O_ASYNC
    requested_status |= O_ASYNC;
    unsupported_status |= O_ASYNC;
#endif
#ifdef O_NOATIME
    requested_status |= O_NOATIME;
    unsupported_status |= O_NOATIME;
#endif
    run(*runtime, append_left.set_flags(requested_status));
    const int updated_flags = run(*runtime, append_left.get_flags());
    require((updated_flags & O_ACCMODE) == O_WRONLY &&
                (updated_flags & (O_APPEND | O_NONBLOCK)) ==
                    (O_APPEND | O_NONBLOCK) &&
                (updated_flags & unsupported_status) == 0,
            "F_SETFL exposed an unimplemented mutable status flag");
    const std::array<std::byte, 1> left_byte{std::byte{'L'}};
    const std::array<std::byte, 1> right_byte{std::byte{'R'}};
    auto left_write = runtime->submit(append_left.write(left_byte));
    auto right_write = runtime->submit(append_right.write(right_byte));
    require(left_write && right_write, "append submit failed");
    auto left_joined = std::move(left_write).value().join();
    auto right_joined = std::move(right_write).value().join();
    require(left_joined && right_joined && left_joined.value() &&
                right_joined.value() && left_joined.value().value() == 1 &&
                right_joined.value().value() == 1,
            "atomic append writes failed");
    run(*runtime, append_left.close());
    run(*runtime, append_right.close());
    auto append_reader =
        run(*runtime, client.open("/append-file", O_RDONLY));
    std::array<std::byte, 3> append_contents{};
    require(run(*runtime, append_reader.read(append_contents)) ==
                append_contents.size(),
            "append readback length mismatch");
    require(append_contents[0] == std::byte{'A'} &&
                ((append_contents[1] == std::byte{'L'} &&
                  append_contents[2] == std::byte{'R'}) ||
                 (append_contents[1] == std::byte{'R'} &&
                  append_contents[2] == std::byte{'L'})),
            "O_APPEND overwrote data or was not atomic");
    run(*runtime, append_reader.close());

    require(run_error(*runtime, client.stat("/missing-stat")) ==
                std::errc::no_such_file_or_directory,
            "missing stat did not return ENOENT");
    require(run_error(*runtime, client.open_directory("/missing-directory")) ==
                std::errc::no_such_file_or_directory,
            "missing opendir did not return ENOENT");

    auto cleanup_on_shutdown = run(
        *runtime, client.open("/cleanup-on-shutdown", O_CREAT | O_RDWR, 0644));
    run(*runtime, client.shutdown());
    (void)cleanup_on_shutdown;
    runtime->request_stop();
    require(static_cast<bool>(runtime->join()), "client Runtime::join failed");
    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "client failure: %s\n", error.what());
    return 1;
  }
}

}  // namespace

extern "C" {

int orchfs_open(const char* pathname, int flags, ...) {
  record_legacy_call();
  mode_t mode = 0;
  if ((flags & O_CREAT) != 0) {
    va_list arguments;
    va_start(arguments, flags);
    mode = static_cast<mode_t>(va_arg(arguments, int));
    va_end(arguments);
  }
  const int fd = ::open(host_path(pathname).c_str(), legacy_open_flags(flags),
                        mode);
  remember_file_handle(fd, pathname == nullptr ? "" : pathname);
  return fd;
}

int orchfs_openat(int dir_handle, const char* pathname, int flags, ...) {
  record_legacy_call();
  mode_t mode = 0;
  if ((flags & O_CREAT) != 0) {
    va_list arguments;
    va_start(arguments, flags);
    mode = static_cast<mode_t>(va_arg(arguments, int));
    va_end(arguments);
  }
  const int directory = directory_fd(dir_handle);
  const int fd = directory < 0
                     ? -1
                     : ::openat(directory, pathname,
                                legacy_open_flags(flags), mode);
  remember_file_handle(fd, pathname == nullptr ? "" : pathname);
  return fd;
}

int orchfs_close(int fd) {
  record_legacy_call();
  if (fail_this_close_once(fd)) {
    errno = EIO;
    return -1;
  }
  const int result = ::close(fd);
  if (result == 0) {
    forget_file_handle(fd);
  }
  return result;
}
int64_t orchfs_pwrite(int fd, const void* buffer, int64_t length,
                      int64_t offset) {
  record_legacy_call();
  const auto result = ::pwrite(fd, buffer, static_cast<std::size_t>(length),
                               offset);
  return result >= 0 && file_path_is(fd, "/over-write") ? length + 1 : result;
}
int64_t orchfs_pread(int fd, void* buffer, int64_t length, int64_t offset) {
  record_legacy_call();
  const auto result = ::pread(fd, buffer, static_cast<std::size_t>(length),
                              offset);
  return result >= 0 && file_path_is(fd, "/over-read") ? length + 1 : result;
}
int64_t orchfs_write(int fd, const void* buffer, size_t length) {
  record_legacy_call();
  const auto result = ::write(fd, buffer, length);
  return result >= 0 && file_path_is(fd, "/over-write")
             ? static_cast<int64_t>(length + 1)
             : result;
}
int64_t orchfs_read(int fd, void* buffer, int64_t length) {
  record_legacy_call();
  const auto result = ::read(fd, buffer, static_cast<std::size_t>(length));
  return result >= 0 && file_path_is(fd, "/over-read") ? length + 1 : result;
}
int orchfs_mkdir(const char* pathname, uint16_t mode) {
  record_legacy_call();
  return ::mkdir(host_path(pathname).c_str(), mode);
}
int orchfs_rmdir(const char* pathname) {
  record_legacy_call();
  return ::rmdir(host_path(pathname).c_str());
}
int orchfs_unlink(const char* pathname) {
  record_legacy_call();
  return ::unlink(host_path(pathname).c_str());
}
int orchfs_fstatfs(int fd, struct statfs* value) {
  record_legacy_call();
  return ::fstatfs(fd, value);
}
int orchfs_lstat(const char* pathname, struct stat* value) {
  record_legacy_call();
  return ::lstat(host_path(pathname).c_str(), value);
}
int orchfs_stat(const char* pathname, struct stat* value) {
  record_legacy_call();
  return ::stat(host_path(pathname).c_str(), value);
}
int orchfs_fstat(int fd, struct stat* value) {
  record_legacy_call();
  return ::fstat(fd, value);
}
int orchfs_lseek(int fd, std::int64_t offset, int whence) {
  record_legacy_call();
  if (offset < std::numeric_limits<off_t>::min() ||
      offset > std::numeric_limits<off_t>::max()) {
    errno = EOVERFLOW;
    return -1;
  }
  return ::lseek(fd, static_cast<off_t>(offset), whence) < 0 ? -1 : 0;
}

DIR* orchfs_opendir(const char* pathname) {
  record_legacy_call();
  DIR* directory = ::opendir(host_path(pathname).c_str());
  if (directory == nullptr) {
    return nullptr;
  }
  std::lock_guard lock(directory_mutex);
  const int handle = next_directory_handle++;
  directories.emplace(handle, directory);
  active_legacy_handles.fetch_add(1, std::memory_order_relaxed);
  return reinterpret_cast<DIR*>(static_cast<std::uintptr_t>(handle));
}

DIR* orchfs_fdopendir(int fd) {
  record_legacy_call();
  const int directory_fd = ::openat(
      fd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd < 0) {
    return nullptr;
  }
  DIR* directory = ::fdopendir(directory_fd);
  if (directory == nullptr) {
    const int saved_error = errno;
    (void)::close(directory_fd);
    errno = saved_error;
    return nullptr;
  }
  std::lock_guard lock(directory_mutex);
  const int handle = next_directory_handle++;
  directories.emplace(handle, directory);
  active_legacy_handles.fetch_add(1, std::memory_order_relaxed);
  return reinterpret_cast<DIR*>(static_cast<std::uintptr_t>(handle));
}

struct dirent* orchfs_readdir(DIR* encoded) {
  record_legacy_call();
  const int handle = static_cast<int>(reinterpret_cast<std::uintptr_t>(encoded));
  std::lock_guard lock(directory_mutex);
  const auto found = directories.find(handle);
  return found == directories.end() ? nullptr : ::readdir(found->second);
}

int orchfs_closedir(DIR* encoded) {
  record_legacy_call();
  const int handle = static_cast<int>(reinterpret_cast<std::uintptr_t>(encoded));
  DIR* directory = nullptr;
  {
    std::lock_guard lock(directory_mutex);
    const auto found = directories.find(handle);
    if (found == directories.end()) {
      errno = EBADF;
      return -1;
    }
    directory = found->second;
    directories.erase(found);
  }
  const int result = ::closedir(directory);
  if (result == 0) {
    active_legacy_handles.fetch_sub(1, std::memory_order_relaxed);
  }
  return result;
}

int orchfs_truncate(const char* pathname, size_t length) {
  record_legacy_call();
  return ::truncate(host_path(pathname).c_str(), static_cast<off_t>(length));
}
int orchfs_ftruncate(int fd, size_t length) {
  record_legacy_call();
  return ::ftruncate(fd, static_cast<off_t>(length));
}
int orchfs_fsync(int fd) {
  record_legacy_call();
  return ::fsync(fd);
}
int64_t orchfs_tell(int fd) {
  record_legacy_call();
  return ::lseek(fd, 0, SEEK_CUR);
}
int64_t orchfs_fd_inode_id(int fd) {
  record_legacy_call();
  struct stat value {};
  int host_fd = directory_fd(fd);
  if (host_fd < 0) {
    host_fd = fd;
  }
  return ::fstat(host_fd, &value) == 0 ? static_cast<int64_t>(value.st_ino) : -1;
}
int orchfs_fd_file_type(int fd) {
  record_legacy_call();
  struct stat value {};
  int host_fd = directory_fd(fd);
  if (host_fd < 0) {
    host_fd = fd;
  }
  if (::fstat(host_fd, &value) != 0) {
    return ORCHFS_FILE_TYPE_ERROR;
  }
  if (S_ISDIR(value.st_mode)) {
    return ORCHFS_FILE_TYPE_DIRECTORY;
  }
  if (S_ISREG(value.st_mode)) {
    return ORCHFS_FILE_TYPE_REGULAR;
  }
  return ORCHFS_FILE_TYPE_UNKNOWN;
}
int orchfs_rename(const char* old_path, const char* new_path) {
  record_legacy_call();
  // Model the production parser boundary: it exits when the parsed directory
  // is empty (including "/name") or when the basename is empty.
  const char* old_slash = std::strrchr(old_path, '/');
  const char* new_slash = std::strrchr(new_path, '/');
  if (old_slash == nullptr || new_slash == nullptr || old_slash == old_path ||
      new_slash == new_path || old_slash[1] == '\0' || new_slash[1] == '\0') {
    errno = EINVAL;
    return -1;
  }
  const std::string_view old_parent(
      old_path, static_cast<std::size_t>(old_slash - old_path));
  const std::string_view new_parent(
      new_path, static_cast<std::size_t>(new_slash - new_path));
  // path_to_inode does not resolve a parent consisting only of slashes; the
  // server must express the mount root through its "." entry.
  if (old_parent.find_first_not_of('/') == std::string_view::npos ||
      new_parent.find_first_not_of('/') == std::string_view::npos) {
    errno = EINVAL;
    return -1;
  }
  if (old_parent != new_parent) {
    errno = EXDEV;
    return -1;
  }
  return ::rename(host_path(old_path).c_str(), host_path(new_path).c_str());
}
int orchfs_fcntl(int fd, int command, ...) {
  record_legacy_call();
  if (command == F_SETFL) {
    va_list arguments;
    va_start(arguments, command);
    const int flags = va_arg(arguments, int);
    va_end(arguments);
    return ::fcntl(fd, command, flags);
  }
  return ::fcntl(fd, command);
}

void read_data_from_devs(void*, int64_t, int64_t) { record_legacy_call(); }
void write_data_to_devs(void*, int64_t, int64_t) { record_legacy_call(); }
int device_sync(void) {
  record_legacy_call();
  return 0;
}

}  // extern "C"

int main() {
  std::array<char, 64> root_template{};
  std::strcpy(root_template.data(), "/tmp/orchfs-async-e2e-XXXXXX");
  char* created_root = ::mkdtemp(root_template.data());
  if (created_root == nullptr) {
    std::perror("mkdtemp");
    return 1;
  }
  root_path = created_root;
  const std::string endpoint = root_path + "/control.sock";

  const pid_t child = ::fork();
  if (child < 0) {
    std::perror("fork");
    return 1;
  }
  if (child == 0) {
    const int result = run_client(endpoint);
    std::fflush(nullptr);
    _exit(result);
  }

  const pid_t wrapper_child = ::fork();
  if (wrapper_child < 0) {
    std::perror("fork wrapper client");
    (void)::kill(child, SIGKILL);
    (void)::waitpid(child, nullptr, 0);
    return 1;
  }
  if (wrapper_child == 0) {
    const int wrapper_result = run_wrapper_client(endpoint);
    std::fflush(nullptr);
    _exit(wrapper_result);
  }

  int result = 1;
  try {
    orchfs::async::RuntimeOptions runtime_options;
    runtime_options.worker_count = 3;
    auto created = orchfs::async::Runtime::create(std::move(runtime_options));
    require(static_cast<bool>(created), "server Runtime::create failed");
    auto runtime = std::move(created).value();

    orchfs::async::ServerOptions server_options;
    server_options.endpoint = endpoint;
    server_options.lane_count = 4;
    server_options.blocking_worker_count = 2;
    server_options.ring_capacity = 8;
    server_options.data_slot_size = 4096;
    auto started = orchfs::async::Server::start(*runtime, server_options);
    require(static_cast<bool>(started), "Server::start failed");
    auto server = std::move(started).value();

    int status = 0;
    require(::waitpid(child, &status, 0) == child, "waitpid failed");
    require(WIFEXITED(status) && WEXITSTATUS(status) == 0,
            "client process failed");
    int wrapper_status = 0;
    require(::waitpid(wrapper_child, &wrapper_status, 0) == wrapper_child,
            "waitpid wrapper client failed");
    require(WIFEXITED(wrapper_status) && WEXITSTATUS(wrapper_status) == 0,
            "wrapper client process failed");
    const auto cleanup_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (active_legacy_handles.load(std::memory_order_acquire) != 0 &&
           std::chrono::steady_clock::now() < cleanup_deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    require(active_legacy_handles.load(std::memory_order_acquire) == 0,
            "disconnect or explicit shutdown leaked legacy handles");
    require(legacy_call_count.load(std::memory_order_relaxed) != 0,
            "legacy mocks were not exercised");
    require(!legacy_called_on_runtime.load(std::memory_order_relaxed),
            "legacy function executed on a Runtime worker");
    {
      std::lock_guard lock(legacy_thread_mutex);
      require(legacy_threads.size() <= server_options.blocking_worker_count,
              "legacy calls escaped the fixed blocking executor");
    }

    exercise_completion_backpressure(endpoint);

    const int stalled_client = connect_without_hello(endpoint);
    const auto healthy_connect_start = std::chrono::steady_clock::now();
    {
      auto healthy_transport = connect_raw(endpoint);
      require(static_cast<bool>(healthy_transport),
              "healthy client did not pass a stalled handshake");
    }
    require(std::chrono::steady_clock::now() - healthy_connect_start <
                std::chrono::seconds(2),
            "stalled hello blocked later clients indefinitely");
    ::close(stalled_client);

    auto saturated_transport = make_saturated_session(endpoint);
    require(active_legacy_handles.load(std::memory_order_acquire) == 1,
            "server-stop cleanup fixture was not opened");
    const int stop_stalled_client = connect_without_hello(endpoint);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto server_stop_start = std::chrono::steady_clock::now();
    server->request_stop();
    require(static_cast<bool>(server->join()), "Server::join failed");
    require(std::chrono::steady_clock::now() - server_stop_start <
                std::chrono::seconds(2),
            "Server::join blocked on a stalled hello or saturated CQ");
    ::close(stop_stalled_client);
    require(active_legacy_handles.load(std::memory_order_acquire) == 0,
            "Server::stop leaked a legacy handle");
    runtime->request_stop();
    require(static_cast<bool>(runtime->join()), "server Runtime::join failed");
    result = 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "server failure: %s\n", error.what());
    (void)::kill(child, SIGKILL);
    (void)::kill(wrapper_child, SIGKILL);
    (void)::waitpid(child, nullptr, 0);
    (void)::waitpid(wrapper_child, nullptr, 0);
  }

  std::error_code cleanup_error;
  std::filesystem::remove_all(root_path, cleanup_error);
  if (result == 0) {
    std::puts("async client/server tests passed");
  }
  return result;
}
