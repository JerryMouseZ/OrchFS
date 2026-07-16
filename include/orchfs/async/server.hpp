#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "orchfs/async/result.hpp"

namespace orchfs::async {

class Runtime;

struct ServerOptions {
  std::string endpoint{"/tmp/orchfs-kfs.sock"};
  std::size_t lane_count{0};
  // Zero selects one fixed blocking worker per Runtime worker. These threads
  // isolate the legacy KFS core from coroutine scheduler workers.
  std::size_t blocking_worker_count{0};
  std::size_t ring_capacity{64};
  std::size_t data_slot_size{1024U * 1024U};
};

class Server {
 public:
  Server(Server&&) noexcept;
  Server& operator=(Server&&) noexcept;
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  ~Server();

  static Result<std::unique_ptr<Server>> start(Runtime& runtime,
                                               ServerOptions options = {});

  void request_stop() noexcept;
  Result<void> join();

 private:
  class Impl;
  explicit Server(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace orchfs::async
