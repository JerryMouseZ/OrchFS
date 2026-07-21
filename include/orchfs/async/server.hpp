#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "orchfs/async/result.hpp"

namespace orchfs::async {

class Runtime;
class AsyncFilesystem;

struct ServerOptions {
  std::string endpoint{"/tmp/orchfs-kfs.sock"};
  std::size_t lane_count{0};
  std::size_t ring_capacity{64};
  std::size_t data_slot_size{1024U * 1024U};
  // The authoritative filesystem module is injected at the server seam.
  // Production supplies KfsCoroutineCore; tests supply an in-memory adapter.
  std::shared_ptr<AsyncFilesystem> filesystem;
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

  // Stop accepting new sessions while allowing existing clients to finish
  // their already-running coroutine graphs and close themselves normally.
  // join() preserves this graceful mode once requested.
  void request_drain() noexcept;
  void request_stop() noexcept;
  Result<void> join();

 private:
  class Impl;
  explicit Server(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace orchfs::async
