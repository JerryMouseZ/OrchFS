#pragma once

#include <memory>
#include <optional>

namespace orchfs::async {
class KfsCoroutineCore;
class Runtime;
}  // namespace orchfs::async

namespace orchfs::kfs {

struct AsyncContext {
  async::Runtime* runtime{};
  std::shared_ptr<async::KfsCoroutineCore> filesystem;
};

// In-process utilities may snapshot the KFS-owned coroutine objects while the
// caller guarantees that kernel shutdown cannot run concurrently.
[[nodiscard]] std::optional<AsyncContext> async_context_snapshot();

}  // namespace orchfs::kfs
