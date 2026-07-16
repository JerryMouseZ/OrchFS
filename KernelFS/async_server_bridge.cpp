#include "async_server_bridge.h"

#include "orchfs/async/runtime.hpp"
#include "orchfs/async/server.hpp"
#include "orchfs/async/ipc_protocol.hpp"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <sched.h>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace {

constexpr std::size_t kDefaultWorkerCount = 4;
constexpr std::size_t kDefaultRingCapacity = 64;
constexpr std::size_t kDefaultDataSlotSize = 1024U * 1024U;
constexpr std::string_view kDefaultEndpoint{"/tmp/orchfs-kfs.sock"};

std::size_t default_worker_count() noexcept {
    cpu_set_t affinity;
    CPU_ZERO(&affinity);
    if (::sched_getaffinity(0, sizeof(affinity), &affinity) == 0) {
        const auto available = static_cast<std::size_t>(CPU_COUNT(&affinity));
        if (available != 0) {
            return std::min(kDefaultWorkerCount, available);
        }
    }
    return kDefaultWorkerCount;
}

template <typename Integer>
int parse_integer(const char *name, Integer minimum, Integer maximum,
                  Integer &value) noexcept {
    const char *text = std::getenv(name);
    if (text == nullptr || *text == '\0') {
        return 0;
    }
    const std::string_view input(text);
    Integer parsed{};
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), parsed);
    if (error != std::errc{} || end != input.data() + input.size() ||
        parsed < minimum || parsed > maximum) {
        return EINVAL;
    }
    value = parsed;
    return 0;
}

int to_errno(std::error_code error) noexcept {
    if (!error) {
        return 0;
    }
    if (error.category() == std::generic_category() ||
        error.category() == std::system_category()) {
        return error.value() > 0 ? error.value() : EIO;
    }
    if (error == orchfs::async::make_error_code(
                     orchfs::async::Errc::runtime_already_exists) ||
        error == orchfs::async::make_error_code(
                     orchfs::async::Errc::runtime_stopping)) {
        return EBUSY;
    }
    return EIO;
}

struct ServerState {
    std::unique_ptr<orchfs::async::Runtime> runtime;
    std::unique_ptr<orchfs::async::Server> server;
};

std::mutex state_mutex;
ServerState state;

} // namespace

extern "C" {

int orchfs_async_server_start(void) {
    std::lock_guard lock(state_mutex);
    if (state.runtime || state.server) {
        return EALREADY;
    }

    try {
        std::size_t worker_count = default_worker_count();
        std::size_t blocking_worker_count = 0;
        std::size_t ring_capacity = kDefaultRingCapacity;
        std::size_t data_slot_size = kDefaultDataSlotSize;
        int error = parse_integer("ORCHFS_KFS_WORKERS", std::size_t{1},
                                  static_cast<std::size_t>(
                                      std::numeric_limits<std::uint32_t>::max()),
                                  worker_count);
        if (error == 0) {
            error = parse_integer("ORCHFS_KFS_BLOCKING_WORKERS", std::size_t{1},
                                  static_cast<std::size_t>(
                                      std::numeric_limits<std::uint32_t>::max()),
                                  blocking_worker_count);
        }
        if (error == 0) {
            error = parse_integer("ORCHFS_IPC_RING_CAPACITY", std::size_t{2},
                                  static_cast<std::size_t>(
                                      std::numeric_limits<std::uint32_t>::max()),
                                  ring_capacity);
        }
        if (error == 0) {
            error = parse_integer("ORCHFS_IPC_DATA_SLOT_SIZE", std::size_t{1},
                                  static_cast<std::size_t>(
                                      std::numeric_limits<std::uint32_t>::max()),
                                  data_slot_size);
        }
        if (error != 0) {
            return error;
        }

        std::string endpoint{kDefaultEndpoint};
        if (const char *configured = std::getenv("ORCHFS_ASYNC_ENDPOINT");
            configured != nullptr && *configured != '\0') {
            endpoint = configured;
        }

        orchfs::async::RuntimeOptions runtime_options;
        runtime_options.worker_count = worker_count;
        auto runtime_result =
            orchfs::async::Runtime::create(std::move(runtime_options));
        if (!runtime_result) {
            return to_errno(runtime_result.error());
        }
        auto runtime = std::move(runtime_result).value();

        orchfs::async::ServerOptions server_options;
        server_options.endpoint = std::move(endpoint);
        // Lanes are a per-client transport choice. They need not equal the
        // KFS Runtime worker count; requests are re-owned by inode below the
        // transport boundary.
        server_options.lane_count = orchfs::async::kMaxIpcWorkerLanes;
        server_options.blocking_worker_count = blocking_worker_count;
        server_options.ring_capacity = ring_capacity;
        server_options.data_slot_size = data_slot_size;
        auto server_result =
            orchfs::async::Server::start(*runtime, std::move(server_options));
        if (!server_result) {
            const int start_error = to_errno(server_result.error());
            runtime->request_stop();
            (void)runtime->join();
            return start_error;
        }

        state.runtime = std::move(runtime);
        state.server = std::move(server_result).value();
        return 0;
    } catch (const std::bad_alloc &) {
        return ENOMEM;
    } catch (const std::system_error &error) {
        return to_errno(error.code());
    } catch (...) {
        return EIO;
    }
}

void orchfs_async_server_request_stop(void) {
    std::lock_guard lock(state_mutex);
    if (state.server) {
        state.server->request_stop();
    }
}

int orchfs_async_server_stop(void) {
    std::lock_guard lock(state_mutex);
    int error = 0;
    if (state.server) {
        state.server->request_stop();
        auto joined = state.server->join();
        if (!joined) {
            error = to_errno(joined.error());
        }
        state.server.reset();
    }
    if (state.runtime) {
        state.runtime->request_stop();
        auto joined = state.runtime->join();
        if (!joined && error == 0) {
            error = to_errno(joined.error());
        }
        state.runtime.reset();
    }
    return error;
}

int orchfs_async_server_is_running(void) {
    std::lock_guard lock(state_mutex);
    return state.server != nullptr ? 1 : 0;
}

} // extern "C"
