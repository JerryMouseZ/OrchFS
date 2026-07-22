#include "async_server_bridge.h"
#include "async_server_bridge_internal.hpp"

#include "orchfs/async/detail/cpu_list.hpp"
#include "orchfs/async/runtime.hpp"
#include "orchfs/async/server.hpp"
#include "orchfs/async/ipc_protocol.hpp"
#include "orchfs/async/kfs_coroutine_core.hpp"

extern "C" {
#include "../LibFS/migrate.h"
#include "../config/config.h"
}

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <new>
#include <sched.h>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {

constexpr std::size_t kDefaultWorkerCount = 4;
constexpr std::size_t kDefaultRingCapacity = 64;
constexpr std::size_t kDefaultDataSlotSize = 1024U * 1024U;
constexpr std::string_view kDefaultEndpoint{"/tmp/orchfs-kfs.sock"};
constexpr std::size_t kMigrationPageCount =
    std::size_t{1} << (ORCH_BLOCK_BW - ORCH_PAGE_BW);

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
    std::shared_ptr<orchfs::async::KfsCoroutineCore> filesystem;
};

class StateGuard final {
  public:
    StateGuard() noexcept {
        while (state_guard.test_and_set(std::memory_order_acquire)) {
            state_guard.wait(true, std::memory_order_relaxed);
        }
    }

    ~StateGuard() {
        state_guard.clear(std::memory_order_release);
        state_guard.notify_one();
    }

    StateGuard(const StateGuard &) = delete;
    StateGuard &operator=(const StateGuard &) = delete;

  private:
    static std::atomic_flag state_guard;
};

std::atomic_flag StateGuard::state_guard = ATOMIC_FLAG_INIT;
ServerState state;

std::atomic<orchfs::async::Runtime *> migration_runtime{nullptr};
std::atomic<std::shared_ptr<orchfs::async::KfsCoroutineCore>>
    migration_filesystem{nullptr};
std::atomic<bool> migration_active{false};
std::atomic<bool> migration_stopping{true};

struct MigrationCandidateNode {
    LRU_node_info_t candidate{};
    MigrationCandidateNode *next{};
};

std::atomic<MigrationCandidateNode *> migration_inbox{nullptr};
MigrationCandidateNode *migration_local{};

void publish_migration_idle() noexcept {
    migration_active.store(false, std::memory_order_release);
    migration_active.notify_all();
}

void drain_migration_inbox() noexcept {
    auto *stack = migration_inbox.exchange(nullptr, std::memory_order_acquire);
    while (stack != nullptr) {
        auto *next = stack->next;
        stack->next = migration_local;
        migration_local = stack;
        stack = next;
    }
}

bool take_migration_candidate(LRU_node_info_t &candidate) noexcept {
    if (migration_local == nullptr) {
        drain_migration_inbox();
    }
    if (migration_local == nullptr) {
        return false;
    }
    auto *node = migration_local;
    migration_local = node->next;
    candidate = node->candidate;
    delete node;
    return true;
}

bool migration_candidates_pending() noexcept {
    return migration_local != nullptr ||
           migration_inbox.load(std::memory_order_acquire) != nullptr;
}

void discard_migration_candidates() noexcept {
    drain_migration_inbox();
    while (migration_local != nullptr) {
        auto *node = migration_local;
        migration_local = node->next;
        delete node;
    }
}

orchfs::async::Task<void> run_migration(
    std::shared_ptr<orchfs::async::KfsCoroutineCore> filesystem) {
    bool retry_more = false;
    if (!migration_stopping.load(std::memory_order_acquire)) {
        auto migrated = co_await filesystem->migrate(DEFAULT_MIGRATE_NUM);
        retry_more = migrated && migrated.value();
    }
    publish_migration_idle();
    if (retry_more && !migration_stopping.load(std::memory_order_acquire) &&
        orchfs_migration_has_pending() != 0) {
        orchfs_async_schedule_migration();
    }
    co_return;
}

} // namespace

namespace orchfs::kfs {

std::optional<AsyncContext> async_context_snapshot() {
    StateGuard guard;
    if (!state.runtime || !state.filesystem) {
        return std::nullopt;
    }
    return AsyncContext{
        .runtime = state.runtime.get(),
        .filesystem = state.filesystem,
    };
}

} // namespace orchfs::kfs

extern "C" {

int orchfs_async_runtime_start(void) {
    StateGuard guard;
    if (state.runtime) {
        return EALREADY;
    }

    try {
        std::size_t worker_count = default_worker_count();
        int error = parse_integer("ORCHFS_KFS_WORKERS", std::size_t{1},
                                  static_cast<std::size_t>(
                                      std::numeric_limits<std::uint32_t>::max()),
                                  worker_count);
        if (error != 0) {
            return error;
        }

        orchfs::async::RuntimeOptions runtime_options;
        runtime_options.worker_count = worker_count;
        if (const char *cpu_list = std::getenv("ORCHFS_KFS_CPU_LIST");
            cpu_list != nullptr && *cpu_list != '\0') {
            const std::error_code cpu_error =
                orchfs::async::detail::parse_cpu_list(
                    cpu_list, runtime_options.cpu_set);
            if (cpu_error) {
                return to_errno(cpu_error);
            }
        }
        auto runtime_result =
            orchfs::async::Runtime::create(std::move(runtime_options));
        if (!runtime_result) {
            return to_errno(runtime_result.error());
        }
        state.runtime = std::move(runtime_result).value();
        auto filesystem =
            orchfs::async::KfsCoroutineCore::create(*state.runtime);
        if (!filesystem) {
            state.runtime->request_stop();
            (void)state.runtime->join();
            state.runtime.reset();
            return to_errno(filesystem.error());
        }
        state.filesystem = std::move(filesystem).value();
        return 0;
    } catch (const std::bad_alloc &) {
        return ENOMEM;
    } catch (const std::system_error &error) {
        return to_errno(error.code());
    } catch (...) {
        return EIO;
    }
}

void *orchfs_async_runtime_handle(void) {
    StateGuard guard;
    return state.runtime.get();
}

size_t orchfs_async_runtime_worker_count(void) {
    StateGuard guard;
    return state.runtime ? state.runtime->worker_count() : 1;
}

size_t orchfs_async_current_worker(void) {
    const auto worker = orchfs::async::Runtime::current_worker();
    return worker == orchfs::async::detail::no_worker ? 0 : worker;
}

int orchfs_async_server_start(void) {
    StateGuard guard;
    if (!state.runtime) {
        return ENODEV;
    }
    if (state.server) {
        return EALREADY;
    }

    try {
        std::size_t ring_capacity = kDefaultRingCapacity;
        std::size_t data_slot_size = kDefaultDataSlotSize;
        int error = parse_integer("ORCHFS_IPC_RING_CAPACITY", std::size_t{2},
                                  static_cast<std::size_t>(
                                      std::numeric_limits<std::uint32_t>::max()),
                                  ring_capacity);
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

        orchfs::async::ServerOptions server_options;
        server_options.endpoint = std::move(endpoint);
        // Lanes are a per-client transport choice. They need not equal the
        // KFS Runtime worker count; requests are re-owned by inode below the
        // transport boundary.
        server_options.lane_count = orchfs::async::kMaxIpcWorkerLanes;
        server_options.ring_capacity = ring_capacity;
        server_options.data_slot_size = data_slot_size;
        server_options.filesystem = state.filesystem;
        auto server_result = orchfs::async::Server::start(
            *state.runtime, std::move(server_options));
        if (!server_result) {
            return to_errno(server_result.error());
        }

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
    StateGuard guard;
    if (state.server) {
        state.server->request_stop();
    }
}

int orchfs_async_server_stop(void) {
    StateGuard guard;
    int error = 0;
    if (state.server) {
        state.server->request_drain();
        auto joined = state.server->join();
        if (!joined) {
            error = to_errno(joined.error());
        }
        state.server.reset();
    }
    return error;
}

int orchfs_async_runtime_stop(void) {
    StateGuard guard;
    if (state.server) {
        return EBUSY;
    }
    if (!state.runtime) {
        return 0;
    }
    state.runtime->request_stop();
    auto joined = state.runtime->join();
    const int error = joined ? 0 : to_errno(joined.error());
    state.filesystem.reset();
    state.runtime.reset();
    return error;
}

int orchfs_async_server_is_running(void) {
    StateGuard guard;
    return state.server != nullptr ? 1 : 0;
}

int orchfs_async_migration_start(void) {
    StateGuard guard;
    if (!state.runtime || !state.filesystem) {
        return ENODEV;
    }
    migration_runtime.store(state.runtime.get(), std::memory_order_release);
    migration_filesystem.store(state.filesystem, std::memory_order_release);
    migration_stopping.store(false, std::memory_order_release);
    return 0;
}

void orchfs_async_schedule_migration(void) {
    if (migration_stopping.load(std::memory_order_acquire)) {
        return;
    }
    bool expected = false;
    if (!migration_active.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
        return;
    }
    if (migration_stopping.load(std::memory_order_acquire)) {
        publish_migration_idle();
        return;
    }
    auto *runtime = migration_runtime.load(std::memory_order_acquire);
    auto filesystem =
        migration_filesystem.load(std::memory_order_acquire);
    if (runtime == nullptr || !filesystem) {
        publish_migration_idle();
        return;
    }
    auto submitted = runtime->submit(
        run_migration(std::move(filesystem)));
    if (!submitted) {
        publish_migration_idle();
    }
}

void orchfs_async_migration_stop(void) {
    migration_stopping.store(true, std::memory_order_release);
    while (migration_active.load(std::memory_order_acquire)) {
        migration_active.wait(true, std::memory_order_acquire);
    }
    migration_runtime.store(nullptr, std::memory_order_release);
    migration_filesystem.store(nullptr, std::memory_order_release);
    discard_migration_candidates();
}

int orchfs_async_migration_enqueue_candidate(
    int64_t inode, int64_t block_offset, const int64_t *nvm_page_addresses,
    size_t page_count) {
    if (nvm_page_addresses == nullptr || page_count != kMigrationPageCount) {
        return EINVAL;
    }
    auto *node = new (std::nothrow) MigrationCandidateNode;
    if (node == nullptr) {
        return ENOMEM;
    }
    node->candidate.ino_id = inode;
    node->candidate.offset = block_offset;
    std::copy_n(nvm_page_addresses, page_count,
                node->candidate.nvm_page_addr);

    auto *head = migration_inbox.load(std::memory_order_relaxed);
    do {
        node->next = head;
    } while (!migration_inbox.compare_exchange_weak(
        head, node, std::memory_order_release, std::memory_order_relaxed));
    return 0;
}

int orchfs_async_migration_take_candidate(
    int64_t *inode, int64_t *block_offset, int64_t *nvm_page_addresses,
    size_t page_count) {
    if (inode == nullptr || block_offset == nullptr ||
        nvm_page_addresses == nullptr || page_count != kMigrationPageCount) {
        return EINVAL;
    }
    LRU_node_info_t candidate{};
    if (!take_migration_candidate(candidate)) {
        return EAGAIN;
    }
    *inode = candidate.ino_id;
    *block_offset = candidate.offset;
    std::copy_n(candidate.nvm_page_addr, page_count, nvm_page_addresses);
    return 0;
}

int orchfs_async_migration_candidates_pending(void) {
    return migration_candidates_pending() ? 1 : 0;
}

} // extern "C"
