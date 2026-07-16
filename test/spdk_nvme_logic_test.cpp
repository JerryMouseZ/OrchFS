#include "../KernelFS/spdk_nvme_backend.hpp"
#include "../KernelFS/spdk_nvme_bridge.h"
#include "../KernelFS/thread_affinity_guard.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

void require(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

void test_zero_length() {
    orchfs::nvme::IoPlan plan;
    const std::error_code error = orchfs::nvme::plan_io(
        orchfs::nvme::Operation::read, 123, 0, 512, 4096, plan);
    require(!error, "zero-length plan succeeds");
    require(plan.segments.empty(), "zero-length plan has no commands");
    require(plan.physical_begin == 123 && plan.physical_end == 123,
            "zero-length plan has an empty physical extent");
}

void test_aligned_split() {
    orchfs::nvme::IoPlan plan;
    const std::error_code error = orchfs::nvme::plan_io(
        orchfs::nvme::Operation::read, 0, 1536, 512, 1024, plan);
    require(!error, "aligned plan succeeds");
    require(plan.physical_begin == 0 && plan.physical_end == 1536,
            "aligned physical extent");
    require(plan.segments.size() == 2, "aligned request splits at max transfer");
    require(plan.segments[0] == orchfs::nvme::IoSegment{
                                    .device_offset = 0,
                                    .device_length = 1024,
                                    .user_offset = 0,
                                    .user_length = 1024,
                                    .bounce_offset = 0,
                                    .read_before_write = false,
                                },
            "first aligned segment");
    require(plan.segments[1] == orchfs::nvme::IoSegment{
                                    .device_offset = 1024,
                                    .device_length = 512,
                                    .user_offset = 1024,
                                    .user_length = 512,
                                    .bounce_offset = 0,
                                    .read_before_write = false,
                                },
            "second aligned segment");
}

void test_unaligned_read() {
    orchfs::nvme::IoPlan plan;
    const std::error_code error = orchfs::nvme::plan_io(
        orchfs::nvme::Operation::read, 100, 1000, 512, 1024, plan);
    require(!error, "unaligned read plan succeeds");
    require(plan.physical_begin == 0 && plan.physical_end == 1536,
            "unaligned read expands to covering LBAs");
    require(plan.segments.size() == 3, "unaligned read has head, middle and tail");
    require(plan.segments[0] == orchfs::nvme::IoSegment{
                                    .device_offset = 0,
                                    .device_length = 512,
                                    .user_offset = 0,
                                    .user_length = 412,
                                    .bounce_offset = 100,
                                    .read_before_write = false,
                                },
            "unaligned read head slice");
    require(plan.segments[1] == orchfs::nvme::IoSegment{
                                    .device_offset = 512,
                                    .device_length = 512,
                                    .user_offset = 412,
                                    .user_length = 512,
                                    .bounce_offset = 0,
                                    .read_before_write = false,
                                },
            "unaligned read middle");
    require(plan.segments[2] == orchfs::nvme::IoSegment{
                                    .device_offset = 1024,
                                    .device_length = 512,
                                    .user_offset = 924,
                                    .user_length = 76,
                                    .bounce_offset = 0,
                                    .read_before_write = false,
                                },
            "unaligned read tail slice");
}

void test_unaligned_write_rmw() {
    orchfs::nvme::IoPlan plan;
    const std::error_code error = orchfs::nvme::plan_io(
        orchfs::nvme::Operation::write, 100, 1000, 512, 1024, plan);
    require(!error, "unaligned write plan succeeds");
    require(plan.segments.size() == 3, "unaligned write segment count");
    require(plan.segments[0].read_before_write,
            "partial head uses asynchronous RMW");
    require(!plan.segments[1].read_before_write,
            "complete middle LBA writes directly");
    require(plan.segments[2].read_before_write,
            "partial tail uses asynchronous RMW");

    orchfs::nvme::IoPlan single_lba;
    require(!orchfs::nvme::plan_io(orchfs::nvme::Operation::write,
                                   100,
                                   200,
                                   512,
                                   4096,
                                   single_lba),
            "single-LBA RMW plan succeeds");
    require(single_lba.segments.size() == 1,
            "single-LBA partial write produces one read and one write stage");
    require(single_lba.segments[0].read_before_write,
            "single-LBA partial write preserves untouched bytes");
}

void test_generic_lba_and_rounded_max() {
    orchfs::nvme::IoPlan plan;
    const std::error_code error = orchfs::nvme::plan_io(
        orchfs::nvme::Operation::write, 4095, 8194, 4096, 8192, plan);
    require(!error, "4KiB LBA plan succeeds");
    require(plan.segments.size() == 3, "4KiB LBA head/middle/tail plan");
    require(plan.segments[0].device_length == 4096 &&
                plan.segments[1].device_length == 8192 &&
                plan.segments[2].device_length == 4096,
            "4KiB LBA command lengths");

    orchfs::nvme::IoPlan rounded;
    require(!orchfs::nvme::plan_io(
                orchfs::nvme::Operation::read, 0, 1024, 512, 1000, rounded),
            "non-multiple max transfer is rounded down");
    require(rounded.segments.size() == 2 &&
                rounded.segments[0].device_length == 512 &&
                rounded.segments[1].device_length == 512,
            "rounded max transfer is respected");
}

void test_invalid_and_overflow() {
    orchfs::nvme::IoPlan plan;
    require(orchfs::nvme::plan_io(
                orchfs::nvme::Operation::read, 0, 1, 0, 4096, plan) ==
                std::make_error_code(std::errc::invalid_argument),
            "zero LBA is rejected");
    require(orchfs::nvme::plan_io(
                orchfs::nvme::Operation::read, 0, 1, 4096, 512, plan) ==
                std::make_error_code(std::errc::invalid_argument),
            "max transfer smaller than LBA is rejected");
    require(orchfs::nvme::plan_io(orchfs::nvme::Operation::read,
                                  std::numeric_limits<std::uint64_t>::max() - 100,
                                  200,
                                  512,
                                  4096,
                                  plan) ==
                std::make_error_code(std::errc::value_too_large),
            "logical range overflow is rejected");
    require(orchfs::nvme::plan_io(orchfs::nvme::Operation::read,
                                  std::numeric_limits<std::uint64_t>::max() - 100,
                                  50,
                                  512,
                                  4096,
                                  plan) ==
                std::make_error_code(std::errc::value_too_large),
            "aligned physical range overflow is rejected");
}

void test_small_range_invariants() {
    constexpr std::uint32_t lba = 512;
    constexpr std::uint32_t max_transfer = 1024;
    for (std::uint64_t offset = 0; offset < 2048; offset += 37) {
        for (std::size_t length = 0; length < 2048; length += 53) {
            for (const auto operation : {orchfs::nvme::Operation::read,
                                         orchfs::nvme::Operation::write}) {
                orchfs::nvme::IoPlan plan;
                require(!orchfs::nvme::plan_io(
                            operation, offset, length, lba, max_transfer, plan),
                        "small-range plan succeeds");

                std::size_t covered = 0;
                for (const orchfs::nvme::IoSegment &segment : plan.segments) {
                    require(segment.device_offset % lba == 0,
                            "segment device offset is LBA aligned");
                    require(segment.device_length != 0 &&
                                segment.device_length % lba == 0 &&
                                segment.device_length <= max_transfer,
                            "segment length is aligned and capped");
                    require(segment.user_offset == covered,
                            "segments cover caller buffer contiguously");
                    require(segment.device_offset + segment.bounce_offset ==
                                offset + segment.user_offset,
                            "bounce slice maps to logical byte");
                    require(segment.bounce_offset + segment.user_length <=
                                segment.device_length,
                            "caller slice fits DMA transfer");
                    const bool partial = segment.bounce_offset != 0 ||
                                         segment.user_length != segment.device_length;
                    require(segment.read_before_write ==
                                (operation == orchfs::nvme::Operation::write && partial),
                            "RMW is used exactly for partial write commands");
                    covered += segment.user_length;
                }
                require(covered == length, "plan covers every caller byte exactly once");
            }
        }
    }
}

void test_error_categories() {
    const std::error_code nvme_error =
        orchfs::nvme::make_nvme_status_error(2, 0x81);
    require(static_cast<bool>(nvme_error),
            "NVMe completion creates a real error_code");
    require(orchfs::nvme::is_nvme_status_error(nvme_error),
            "NVMe status category is identifiable");
    require(nvme_error.message().find("sct=2") != std::string::npos,
            "NVMe status message retains SCT");
}

void test_write_range_and_flush_fences() {
    using orchfs::nvme::detail::WriteCoordinator;
    WriteCoordinator coordinator;
    std::error_code error;

    auto first = coordinator.accept(0, 512, error);
    require(first && !error, "first physical write is accepted");
    auto overlapping = coordinator.accept(256, 1024, error);
    require(overlapping && !error, "overlapping physical write is accepted");
    auto disjoint = coordinator.accept(1024, 1536, error);
    require(disjoint && !error, "disjoint physical write is accepted");

    const std::uint64_t fence = coordinator.capture_fence();
    require(!coordinator.fence_ready(fence),
            "flush fence waits for accepted writes");
    require(coordinator.try_grant(first), "oldest write is granted");
    require(!coordinator.try_grant(overlapping),
            "overlapping RMW cannot pass an older write");
    require(coordinator.try_grant(disjoint),
            "non-overlapping write remains concurrent");

    coordinator.complete(disjoint);
    require(!coordinator.fence_ready(fence),
            "out-of-order completion does not release flush fence");
    coordinator.complete(first);
    require(coordinator.try_grant(overlapping),
            "overlapping write runs after predecessor completion");
    coordinator.complete(overlapping);
    require(coordinator.fence_ready(fence),
            "flush fence opens after every captured write completes");

    const std::uint64_t completed_fence = coordinator.capture_fence();
    auto later = coordinator.accept(2048, 2560, error);
    require(later && !error, "later write is accepted");
    require(coordinator.fence_ready(completed_fence),
            "writes accepted after a flush fence do not delay it");
    coordinator.complete(later);

    auto invalid = coordinator.accept(4096, 4096, error);
    require(!invalid && error == std::make_error_code(std::errc::invalid_argument),
            "empty physical reservation is rejected");
}

void test_overlapping_write_concurrency() {
    using orchfs::nvme::detail::WriteCoordinator;
    WriteCoordinator coordinator;
    constexpr std::size_t request_count = 16;
    std::vector<WriteCoordinator::Ticket> tickets;
    tickets.reserve(request_count);
    std::error_code error;
    for (std::size_t index = 0; index < request_count; ++index) {
        auto ticket = coordinator.accept(4096, 8192, error);
        require(ticket && !error, "concurrent write ticket accepted");
        tickets.push_back(std::move(ticket));
    }

    const std::uint64_t fence = coordinator.capture_fence();
    std::atomic<bool> start{false};
    std::atomic<unsigned> active{0};
    std::atomic<unsigned> overlap_violations{0};
    std::vector<std::thread> threads;
    threads.reserve(request_count);
    for (const auto &ticket : tickets) {
        threads.emplace_back([&coordinator, &start, &active,
                              &overlap_violations, ticket] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (!coordinator.try_grant(ticket)) {
                std::this_thread::yield();
            }
            if (active.fetch_add(1, std::memory_order_acq_rel) != 0) {
                overlap_violations.fetch_add(1, std::memory_order_relaxed);
            }
            std::this_thread::yield();
            active.fetch_sub(1, std::memory_order_acq_rel);
            coordinator.complete(ticket);
        });
    }
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }
    require(overlap_violations.load(std::memory_order_acquire) == 0,
            "overlapping physical writes are mutually exclusive");
    require(coordinator.fence_ready(fence),
            "concurrent completions release the captured flush fence");
}

void test_no_spdk_fallback() {
    if (orchfs_spdk_is_compiled() != 0) {
        return;
    }
    orchfs_spdk_config config;
    orchfs_spdk_config_init(&config);
    config.pci_bdf = "0000:00:00.0";
    orchfs_spdk_backend *backend = nullptr;
    require(orchfs_spdk_open(&config, &backend) == ENOTSUP,
            "non-SPDK build returns ENOTSUP");
    require(backend == nullptr, "non-SPDK build does not create a handle");
}

void test_thread_affinity_restoration() {
    cpu_set_t original;
    CPU_ZERO(&original);
    require(::pthread_getaffinity_np(::pthread_self(), sizeof(original),
                                    &original) == 0,
            "read original thread affinity");

    std::error_code error;
    orchfs::nvme::detail::ThreadAffinityGuard guard(error);
    require(!error, "capture thread affinity");

    cpu_set_t narrowed;
    CPU_ZERO(&narrowed);
    for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &original)) {
            CPU_SET(cpu, &narrowed);
            break;
        }
    }
    require(CPU_COUNT(&narrowed) == 1, "select an allowed CPU");
    require(::pthread_setaffinity_np(::pthread_self(), sizeof(narrowed),
                                    &narrowed) == 0,
            "narrow thread affinity like EAL");
    require(!guard.restore(), "restore thread affinity after environment init");

    cpu_set_t restored;
    CPU_ZERO(&restored);
    require(::pthread_getaffinity_np(::pthread_self(), sizeof(restored),
                                    &restored) == 0,
            "read restored thread affinity");
    require(std::memcmp(&original, &restored, sizeof(original)) == 0,
            "restore the complete caller CPU set");
}

} // namespace

int main() {
    test_zero_length();
    test_aligned_split();
    test_unaligned_read();
    test_unaligned_write_rmw();
    test_generic_lba_and_rounded_max();
    test_invalid_and_overflow();
    test_small_range_invariants();
    test_error_categories();
    test_write_range_and_flush_fences();
    test_overlapping_write_concurrency();
    test_no_spdk_fallback();
    test_thread_affinity_restoration();
    std::cout << "spdk_nvme_logic_test: PASS\n";
    return 0;
}
