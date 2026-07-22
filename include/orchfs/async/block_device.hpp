#pragma once

#include "orchfs/async/runtime.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

namespace orchfs::async {

enum class DeviceWriteDurability : std::uint8_t {
    completion = 1,
    fua = 2,
    flush = 3,
};

struct BlockRead {
    std::uint64_t offset{};
    std::span<std::byte> destination;
};

struct BlockWrite {
    std::uint64_t offset{};
    std::span<const std::byte> source;
};

// Coroutine-facing device module. The implementation owns completion routing;
// callers only retain their borrowed buffer until the returned Task completes.
class AsyncBlockDevice final {
public:
    explicit AsyncBlockDevice(Runtime& runtime) noexcept : runtime_(&runtime) {}

    [[nodiscard]] Task<Result<std::size_t>> read(
        std::uint64_t offset, std::span<std::byte> destination) const;
    [[nodiscard]] Task<Result<std::size_t>> write(
        std::uint64_t offset, std::span<const std::byte> source) const;
    // Every request in a batch is submitted in the same phase. The coroutine
    // resumes only after all successfully submitted requests have completed,
    // including when a later submission fails.
    [[nodiscard]] Task<Result<std::size_t>> read_batch(
        std::span<const BlockRead> requests) const;
    [[nodiscard]] Task<Result<std::size_t>> write_batch(
        std::span<const BlockWrite> requests) const;
    [[nodiscard]] Task<Result<void>> flush() const;
    [[nodiscard]] DeviceWriteDurability write_durability() const noexcept;

private:
    Runtime* runtime_;
};

} // namespace orchfs::async
