#include "../KernelFS/spdk_nvme_bridge.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

constexpr std::size_t kPollerId = 0;
volatile std::sig_atomic_t interrupted = 0;

void handle_signal(int) noexcept {
    interrupted = 1;
}

bool install_signal_handlers() noexcept {
    struct sigaction action {};
    action.sa_handler = handle_signal;
    ::sigemptyset(&action.sa_mask);
    constexpr std::array<int, 5> signals{
        SIGHUP, SIGINT, SIGQUIT, SIGPIPE, SIGTERM};
    for (const int signal_number : signals) {
        if (::sigaction(signal_number, &action, nullptr) != 0) {
            std::cerr << "failed to install handler for signal "
                      << signal_number << ": " << std::strerror(errno) << '\n';
            return false;
        }
    }
    return true;
}

const char *env_value(const char *name) noexcept {
    const char *value = std::getenv(name);
    return value != nullptr && *value != '\0' ? value : nullptr;
}

template <typename Integer>
bool parse_integer(std::string_view text, Integer &result) noexcept {
    const char *begin = text.data();
    const char *end = begin + text.size();
    const auto parsed = std::from_chars(begin, end, result, 10);
    return parsed.ec == std::errc{} && parsed.ptr == end;
}

bool parse_u32_env(const char *name,
                   std::uint32_t default_value,
                   std::uint32_t &result) {
    const char *value = env_value(name);
    if (value == nullptr) {
        result = default_value;
        return true;
    }
    if (!parse_integer<std::uint32_t>(value, result) || result == 0) {
        std::cerr << "invalid " << name << "='" << value
                  << "' (expected an integer greater than zero)\n";
        return false;
    }
    return true;
}

bool parse_i32_env(const char *name, int default_value, int &result) {
    const char *value = env_value(name);
    if (value == nullptr) {
        result = default_value;
        return true;
    }
    if (!parse_integer<int>(value, result) || result < -1) {
        std::cerr << "invalid " << name << "='" << value
                  << "' (expected -1 or a non-negative integer)\n";
        return false;
    }
    return true;
}

bool parse_offset_env(std::uint64_t &result) {
    const char *value = env_value("ORCHFS_SPDK_TEST_OFFSET");
    if (value == nullptr) {
        std::cerr << "ORCHFS_SPDK_TEST_OFFSET is required and must name an "
                     "explicitly reserved scratch byte offset\n";
        return false;
    }
    if (!parse_integer<std::uint64_t>(value, result)) {
        std::cerr << "invalid ORCHFS_SPDK_TEST_OFFSET='" << value
                  << "' (expected an absolute byte offset)\n";
        return false;
    }
    return true;
}

std::string errno_message(int error_number) {
    return std::string(std::strerror(error_number));
}

struct BackupHeader {
    std::array<char, 16> magic{};
    std::array<char, 32> pci_bdf{};
    std::uint32_t version{};
    std::uint32_t header_size{};
    std::uint32_t namespace_id{};
    std::uint32_t lba_size{};
    std::uint64_t capacity{};
    std::uint64_t write_begin{};
    std::uint64_t write_end{};
    std::uint64_t save_begin{};
    std::uint64_t save_length{};
    std::uint64_t checksum{};
};

std::uint64_t checksum(const std::vector<std::byte> &bytes) noexcept {
    std::uint64_t value = 14695981039346656037ULL;
    for (const std::byte byte : bytes) {
        value ^= std::to_integer<std::uint8_t>(byte);
        value *= 1099511628211ULL;
    }
    return value;
}

bool write_all(int fd, const void *data, std::size_t length) noexcept {
    const auto *cursor = static_cast<const std::byte *>(data);
    while (length != 0) {
        const std::size_t chunk = std::min<std::size_t>(
            length, static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t written = ::write(fd, cursor, chunk);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (written == 0) {
            errno = EIO;
            return false;
        }
        cursor += written;
        length -= static_cast<std::size_t>(written);
    }
    return true;
}

std::string parent_directory(std::string_view path) {
    const std::size_t slash = path.find_last_of('/');
    if (slash == std::string_view::npos) {
        return ".";
    }
    if (slash == 0) {
        return "/";
    }
    return std::string(path.substr(0, slash));
}

bool persist_backup(const char *path,
                    const char *pci_bdf,
                    std::uint32_t namespace_id,
                    std::uint32_t lba_size,
                    std::uint64_t capacity,
                    std::uint64_t write_begin,
                    std::uint64_t write_end,
                    std::uint64_t save_begin,
                    const std::vector<std::byte> &saved) noexcept {
    BackupHeader header{};
    constexpr std::string_view magic{"ORCHFS-SPDK-BK1"};
    std::copy(magic.begin(), magic.end(), header.magic.begin());
    const std::string_view bdf(pci_bdf);
    if (bdf.size() >= header.pci_bdf.size()) {
        std::cerr << "PCI BDF is too long for the recovery header\n";
        return false;
    }
    std::copy(bdf.begin(), bdf.end(), header.pci_bdf.begin());
    header.version = 1;
    header.header_size = static_cast<std::uint32_t>(sizeof(header));
    header.namespace_id = namespace_id;
    header.lba_size = lba_size;
    header.capacity = capacity;
    header.write_begin = write_begin;
    header.write_end = write_end;
    header.save_begin = save_begin;
    header.save_length = saved.size();
    header.checksum = checksum(saved);

    const int fd = ::open(path,
                          O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                          S_IRUSR | S_IWUSR);
    if (fd < 0) {
        std::cerr << "cannot create exclusive backup file '" << path
                  << "': " << std::strerror(errno) << '\n';
        return false;
    }

    bool ok = write_all(fd, &header, sizeof(header)) &&
              write_all(fd, saved.data(), saved.size());
    if (ok && ::fsync(fd) != 0) {
        ok = false;
    }
    const int saved_errno = ok ? 0 : errno;
    if (::close(fd) != 0 && ok) {
        ok = false;
    }
    if (!ok) {
        std::cerr << "cannot persist backup file '" << path << "': "
                  << std::strerror(saved_errno != 0 ? saved_errno : errno)
                  << '\n';
        return false;
    }

    const std::string directory = parent_directory(path);
    const int directory_fd =
        ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory_fd < 0 || ::fsync(directory_fd) != 0) {
        const int directory_error = errno;
        if (directory_fd >= 0) {
            (void)::close(directory_fd);
        }
        std::cerr << "cannot fsync backup directory '" << directory
                  << "': " << std::strerror(directory_error) << '\n';
        return false;
    }
    if (::close(directory_fd) != 0) {
        std::cerr << "cannot close backup directory: " << std::strerror(errno)
                  << '\n';
        return false;
    }

    std::cout << "durable recovery backup='" << path
              << "' header_bytes=" << sizeof(header)
              << " data_bytes=" << saved.size() << " checksum="
              << header.checksum << '\n'
              << std::flush;
    return true;
}

struct Completion {
    bool done{};
    int error_number{};
    std::size_t bytes{};
};

void complete(void *context, int error_number, std::size_t bytes) noexcept {
    auto &completion = *static_cast<Completion *>(context);
    completion.error_number = error_number;
    completion.bytes = bytes;
    completion.done = true;
}

class Device {
public:
    explicit Device(orchfs_spdk_backend *backend) noexcept : backend_(backend) {}

    Device(const Device &) = delete;
    Device &operator=(const Device &) = delete;

    ~Device() { (void)close(); }

    bool read(std::uint64_t offset, std::vector<std::byte> &buffer) noexcept {
        return read_bytes(offset, buffer.data(), buffer.size());
    }

    bool read_bytes(std::uint64_t offset, void *buffer,
                    std::size_t length) noexcept {
        return run("read", length, [&](Completion &completion) {
            return orchfs_spdk_submit_read(backend_,
                                           kPollerId,
                                           offset,
                                           buffer,
                                           length,
                                           complete,
                                           &completion);
        });
    }

    bool write(std::uint64_t offset,
               const std::vector<std::byte> &buffer) noexcept {
        return write_bytes(offset, buffer.data(), buffer.size());
    }

    bool write_bytes(std::uint64_t offset, const void *buffer,
                     std::size_t length) noexcept {
        return run("write", length, [&](Completion &completion) {
            return orchfs_spdk_submit_write(backend_,
                                            kPollerId,
                                            offset,
                                            buffer,
                                            length,
                                            complete,
                                            &completion);
        });
    }

    bool flush() noexcept {
        return run("flush", 0, [&](Completion &completion) {
            return orchfs_spdk_submit_flush(
                backend_, kPollerId, complete, &completion);
        });
    }

    bool close() noexcept {
        if (backend_ == nullptr) {
            return true;
        }

        bool ok = true;
        orchfs_spdk_request_stop(backend_);
        while (orchfs_spdk_poller_stopped(backend_, kPollerId) == 0) {
            const int error =
                orchfs_spdk_poll(backend_, kPollerId, 0, nullptr, nullptr);
            if (error != 0 && !reported_shutdown_error_) {
                ok = false;
                reported_shutdown_error_ = true;
                std::cerr << "SPDK shutdown poll failed: " << error << " ("
                          << errno_message(error) << ")\n";
            }
        }

        const int error = orchfs_spdk_close(backend_);
        if (error != 0) {
            ok = false;
            std::cerr << "SPDK close failed: " << error << " ("
                      << errno_message(error) << ")\n";
        } else {
            backend_ = nullptr;
        }
        return ok;
    }

private:
    template <typename Submit>
    bool run(const char *operation,
             std::size_t expected_bytes,
             Submit &&submit) noexcept {
        Completion completion;
        const int submit_error = submit(completion);
        if (submit_error != 0) {
            std::cerr << operation << " submission failed: " << submit_error
                      << " (" << errno_message(submit_error) << ")\n";
            return false;
        }

        int poll_error = 0;
        while (!completion.done) {
            // This executable has exactly one poller and never leaves the main
            // thread, preserving SPDK qpair submit/poll/free thread affinity.
            poll_error =
                orchfs_spdk_poll(backend_, kPollerId, 0, nullptr, nullptr);
            if (poll_error != 0) {
                break;
            }
        }

        if (poll_error != 0) {
            std::cerr << operation << " poll failed: " << poll_error << " ("
                      << errno_message(poll_error) << ")\n";
            return false;
        }
        if (!completion.done) {
            std::cerr << operation
                      << " failed without delivering its completion\n";
            return false;
        }
        if (completion.error_number != 0) {
            std::cerr << operation << " completion failed: "
                      << completion.error_number << " ("
                      << errno_message(completion.error_number) << ")\n";
            return false;
        }
        if (completion.bytes != expected_bytes) {
            std::cerr << operation << " completed " << completion.bytes
                      << " bytes, expected " << expected_bytes << '\n';
            return false;
        }
        return true;
    }

    orchfs_spdk_backend *backend_{};
    bool reported_shutdown_error_{};
};

class RegisteredBuffer {
public:
    RegisteredBuffer(orchfs_spdk_backend *backend, std::size_t length)
        : backend_(backend), length_(length) {
        if (::posix_memalign(&memory_, 4096, length_) != 0) {
            memory_ = nullptr;
            return;
        }
        const int error = orchfs_spdk_register_memory(
            backend_, memory_, length_);
        if (error != 0) {
            std::cerr << "SPDK memory registration failed: " << error << " ("
                      << errno_message(error) << ")\n";
            std::free(memory_);
            memory_ = nullptr;
        }
    }

    ~RegisteredBuffer() {
        if (memory_ != nullptr) {
            const int error = orchfs_spdk_unregister_memory(
                backend_, memory_, length_);
            if (error != 0) {
                std::cerr << "SPDK memory unregister failed: " << error
                          << " (" << errno_message(error) << ")\n";
            }
            std::free(memory_);
        }
    }

    RegisteredBuffer(const RegisteredBuffer &) = delete;
    RegisteredBuffer &operator=(const RegisteredBuffer &) = delete;

    explicit operator bool() const noexcept { return memory_ != nullptr; }
    std::byte *data() noexcept { return static_cast<std::byte *>(memory_); }

private:
    orchfs_spdk_backend *backend_{};
    std::size_t length_{};
    void *memory_{};
};

class RestoreGuard {
public:
    RestoreGuard(Device &device,
                 std::uint64_t offset,
                 const std::vector<std::byte> &saved) noexcept
        : device_(device), offset_(offset), saved_(saved) {}

    RestoreGuard(const RestoreGuard &) = delete;
    RestoreGuard &operator=(const RestoreGuard &) = delete;

    ~RestoreGuard() {
        if (!armed_) {
            return;
        }
        std::cerr << "attempting best-effort restore of [" << offset_ << ", "
                  << offset_ + saved_.size() << ")\n";
        const bool wrote = device_.write(offset_, saved_);
        const bool flushed = wrote && device_.flush();
        std::cerr << (wrote && flushed ? "best-effort restore flushed\n"
                                      : "best-effort restore FAILED\n");
    }

    void disarm() noexcept { armed_ = false; }

private:
    Device &device_;
    std::uint64_t offset_;
    const std::vector<std::byte> &saved_;
    bool armed_{true};
};

bool add_overflows(std::uint64_t left, std::uint64_t right) noexcept {
    return right > std::numeric_limits<std::uint64_t>::max() - left;
}

} // namespace

int main() {
    const char *destructive = env_value("ORCHFS_RUN_DESTRUCTIVE_SPDK_TEST");
    if (destructive == nullptr || std::string_view(destructive) != "1") {
        std::cout
            << "spdk_nvme_device_test: SKIP (set "
               "ORCHFS_RUN_DESTRUCTIVE_SPDK_TEST=1 to open and modify a real "
               "NVMe namespace)\n";
        return 0;
    }

    const char *bdf = env_value("ORCHFS_SPDK_PCI_BDF");
    if (bdf == nullptr) {
        std::cerr << "ORCHFS_SPDK_PCI_BDF is required for the destructive "
                     "device test\n";
        return 2;
    }
    const char *backup_file = env_value("ORCHFS_SPDK_TEST_BACKUP_FILE");
    if (backup_file == nullptr) {
        std::cerr << "ORCHFS_SPDK_TEST_BACKUP_FILE is required and must be on "
                     "storage independent of the tested namespace\n";
        return 2;
    }
    if (!install_signal_handlers()) {
        return 2;
    }
    std::uint64_t write_offset = 0;
    if (!parse_offset_env(write_offset)) {
        return 2;
    }

    std::uint32_t namespace_id = 1;
    int shared_memory_id = -1;
    if (!parse_u32_env("ORCHFS_SPDK_NSID", 1, namespace_id) ||
        !parse_i32_env("ORCHFS_SPDK_SHM_ID", -1, shared_memory_id)) {
        return 2;
    }

    const char *hugepage_directory = env_value("ORCHFS_SPDK_HUGEPAGE_DIR");
    const char *reactor_mask = env_value("ORCHFS_SPDK_REACTOR_MASK");
    if (reactor_mask == nullptr) {
        reactor_mask = "0x1";
    }

    orchfs_spdk_config config;
    orchfs_spdk_config_init(&config);
    config.pci_bdf = bdf;
    config.namespace_id = namespace_id;
    config.poller_count = 1;
    config.application_name = "orchfs_spdk_destructive_test";
    config.reactor_mask = reactor_mask;
    config.hugepage_directory = hugepage_directory;
    config.shared_memory_id = shared_memory_id;

    std::cout << "WARNING: destructive SPDK test explicitly enabled\n"
              << "controller=" << bdf << " nsid=" << namespace_id
              << " reactor_mask=" << reactor_mask
              << " shm_id=" << shared_memory_id << " hugepage_dir="
              << (hugepage_directory == nullptr ? "<SPDK default>"
                                                : hugepage_directory)
              << '\n';

    orchfs_spdk_backend *raw_backend = nullptr;
    const int open_error = orchfs_spdk_open(&config, &raw_backend);
    if (open_error != 0) {
        std::cerr << "SPDK open failed: " << open_error << " ("
                  << errno_message(open_error) << ")\n";
        return 1;
    }
    Device device(raw_backend);

    const std::uint32_t lba_size = orchfs_spdk_lba_size(raw_backend);
    const std::uint64_t capacity = orchfs_spdk_capacity_bytes(raw_backend);
    if (lba_size < 2 || capacity < static_cast<std::uint64_t>(lba_size) * 6) {
        std::cerr << "namespace geometry is too small for the guarded test: "
                  << "capacity=" << capacity << " lba=" << lba_size << '\n';
        return 1;
    }

    if (write_offset % lba_size == 0) {
        std::cerr << "ORCHFS_SPDK_TEST_OFFSET must be unaligned to the "
                  << lba_size << "-byte LBA; requested " << write_offset
                  << '\n';
        return 2;
    }

    std::uint64_t payload_length =
        static_cast<std::uint64_t>(lba_size) * 2 + 37;
    if (add_overflows(write_offset, payload_length)) {
        std::cerr << "test write range overflows: offset=" << write_offset
                  << " length=" << payload_length << '\n';
        return 2;
    }
    if ((write_offset + payload_length) % lba_size == 0) {
        if (payload_length == std::numeric_limits<std::uint64_t>::max() ||
            add_overflows(write_offset, payload_length + 1)) {
            std::cerr << "cannot make the configured test range unaligned\n";
            return 2;
        }
        ++payload_length;
    }
    if (payload_length > std::numeric_limits<std::size_t>::max() ||
        write_offset + payload_length > capacity) {
        std::cerr << "test write range exceeds namespace capacity: offset="
                  << write_offset << " length=" << payload_length
                  << " capacity=" << capacity << '\n';
        return 2;
    }

    const std::uint64_t write_end = write_offset + payload_length;
    const std::uint64_t save_begin = write_offset - (write_offset % lba_size);
    const std::uint64_t tail = write_end % lba_size;
    if (tail != 0 && add_overflows(write_end, lba_size - tail)) {
        std::cerr << "aligned save range overflows\n";
        return 2;
    }
    const std::uint64_t save_end =
        tail == 0 ? write_end : write_end + (lba_size - tail);
    if (save_end > capacity || save_end - save_begin >
                                   std::numeric_limits<std::size_t>::max()) {
        std::cerr << "aligned save range exceeds namespace capacity\n";
        return 2;
    }

    const std::size_t save_length =
        static_cast<std::size_t>(save_end - save_begin);
    const std::size_t write_length = static_cast<std::size_t>(payload_length);
    const std::size_t payload_in_save =
        static_cast<std::size_t>(write_offset - save_begin);

    std::cout << "namespace_capacity=" << capacity << " lba=" << lba_size
              << " max_transfer="
              << orchfs_spdk_max_transfer_size(raw_backend) << '\n'
              << "DESTRUCTIVE write range [" << write_offset << ", "
              << write_end << ") length=" << write_length << '\n'
              << "save/restore range [" << save_begin << ", " << save_end
              << ") length=" << save_length << '\n'
              << std::flush;

    const std::string confirmation =
        std::string(bdf) + "/nsid=" + std::to_string(namespace_id) +
        "/capacity=" + std::to_string(capacity) +
        "/lba=" + std::to_string(lba_size) +
        "/write=" + std::to_string(write_offset) + "-" +
        std::to_string(write_end) + "/save=" + std::to_string(save_begin) +
        "-" + std::to_string(save_end);
    const char *confirmed = env_value("ORCHFS_SPDK_TEST_CONFIRM_RANGE");
    if (confirmed == nullptr || std::string_view(confirmed) != confirmation) {
        std::cerr << "exact range confirmation required; set:\n"
                  << "  ORCHFS_SPDK_TEST_CONFIRM_RANGE='" << confirmation
                  << "'\nNo namespace data was read or modified.\n";
        return 2;
    }

    std::vector<std::byte> saved(save_length);
    std::vector<std::byte> payload(write_length);
    std::vector<std::byte> expected(save_length);
    std::vector<std::byte> observed(save_length);

    if (!device.read(save_begin, saved)) {
        std::cerr << "initial read-save failed; no write was submitted\n";
        return 1;
    }
    if (interrupted != 0) {
        std::cerr << "interrupted before any namespace write\n";
        return 1;
    }
    if (!persist_backup(backup_file,
                        bdf,
                        namespace_id,
                        lba_size,
                        capacity,
                        write_offset,
                        write_end,
                        save_begin,
                        saved)) {
        std::cerr << "durable backup failed; no namespace write was submitted\n";
        return 1;
    }
    if (interrupted != 0) {
        std::cerr << "interrupted after backup and before namespace write\n";
        return 1;
    }
    expected = saved;
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = saved[payload_in_save + index] ^ std::byte{0xa5};
    }
    std::copy(payload.begin(), payload.end(),
              expected.begin() + static_cast<std::ptrdiff_t>(payload_in_save));

    if (interrupted != 0) {
        std::cerr << "interrupted after preparing payload and before namespace "
                     "write\n";
        return 1;
    }
    RestoreGuard restore_guard(device, save_begin, saved);

    if (!device.write(write_offset, payload) || !device.flush()) {
        std::cerr << "test write/flush failed\n";
        return 1;
    }
    if (interrupted != 0) {
        std::cerr << "interrupted after test write; restoring\n";
        return 1;
    }
    if (!device.read(save_begin, observed) || observed != expected) {
        std::cerr << "post-write read verification failed\n";
        return 1;
    }
    std::cout << "unaligned write/read verification: PASS\n";

    {
    const std::size_t registered_length =
        (std::max(save_length, write_length) + 4099U) & ~std::size_t{4095U};
    RegisteredBuffer registered(raw_backend, registered_length);
    if (!registered) {
        std::cerr << "cannot allocate the registered odd-address fixture\n";
        return 1;
    }
    for (std::size_t shift = 1; shift <= 3; ++shift) {
        std::byte *odd = registered.data() + shift;
        for (std::size_t index = 0; index < write_length; ++index) {
            odd[index] = payload[index] ^ static_cast<std::byte>(shift * 0x11U);
        }
        std::copy(odd, odd + write_length,
                  expected.begin() +
                      static_cast<std::ptrdiff_t>(payload_in_save));
        if (!device.write_bytes(write_offset, odd, write_length) ||
            !device.flush() || !device.read(save_begin, observed) ||
            observed != expected) {
            std::cerr << "registered source +" << shift
                      << " write verification failed\n";
            return 1;
        }
        std::memset(odd, 0, save_length);
        if (!device.read_bytes(save_begin, odd, save_length) ||
            !std::equal(expected.begin(), expected.end(), odd)) {
            std::cerr << "registered destination +" << shift
                      << " read verification failed\n";
            return 1;
        }
    }
    std::cout << "registered odd-address +1/+2/+3 verification: PASS\n";
    }

    if (!device.write(save_begin, saved) || !device.flush()) {
        std::cerr << "explicit restore/flush failed\n";
        return 1;
    }
    std::fill(observed.begin(), observed.end(), std::byte{});
    if (!device.read(save_begin, observed) || observed != saved) {
        std::cerr << "post-restore read verification failed\n";
        return 1;
    }

    restore_guard.disarm();
    if (!device.close()) {
        std::cerr << "device shutdown failed after successful restore\n";
        return 1;
    }
    if (interrupted != 0) {
        std::cerr << "interrupted; namespace was restored and flushed, backup "
                     "retained at '"
                  << backup_file << "'\n";
        return 1;
    }
    std::cout << "restore/read verification: PASS\n"
              << "durable backup retained at '" << backup_file << "'\n"
              << "spdk_nvme_device_test: PASS\n";
    return 0;
}
