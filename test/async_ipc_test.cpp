#include "orchfs/async/ipc_transport.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <thread>

#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

using namespace orchfs::async;
using namespace std::chrono_literals;

[[nodiscard]] std::span<const std::byte> bytes(std::string_view value) {
    return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

[[nodiscard]] bool wait_until(const std::atomic<bool>& flag,
                              std::chrono::milliseconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!flag.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    return flag.load(std::memory_order_acquire);
}

void wait_readable(int fd) {
    pollfd descriptor{fd, POLLIN, 0};
    int result = -1;
    do {
        result = ::poll(&descriptor, 1, 5000);
    } while (result < 0 && errno == EINTR);
    assert(result == 1);
    assert((descriptor.revents & POLLIN) != 0);
}

void assert_payload(std::span<const std::byte> actual,
                    std::string_view expected) {
    assert(actual.size() == expected.size());
    assert(std::memcmp(actual.data(), expected.data(), expected.size()) == 0);
}

}  // namespace

int main() {
    const std::string socket_path =
        "/tmp/orchfs-async-ipc-" + std::to_string(::getpid()) + ".sock";
    ::unlink(socket_path.c_str());

    const std::string stale_path = socket_path + "-stale";
    ::unlink(stale_path.c_str());
    const pid_t stale_owner = ::fork();
    assert(stale_owner >= 0);
    if (stale_owner == 0) {
        std::error_code child_error;
        auto stale = ControlServer::listen(
            stale_path, TransportConfig{1, 2, 64}, child_error);
        _exit(stale && !child_error ? 0 : 1);
    }
    int stale_status = 0;
    assert(::waitpid(stale_owner, &stale_status, 0) == stale_owner);
    assert(WIFEXITED(stale_status) && WEXITSTATUS(stale_status) == 0);
    std::error_code stale_error;
    auto recovered_listener = ControlServer::listen(
        stale_path, TransportConfig{1, 2, 64}, stale_error);
    assert(recovered_listener && !stale_error);
    recovered_listener = ControlServer{};
    assert(::access(stale_path.c_str(), F_OK) != 0 && errno == ENOENT);

    std::error_code error;
    auto invalid_listener =
        ControlServer::listen(socket_path + "-invalid", {1, 1, 64}, error);
    assert(!invalid_listener);
    assert(error == std::errc::invalid_argument);
    error.clear();
    const TransportConfig limits{2, 4, 256};
    auto listener = ControlServer::listen(socket_path, limits, error);
    assert(!error);
    assert(listener);

    std::atomic<bool> allow_server_consume{false};
    std::atomic<bool> first_two_consumed{false};
    std::atomic<bool> third_completion_blocked{false};
    std::atomic<bool> allow_third_completion{false};
    std::atomic<bool> ready_for_second_client{false};
    std::exception_ptr server_failure;
    std::uint64_t first_client_id = 0;
    std::uint64_t first_generation = 0;

    std::thread server_thread([&] {
        try {
            std::error_code server_error;
            auto session = listener.accept(server_error);
            assert(!server_error);
            assert(session);
            assert(session.peer_alive());
            assert(session.client_pid() == static_cast<std::uint32_t>(::getpid()));
            first_client_id = session.client_id();
            first_generation = session.session_generation();

            while (!allow_server_consume.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            wait_readable(session.submission_event_fd(0));
            std::uint64_t notifications = 0;
            server_error = session.drain_submission_notifications(0, notifications);
            assert(!server_error);
            assert(notifications == 2);

            std::array<IpcDescriptor, 3> requests{};
            std::array<std::byte, 2> undersized{};
            std::size_t payload_size = 0;
            server_error = session.try_receive_submission(
                0, requests[0], undersized, payload_size);
            assert(server_error == TransportErrc::buffer_too_small);
            assert(payload_size == 3);

            std::array<std::byte, 64> payload{};
            server_error = session.try_receive_submission(0, requests[0], payload,
                                                          payload_size);
            assert(!server_error);
            assert_payload(std::span(payload).first(payload_size), "one");
            assert(has_flag(requests[0].flags, DescriptorFlag::request));
            assert(!has_flag(requests[0].flags, DescriptorFlag::response));

            server_error = session.try_receive_submission(0, requests[1], payload,
                                                          payload_size);
            assert(!server_error);
            assert_payload(std::span(payload).first(payload_size), "two");
            assert(requests[1].request_id != requests[0].request_id);

            requests[0].status = 0;
            requests[0].result_length = 3;
            server_error = session.try_complete(0, requests[0], bytes("ONE"));
            assert(!server_error);
            requests[1].status = 0;
            requests[1].result_length = 3;
            server_error = session.try_complete(0, requests[1], bytes("TWO"));
            assert(!server_error);
            first_two_consumed.store(true, std::memory_order_release);

            wait_readable(session.submission_event_fd(0));
            server_error = session.drain_submission_notifications(0, notifications);
            assert(!server_error);
            assert(notifications == 1);
            server_error = session.try_receive_submission(0, requests[2], payload,
                                                          payload_size);
            assert(!server_error);
            assert_payload(std::span(payload).first(payload_size), "three");
            requests[2].status = 0;
            requests[2].result_length = 5;
            server_error = session.try_complete(0, requests[2], bytes("THREE"));
            assert(server_error == TransportErrc::would_block);
            third_completion_blocked.store(true, std::memory_order_release);

            while (!allow_third_completion.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            server_error = session.try_complete(0, requests[2], bytes("THREE"));
            assert(!server_error);

            const auto close_deadline = std::chrono::steady_clock::now() + 5s;
            while (session.peer_alive() &&
                   std::chrono::steady_clock::now() < close_deadline) {
                std::this_thread::sleep_for(1ms);
            }
            assert(!session.peer_alive());
            ready_for_second_client.store(true, std::memory_order_release);

            auto second_session = listener.accept(server_error);
            assert(!server_error);
            assert(second_session);
            assert(second_session.client_id() != first_client_id);
            assert(second_session.session_generation() != first_generation);
            wait_readable(second_session.submission_event_fd(1));
            server_error =
                second_session.drain_submission_notifications(1, notifications);
            assert(!server_error);
            assert(notifications == 1);
            IpcDescriptor ping{};
            server_error = second_session.try_receive_submission(1, ping, payload,
                                                                 payload_size);
            assert(!server_error);
            assert(ping.opcode == Opcode::ping);
            assert(payload_size == 0);
            ping.result_length = 0;
            server_error = second_session.try_complete(1, ping);
            assert(!server_error);
        } catch (...) {
            server_failure = std::current_exception();
        }
    });

    const TransportConfig requested{2, 2, 64};
    {
        auto client = ClientTransport::connect(socket_path, requested, error);
        assert(!error);
        assert(client);
        assert(client.config() == requested);
        assert(client.peer_alive());
        assert(client.client_id() != 0);
        assert(client.session_generation() != 0);
        client.heartbeat();

        IpcDescriptor descriptor{};
        descriptor.opcode = Opcode::write_at;
        descriptor.offset = 32U * 1024U;
        descriptor.length = 3;
        descriptor.resume_worker = 0;
        std::uint64_t first_request_id = 0;
        error = client.try_submit(0, descriptor, bytes("one"), &first_request_id);
        assert(!error);
        assert(first_request_id != 0);
        descriptor.offset += 32U * 1024U;
        error = client.try_submit(0, descriptor, bytes("two"));
        assert(!error);
        error = client.try_submit(0, descriptor, bytes("full"));
        assert(error == TransportErrc::would_block);

        allow_server_consume.store(true, std::memory_order_release);
        assert(wait_until(first_two_consumed));
        descriptor.offset += 32U * 1024U;
        descriptor.length = 5;
        error = client.try_submit(0, descriptor, bytes("three"));
        assert(!error);
        assert(wait_until(third_completion_blocked));

        wait_readable(client.completion_event_fd(0));
        std::uint64_t notifications = 0;
        error = client.drain_completion_notifications(0, notifications);
        assert(!error);
        assert(notifications == 2);
        std::array<std::byte, 64> response_payload{};
        std::size_t response_size = 0;
        for (std::string_view expected : {"ONE", "TWO"}) {
            IpcDescriptor response{};
            error = client.try_receive_completion(0, response, response_payload,
                                                  response_size);
            assert(!error);
            assert(has_flag(response.flags, DescriptorFlag::response));
            assert(!has_flag(response.flags, DescriptorFlag::request));
            assert(response.status == 0);
            assert_payload(std::span(response_payload).first(response_size),
                           expected);
        }

        allow_third_completion.store(true, std::memory_order_release);
        wait_readable(client.completion_event_fd(0));
        error = client.drain_completion_notifications(0, notifications);
        assert(!error);
        assert(notifications == 1);
        IpcDescriptor response{};
        error = client.try_receive_completion(0, response, response_payload,
                                              response_size);
        assert(!error);
        assert_payload(std::span(response_payload).first(response_size), "THREE");
    }

    assert(wait_until(ready_for_second_client));
    {
        auto client = ClientTransport::connect(socket_path, requested, error);
        assert(!error);
        assert(client);
        assert(client.client_id() != first_client_id);
        IpcDescriptor ping{};
        ping.opcode = Opcode::ping;
        ping.resume_worker = 1;
        error = client.try_submit(1, ping);
        assert(!error);
        wait_readable(client.completion_event_fd(1));
        std::uint64_t notifications = 0;
        error = client.drain_completion_notifications(1, notifications);
        assert(!error);
        assert(notifications == 1);
        IpcDescriptor response{};
        std::size_t payload_size = 0;
        error = client.try_receive_completion(1, response, {}, payload_size);
        assert(!error);
        assert(response.opcode == Opcode::ping);
        assert(response.resume_worker == 1);
    }

    server_thread.join();
    if (server_failure) {
        std::rethrow_exception(server_failure);
    }

    // Exercise the actual cross-process ABI as well as the in-process aliases
    // above. Fork only after the helper thread has joined.
    const std::string process_socket_path =
        "/tmp/orchfs-async-ipc-process-" + std::to_string(::getpid()) + ".sock";
    ::unlink(process_socket_path.c_str());
    auto process_listener =
        ControlServer::listen(process_socket_path, {1, 2, 64}, error);
    assert(!error);
    assert(process_listener);
    const pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        // Inherited transport/listener objects are invalid in the child and
        // their destruction must not shut down or unlink the parent's endpoint.
        process_listener = ControlServer{};
        std::error_code child_error;
        auto child_client =
            ClientTransport::connect(process_socket_path, {1, 2, 64}, child_error);
        if (child_error || !child_client) {
            ::_exit(10);
        }
        IpcDescriptor request{};
        request.opcode = Opcode::ping;
        child_error = child_client.try_submit(0, request, bytes("child"));
        if (child_error) {
            ::_exit(11);
        }
        wait_readable(child_client.completion_event_fd(0));
        std::uint64_t notifications = 0;
        child_error =
            child_client.drain_completion_notifications(0, notifications);
        if (child_error || notifications != 1) {
            ::_exit(12);
        }
        std::array<std::byte, 64> payload{};
        std::size_t payload_size = 0;
        IpcDescriptor response{};
        child_error = child_client.try_receive_completion(
            0, response, payload, payload_size);
        if (child_error || response.opcode != Opcode::ping || payload_size != 2 ||
            std::memcmp(payload.data(), "ok", 2) != 0) {
            ::_exit(13);
        }
        ::_exit(0);
    }

    auto child_session = process_listener.accept(error);
    assert(!error);
    assert(child_session);
    assert(child_session.client_pid() == static_cast<std::uint32_t>(child));
    wait_readable(child_session.submission_event_fd(0));
    std::uint64_t notifications = 0;
    error = child_session.drain_submission_notifications(0, notifications);
    assert(!error);
    assert(notifications == 1);
    std::array<std::byte, 64> child_payload{};
    std::size_t child_payload_size = 0;
    IpcDescriptor child_request{};
    error = child_session.try_receive_submission(
        0, child_request, child_payload, child_payload_size);
    assert(!error);
    assert_payload(std::span(child_payload).first(child_payload_size), "child");
    error = child_session.try_complete(0, child_request, bytes("ok"));
    assert(!error);
    int child_status = 0;
    assert(::waitpid(child, &child_status, 0) == child);
    assert(WIFEXITED(child_status));
    assert(WEXITSTATUS(child_status) == 0);
    // _exit bypassed ClientTransport's destructor, so this specifically checks
    // control-socket death detection rather than the shared closing state.
    assert(!child_session.peer_alive());
    child_session.mark_dead();

    std::cout << "async IPC transport tests passed\n";
    return 0;
}
