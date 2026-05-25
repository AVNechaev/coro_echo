#pragma once

// POSIX echo server built on epoll + timerfd.
// Public API mirrors the Boost.Asio version so the high-level coroutine
// code (echo / watchdog / handle_connection / listen / run) is identical.

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <utility>

// ── forward declarations ─────────────────────────────────────────────────────
namespace posix_echo {

struct io_context;

// ── task<T> (equivalent to boost::asio::awaitable<T>) ────────────────────────

template<typename T = void>
struct task;

namespace detail {

struct promise_base
{
    io_context*             ctx  = nullptr;
    std::exception_ptr      excp;
    std::coroutine_handle<> cont; // continuation to resume when done

    struct final_awaiter
    {
        bool await_ready() noexcept { return false; }
        template<typename P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept
        {
            auto& p = h.promise();
            if (p.cont)
                return p.cont;
            return std::noop_coroutine();
        }
        void await_resume() noexcept {}
    };

    std::suspend_always initial_suspend() noexcept { return {}; }
    final_awaiter       final_suspend()   noexcept { return {}; }
    void unhandled_exception() { excp = std::current_exception(); }
};

template<typename T>
struct promise : promise_base
{
    std::optional<T> value;
    void return_value(T v) { value = std::move(v); }
    task<T> get_return_object();
};

template<>
struct promise<void> : promise_base
{
    void return_void() {}
    task<void> get_return_object();
};

} // namespace detail

template<typename T>
struct task
{
    using promise_type = detail::promise<T>;
    std::coroutine_handle<promise_type> handle;

    explicit task(std::coroutine_handle<promise_type> h) : handle(h) {}
    task(task&& o) noexcept : handle(std::exchange(o.handle, {})) {}
    ~task() { if (handle) handle.destroy(); }

    // Awaitable interface so tasks can be co_await-ed from other tasks.
    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> caller)
    {
        handle.promise().cont = caller;
        handle.resume();
    }
    T await_resume()
    {
        if (handle.promise().excp)
            std::rethrow_exception(handle.promise().excp);
        if constexpr (!std::is_void_v<T>)
            return std::move(*handle.promise().value);
    }
};

namespace detail {
template<typename T> task<T> promise<T>::get_return_object()
    { return task<T>{std::coroutine_handle<promise<T>>::from_promise(*this)}; }
inline task<void> promise<void>::get_return_object()
    { return task<void>{std::coroutine_handle<promise<void>>::from_promise(*this)}; }
} // namespace detail

// ── io_context ───────────────────────────────────────────────────────────────

struct io_context
{
    io_context();
    ~io_context();

    void run();

    // Schedule a coroutine to start on the next run() iteration.
    void post(std::coroutine_handle<> h);

    // Register fd for one-shot EPOLLIN/EPOLLOUT and resume h when ready.
    void watch_read (int fd, std::coroutine_handle<> h);
    void watch_write(int fd, std::coroutine_handle<> h);
    // Register a timerfd for one-shot expiry.
    void watch_timer(int fd, std::coroutine_handle<> h);

    int epoll_fd() const { return epfd_; }

private:
    int epfd_;
    int event_fd_; // used to wake epoll_wait from post()

    struct pending_t;
    std::unique_ptr<pending_t> pending_;
};

// ── this_coro::executor ──────────────────────────────────────────────────────

namespace this_coro {

struct executor_t {};
constexpr executor_t executor{};

struct executor_awaiter
{
    io_context* ctx;
    bool await_ready() noexcept { return false; }
    bool await_suspend(std::coroutine_handle<>) noexcept { return false; }
    io_context* await_resume() noexcept { return ctx; }
};

} // namespace this_coro

// Defined in posix_echo.cpp — returns the context stored in tl_ctx.
this_coro::executor_awaiter operator co_await(
    this_coro::executor_t) noexcept;

// ── tcp namespace ─────────────────────────────────────────────────────────────

namespace tcp {

struct socket;
struct acceptor;

struct socket
{
    explicit socket(io_context& ctx, int fd = -1);
    socket(socket&&) noexcept;
    socket& operator=(socket&&) noexcept;
    ~socket();

    // Returns a task that reads up to buf_size bytes into buf.
    task<std::size_t> async_read_some(char* buf, std::size_t buf_size);
    // Returns a task that writes exactly n bytes from buf.
    task<void>        async_write    (const char* buf, std::size_t n);

    int fd() const { return fd_; }

private:
    io_context& ctx_;
    int fd_;
};

struct acceptor
{
    acceptor(io_context& ctx, unsigned short port);
    ~acceptor();

    // Returns a task that yields a connected socket.
    task<socket> async_accept();

    io_context& get_executor() { return ctx_; }

private:
    io_context& ctx_;
    int fd_;
};

} // namespace tcp

// ── steady_timer ──────────────────────────────────────────────────────────────

struct steady_timer
{
    explicit steady_timer(io_context* ctx);
    ~steady_timer();

    void   expires_at(std::chrono::steady_clock::time_point tp);
    task<void> async_wait();

private:
    io_context* ctx_;
    int tfd_;
};

// ── co_spawn ──────────────────────────────────────────────────────────────────

// Detached tag — mirrors boost::asio::detached.
struct detached_t {};
constexpr detached_t detached{};

void co_spawn(io_context& ctx, task<void> t, detached_t);

// ── awaitable_operators && ────────────────────────────────────────────────────
// Runs left and right concurrently; cancels the other when one finishes/throws.
// Mirrors boost::asio::experimental::awaitable_operators::operator&&.

namespace detail {

task<void> and_tasks(task<void> left, task<void> right, io_context& ctx);

} // namespace detail

// operator&& must be used inside a coroutine that has access to the context.
// We model it as a small awaitable that captures both tasks.
struct and_awaitable
{
    task<void> left;
    task<void> right;
    io_context* ctx = nullptr;

    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> caller);
    void await_resume();

    std::exception_ptr excp;
    bool done = false;
};

and_awaitable operator&&(task<void> l, task<void> r);

} // namespace posix_echo
