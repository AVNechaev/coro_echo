#include "posix_echo.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <deque>
#include <mutex>
#include <utility>
#include <vector>

namespace posix_echo {

// ── helpers ──────────────────────────────────────────────────────────────────

static void throw_errno(const char* msg)
{
    throw std::system_error(errno, std::generic_category(), msg);
}

static void set_nonblock(int fd)
{
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) throw_errno("fcntl F_GETFL");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) throw_errno("fcntl F_SETFL");
}

// ── io_context ───────────────────────────────────────────────────────────────

struct io_context::pending_t
{
    std::mutex              mtx;
    std::deque<std::coroutine_handle<>> ready; // posted handles
};

io_context::io_context()
    : epfd_(::epoll_create1(EPOLL_CLOEXEC))
    , event_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC))
    , pending_(std::make_unique<pending_t>())
{
    if (epfd_ < 0)    throw_errno("epoll_create1");
    if (event_fd_ < 0) throw_errno("eventfd");

    epoll_event ev{};
    ev.events  = EPOLLIN;
    ev.data.ptr = nullptr; // nullptr == event_fd_ wake-up signal
    ::epoll_ctl(epfd_, EPOLL_CTL_ADD, event_fd_, &ev);
}

io_context::~io_context()
{
    ::close(event_fd_);
    ::close(epfd_);
}

void io_context::post(std::coroutine_handle<> h)
{
    {
        std::lock_guard lk(pending_->mtx);
        pending_->ready.push_back(h);
    }
    uint64_t v = 1;
    ::write(event_fd_, &v, sizeof(v));
}

void io_context::watch_read(int fd, std::coroutine_handle<> h)
{
    epoll_event ev{};
    ev.events   = EPOLLIN | EPOLLONESHOT;
    ev.data.ptr = h.address();
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0)
        ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
}

void io_context::watch_write(int fd, std::coroutine_handle<> h)
{
    epoll_event ev{};
    ev.events   = EPOLLOUT | EPOLLONESHOT;
    ev.data.ptr = h.address();
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0)
        ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
}

void io_context::watch_timer(int fd, std::coroutine_handle<> h)
{
    epoll_event ev{};
    ev.events   = EPOLLIN | EPOLLONESHOT;
    ev.data.ptr = h.address();
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0)
    {
        // fd already registered — modify instead
        ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
    }
}

void io_context::run()
{
    constexpr int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    for (;;)
    {
        // Drain ready queue first.
        for (;;)
        {
            std::coroutine_handle<> h;
            {
                std::lock_guard lk(pending_->mtx);
                if (pending_->ready.empty()) break;
                h = pending_->ready.front();
                pending_->ready.pop_front();
            }
            if (h && !h.done()) h.resume();
        }

        int n = ::epoll_wait(epfd_, events, MAX_EVENTS, -1);
        if (n < 0)
        {
            if (errno == EINTR) continue;
            throw_errno("epoll_wait");
        }

        for (int i = 0; i < n; ++i)
        {
            if (events[i].data.ptr == nullptr)
            {
                // event_fd_ wake-up: drain and loop to process ready queue
                uint64_t v;
                ::read(event_fd_, &v, sizeof(v));
                continue;
            }
            auto h = std::coroutine_handle<>::from_address(events[i].data.ptr);
            if (h && !h.done()) h.resume();
        }
    }
}

// ── this_coro::executor awaiter ───────────────────────────────────────────────
// Each promise stores the ctx pointer; we expose it via a special awaitable.
// To make `co_await this_coro::executor` work we need the promise to supply it.
// We do this by intercepting await_transform in a base.  For simplicity, the
// posix_echo tasks carry a thread-local context pointer set by co_spawn/run.

thread_local static io_context* tl_ctx = nullptr;

this_coro::executor_awaiter operator co_await(
    this_coro::executor_t) noexcept
{
    return this_coro::executor_awaiter{tl_ctx};
}

} // namespace posix_echo

namespace posix_echo {

// ── tcp::socket ───────────────────────────────────────────────────────────────

namespace tcp {

socket::socket(io_context& ctx, int fd) : ctx_(ctx), fd_(fd) {}

socket::socket(socket&& o) noexcept
    : ctx_(o.ctx_), fd_(std::exchange(o.fd_, -1)) {}

socket& socket::operator=(socket&& o) noexcept
{
    if (this != &o)
    {
        if (fd_ >= 0) ::close(fd_);
        fd_ = std::exchange(o.fd_, -1);
    }
    return *this;
}

socket::~socket() { if (fd_ >= 0) ::close(fd_); }

task<std::size_t> socket::async_read_some(char* buf, std::size_t buf_size)
{
    for (;;)
    {
        ssize_t n = ::recv(fd_, buf, buf_size, 0);
        if (n > 0) co_return static_cast<std::size_t>(n);
        if (n == 0) throw std::system_error(
            std::make_error_code(std::errc::connection_reset));
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            // Suspend until the fd is readable.
            struct read_awaiter
            {
                io_context& ctx;
                int fd;
                bool await_ready() noexcept { return false; }
                void await_suspend(std::coroutine_handle<> h)
                { ctx.watch_read(fd, h); }
                void await_resume() noexcept {}
            };
            co_await read_awaiter{ctx_, fd_};
            continue;
        }
        throw_errno("recv");
    }
}

task<void> socket::async_write(const char* buf, std::size_t n)
{
    std::size_t sent = 0;
    while (sent < n)
    {
        ssize_t r = ::send(fd_, buf + sent, n - sent, MSG_NOSIGNAL);
        if (r >= 0) { sent += r; continue; }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            struct write_awaiter
            {
                io_context& ctx;
                int fd;
                bool await_ready() noexcept { return false; }
                void await_suspend(std::coroutine_handle<> h)
                { ctx.watch_write(fd, h); }
                void await_resume() noexcept {}
            };
            co_await write_awaiter{ctx_, fd_};
            continue;
        }
        throw_errno("send");
    }
}

// ── tcp::acceptor ─────────────────────────────────────────────────────────────

acceptor::acceptor(io_context& ctx, unsigned short port) : ctx_(ctx)
{
    fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd_ < 0) throw_errno("socket");

    int opt = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    set_nonblock(fd_);

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);
    if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw_errno("bind");
    if (::listen(fd_, SOMAXCONN) < 0)
        throw_errno("listen");
}

acceptor::~acceptor() { if (fd_ >= 0) ::close(fd_); }

task<socket> acceptor::async_accept()
{
    for (;;)
    {
        int cfd = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (cfd >= 0) co_return socket{ctx_, cfd};
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            struct accept_awaiter
            {
                io_context& ctx;
                int fd;
                bool await_ready() noexcept { return false; }
                void await_suspend(std::coroutine_handle<> h)
                { ctx.watch_read(fd, h); }
                void await_resume() noexcept {}
            };
            co_await accept_awaiter{ctx_, fd_};
            continue;
        }
        throw_errno("accept4");
    }
}

} // namespace tcp

// ── steady_timer ──────────────────────────────────────────────────────────────

steady_timer::steady_timer(io_context* ctx)
    : ctx_(ctx)
    , tfd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC))
{
    if (tfd_ < 0) throw_errno("timerfd_create");
}

steady_timer::~steady_timer() { if (tfd_ >= 0) ::close(tfd_); }

void steady_timer::expires_at(std::chrono::steady_clock::time_point tp)
{
    auto dur   = tp - std::chrono::steady_clock::now();
    auto secs  = std::chrono::duration_cast<std::chrono::seconds>(dur);
    auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(dur - secs);
    if (secs.count() < 0) { secs = {}; nsecs = {}; }

    itimerspec its{};
    its.it_value.tv_sec  = secs.count();
    its.it_value.tv_nsec = nsecs.count();
    ::timerfd_settime(tfd_, 0, &its, nullptr);
}

task<void> steady_timer::async_wait()
{
    struct timer_awaiter
    {
        io_context* ctx;
        int tfd;
        bool await_ready() noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h)
        { ctx->watch_timer(tfd, h); }
        void await_resume() noexcept
        {
            uint64_t exp;
            ::read(tfd, &exp, sizeof(exp));
        }
    };
    co_await timer_awaiter{ctx_, tfd_};
}

// ── co_spawn ──────────────────────────────────────────────────────────────────

void co_spawn(io_context& ctx, task<void> t, detached_t)
{
    // Transfer ownership to a heap-allocated wrapper so the task outlives
    // the call site, then post the coroutine handle for the event loop.
    struct wrapper
    {
        task<void> t;
        io_context& ctx;

        static task<void> run(task<void> inner, io_context& ctx)
        {
            tl_ctx = &ctx;
            try { co_await inner; }
            catch (...) { /* detached: swallow */ }
        }
    };

    // Start the wrapper coroutine (suspended at initial_suspend).
    auto outer = wrapper::run(std::move(t), ctx);
    auto h     = outer.handle;
    outer.handle = {}; // release ownership — the coroutine is self-managed
    ctx.post(h);
}

// ── operator&& ───────────────────────────────────────────────────────────────

and_awaitable operator&&(task<void> l, task<void> r)
{
    return and_awaitable{std::move(l), std::move(r)};
}

namespace detail {

// Runs two tasks concurrently on the same io_context.
// Whichever finishes first (normally or via exception) wins;
// the other is abandoned (its coroutine is simply not resumed again).
task<void> and_tasks(task<void> left, task<void> right, io_context& ctx)
{
    // We run each child as a detached co_spawn but track completion via
    // shared state written back to and_awaitable.
    struct state_t
    {
        std::exception_ptr excp;
        int                remaining = 2;
        std::coroutine_handle<> parent;
    };
    auto st = std::make_shared<state_t>();

    auto wrap = [&ctx, st](task<void> t) -> task<void> {
        tl_ctx = &ctx;
        try { co_await t; }
        catch (...) { if (!st->excp) st->excp = std::current_exception(); }
        --st->remaining;
        if (st->remaining == 0 || st->excp)
            if (st->parent) ctx.post(st->parent);
    };

    // Launch both children detached.
    {
        auto h1 = wrap(std::move(left));
        auto h = h1.handle; h1.handle = {};
        ctx.post(h);
    }
    {
        auto h2 = wrap(std::move(right));
        auto h = h2.handle; h2.handle = {};
        ctx.post(h);
    }

    // Suspend the parent until at least one child is done.
    struct wait_awaiter
    {
        std::shared_ptr<state_t> st;
        bool await_ready() noexcept { return st->remaining == 0 || st->excp != nullptr; }
        void await_suspend(std::coroutine_handle<> h) { st->parent = h; }
        void await_resume()
        {
            if (st->excp) std::rethrow_exception(st->excp);
        }
    };
    co_await wait_awaiter{st};
}

} // namespace detail

void and_awaitable::await_suspend(std::coroutine_handle<> caller)
{
    // We need the io_context.  It is stored in tl_ctx at this point because
    // this co_await runs inside a coroutine launched by co_spawn.
    ctx = tl_ctx;
    assert(ctx);

    // Build the combined task and post it; when it finishes it will resume caller.
    auto combined = detail::and_tasks(std::move(left), std::move(right), *ctx);
    // Attach caller as the continuation.
    combined.handle.promise().cont = caller;
    auto h = combined.handle;
    combined.handle = {};
    ctx->post(h);
}

void and_awaitable::await_resume()
{
    if (excp) std::rethrow_exception(excp);
}

// ── posix echo server ─────────────────────────────────────────────────────────

} // namespace posix_echo
