#include "posix_echo.h"

#include <arpa/inet.h>
#include <array>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <cassert>
#include <utility>

namespace posix_echo {

namespace {

static void throw_errno(const char *msg) {
  throw std::system_error(errno, std::generic_category(), msg);
}

thread_local io_context *tl_ctx = nullptr;

} // namespace

// ── io_context ───────────────────────────────────────────────────────────────

io_context::io_context() : epfd_(::epoll_create1(EPOLL_CLOEXEC)) {
  if (epfd_ < 0)
    throw_errno("epoll_create1");
}

io_context::~io_context() { ::close(epfd_); }

void io_context::post(std::coroutine_handle<> h) { ready_.push_back(h); }

void io_context::watch(int fd, uint32_t events, std::coroutine_handle<> h) {
  epoll_event ev{events | EPOLLONESHOT, {h.address()}};
  if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0)
    ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
}

void io_context::run() {
  tl_ctx = this; // set once; all coroutines running here share this context

  std::array<epoll_event, 64> evs;
  for (;;) {
    while (!ready_.empty()) {
      auto h = ready_.front();
      ready_.pop_front();
      if (!h.done())
        h.resume();
    }

    // Block indefinitely when idle; poll without blocking when work is pending.
    int n =
        ::epoll_wait(epfd_, evs.data(), evs.size(), ready_.empty() ? -1 : 0);
    if (n < 0 && errno == EINTR)
      continue;
    for (int i = 0; i < n; ++i)
      ready_.push_back(std::coroutine_handle<>::from_address(evs[i].data.ptr));
  }
}

// ── this_coro::executor ──────────────────────────────────────────────────────

this_coro::executor_awaiter operator co_await(this_coro::executor_t) noexcept {
  return {.ctx = tl_ctx};
}

// ── tcp::socket
// ───────────────────────────────────────────────────────────────

namespace tcp {

// Small awaiter reused by read, write and accept: suspends until fd is ready.
struct fd_awaiter {
  io_context &ctx;
  int fd;
  uint32_t events;
  bool await_ready() noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) { ctx.watch(fd, events, h); }
  void await_resume() noexcept {}
};

socket::socket(io_context &ctx, int fd) : ctx_(ctx), fd_(fd) {}

socket::socket(socket &&o) noexcept
    : ctx_(o.ctx_), fd_(std::exchange(o.fd_, -1)) {}

socket &socket::operator=(socket &&o) noexcept {
  if (this != &o) {
    if (fd_ >= 0)
      ::close(fd_);
    fd_ = std::exchange(o.fd_, -1);
  }
  return *this;
}

socket::~socket() {
  if (fd_ >= 0)
    ::close(fd_);
}

task<std::size_t> socket::async_read_some(char *buf, std::size_t buf_size) {
  for (;;) {
    ssize_t n = ::recv(fd_, buf, buf_size, 0);
    if (n > 0)
      co_return static_cast<std::size_t>(n);
    if (n == 0)
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset));
    if (errno != EWOULDBLOCK)
      throw_errno("recv");
    co_await fd_awaiter{.ctx = ctx_, .fd = fd_, .events = EPOLLIN};
  }
}

task<void> socket::async_write(const char *buf, std::size_t n) {
  for (std::size_t sent = 0; sent < n;) {
    ssize_t r = ::send(fd_, buf + sent, n - sent, MSG_NOSIGNAL);
    if (r >= 0) {
      sent += r;
      continue;
    }
    if (errno != EWOULDBLOCK)
      throw_errno("send");
    co_await fd_awaiter{.ctx = ctx_, .fd = fd_, .events = EPOLLOUT};
  }
}

// ── tcp::acceptor
// ─────────────────────────────────────────────────────────────

acceptor::acceptor(io_context &ctx, unsigned short port) : ctx_(ctx) {
  fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd_ < 0)
    throw_errno("socket");

  int opt = 1;
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr{AF_INET, htons(port), {INADDR_ANY}, {}};
  if (::bind(fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    throw_errno("bind");
  if (::listen(fd_, SOMAXCONN) < 0)
    throw_errno("listen");
}

acceptor::~acceptor() {
  if (fd_ >= 0)
    ::close(fd_);
}

task<socket> acceptor::async_accept() {
  for (;;) {
    int cfd = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
    if (cfd >= 0)
      co_return socket{ctx_, cfd};
    if (errno != EWOULDBLOCK)
      throw_errno("accept4");
    co_await fd_awaiter{.ctx = ctx_, .fd = fd_, .events = EPOLLIN};
  }
}

} // namespace tcp

// ── steady_timer
// ──────────────────────────────────────────────────────────────

steady_timer::steady_timer(io_context *ctx)
    : ctx_(ctx),
      tfd_(::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)) {
  if (tfd_ < 0)
    throw_errno("timerfd_create");
}

steady_timer::~steady_timer() {
  if (tfd_ >= 0)
    ::close(tfd_);
}

void steady_timer::expires_at(std::chrono::steady_clock::time_point tp) {
  auto d = tp - std::chrono::steady_clock::now();
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(d);
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(d - secs);
  if (secs.count() < 0) {
    secs = {};
    ns = {};
  }
  itimerspec its{{}, {secs.count(), ns.count()}};
  ::timerfd_settime(tfd_, 0, &its, nullptr);
}

task<void> steady_timer::async_wait() {
  struct timer_awaiter {
    io_context *ctx;
    int tfd;
    bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
      ctx->watch(tfd, EPOLLIN, h);
    }
    void await_resume() noexcept {
      uint64_t e;
      ::read(tfd, &e, 8);
    }
  };
  co_await timer_awaiter{ctx_, tfd_};
}

// ── co_spawn
// ────────────────────────────────────────────────────────────────── tl_ctx is
// set by run(); all coroutines run inside run() inherit it. Detached tasks have
// no continuation, so final_awaiter self-destroys the frame.

void co_spawn(io_context &ctx, task<void> t, detached_t) {
  auto h = t.handle;
  t.handle = {};
  ctx.post(h);
}

// ── operator&& ───────────────────────────────────────────────────────────────

and_awaitable operator&&(task<void> l, task<void> r) {
  return {.left = std::move(l), .right = std::move(r)};
}

static task<void> and_tasks(task<void> left, task<void> right) {
  struct State {
    std::exception_ptr excp;
    int remaining = 2;
    std::coroutine_handle<> parent;
  };
  auto st = std::make_shared<State>();

  auto child = [st](task<void> t) -> task<void> {
    try {
      co_await t;
    } catch (...) {
      if (!st->excp)
        st->excp = std::current_exception();
    }
    if (--st->remaining == 0 || st->excp)
      if (st->parent)
        tl_ctx->post(st->parent);
  };

  co_spawn(*tl_ctx, child(std::move(left)), detached);
  co_spawn(*tl_ctx, child(std::move(right)), detached);

  struct wait_awaiter {
    std::shared_ptr<State> st;
    bool await_ready() noexcept { return st->remaining == 0; }
    void await_suspend(std::coroutine_handle<> h) noexcept { st->parent = h; }
    void await_resume() {
      if (st->excp)
        std::rethrow_exception(st->excp);
    }
  };
  co_await wait_awaiter{st};
}

void and_awaitable::await_suspend(std::coroutine_handle<> caller) {
  assert(tl_ctx);
  combined = and_tasks(std::move(left), std::move(right));
  combined.handle.promise().cont = caller;
  combined.handle.resume(); // runs until it suspends at wait_awaiter
}

void and_awaitable::await_resume() {
  if (combined.handle.promise().excp)
    std::rethrow_exception(combined.handle.promise().excp);
}

} // namespace posix_echo
