#pragma once

// POSIX echo server built on epoll + timerfd.
// Public API mirrors the Boost.Asio version so the high-level coroutine
// code (echo / watchdog / handle_connection / listen / run) is identical.

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <deque>
#include <exception>
#include <optional>
#include <utility>

namespace posix_echo {

struct io_context;

// ── task<T> (equivalent to boost::asio::awaitable<T>) ────────────────────────

template <typename T = void> struct task;

namespace detail {

struct promise_base {
  std::exception_ptr excp;
  std::coroutine_handle<>
      cont; // continuation resumed when the coroutine finishes

  struct final_awaiter {
    bool await_ready() noexcept { return false; }
    template <typename P>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
      auto &p = h.promise();
      if (p.cont) {
        return p.cont; // owned: resume caller
      }
      h.destroy();     // detached: no owner, self-destruct
      return std::noop_coroutine();
    }
    void await_resume() noexcept {}
  };

  std::suspend_always initial_suspend() noexcept { return {}; }
  final_awaiter final_suspend() noexcept { return {}; }
  void unhandled_exception() { excp = std::current_exception(); }
};

template <typename T> struct promise : promise_base {
  std::optional<T> value;
  void return_value(T v) { value = std::move(v); }
  task<T> get_return_object();
};

template <> struct promise<void> : promise_base {
  void return_void() {}
  task<void> get_return_object();
};

} // namespace detail

template <typename T> struct task {
  using promise_type = detail::promise<T>;
  std::coroutine_handle<promise_type> handle;

  task() noexcept : handle{} {}
  explicit task(std::coroutine_handle<promise_type> h) : handle(h) {}
  task(task &&o) noexcept : handle(std::exchange(o.handle, {})) {}
  task &operator=(task &&o) noexcept {
    if (this != &o) {
      if (handle)
        handle.destroy();
      handle = std::exchange(o.handle, {});
    }
    return *this;
  }
  ~task() {
    if (handle)
      handle.destroy();
  }

  bool await_ready() noexcept { return false; }
  void await_suspend(std::coroutine_handle<> caller) {
    handle.promise().cont = caller;
    handle.resume();
  }
  T await_resume() {
    if (handle.promise().excp)
      std::rethrow_exception(handle.promise().excp);
    if constexpr (!std::is_void_v<T>)
      return std::move(*handle.promise().value);
  }
};

namespace detail {
template <typename T> task<T> promise<T>::get_return_object() {
  return task<T>{std::coroutine_handle<promise<T>>::from_promise(*this)};
}
inline task<void> promise<void>::get_return_object() {
  return task<void>{std::coroutine_handle<promise<void>>::from_promise(*this)};
}
} // namespace detail

// ── io_context ───────────────────────────────────────────────────────────────

struct io_context {
  io_context();
  ~io_context();

  void run();
  void post(std::coroutine_handle<> h);

  // Register fd for a one-shot epoll event; resume h when it fires.
  void watch(int fd, uint32_t events, std::coroutine_handle<> h);

private:
  int epfd_;
  std::deque<std::coroutine_handle<>> ready_;
};

// ── this_coro::executor ──────────────────────────────────────────────────────

namespace this_coro {

struct executor_t {};
constexpr executor_t executor{};

struct executor_awaiter {
  io_context *ctx;
  bool await_ready() noexcept { return false; }
  bool await_suspend(std::coroutine_handle<>) noexcept { return false; }
  io_context *await_resume() noexcept { return ctx; }
};

} // namespace this_coro

// Defined in posix_echo.cpp — reads the context from a thread-local.
this_coro::executor_awaiter operator co_await(this_coro::executor_t) noexcept;

// ── tcp
// ───────────────────────────────────────────────────────────────────────

namespace tcp {

struct socket {
  explicit socket(io_context &ctx, int fd = -1);
  socket(socket &&) noexcept;
  socket &operator=(socket &&) noexcept;
  ~socket();

  task<std::size_t> async_read_some(char *buf, std::size_t buf_size);
  task<void> async_write(const char *buf, std::size_t n);

private:
  io_context &ctx_;
  int fd_;
};

struct acceptor {
  acceptor(io_context &ctx, unsigned short port);
  ~acceptor();

  task<socket> async_accept();
  io_context &get_executor() { return ctx_; }

private:
  io_context &ctx_;
  int fd_;
};

} // namespace tcp

// ── steady_timer
// ──────────────────────────────────────────────────────────────

struct steady_timer {
  explicit steady_timer(io_context *ctx);
  ~steady_timer();

  void expires_at(std::chrono::steady_clock::time_point tp);
  task<void> async_wait();

private:
  io_context *ctx_;
  int tfd_;
};

// ── co_spawn
// ──────────────────────────────────────────────────────────────────

struct detached_t {};
constexpr detached_t detached{};

void co_spawn(io_context &ctx, task<void> t, detached_t);

// ── operator&& ───────────────────────────────────────────────────────────────
// Runs left and right concurrently; resumes the parent when one throws or
// both complete.  Mirrors boost::asio::experimental::awaitable_operators::&&.

struct and_awaitable {
  task<void> left, right;
  task<void> combined; // kept alive so await_resume can read its exception

  bool await_ready() noexcept { return false; }
  void await_suspend(std::coroutine_handle<> caller);
  void await_resume();
};

and_awaitable operator&&(task<void> l, task<void> r);

} // namespace posix_echo
