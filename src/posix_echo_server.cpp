#include "posix_echo_server.h"

#include "posix_echo.h"
#include <chrono>

namespace posix_echo {

using time_point = std::chrono::steady_clock::time_point;

task<void> echo(tcp::socket &sock, time_point &deadline);
task<void> watchdog(time_point &deadline);
task<void> handle_connection(tcp::socket sock);
task<void> listen(tcp::acceptor &acceptor);

task<void> echo(tcp::socket &sock, time_point &deadline) {
  char data[4096];
  for (;;) {
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    auto n = co_await sock.async_read_some(data, sizeof(data));
    co_await sock.async_write(data, n);
  }
}

task<void> watchdog(time_point &deadline) {
  steady_timer timer(co_await this_coro::executor);
  auto now = std::chrono::steady_clock::now();
  while (deadline > now) {
    timer.expires_at(deadline);
    co_await timer.async_wait();
    now = std::chrono::steady_clock::now();
  }
  throw std::system_error(std::make_error_code(std::errc::timed_out));
}

task<void> handle_connection(tcp::socket sock) {
  time_point deadline{};
  co_await (echo(sock, deadline) && watchdog(deadline));
}

task<void> listen(tcp::acceptor &acceptor) {
  for (;;) {
    co_spawn(acceptor.get_executor(),
             handle_connection(co_await acceptor.async_accept()), detached);
  }
}

void run(unsigned short port) {
  io_context ctx;
  tcp::acceptor acceptor(ctx, port);
  co_spawn(ctx, listen(acceptor), detached);
  ctx.run();
}

} // namespace posix_echo
