#pragma once

#include "posix_echo.h"
#include <chrono>

namespace posix_echo {

using time_point = std::chrono::steady_clock::time_point;

task<void> echo(tcp::socket& sock, time_point& deadline);
task<void> watchdog(time_point& deadline);
task<void> handle_connection(tcp::socket sock);
task<void> listen(tcp::acceptor& acceptor);

void run(unsigned short port);

} // namespace posix_echo
