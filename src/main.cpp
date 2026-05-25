#include "boost_echo.h"
#include "posix_echo_server.h"

#include <iostream>

int main(int argc, char* argv[])
{
    const std::string mode  = argc > 1 ? argv[1] : "boost";
    const unsigned short port = argc > 2 ? static_cast<unsigned short>(std::stoi(argv[2])) : 54321;

    if (mode == "boost")
    {
        std::cout << "Starting boost echo server on port " << port << "\n";
        boost_echo::run(port);
    }
    else if (mode == "posix")
    {
        std::cout << "Starting posix echo server on port " << port << "\n";
        posix_echo::run(port);
    }
    else
    {
        std::cerr << "Usage: " << argv[0] << " [boost|posix] [port]\n";
        return 1;
    }
}
