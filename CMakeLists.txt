cmake_minimum_required(VERSION 3.20)
project(boost_asio_app CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Build type ───────────────────────────────────────────────────────────────
# Default to Release when not specified.
# To build with full debug info:
#   cmake -B build -DCMAKE_BUILD_TYPE=Debug
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
endif()
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# ── Submodule init target ────────────────────────────────────────────────────
# Run once (or whenever submodules are missing) with:
#   cmake --build build --target update_submodules
find_package(Git REQUIRED)

add_custom_target(update_submodules
    COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Initialising / updating git submodules"
)

# ── Boost (header-only, from git submodules) ─────────────────────────────────
# All Boost components are provided as submodules under third_party/.
# boost::system is header-only since Boost 1.69 (error_code etc. are inline).
set(BOOST_SUBMODULE_DIRS
    third_party/boost/asio/include
    third_party/boost/config/include
    third_party/boost/system/include
    third_party/boost/assert/include
    third_party/boost/core/include
    third_party/boost/throw_exception/include
    third_party/boost/variant2/include
    third_party/boost/mp11/include
)

# ── Executable ───────────────────────────────────────────────────────────────
add_executable(${PROJECT_NAME}
    src/main.cpp
    src/boost_echo.cpp
    src/posix_echo.cpp
    src/posix_echo_server.cpp
)

foreach(dir IN LISTS BOOST_SUBMODULE_DIRS)
    target_include_directories(${PROJECT_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/${dir}
    )
endforeach()

# Asio requires a threading backend on Linux.
find_package(Threads REQUIRED)
target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
