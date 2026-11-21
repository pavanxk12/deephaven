# Copy this file next to your strategy .cpp and change the two names.
#   cmake -B build -DALGO_SDK_DIR=/path/to/deephaven/cpp/sdk && cmake --build build
cmake_minimum_required(VERSION 3.18)
project(NiftySmaCross CXX)

if(NOT DEFINED ALGO_SDK_DIR)
  message(FATAL_ERROR "Set -DALGO_SDK_DIR=/path/to/deephaven/cpp/sdk")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_library(NiftySmaCross SHARED NiftySmaCross.cpp)
target_include_directories(NiftySmaCross PRIVATE ${ALGO_SDK_DIR})
target_compile_options(NiftySmaCross PRIVATE
  -fPIC -fvisibility=hidden -fvisibility-inlines-hidden
  -Wall -Wextra
  $<$<CONFIG:Release>:-O3 -march=native>)
set_target_properties(NiftySmaCross PROPERTIES PREFIX "lib" SUFFIX ".so")
if(NOT APPLE)
  target_link_options(NiftySmaCross PRIVATE -Wl,--no-undefined -Wl,-z,defs)   # an unresolved symbol fails the build, not the load
endif()
# No -flto, and no algod library to link: every service arrives as a function pointer.
