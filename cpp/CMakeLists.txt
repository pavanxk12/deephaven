cmake_minimum_required(VERSION 3.20)
project(algod CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native")
add_compile_options(-Wall -Wextra -Wno-unused-parameter)

option(ALGO_TSAN "Build with ThreadSanitizer" OFF)
if(ALGO_TSAN)
  add_compile_options(-fsanitize=thread -g)
  add_link_options(-fsanitize=thread)
endif()

# core knows nothing of the others; data uses core (the import rules of the Python package, enforced by link deps)
add_library(algo_core STATIC core/ini.cpp core/tz.cpp core/market.cpp core/mirror_ring.cpp)
target_include_directories(algo_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
if(UNIX AND NOT APPLE)
  target_link_libraries(algo_core PUBLIC rt pthread)
endif()

add_library(algo_data STATIC data/ltt_parser.cpp data/token_index.cpp data/candle_aggregator.cpp)
target_link_libraries(algo_data PUBLIC algo_core)

add_executable(candles_replay tools/candles_replay.cpp)
target_link_libraries(candles_replay PRIVATE algo_data)

enable_testing()
add_executable(test_core tests/test_core.cpp)
target_link_libraries(test_core PRIVATE algo_data)
add_test(NAME test_core COMMAND test_core WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
add_test(NAME candles_parity
         COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/tests/parity/candles_parity.py --bin $<TARGET_FILE:candles_replay>
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..)
