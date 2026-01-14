cmake_minimum_required(VERSION 3.21)

project(tiforth VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

option(TIFORTH_USE_INTERNAL_ARROW "Use internal Arrow library, mostly for TiForth developers" OFF)
option(TIFORTH_BUILD_TESTS "Build the TiForth googletest unit tests" OFF)

include(cmake/find/arrow.cmake)
include(cmake/testing/testing.cmake)

add_subdirectory("${CMAKE_SOURCE_DIR}/src/tiforth")

if(TIFORTH_BUILD_TESTS)
  add_subdirectory("${CMAKE_SOURCE_DIR}/tests")
endif()
