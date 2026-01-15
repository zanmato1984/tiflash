cmake_minimum_required(VERSION 3.23)

project(noarrow__tiforth_system_shared LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(tiforth CONFIG REQUIRED)

get_target_property(_tiforth_type tiforth::tiforth TYPE)
if(NOT _tiforth_type STREQUAL "SHARED_LIBRARY")
  message(FATAL_ERROR "Expected tiforth::tiforth to be SHARED_LIBRARY, got '${_tiforth_type}'")
endif()

add_executable(tiforth_example ../../src/noarrow_main.cc)
target_link_libraries(tiforth_example PRIVATE tiforth::tiforth)

