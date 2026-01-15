cmake_minimum_required(VERSION 3.23)

project(arrow_system_shared__tiforth_system_static LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(tiforth CONFIG REQUIRED)

get_target_property(_tiforth_type tiforth::tiforth TYPE)
if(NOT _tiforth_type STREQUAL "STATIC_LIBRARY")
  message(FATAL_ERROR "Expected tiforth::tiforth to be STATIC_LIBRARY, got '${_tiforth_type}'")
endif()

if(NOT (TARGET Arrow::arrow_shared OR TARGET Arrow::arrow))
  message(FATAL_ERROR "Expected a shared Arrow target (Arrow::arrow_shared or Arrow::arrow)")
endif()
if(NOT (TARGET ArrowCompute::arrow_compute_shared OR TARGET ArrowCompute::arrow_compute))
  message(
    FATAL_ERROR
    "Expected a shared ArrowCompute target (ArrowCompute::arrow_compute_shared or ArrowCompute::arrow_compute)")
endif()

add_executable(tiforth_example ../../src/witharrow_main.cc)
target_link_libraries(tiforth_example PRIVATE tiforth::tiforth)

