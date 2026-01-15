include_guard(GLOBAL)

# TiForth Arrow integration
#
# Goals:
# - Support both "system" and "bundled" Arrow (bundled is Arrow 22.x only).
# - Link only Arrow core + Arrow compute (Arrow 22 split compute into a separate library).
# - Keep Arrow surface minimal (no IPC/dataset/parquet/flight/...) to reduce build time and dependency fanout.
# - Make Arrow a PUBLIC dependency of `tiforth`, so consumers only link `tiforth::tiforth`.
#
# Notes:
# - When TiForth is included as a subproject, the parent project's global compile flags can include
#   warnings-as-errors or thread-safety analysis, which may cause Arrow's build to fail.
#   We temporarily relax a small set of warnings *only for the Arrow sub-build*.
# - When Arrow is built as a subproject, its CMake targets don't always export all build-tree include
#   directories (notably generated headers). We compute `TIFORTH_ARROW_INCLUDE_DIRS` so `tiforth`
#   can expose them as BUILD_INTERFACE includes.

set(_tiforth_allowed_arrow_providers bundled system)
if(NOT TIFORTH_ARROW_PROVIDER IN_LIST _tiforth_allowed_arrow_providers)
  message(
    FATAL_ERROR
    "Invalid TIFORTH_ARROW_PROVIDER='${TIFORTH_ARROW_PROVIDER}'. Allowed: bundled|system")
endif()

set(_tiforth_allowed_arrow_linkages shared static)
if(NOT TIFORTH_ARROW_LINKAGE IN_LIST _tiforth_allowed_arrow_linkages)
  message(
    FATAL_ERROR
    "Invalid TIFORTH_ARROW_LINKAGE='${TIFORTH_ARROW_LINKAGE}'. Allowed: shared|static")
endif()

set(TIFORTH_ARROW_CORE_TARGET "")
set(TIFORTH_ARROW_COMPUTE_TARGET "")
set(TIFORTH_ARROW_TESTING_TARGET "")
set(TIFORTH_ARROW_INCLUDE_DIRS "")

function(_tiforth_try_select_target OUT_VAR)
  foreach(_tiforth_candidate IN ITEMS ${ARGN})
    if(TARGET "${_tiforth_candidate}")
      set(${OUT_VAR} "${_tiforth_candidate}" PARENT_SCOPE)
      return()
    endif()
  endforeach()
  set(${OUT_VAR} "" PARENT_SCOPE)
endfunction()

# If the parent project already provides Arrow targets, reuse them and avoid
# building/finding a second copy.
#
# This is intentionally checked before TIFORTH_ARROW_PROVIDER, because mixing
# multiple Arrow builds in one process is fragile.
set(_tiforth_parent_arrow_core "")
set(_tiforth_parent_arrow_compute "")
if(TIFORTH_ARROW_LINKAGE STREQUAL "shared")
  _tiforth_try_select_target(_tiforth_parent_arrow_core arrow Arrow::arrow Arrow::arrow_shared arrow_shared)
  _tiforth_try_select_target(
    _tiforth_parent_arrow_compute
    arrow_compute
    ArrowCompute::arrow_compute
    ArrowCompute::arrow_compute_shared
    arrow_compute_shared)
else()
  _tiforth_try_select_target(_tiforth_parent_arrow_core arrow Arrow::arrow Arrow::arrow_static arrow_static)
  _tiforth_try_select_target(
    _tiforth_parent_arrow_compute
    arrow_compute
    ArrowCompute::arrow_compute
    ArrowCompute::arrow_compute_static
    arrow_compute_static)
endif()

if(_tiforth_parent_arrow_core)
  if(TIFORTH_BUILD_TESTS)
    message(
      FATAL_ERROR
      "TIFORTH_BUILD_TESTS must be OFF when using parent-provided Arrow targets")
  endif()

  if(NOT _tiforth_parent_arrow_compute)
    message(
      FATAL_ERROR
      "Parent-provided Arrow target '${_tiforth_parent_arrow_core}' is missing Arrow compute; "
      "provide a compute target (arrow_compute / ArrowCompute::arrow_compute*)")
  endif()

  message(STATUS "Using parent-provided Arrow core target: '${_tiforth_parent_arrow_core}'")
  message(STATUS "Using parent-provided Arrow compute target: '${_tiforth_parent_arrow_compute}'")

  set(TIFORTH_ARROW_CORE_TARGET "${_tiforth_parent_arrow_core}")
  set(TIFORTH_ARROW_COMPUTE_TARGET "${_tiforth_parent_arrow_compute}")
  set(TIFORTH_ARROW_TESTING_TARGET "")
  set(TIFORTH_ARROW_INCLUDE_DIRS "")
  return()
endif()

if(TIFORTH_ARROW_PROVIDER STREQUAL "system")
  message(STATUS "Using system Arrow")

  set(Arrow_FIND_QUIETLY 0)
  find_package(Arrow CONFIG REQUIRED)
  find_package(ArrowCompute CONFIG REQUIRED)

  if(TIFORTH_ARROW_LINKAGE STREQUAL "shared")
    if(TARGET Arrow::arrow_shared)
      set(TIFORTH_ARROW_CORE_TARGET Arrow::arrow_shared)
    elseif(TARGET Arrow::arrow)
      set(TIFORTH_ARROW_CORE_TARGET Arrow::arrow)
    else()
      message(FATAL_ERROR "System Arrow is missing a shared library target (need Arrow::arrow_shared)")
    endif()

    if(TARGET ArrowCompute::arrow_compute_shared)
      set(TIFORTH_ARROW_COMPUTE_TARGET ArrowCompute::arrow_compute_shared)
    elseif(TARGET ArrowCompute::arrow_compute)
      set(TIFORTH_ARROW_COMPUTE_TARGET ArrowCompute::arrow_compute)
    else()
      message(
        FATAL_ERROR
        "System ArrowCompute is missing a usable target (need ArrowCompute::arrow_compute_shared or ArrowCompute::arrow_compute)"
      )
    endif()
  else()
    if(TARGET Arrow::arrow_static)
      set(TIFORTH_ARROW_CORE_TARGET Arrow::arrow_static)
    else()
      message(FATAL_ERROR "System Arrow is missing a static library target (need Arrow::arrow_static)")
    endif()

    if(TARGET ArrowCompute::arrow_compute_static)
      set(TIFORTH_ARROW_COMPUTE_TARGET ArrowCompute::arrow_compute_static)
    elseif(TARGET ArrowCompute::arrow_compute)
      set(TIFORTH_ARROW_COMPUTE_TARGET ArrowCompute::arrow_compute)
    else()
      message(
        FATAL_ERROR
        "System ArrowCompute is missing a usable target (need ArrowCompute::arrow_compute_static or ArrowCompute::arrow_compute)"
      )
    endif()
  endif()

  if(TIFORTH_BUILD_TESTS)
    find_package(ArrowTesting CONFIG REQUIRED)

    if(TIFORTH_ARROW_LINKAGE STREQUAL "shared")
      if(TARGET ArrowTesting::arrow_testing_shared)
        set(TIFORTH_ARROW_TESTING_TARGET ArrowTesting::arrow_testing_shared)
      elseif(TARGET ArrowTesting::arrow_testing)
        set(TIFORTH_ARROW_TESTING_TARGET ArrowTesting::arrow_testing)
      elseif(TARGET arrow_testing_shared)
        set(TIFORTH_ARROW_TESTING_TARGET arrow_testing_shared)
      elseif(TARGET arrow_testing)
        set(TIFORTH_ARROW_TESTING_TARGET arrow_testing)
      else()
        message(
          FATAL_ERROR
          "System ArrowTesting is missing a usable target (need ArrowTesting::arrow_testing_shared)"
        )
      endif()
    else()
      if(TARGET ArrowTesting::arrow_testing_static)
        set(TIFORTH_ARROW_TESTING_TARGET ArrowTesting::arrow_testing_static)
      elseif(TARGET ArrowTesting::arrow_testing)
        set(TIFORTH_ARROW_TESTING_TARGET ArrowTesting::arrow_testing)
      elseif(TARGET arrow_testing_static)
        set(TIFORTH_ARROW_TESTING_TARGET arrow_testing_static)
      elseif(TARGET arrow_testing)
        set(TIFORTH_ARROW_TESTING_TARGET arrow_testing)
      else()
        message(
          FATAL_ERROR
          "System ArrowTesting is missing a usable target (need ArrowTesting::arrow_testing_static)"
        )
      endif()
    endif()
  endif()

  set(TIFORTH_ARROW_INCLUDE_DIRS "")
  return()
endif()

message(STATUS "Using bundled Arrow (FetchContent, Arrow 22.0.0)")

include(FetchContent)

FetchContent_Declare(
  arrow
  URL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-22.0.0.tar.gz"
  URL_HASH "SHA256=8a95e6c7b9bec2bc0058feb73efe38ad6cfd49a0c7094db29b37ecaa8ab16051"
  SOURCE_SUBDIR cpp)

# Build only Arrow core + compute.
# TiForth tests are separate and must not require Arrow's own test targets.
# Avoid Arrow's ExternalProject dependency downloads.
if(TIFORTH_BUILD_TESTS)
  set(ARROW_DEPENDENCY_SOURCE "AUTO" CACHE STRING "" FORCE)
else()
  set(ARROW_DEPENDENCY_SOURCE "SYSTEM" CACHE STRING "" FORCE)
endif()
set(ARROW_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_RUNTIME_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)

if(TIFORTH_ARROW_LINKAGE STREQUAL "shared")
  set(ARROW_BUILD_SHARED ON CACHE BOOL "" FORCE)
  set(ARROW_BUILD_STATIC OFF CACHE BOOL "" FORCE)
else()
  set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
  set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
endif()

set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)

set(ARROW_COMPUTE ON CACHE BOOL "" FORCE)
if(TIFORTH_BUILD_TESTS)
  set(ARROW_TESTING ON CACHE BOOL "" FORCE)
else()
  set(ARROW_TESTING OFF CACHE BOOL "" FORCE)
endif()

set(ARROW_ACERO OFF CACHE BOOL "" FORCE)
set(ARROW_CSV OFF CACHE BOOL "" FORCE)
set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
set(ARROW_FILESYSTEM OFF CACHE BOOL "" FORCE)
set(ARROW_FLIGHT OFF CACHE BOOL "" FORCE)
set(ARROW_GANDIVA OFF CACHE BOOL "" FORCE)
if(TIFORTH_BUILD_TESTS)
  set(ARROW_IPC ON CACHE BOOL "" FORCE)
  set(ARROW_JSON ON CACHE BOOL "" FORCE)
else()
  set(ARROW_IPC OFF CACHE BOOL "" FORCE)
  set(ARROW_JSON OFF CACHE BOOL "" FORCE)
endif()
set(ARROW_ORC OFF CACHE BOOL "" FORCE)
set(ARROW_PARQUET OFF CACHE BOOL "" FORCE)
set(ARROW_PLASMA OFF CACHE BOOL "" FORCE)
set(ARROW_SUBSTRAIT OFF CACHE BOOL "" FORCE)

set(ARROW_WITH_UTF8PROC OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_RE2 OFF CACHE BOOL "" FORCE)

set(ARROW_JEMALLOC OFF CACHE BOOL "" FORCE)
set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)

set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_BZ2 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_SNAPPY OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_ZSTD OFF CACHE BOOL "" FORCE)

include(CheckCXXCompilerFlag)

set(_tiforth_saved_cxx_flags "${CMAKE_CXX_FLAGS}")
set(_tiforth_arrow_extra_cxx_flags "")

function(_tiforth_maybe_append_cxx_flag FLAG)
  string(REPLACE "-" "_" _tiforth_flag_var "${FLAG}")
  string(REPLACE "+" "x" _tiforth_flag_var "${_tiforth_flag_var}")
  set(_tiforth_check_var "TIFORTH_SUPPORTS_${_tiforth_flag_var}")

  check_cxx_compiler_flag("${FLAG}" "${_tiforth_check_var}")
  if(${_tiforth_check_var})
    set(_tiforth_arrow_extra_cxx_flags "${_tiforth_arrow_extra_cxx_flags} ${FLAG}" PARENT_SCOPE)
  endif()
endfunction()

_tiforth_maybe_append_cxx_flag("-Wno-thread-safety-analysis")
_tiforth_maybe_append_cxx_flag("-Wno-non-virtual-dtor")
_tiforth_maybe_append_cxx_flag("-Wno-deprecated-declarations")
_tiforth_maybe_append_cxx_flag("-Wno-unused-but-set-variable")
_tiforth_maybe_append_cxx_flag("-Wno-implicit-const-int-float-conversion")
_tiforth_maybe_append_cxx_flag("-Wno-non-c-typedef-for-linkage")
_tiforth_maybe_append_cxx_flag("-Wno-unused-function")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${_tiforth_arrow_extra_cxx_flags}")

FetchContent_MakeAvailable(arrow)

set(CMAKE_CXX_FLAGS "${_tiforth_saved_cxx_flags}")

set(_tiforth_arrow_cpp_source_dir "${arrow_SOURCE_DIR}")
if(EXISTS "${_tiforth_arrow_cpp_source_dir}/cpp/src/arrow/api.h")
  set(_tiforth_arrow_cpp_source_dir "${_tiforth_arrow_cpp_source_dir}/cpp")
elseif(NOT EXISTS "${_tiforth_arrow_cpp_source_dir}/src/arrow/api.h")
  message(
    FATAL_ERROR
    "Unexpected Arrow source layout at '${arrow_SOURCE_DIR}': can't locate cpp/src/arrow/api.h")
endif()

set(TIFORTH_ARROW_INCLUDE_DIRS
    "${_tiforth_arrow_cpp_source_dir}/src"
    "${_tiforth_arrow_cpp_source_dir}/src/generated"
    "${arrow_BINARY_DIR}/src")

function(_tiforth_select_target OUT_VAR)
  foreach(_tiforth_candidate IN LISTS ARGN)
    if(TARGET "${_tiforth_candidate}")
      set(${OUT_VAR} "${_tiforth_candidate}" PARENT_SCOPE)
      return()
    endif()
  endforeach()
  message(FATAL_ERROR "Failed to select a required CMake target")
endfunction()

if(TIFORTH_ARROW_LINKAGE STREQUAL "shared")
  _tiforth_select_target(TIFORTH_ARROW_CORE_TARGET arrow_shared Arrow::arrow_shared)
  _tiforth_select_target(TIFORTH_ARROW_COMPUTE_TARGET arrow_compute_shared ArrowCompute::arrow_compute_shared)
  if(TIFORTH_BUILD_TESTS)
    _tiforth_select_target(TIFORTH_ARROW_TESTING_TARGET arrow_testing_shared ArrowTesting::arrow_testing_shared)
  endif()
else()
  _tiforth_select_target(TIFORTH_ARROW_CORE_TARGET arrow_static Arrow::arrow_static)
  _tiforth_select_target(TIFORTH_ARROW_COMPUTE_TARGET arrow_compute_static ArrowCompute::arrow_compute_static)
  if(TIFORTH_BUILD_TESTS)
    _tiforth_select_target(TIFORTH_ARROW_TESTING_TARGET arrow_testing_static ArrowTesting::arrow_testing_static)
  endif()
endif()
