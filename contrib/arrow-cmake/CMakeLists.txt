set (CMAKE_CXX_STANDARD 20)

set(_Arrow_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/arrow/cpp")
set(_Arrow_BINARY_DIR "${TiFlash_BINARY_DIR}/contrib/arrow")

# Build only static libarrow, no other Arrow ecosystem libraries.
# Avoid Arrow's ExternalProject dependency downloads in our build.
set(ARROW_DEPENDENCY_SOURCE "SYSTEM" CACHE STRING "" FORCE)
set(ARROW_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_RUNTIME_SIMD_LEVEL "NONE" CACHE STRING "" FORCE)
set(ARROW_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)
set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)

set(ARROW_ACERO OFF CACHE BOOL "" FORCE)
set(ARROW_COMPUTE OFF CACHE BOOL "" FORCE)
set(ARROW_CSV OFF CACHE BOOL "" FORCE)
set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
set(ARROW_FLIGHT OFF CACHE BOOL "" FORCE)
set(ARROW_GANDIVA OFF CACHE BOOL "" FORCE)
set(ARROW_JSON OFF CACHE BOOL "" FORCE)
set(ARROW_ORC OFF CACHE BOOL "" FORCE)
set(ARROW_PARQUET OFF CACHE BOOL "" FORCE)
set(ARROW_PLASMA OFF CACHE BOOL "" FORCE)
set(ARROW_SUBSTRAIT OFF CACHE BOOL "" FORCE)

# Avoid building Arrow allocators (and their extra dependencies) for now.
set(ARROW_JEMALLOC OFF CACHE BOOL "" FORCE)
set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)

# Avoid pulling in extra compression libraries for now.
set(ARROW_WITH_BROTLI OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_BZ2 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_SNAPPY OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(ARROW_WITH_ZSTD OFF CACHE BOOL "" FORCE)

add_subdirectory("${_Arrow_SOURCE_DIR}" "${_Arrow_BINARY_DIR}" EXCLUDE_FROM_ALL)

if (TARGET arrow_static)
    set(_ARROW_IMPL_TARGET arrow_static)
elseif (TARGET Arrow::arrow_static)
    set(_ARROW_IMPL_TARGET Arrow::arrow_static)
elseif (TARGET arrow)
    set(_ARROW_IMPL_TARGET arrow)
elseif (TARGET Arrow::arrow)
    set(_ARROW_IMPL_TARGET Arrow::arrow)
else ()
    message(FATAL_ERROR "Arrow target is missing after building contrib/arrow")
endif ()

add_library(tiflash_arrow INTERFACE)
target_link_libraries(tiflash_arrow INTERFACE ${_ARROW_IMPL_TARGET})

target_no_warning(${_ARROW_IMPL_TARGET} deprecated-declarations)
target_no_warning(${_ARROW_IMPL_TARGET} unused-but-set-variable)
target_no_warning(${_ARROW_IMPL_TARGET} implicit-const-int-float-conversion)
target_no_warning(${_ARROW_IMPL_TARGET} non-c-typedef-for-linkage)
target_no_warning(${_ARROW_IMPL_TARGET} unused-function)
