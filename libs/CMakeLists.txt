# Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/libs/CMakeLists.txt
#
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

# Optimized routines under arm
add_subdirectory (libarm-optimized-routines)
add_subdirectory (libcommon)
add_subdirectory (libpocoext)
add_subdirectory (libdaemon)
# Optimized memcpy under x86_64
add_subdirectory (libmemcpy)

if (GLIBC_COMPATIBILITY)
    add_subdirectory (libglibc-compatibility)
endif ()

add_subdirectory (libsymbolization)
add_subdirectory (libprocess_metrics)

if (ENABLE_CLARA AND USE_INTERNAL_LIBCLARA)
    add_subdirectory (libclara-cmake)
endif ()

if (ENABLE_TIFORTH)
    include(FetchContent)

    if (TIFLASH_USE_SYSTEM_TIFORTH)
        find_package(tiforth CONFIG REQUIRED)
    else()
        if (TIFLASH_TIFORTH_LINKAGE STREQUAL "auto")
            if (USE_STATIC_LIBRARIES OR MAKE_STATIC_LIBRARIES)
                set(TIFORTH_LIBRARY_LINKAGE "static" CACHE STRING "TiForth library linkage (shared|static)" FORCE)
            else ()
                set(TIFORTH_LIBRARY_LINKAGE "shared" CACHE STRING "TiForth library linkage (shared|static)" FORCE)
            endif ()
        elseif (TIFLASH_TIFORTH_LINKAGE STREQUAL "shared" OR TIFLASH_TIFORTH_LINKAGE STREQUAL "static")
            set(TIFORTH_LIBRARY_LINKAGE "${TIFLASH_TIFORTH_LINKAGE}"
                CACHE STRING "TiForth library linkage (shared|static)" FORCE)
        else ()
            message (FATAL_ERROR
                "Invalid TIFLASH_TIFORTH_LINKAGE='${TIFLASH_TIFORTH_LINKAGE}'. Allowed: auto|shared|static")
        endif ()

        if (NOT DEFINED TIFORTH_ARROW_PROVIDER)
            set(TIFORTH_ARROW_PROVIDER "bundled" CACHE STRING "Arrow provider (bundled|system)" FORCE)
        endif ()
        if (NOT DEFINED TIFORTH_ARROW_LINKAGE)
            set(TIFORTH_ARROW_LINKAGE "shared" CACHE STRING "Arrow linkage (shared|static)" FORCE)
        endif ()

        # Prefer an in-tree TiForth checkout (including a local symlink), but allow
        # TiForth to live in its own repo when `libs/tiforth` is no longer vendored.
        set(_tiforth_source_dir "${TiFlash_SOURCE_DIR}/libs/tiforth")
        if (EXISTS "${_tiforth_source_dir}/CMakeLists.txt")
            FetchContent_Declare(tiforth SOURCE_DIR "${_tiforth_source_dir}")
        else ()
            set(TIFLASH_TIFORTH_GIT_REPOSITORY "https://github.com/zanmato1984/tiforth.git"
                CACHE STRING "TiForth git repository used when no in-tree TiForth source is present")
            # Keep this pinned; update when bumping the TiForth dependency.
            set(TIFLASH_TIFORTH_GIT_TAG "ff5da889ef440d521aca7b253d58c7b3ee2958da"
                CACHE STRING "TiForth git tag/sha used when no in-tree TiForth source is present")
            FetchContent_Declare(tiforth
                GIT_REPOSITORY "${TIFLASH_TIFORTH_GIT_REPOSITORY}"
                GIT_TAG "${TIFLASH_TIFORTH_GIT_TAG}")
        endif ()
        FetchContent_MakeAvailable(tiforth)
    endif ()
endif ()
