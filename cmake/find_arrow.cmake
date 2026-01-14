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

# Apache Arrow (C++), we only use libarrow.
#
# - Set ENABLE_ARROW=ON to enable Arrow integration.
# - By default, we prefer bundled (contrib/arrow) unless UNBUNDLED is set.
# - When using system Arrow, prefer Arrow::arrow_static for static linking.

option(USE_INTERNAL_ARROW_LIBRARY "Set to FALSE to use system Arrow library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/arrow/cpp/CMakeLists.txt")
    if(USE_INTERNAL_ARROW_LIBRARY)
        message(WARNING "submodule contrib/arrow is missing. to fix try run: \n git submodule update --init --recursive contrib/arrow")
        message(WARNING "Can't use internal Arrow")
        set(USE_INTERNAL_ARROW_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_ARROW_LIBRARY 1)
endif()

if(NOT USE_INTERNAL_ARROW_LIBRARY)
    # Prefer the Arrow CMake package to get the correct link options.
    find_package(Arrow CONFIG QUIET)
    if(TARGET Arrow::arrow_static)
        set(ARROW_LIBRARY Arrow::arrow_static)
        set(EXTERNAL_ARROW_LIBRARY_FOUND 1)
    elseif(TARGET Arrow::arrow)
        # Some distributions only export Arrow::arrow (usually shared).
        message(WARNING "Found Arrow but missing Arrow::arrow_static; static linking is required, try USE_INTERNAL_ARROW_LIBRARY=ON")
        set(EXTERNAL_ARROW_LIBRARY_FOUND 0)
    else()
        set(EXTERNAL_ARROW_LIBRARY_FOUND 0)
    endif()
endif()

if(NOT EXTERNAL_ARROW_LIBRARY_FOUND)
    if(NOT MISSING_INTERNAL_ARROW_LIBRARY)
        set(USE_INTERNAL_ARROW_LIBRARY 1)
        # Built by contrib/arrow-cmake
        set(ARROW_LIBRARY tiflash_arrow)
    else()
        message(FATAL_ERROR "Can't find Apache Arrow library (enable contrib/arrow submodule or install system Arrow with Arrow::arrow_static)")
    endif()
endif()

set(ARROW_FOUND TRUE)
set(Arrow_FOUND TRUE)

message(STATUS "Using Arrow: ${USE_INTERNAL_ARROW_LIBRARY} : ${ARROW_LIBRARY}")
