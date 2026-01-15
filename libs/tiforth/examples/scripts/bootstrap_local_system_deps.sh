#!/usr/bin/env bash
set -euo pipefail

BUILD_TYPE="${BUILD_TYPE:-Release}"
GENERATOR="${GENERATOR:-Ninja}"

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TIFORTH_DIR="$(cd "${EXAMPLES_DIR}/.." && pwd)"

BUILD_DIR="${EXAMPLES_DIR}/_build"
INSTALL_DIR="${EXAMPLES_DIR}/_install"

ARROW_BUILD_DIR="${BUILD_DIR}/arrow"
ARROW_INSTALL_DIR="${INSTALL_DIR}/arrow"

mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}"

arrow_extra_cmake_args=()
if [[ -n "${FETCHCONTENT_SOURCE_DIR_ARROW:-}" ]]; then
  arrow_extra_cmake_args+=("-DFETCHCONTENT_SOURCE_DIR_ARROW=${FETCHCONTENT_SOURCE_DIR_ARROW}")
fi

echo "[tiforth/examples] Building Arrow (system prefix) -> ${ARROW_INSTALL_DIR}"
cmake -S "${EXAMPLES_DIR}/deps/arrow" -B "${ARROW_BUILD_DIR}" -G "${GENERATOR}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DCMAKE_INSTALL_PREFIX="${ARROW_INSTALL_DIR}" \
  "${arrow_extra_cmake_args[@]}"
cmake --build "${ARROW_BUILD_DIR}"
cmake --install "${ARROW_BUILD_DIR}"

build_tiforth() {
  local tiforth_linkage="$1"
  local arrow_linkage="$2"

  local build_dir="${BUILD_DIR}/tiforth-${tiforth_linkage}-arrow-${arrow_linkage}"
  local install_dir="${INSTALL_DIR}/tiforth-${tiforth_linkage}-arrow-${arrow_linkage}"

  echo "[tiforth/examples] Building TiForth (${tiforth_linkage}, Arrow ${arrow_linkage}) -> ${install_dir}"

  cmake -S "${TIFORTH_DIR}" -B "${build_dir}" -G "${GENERATOR}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DCMAKE_INSTALL_PREFIX="${install_dir}" \
    -DCMAKE_PREFIX_PATH="${ARROW_INSTALL_DIR}" \
    -DTIFORTH_ARROW_PROVIDER=system \
    -DTIFORTH_ARROW_LINKAGE="${arrow_linkage}" \
    -DTIFORTH_LIBRARY_LINKAGE="${tiforth_linkage}" \
    -DTIFORTH_BUILD_TESTS=OFF
  cmake --build "${build_dir}"
  cmake --install "${build_dir}"

  echo "  CMAKE_PREFIX_PATH=\"${ARROW_INSTALL_DIR};${install_dir}\""
}

build_tiforth shared shared
build_tiforth static shared
build_tiforth shared static
build_tiforth static static
