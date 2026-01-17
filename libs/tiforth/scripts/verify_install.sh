#!/usr/bin/env bash
set -euo pipefail

TIFORTH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-"${TIFORTH_DIR}/build-debug"}"

if [[ ! -d "${BUILD_DIR}" ]]; then
  echo "error: build dir not found: ${BUILD_DIR}" 1>&2
  exit 1
fi

PREFIX="$(mktemp -d "${TMPDIR:-/tmp}/tiforth-install-XXXXXX")"
CONSUMER_DIR="$(mktemp -d "${TMPDIR:-/tmp}/tiforth-consumer-XXXXXX")"

cleanup() {
  rm -rf "${PREFIX}" "${CONSUMER_DIR}"
}
trap cleanup EXIT

cmake --build "${BUILD_DIR}"
cmake --install "${BUILD_DIR}" --prefix "${PREFIX}"

# Ensure internal-only headers are not installed, while public dependencies remain.
test ! -f "${PREFIX}/include/tiforth/detail/arrow_compute.h"
test ! -f "${PREFIX}/include/tiforth/detail/expr_compiler.h"
test -f "${PREFIX}/include/tiforth/detail/tidb_collation_lut.h"

cat >"${CONSUMER_DIR}/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.23)
project(tiforth_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(tiforth CONFIG REQUIRED)

add_executable(tiforth_consumer main.cpp)
target_link_libraries(tiforth_consumer PRIVATE tiforth::tiforth)
EOF

cat >"${CONSUMER_DIR}/main.cpp" <<'EOF'
#include <arrow/result.h>
#include <tiforth/engine.h>

int main() {
  tiforth::EngineOptions opts;
  auto engine = tiforth::Engine::Create(opts);
  return engine.ok() ? 0 : 1;
}
EOF

ARROW_PREFIX=""
if [[ -d "${BUILD_DIR}/_deps/arrow-build/src/arrow" ]]; then
  ARROW_PREFIX="${BUILD_DIR}/_deps/arrow-build/src/arrow"
fi

cmake -S "${CONSUMER_DIR}" -B "${CONSUMER_DIR}/build" \
  -DCMAKE_PREFIX_PATH="${PREFIX}${ARROW_PREFIX:+;${ARROW_PREFIX}}"
cmake --build "${CONSUMER_DIR}/build"
