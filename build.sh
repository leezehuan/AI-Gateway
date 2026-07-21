#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build/wsl}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_JOBS="${BUILD_JOBS:-$(nproc)}"

cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
cmake --build "${BUILD_DIR}" --parallel "${BUILD_JOBS}"

echo "Build complete: ${BUILD_DIR}/bin"
