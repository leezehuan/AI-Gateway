#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build/wsl}"

if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi

GATEWAY_BIN="${BUILD_DIR}/bin/AiGateway"
if [[ ! -x "${GATEWAY_BIN}" ]]; then
    echo "AiGateway is not built. Run ./build.sh first." >&2
    exit 1
fi

exec "${GATEWAY_BIN}"
