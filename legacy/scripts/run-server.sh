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

SERVER_IP="${CHAT_SERVER_IP:-0.0.0.0}"
SERVER_PORT="${CHAT_SERVER_PORT:-6000}"
SERVER_BIN="${BUILD_DIR}/bin/ChatServer"

if [[ ! -x "${SERVER_BIN}" ]]; then
    echo "ChatServer is not built. Run ./build.sh first." >&2
    exit 1
fi

exec "${SERVER_BIN}" "${SERVER_IP}" "${SERVER_PORT}"
