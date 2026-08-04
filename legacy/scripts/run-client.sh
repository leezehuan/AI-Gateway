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

SERVER_HOST="${1:-${CHAT_CLIENT_HOST:-127.0.0.1}}"
SERVER_PORT="${2:-${CHAT_SERVER_PORT:-6000}}"
CLIENT_BIN="${BUILD_DIR}/bin/ChatClient"

if [[ ! -x "${CLIENT_BIN}" ]]; then
    echo "ChatClient is not built. Run ./build.sh first." >&2
    exit 1
fi

exec "${CLIENT_BIN}" "${SERVER_HOST}" "${SERVER_PORT}"
