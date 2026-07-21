#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! grep -qi microsoft /proc/version 2>/dev/null; then
    echo "Notice: WSL was not detected; continuing with the Ubuntu setup."
fi

if ! command -v apt-get >/dev/null 2>&1; then
    echo "This setup script supports Ubuntu/Debian systems with apt." >&2
    exit 1
fi

sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    libboost-all-dev \
    libhiredis-dev \
    libmariadb-dev \
    mariadb-client \
    mariadb-server \
    redis-server

if [[ ! -f /usr/local/include/muduo/net/TcpServer.h ]] || \
   [[ ! -f /usr/local/lib/libmuduo_net.a ]] || \
   [[ ! -f /usr/local/lib/libmuduo_base.a ]]; then
    cat >&2 <<'EOF'
Muduo was not found under /usr/local. Install the Muduo version used by the
course, then rerun this script. The project requires these files:
  /usr/local/include/muduo/net/TcpServer.h
  /usr/local/lib/libmuduo_net.a
  /usr/local/lib/libmuduo_base.a
EOF
    exit 1
fi

start_service() {
    local systemd_name="$1"
    local sysv_name="$2"

    if [[ "$(ps -p 1 -o comm= 2>/dev/null)" == "systemd" ]]; then
        sudo systemctl enable --now "${systemd_name}"
    else
        sudo service "${sysv_name}" start
    fi
}

start_service mariadb mariadb
start_service redis-server redis-server

sudo mariadb --protocol=socket <<'SQL'
CREATE DATABASE IF NOT EXISTS chat CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'chat'@'localhost' IDENTIFIED BY 'chat';
CREATE USER IF NOT EXISTS 'chat'@'127.0.0.1' IDENTIFIED BY 'chat';
GRANT ALL PRIVILEGES ON chat.* TO 'chat'@'localhost';
GRANT ALL PRIVILEGES ON chat.* TO 'chat'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL

if ! sudo mariadb --protocol=socket -Nse \
    "SELECT 1 FROM information_schema.tables WHERE table_schema='chat' AND table_name='user'" \
    | grep -q 1; then
    sudo mariadb --protocol=socket chat < "${ROOT_DIR}/chat.sql"
fi

sudo mariadb --protocol=socket chat -e "UPDATE user SET state='offline'"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
    cp "${ROOT_DIR}/.env.example" "${ROOT_DIR}/.env"
fi

"${ROOT_DIR}/build.sh"

echo
echo "WSL setup complete. Start the server with: ./scripts/run-server.sh"
