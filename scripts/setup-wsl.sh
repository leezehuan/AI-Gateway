#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi

CHAT_DB_USER="${CHAT_DB_USER:-chat}"
CHAT_DB_NAME="${CHAT_DB_NAME:-chat}"
CHAT_DB_PASSWORD="${CHAT_DB_PASSWORD:-}"

if [[ ! "${CHAT_DB_USER}" =~ ^[A-Za-z0-9_]+$ ]] || \
   [[ ! "${CHAT_DB_NAME}" =~ ^[A-Za-z0-9_]+$ ]]; then
    echo "CHAT_DB_USER and CHAT_DB_NAME may contain only letters, digits, and underscores." >&2
    exit 1
fi

if [[ -z "${CHAT_DB_PASSWORD}" ]]; then
    if [[ -t 0 ]]; then
        read -r -s -p "Password for the legacy ${CHAT_DB_USER} database user: " CHAT_DB_PASSWORD
        echo
    else
        echo "Set CHAT_DB_PASSWORD before running setup-wsl.sh." >&2
        exit 1
    fi
fi

DB_PASSWORD_BASE64="$(printf '%s' "${CHAT_DB_PASSWORD}" | base64 -w0)"

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
    libcurl4-openssl-dev \
    libhiredis-dev \
    libmariadb-dev \
    mariadb-client \
    mariadb-server \
    python3 \
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

sudo mariadb --protocol=socket <<SQL
CREATE DATABASE IF NOT EXISTS \`${CHAT_DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
SET @chat_password = CONVERT(FROM_BASE64('${DB_PASSWORD_BASE64}') USING utf8mb4);
SET @create_local = CONCAT("CREATE USER IF NOT EXISTS '${CHAT_DB_USER}'@'localhost' IDENTIFIED BY ", QUOTE(@chat_password));
PREPARE create_local FROM @create_local; EXECUTE create_local; DEALLOCATE PREPARE create_local;
SET @alter_local = CONCAT("ALTER USER '${CHAT_DB_USER}'@'localhost' IDENTIFIED BY ", QUOTE(@chat_password));
PREPARE alter_local FROM @alter_local; EXECUTE alter_local; DEALLOCATE PREPARE alter_local;
SET @create_tcp = CONCAT("CREATE USER IF NOT EXISTS '${CHAT_DB_USER}'@'127.0.0.1' IDENTIFIED BY ", QUOTE(@chat_password));
PREPARE create_tcp FROM @create_tcp; EXECUTE create_tcp; DEALLOCATE PREPARE create_tcp;
SET @alter_tcp = CONCAT("ALTER USER '${CHAT_DB_USER}'@'127.0.0.1' IDENTIFIED BY ", QUOTE(@chat_password));
PREPARE alter_tcp FROM @alter_tcp; EXECUTE alter_tcp; DEALLOCATE PREPARE alter_tcp;
GRANT ALL PRIVILEGES ON \`${CHAT_DB_NAME}\`.* TO '${CHAT_DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON \`${CHAT_DB_NAME}\`.* TO '${CHAT_DB_USER}'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL

if ! sudo mariadb --protocol=socket -Nse \
    "SELECT 1 FROM information_schema.tables WHERE table_schema='${CHAT_DB_NAME}' AND table_name='user'" \
    | grep -q 1; then
    sudo mariadb --protocol=socket "${CHAT_DB_NAME}" < "${ROOT_DIR}/chat.sql"
fi

sudo mariadb --protocol=socket "${CHAT_DB_NAME}" -e "UPDATE user SET state='offline'"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
    cp "${ROOT_DIR}/.env.example" "${ROOT_DIR}/.env"
fi

"${ROOT_DIR}/build.sh"

echo
echo "WSL setup complete. Start the server with: ./scripts/run-server.sh"
