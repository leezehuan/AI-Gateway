#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${ROOT_DIR}/.env"
    set +a
fi

AI_GATEWAY_DB_USER="${AI_GATEWAY_DB_USER:-ai_gateway}"
AI_GATEWAY_DB_NAME="${AI_GATEWAY_DB_NAME:-ai_gateway}"
AI_GATEWAY_DB_PASSWORD="${AI_GATEWAY_DB_PASSWORD:-}"

if [[ ! "${AI_GATEWAY_DB_USER}" =~ ^[A-Za-z0-9_]+$ ]] || \
   [[ ! "${AI_GATEWAY_DB_NAME}" =~ ^[A-Za-z0-9_]+$ ]]; then
    echo "Gateway database user and name may contain only letters, digits, and underscores." >&2
    exit 1
fi

if [[ -z "${AI_GATEWAY_DB_PASSWORD}" ]]; then
    if [[ -t 0 ]]; then
        read -r -s -p "Password for the ${AI_GATEWAY_DB_USER} Gateway database user: " AI_GATEWAY_DB_PASSWORD
        echo
    else
        echo "Set AI_GATEWAY_DB_PASSWORD before running setup-wsl.sh." >&2
        exit 1
    fi
fi

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
    libssl-dev \
    mariadb-client \
    mariadb-server \
    python3 \
    redis-server

if [[ "$(ps -p 1 -o comm= 2>/dev/null)" == "systemd" ]]; then
    sudo systemctl enable --now mariadb redis-server
else
    sudo service mariadb start
    sudo service redis-server start
fi

export AI_GATEWAY_DB_PASSWORD
sudo mariadb --protocol=socket <<SQL
CREATE DATABASE IF NOT EXISTS \`${AI_GATEWAY_DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
SET @gateway_password = CONVERT(FROM_BASE64('$(printf '%s' "${AI_GATEWAY_DB_PASSWORD}" | base64 -w0)') USING utf8mb4);
SET @create_local = CONCAT("CREATE USER IF NOT EXISTS '${AI_GATEWAY_DB_USER}'@'localhost' IDENTIFIED BY ", QUOTE(@gateway_password));
PREPARE create_local FROM @create_local; EXECUTE create_local; DEALLOCATE PREPARE create_local;
SET @alter_local = CONCAT("ALTER USER '${AI_GATEWAY_DB_USER}'@'localhost' IDENTIFIED BY ", QUOTE(@gateway_password));
PREPARE alter_local FROM @alter_local; EXECUTE alter_local; DEALLOCATE PREPARE alter_local;
SET @create_tcp = CONCAT("CREATE USER IF NOT EXISTS '${AI_GATEWAY_DB_USER}'@'127.0.0.1' IDENTIFIED BY ", QUOTE(@gateway_password));
PREPARE create_tcp FROM @create_tcp; EXECUTE create_tcp; DEALLOCATE PREPARE create_tcp;
SET @alter_tcp = CONCAT("ALTER USER '${AI_GATEWAY_DB_USER}'@'127.0.0.1' IDENTIFIED BY ", QUOTE(@gateway_password));
PREPARE alter_tcp FROM @alter_tcp; EXECUTE alter_tcp; DEALLOCATE PREPARE alter_tcp;
GRANT ALL PRIVILEGES ON \`${AI_GATEWAY_DB_NAME}\`.* TO '${AI_GATEWAY_DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON \`${AI_GATEWAY_DB_NAME}\`.* TO '${AI_GATEWAY_DB_USER}'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
    cp "${ROOT_DIR}/.env.example" "${ROOT_DIR}/.env"
fi

"${ROOT_DIR}/build.sh"

echo
echo "AI Gateway setup complete. Apply migrations with AiGatewayAdmin migrate."
