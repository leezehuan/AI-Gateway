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
    libjsoncpp-dev \
    libcurl4-openssl-dev \
    libhiredis-dev \
    libmariadb-dev \
    libssl-dev \
    uuid-dev \
    zlib1g-dev \
    mariadb-client \
    mariadb-server \
    python3 \
    redis-server

DROGON_VERSION=""
for version_header in /usr/local/include/drogon/version.h /usr/include/drogon/version.h; do
    if [[ -f "${version_header}" ]]; then
        DROGON_VERSION="$(sed -n 's/^#define DROGON_VERSION "\([^"]*\)"/\1/p' "${version_header}")"
        [[ -n "${DROGON_VERSION}" ]] && break
    fi
done

if [[ -z "${DROGON_VERSION}" ]] || ! dpkg --compare-versions "${DROGON_VERSION}" ge 1.9; then
    DROGON_SOURCE_DIR="$(mktemp -d)"
    trap 'rm -rf "${DROGON_SOURCE_DIR}"' EXIT
    git clone --branch v1.9.11 --depth 1 --recurse-submodules --shallow-submodules \
        https://github.com/drogonframework/drogon.git "${DROGON_SOURCE_DIR}/src"
    cmake -S "${DROGON_SOURCE_DIR}/src" -B "${DROGON_SOURCE_DIR}/build" \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_BROTLI=OFF \
        -DBUILD_CTL=OFF \
        -DBUILD_DOC=OFF \
        -DBUILD_EXAMPLES=OFF \
        -DBUILD_ORM=OFF \
        -DBUILD_TESTING=OFF \
        -DBUILD_YAML_CONFIG=OFF
    cmake --build "${DROGON_SOURCE_DIR}/build" --parallel
    sudo cmake --install "${DROGON_SOURCE_DIR}/build"
    sudo ldconfig
    rm -rf "${DROGON_SOURCE_DIR}"
    trap - EXIT
fi

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
