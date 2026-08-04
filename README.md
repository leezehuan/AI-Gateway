# C++ AI Gateway

This repository builds a multi-tenant HTTP/SSE AI API gateway. The default build contains
`AiGateway` and `AiGatewayAdmin`; the gateway is independent of the archived pre-migration
application under `legacy/`.

The request path is:

```text
OpenAI Responses / Chat Completions / Anthropic Messages client
        -> Nginx HTTP upstream (optional)
        -> AiGateway node(s)
        -> MySQL + Redis + Provider
```

The gateway does not run a CLI, persist conversations, or store Prompt/Response bodies.

## Build

Install Boost, libcurl, hiredis, MariaDB Connector/C, OpenSSL, Python 3 and Redis, then:

```bash
cp .env.example .env
# Set AI_GATEWAY_DB_PASSWORD, AI_GATEWAY_API_KEY_HMAC_PEPPER and provider secrets in .env.
./build.sh
```

For a Gateway-only build:

```bash
cmake -S . -B build/gateway \
  -DBUILD_AI_GATEWAY=ON \
  -DCMAKE_BUILD_TYPE=Debug
cmake --build build/gateway --parallel
```

## Database and configuration

Create the Gateway database with `scripts/setup-wsl.sh`, then apply the versioned schema and
resources:

```bash
build/wsl/bin/AiGatewayAdmin migrate --dir migrations/gateway
build/wsl/bin/AiGatewayAdmin apply-config --file config/ai-gateway.example.json
```

`config/ai-gateway.protocols.example.json` shows one logical model exposed through Responses,
Chat Completions and Anthropic Messages. It uses a local mock Provider and is suitable for the
integration seam. The Lingsuan configuration remains Responses-only and uses `gpt-5.6-terra` for
the live check.

Issue and revoke a Gateway Key with:

```bash
build/wsl/bin/AiGatewayAdmin issue-key --tenant example --policy default --name local
build/wsl/bin/AiGatewayAdmin set-key-status --key-id <public-id> --status disabled
```

The complete Key is printed once. Only its HMAC digest is stored in MySQL.

## HTTP protocols

Supported protocol names in MySQL are `responses`, `chat_completions`, and
`anthropic_messages`.

- `POST /v1/responses` supports native JSON and Responses SSE.
- `POST /v1/chat/completions` supports native JSON and data-only Chat SSE, including `[DONE]`.
- `POST /v1/messages` supports native Anthropic JSON and Messages SSE.
- `GET /v1/models` returns the de-duplicated logical models authorized for the current Key.
- `GET /healthz`, `GET /readyz`, and internal `GET /metrics` expose node status.

OpenAI protocols use Provider Bearer authentication. Anthropic uses `x-api-key` and
`anthropic-version: 2023-06-01`. Client credentials, Host, proxy credentials and hop-by-hop
headers are never forwarded.

## Cluster deployment

Use `deploy/nginx/ai-gateway.conf.example` with the two local Gateway listeners and
`deploy/systemd/ai-gateway@.service.example` for systemd instances. Nginx disables buffering,
cache and request buffering for SSE and does not replay non-idempotent POST requests.

The rolling restart sequence is documented in `docs/operations/phase6-cluster.md`: remove one
node from the upstream, validate and reload Nginx, signal the node, wait for drain, then restore
the node after `/readyz` returns `200`.

## Tests

CTest drives the public seam with a real HTTP client, a running Gateway, a localhost mock Provider,
temporary MariaDB and Redis:

```bash
ctest --test-dir build/wsl --output-on-failure
```

The suite covers protocol contracts, native authentication, model rewriting, SSE commit gates,
failover, governance, cancellation, drain, Nginx assets and sensitive-data redaction. Run the
ASan and UBSan build directories for sanitizer coverage. If `LINGSUAN_API_KEY` is configured,
`scripts/test-lingsuan-nginx.py` performs one Responses JSON and SSE check through Nginx.

## Migration record

The former TCP application, private message protocol and relational schema are retained only as a
read-only rollback archive in `legacy/`. They are not part of the default CMake graph, Gateway
database migrations, setup script or deployment topology. Phase 6 remains the rollback baseline.
