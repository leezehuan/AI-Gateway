# C++ AI Gateway

本项目基于 C++ 实现 AI API 网关，面向多租户场景提供统一的 OpenAI Responses 与 Chat
Completions 接入、模型路由和集群化治理能力。网关支持普通 JSON 与 SSE 流式转发，并提供
API Key 鉴权、租户/密钥配额、RPM 限流、并发租约、熔断、故障切换、节点容量控制、优雅
下线和用量审计。

项目源码：[https://github.com/leezehuan/AI-Gateway](https://github.com/leezehuan/AI-Gateway)

技术栈：C++20、Boost.Asio/Beast、libcurl、OpenSSL、MariaDB/MySQL、Redis（hiredis）、
Nginx、SSE。

## 实现与测试摘要

1. `Boost.Asio/Beast` 提供异步 HTTP 服务与 SSE 响应写出，`libcurl` multi 接口在独立
   worker 中执行上游 Provider 请求；普通响应和 SSE 流均支持超时、取消及有界背压。
2. Redis 通过原子脚本维护跨节点 RPM、并发 lease、续租和路由熔断状态；MySQL 保存租户、
   API Key、访问策略、模型映射、预算预留和请求/Attempt 用量审计；Nginx 可作为多节点
   入口并负责上游节点转发。
3. 仓库记录的 A800、Qwen3-30B-A3B、C32 对比测试中，直连 vLLM 的 TTFT p50/p95 为
   `73.41/326.42 ms`，经 AiGateway 为 `81.38/403.37 ms`；请求吞吐基本相近，E2E
   中位数基本持平。完整参数和限制见
   [ops/ai_gateway/PRESS_RESULTS.md](ops/ai_gateway/PRESS_RESULTS.md#c32-串行直连与网关2026-10-18)。

项目提供两个可执行程序：

- `AiGateway`：处理 API Key 鉴权、访问策略、模型路由、Provider 调用、流式响应、限流、额度治理、健康检查和用量审计。
- `AiGatewayAdmin`：执行数据库迁移、导入配置和管理 Gateway API Key。

请求链路如下：

```text
OpenAI Responses / Chat Completions 客户端
        -> Nginx（可选）
        -> AiGateway 节点
        -> MySQL + Redis + AI Provider
```

网关不托管 CLI/Agent 进程，不保存对话、Prompt 或完整响应正文；客户端凭据和 Provider 密钥也不会写入日志。

## 构建

构建需要 CMake、C++20 编译器、Boost、libcurl、OpenSSL、MariaDB Connector/C、hiredis、Redis 和 Python 3。

```bash
cp .env.example .env
# 编辑 .env，填写 AI_GATEWAY_DB_PASSWORD、AI_GATEWAY_API_KEY_HMAC_PEPPER 以及 Provider 密钥。
./build.sh
```

也可以只构建网关：

```bash
cmake -S . -B build/gateway -DBUILD_AI_GATEWAY=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build/gateway --parallel
```

## 数据库与配置

在开发机或 WSL 中可使用初始化脚本准备 MariaDB、Redis 和 Gateway 数据库：

```bash
./scripts/setup-wsl.sh
```

然后执行版本化迁移并导入示例配置：

```bash
build/wsl/bin/AiGatewayAdmin migrate --dir migrations/gateway
build/wsl/bin/AiGatewayAdmin apply-config --file config/ai-gateway.example.json
```

`config/ai-gateway.protocols.example.json` 展示同一个逻辑模型如何同时提供 Responses 和 Chat Completions 两种协议；Provider 地址为本地 Mock 服务，适合联调。生产环境请通过环境变量或 Secret 文件注入密钥，不要把明文密钥写入配置文件。

签发和管理 Gateway API Key：

```bash
build/wsl/bin/AiGatewayAdmin issue-key --tenant example --policy default --name local
build/wsl/bin/AiGatewayAdmin set-key-status --key-id <public-id> --status disabled
```

完整 Key 只在签发时输出一次，数据库只保存其 HMAC 摘要。

## HTTP 接口

支持的协议名称为 `responses` 和 `chat_completions`：

- `POST /v1/responses`：OpenAI Responses JSON 或 SSE。
- `POST /v1/chat/completions`：OpenAI Chat Completions JSON 或 data-only SSE（包含 `[DONE]`）。
- `GET /v1/models`：返回当前 API Key 有权访问的去重逻辑模型。
- `GET /healthz`、`GET /readyz`、内部 `GET /metrics`：进程、依赖就绪和运行指标。

除健康检查和内部指标外，请求必须携带 Gateway 签发的凭据：

```http
Authorization: Bearer <Gateway API Key>
Content-Type: application/json
```

Gateway 会根据配置选择 Provider、Endpoint、Credential 和真实模型，并按原协议转发请求与 SSE，不在协议之间做隐式转换。

## 集群部署

可使用 `deploy/nginx/ai-gateway.conf.example` 配置 Nginx 双节点入口，使用
`deploy/systemd/ai-gateway@.service.example` 管理多个 Gateway 实例。Nginx 对 SSE 关闭缓存、请求和响应缓冲，不会重放非幂等 POST 请求。

滚动重启步骤见 [docs/operations/phase6-cluster.md](docs/operations/phase6-cluster.md)：先从 upstream 移除节点并 reload Nginx，再发送终止信号，等待节点完成 drain 和 `/readyz` 恢复后重新加入。

## 测试

CTest 会启动真实 Gateway、本地 Mock Provider、临时 MariaDB 和 Redis，并通过 HTTP 客户端验证公开行为：

```bash
ctest --test-dir build/wsl --output-on-failure
```

测试覆盖协议契约、鉴权、模型重写、SSE、故障切换、治理、取消、节点 drain、Nginx 配置和敏感数据脱敏。配置 `LINGSUAN_API_KEY` 后，还可以运行 `scripts/test-lingsuan-nginx.py` 做一次真实 Provider 验收。

## 压测

真实 vLLM 的直连、单网关和 Nginx 三网关对比方法见 [ops/ai_gateway/BENCHMARK.md](ops/ai_gateway/BENCHMARK.md)。流程统一输入与生成参数，以 C32 为起点逐步验证 C48，每轮检查活动请求排空，并分别记录 TTFT、端到端延迟、吞吐和治理拒绝。容量与故障实验使用 Mock Provider，单独报告结果。

更多中文操作说明请参阅 [docs/USAGE_CN.md](docs/USAGE_CN.md)。
