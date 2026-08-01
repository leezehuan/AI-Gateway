# C++ AI Gateway Phase 0 + Phase 1

## Summary

在保留旧 `ChatServer`/`ChatClient` 可编译的前提下，新增独立的 `AiGateway` 可执行目标，完成：

- Phase 0：迁移护栏、统一配置、结构化日志、request ID、CTest 集成测试骨架和 ADR。
- Phase 1：单节点、单租户、单 Provider 的 OpenAI Responses 非流式代理。
- 暂不实现 SSE、取消、背压、Redis 分布式配额、MySQL 多租户、故障切换或 Chat Completions。

`Aether/` 只作为行为参考，不加入任何源码、依赖、运行时、Schema 或部署配置。

## 关键实现

### 构建与运行目标

- 将项目标准升级为 C++17。
- 根 CMake 新增 `BUILD_AI_GATEWAY=ON`，保留现有 `BUILD_CHAT_SERVER` 和 `BUILD_CHAT_CLIENT`。
- 新增独立 `AiGateway` 目标，不依赖 Muduo、旧 `ChatService`、旧 Redis Pub/Sub 或聊天 Model。
- 入站 HTTP 使用 Drogon；上游使用一个进程级 `libcurl multi` transport。
- `setup-wsl.sh` 增加 Drogon、libcurl 开发包和 Python3 测试依赖。
- 保持旧聊天目标独立编译；允许使用 `-DBUILD_CHAT_SERVER=OFF` 单独构建网关。

### 深模块与 Seam

新增 `AiGateway` 深模块，HTTP Handler 只负责 Drogon 请求转换和响应写入。

核心接口固定为以下语义：

```cpp
struct GatewayRequest {
    std::string method;
    std::string path;
    std::string request_id;
    HeaderMap headers;
    std::string body;
};

struct ResponseWriter {
    virtual void begin(int status, const HeaderMap& headers) = 0;
    virtual bool write(std::string_view bytes) = 0;
    virtual void end() = 0;
    virtual bool client_connected() const = 0;
    virtual ~ResponseWriter() = default;
};

class AiGateway {
public:
    void handle(const GatewayRequest& request,
                ResponseWriter& response,
                CancellationToken cancellation);
};
```

Phase 1 只调用一次 `begin/write/end` 完成非流式响应，但接口保留后续 SSE、取消和背压所需的响应写入语义。

外部真实 Seam：

- `ProviderTransport`：生产使用 `CurlMultiProviderTransport`，测试通过 HTTP 模拟 Provider。
- `RuntimeState`、`GatewayRepository` 暂不进入 Phase 1；静态配置 Adapter 提供单 Key、单 Provider 快照。

### 请求生命周期

实现固定的 Phase 1 生命周期：

1. 生成 `request_id`，限制 Header 和 Body。
2. 验证二级 API Key。
3. 校验路径、JSON 顶层对象、`model` 和 `stream`。
4. 检查单一逻辑模型 allowlist。
5. 将逻辑模型改写成配置中的 upstream model。
6. 删除客户端 `Authorization`、`Host`、hop-by-hop 和代理认证 Header。
7. 注入 Provider API Key、`Content-Type` 和 Gateway request ID。
8. 通过共享 `libcurl multi` 发起一次上游请求，不进行重试。
9. 校验上游非流式 JSON 响应并转发。
10. 记录脱敏结构化终态日志。

上游 transport 不创建每请求阻塞线程；所有 easy handle 由一个共享 multi loop 管理，完成回调再切回 Drogon 事件循环。响应和请求体均有硬上限。

### HTTP 接口

- `GET /healthz`
  - 无鉴权。
  - 进程和事件循环存活时返回 `200`。
- `GET /readyz`
  - 无鉴权。
  - 静态配置、Provider URL、Provider Key 和监听配置完整时返回 `200`，否则 `503`。
- `GET /v1/models`
  - 需要二级 API Key。
  - 只返回配置的逻辑模型，不暴露 Provider、Credential 或内部 ID。
- `POST /v1/responses`
  - 需要二级 API Key。
  - 支持非流式 Responses 原生透传。
  - `stream=true` 明确返回 `400`，错误码为 `stream_unsupported`，为 Phase 2 的显式门禁。
  - 只理解 `model`、`stream` 和请求对象类型，其余 Responses 字段原样保留并转发。

错误统一使用 OpenAI 风格结构：

```json
{
  "error": {
    "message": "...",
    "type": "...",
    "param": null,
    "code": "..."
  }
}
```

状态约定：

- 缺失或错误 Key：`401`
- 模型未允许：`403`
- JSON、模型或 stream 字段错误：`400`
- Body 超限：`413`
- Provider 4xx/429：保留对应客户端可理解的状态和脱敏 JSON
- Provider 5xx、超时、连接失败、响应超限或无效 JSON：`502`
- 不向客户端返回真实 Provider URL、Credential、内部路径或堆栈。

### 配置

新增统一 `GatewayConfig::from_env()`，关键变量为：

- `AI_GATEWAY_LISTEN_ADDRESS`，默认 `127.0.0.1`
- `AI_GATEWAY_LISTEN_PORT`，默认 `8080`
- `AI_GATEWAY_API_KEY`，无默认值
- `AI_GATEWAY_PROVIDER_RESPONSES_URL`，必须显式配置
- `AI_GATEWAY_PROVIDER_API_KEY`，必须显式配置
- `AI_GATEWAY_LOGICAL_MODEL`，必须显式配置
- `AI_GATEWAY_UPSTREAM_MODEL`，必须显式配置
- `AI_GATEWAY_MAX_BODY_BYTES`，默认 `1048576`
- `AI_GATEWAY_MAX_RESPONSE_BYTES`，默认 `16777216`
- `AI_GATEWAY_UPSTREAM_TIMEOUT_MS`，默认 `30000`

二级 Key 在 Phase 1 使用固定配置值和常量时间比较；不可逆 HMAC 存储留到 Phase 3。Provider URL 只允许来自管理员配置，客户端不能指定任意上游 URL。

### 安全和旧代码护栏

- 删除旧数据库连接中的弱密码 fallback；缺少 `CHAT_DB_PASSWORD` 时旧服务不应静默使用默认密码。
- `setup-wsl.sh` 不再自动创建固定的 `chat/chat` 生产式凭据，改为显式环境变量或交互式配置。
- 停止旧 MySQL wrapper 打印完整 SQL 和密码相关内容。
- 本阶段不重写旧聊天 Model 的 SQL 和业务职责，避免迁移过程中改变旧聊天行为；这些代码继续作为冻结的 Legacy 目标。
- 新网关日志只输出 `request_id`、状态、耗时、模型、stream 标志、Provider 状态分类和响应字节数，不记录 Authorization、API Key、Prompt、完整 Response 或任意请求正文。
- 新增 ADR，记录 Drogon、libcurl multi、错误模型、无正文审计和 Phase 1 不支持流式的决策。

## 测试计划

使用 CTest 驱动真实 HTTP 集成测试：

```text
Python HTTP client
    -> running AiGateway
    -> localhost mock Provider
```

测试不访问 `AiGateway` 私有成员，也不验证内部调用次数。

覆盖场景：

- `/healthz` 成功，`/readyz` 在配置缺失时返回 `503`。
- `/v1/models` 的成功、缺失 Key 和错误 Key。
- 非流式 `/v1/responses` 成功转发。
- 请求中的逻辑模型被改写为 upstream model。
- Provider 收到注入的 Provider Key，但没有收到客户端二级 Key。
- `Host`、hop-by-hop、代理认证和不允许的 `x-codex-*` Header 被移除。
- `x-client-request-id` 等受控元数据按 allowlist 转发。
- 缺失或错误 JSON、缺失模型、禁用模型、`stream=true`、Body 超限。
- Provider 4xx、429、5xx、超时、无效 JSON 和响应超限。
- Gateway 不会因一个慢上游请求阻塞同时到达的 `/healthz`。
- Provider Key、Gateway Key、Prompt 和完整响应不会出现在 Gateway 日志。
- request ID 出现在下游响应 Header 和结构化日志中。
- 本机 Codex `0.146.0-alpha.9.2` 的脱敏契约夹具确认：
  - 请求路径为 `/v1/responses`
  - 鉴权方案为 Bearer
  - 请求包含 `model`、`input`、`instructions`、`stream` 等字段
  - 当前 Codex 默认使用 `stream:true`，因此在 Phase 1 明确收到 `stream_unsupported`；Codex 端到端成功验收留到 Phase 2。

## 假设与阶段边界

- 本次交付严格止于 Phase 1，不提前实现 SSE、流取消、背压、Nginx SSE 配置、Redis 共享配额、MySQL tenant/API Key Schema、Provider 路由和 failover。
- Phase 1 使用静态环境配置，不建立 `conversations`、`messages`、`offline_messages` 或 Prompt/Response 持久化。
- MySQL migration 目录只建立版本化脚手架和 ADR 约定；tenant、provider、usage 等正式 Schema 留到 Phase 3。
- `libdrogon-dev` 和 `libcurl4-openssl-dev` 作为构建前置依赖安装，不将 Aether 或其许可证代码带入项目。
- 通过 Phase 0/1 的 CMake、CTest、真实 HTTP 模拟 Provider 和安全脱敏验收后，才进入附件中的 Phase 2。
