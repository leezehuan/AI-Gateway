# C++ AI Gateway Phase 4 实施计划

## Summary

在现有 Phase 3 多租户、MySQL Auth Snapshot 和 Phase 2 SSE 执行链上增加多候选路由、Redis 共享运行态、提交前 failover 和 MySQL attempt 审计。保留 Responses 非流式/SSE、旧聊天目标和现有配置兼容性。

默认采用：

- Redis 不可用时 `/readyz=503`，新的模型代理请求返回脱敏 `503 routing_unavailable`；`/healthz` 和可缓存认证的 `/v1/models` 不依赖 Redis。
- 旧 Logical Model 未配置路由时使用 `fixed_order`。
- 首字节超时、可重试 5xx 和提交前协议错误允许切换，并记录 `possible_duplicate_cost=true`。
- 单个候选每个请求最多尝试一次，默认最多 3 个候选，上限 10。

## Key Changes

### 数据与管理配置

- 新增 `0002_phase4_routing_attempts.sql`：
  - `route_policies`：Logical Model 一对一关联，保存 `fixed_order|load_balance|cache_affinity` 和 `max_attempts`。
  - `model_mappings.priority`：默认 `100`，数值越小优先级越高。
  - `request_attempts`：保存 `attempt_id`、`request_id`、序号、Tenant/API Key/Logical Model/Mapping/Provider/Endpoint/Credential 内部 ID、stream、开始与结束时间、Provider 状态、错误类、字节数、retryable、`possible_duplicate_cost` 和终态。
  - `attempt_id` 唯一，`(request_id, attempt_number)` 唯一；不保存 URL、Secret、Prompt、响应正文或原始会话 ID。
- `apply-config` 支持 Logical Model 的可选配置：
  ```json
  "routing": {"mode": "cache_affinity", "max_attempts": 3}
  ```
  Mapping 支持 `"priority": 100`。旧 JSON 自动得到 `fixed_order/3/100`，省略记录仍不删除。
- `AuthSnapshot` 返回全部静态有效且被 Policy 授权的候选；多个候选不再触发 `model_unavailable`。`/v1/models` 只要求至少一个静态可路由候选，不随临时熔断波动。
- `CONTEXT.md` 增加 Route Policy、Route Plan、Candidate、Attempt、Session Affinity 和 Circuit Breaker 术语。

### Redis 与路由模块

- 新增深模块 `RoutingRuntime` 和外部 seam `RoutingStore`；生产 Adapter 使用 hiredis 固定 worker pool、连接复用、后台重连和有界命令队列，不阻塞 Beast 事件循环。
- `AiGateway` 构造函数改为接收 `RuntimeState&`、`RoutingRuntime&` 和 `ProviderTransport&`；`GatewayRequest`、`ResponseWriter` 与 SSE 写入接口保持不变。
- `ModelTarget` 增加 Mapping/Provider/Endpoint/Credential ID、mapping name、priority 和不含 Secret 的候选指纹。
- Redis Key 使用可配置前缀和配置版本；只保存候选指纹、整数健康状态和 HMAC 后的会话摘要。
- Affinity 信号按 `session-id`、`thread-id`、JSON `prompt_cache_key` 顺序提取；Codex family 通过 `originator` 或 User-Agent 识别。排除 `x-client-request-id`、完整 Prompt 和正文哈希。
- Affinity TTL 默认 300 秒，只在非流式合法成功或 SSE 观察到成功终止事件后写入。目标失败、熔断、失权或配置版本变化时忽略并删除。
- 候选排序：
  - `fixed_order`：priority、健康桶、mapping name。
  - `load_balance`：同优先级健康候选使用 request ID 的 rendezvous hash 稳定分散。
  - `cache_affinity`：优先可用的 Redis 目标；未命中时使用会话摘要做 rendezvous hash，无会话提示时退化为 `load_balance`。
- 健康分初始 100，成功 `+5`，可重试失败 `-20`；连续 3 次可重试失败打开熔断 30 秒。冷却后通过 Redis `SET NX` 取得单一 10 秒 half-open probe；成功关闭，失败重新打开。429 的有效 `Retry-After` 可延长冷却，但最多 300 秒。
- 客户端取消和客户端可归因的 4xx 不降低健康分。健康/熔断状态 TTL 默认 3600 秒。

### Failover 与审计

- 将 `BasicExecution` 和 `StreamExecution` 收敛为共享的多 attempt 状态机；每次从原始 JSON 克隆请求并仅改写当前候选的 upstream model、URL 和 Provider Key。
- Provider Transport 增加 DNS/connect/TLS、首字节超时、响应超限、取消和一般传输失败分类，并标记请求是否可能已发送。
- 提交前可切换：连接建立失败、429、500/502/503/504、首字节超时、响应超限、无效 JSON/SSE、错误 Content-Type、首个 Provider error 事件。
- 429 和明确连接建立失败不标记重复计费风险；超时、5xx、响应超限、协议错误和 Provider error 标记 `possible_duplicate_cost=true`。
- 其他 Provider 4xx 保持 Phase 1 的脱敏状态转发，不切换；取消、shutdown 和客户端断开也不切换。
- SSE 首个合法业务事件仍是提交门。提交后任何失败只发送一个 Phase 2 官方 terminal error 并结束，绝不执行下一候选。
- 每次 Provider 调用前异步插入 `started` attempt；插入失败则不调用 Provider并返回 `503 audit_unavailable`。结束更新使用幂等条件，数据库短暂失败进入有界重试队列并使 readiness 失败。
- Redis 失败反馈必须成功写入后才能继续下一候选；否则停止 failover。成功响应后的 Redis 写失败不撤回已提交响应，但 Gateway 立即变为 not ready。
- 所有 attempt 的预读字节共同计入 `max_response_bytes`；流最大持续时间从第一次尝试开始，不因 failover 重置。
- 日志增加 `attempt_id`、attempt number、候选指纹、终态、retryable、重复计费风险和 failover 次数，不记录请求或事件正文。

### 构建与配置

- CMake 检测并链接 hiredis；`setup-wsl.sh` 增加 `libhiredis-dev` 和 `redis-server`。
- 新增 Redis host/port/user/password/database、worker/queue/timeout，以及 affinity TTL、健康 TTL、熔断阈值、冷却和 probe lease 环境变量。
- 更新 `.env.example`、README、migration 文档和 ADR 0004；保留 Lingsuan Responses 配置及实时测试默认模型 `gpt-5.6-terra`。
- Nginx 示例继续关闭 buffering/cache/request buffering，不写入系统 `/etc/nginx`。

## Test Plan

按真实 HTTP seam 执行逐个 red-green 垂直切片：

1. 两个 Mock Provider 验证多 Mapping、优先级、模型改写和不同 Credential。
2. 固定顺序主候选在连接失败、429、5xx、超时、无效 JSON/SSE 和首事件错误时切换到次候选。
3. SSE 提交后断开只产生一个 terminal error，次候选请求数保持为零。
4. `load_balance` 在同优先级候选间分散；`cache_affinity` 在两个 Gateway 节点间命中相同候选。
5. Affinity 过期、配置版本变化、目标熔断和失权后失效；Redis 中不存在原始 session、Key、Prompt 或正文。
6. 连续失败打开共享熔断，另一个节点跳过目标；冷却后仅一个 half-open probe，成功后恢复。
7. Redis 停止时 `/healthz=200`、`/readyz=503`、代理请求 `503`；重启后自动恢复。
8. MariaDB 中每个真实 Provider 调用恰有一个 attempt，序号、终态和 `possible_duplicate_cost` 正确；取消和提交后失败同样完成审计。
9. 保留全部 Phase 1-3、SSE 背压、取消、Keep-Alive、多租户和安全脱敏测试。
10. CTest 启动临时 MariaDB、Redis、两个 Gateway 和两个 Mock Provider。已安装 Nginx 时使用临时非 root 配置执行 `nginx -t` 和经 Nginx 的实时 SSE/双节点测试。
11. 完成 Gateway 独立构建、Legacy 全目标构建、普通 CTest、ASan 和 UBSan；有 `LINGSUAN_API_KEY` 时额外执行一次 `gpt-5.6-terra` Nginx 实时验收。

## Assumptions And Boundaries

- Phase 4 仍仅支持 OpenAI Responses，单次 attempt 不重试同一候选。
- 不实现 Phase 5 的 RPM、并发 lease、预算、Usage、价格和 Prometheus。
- 不实现 Chat Completions、多协议转换、管理 HTTP API、优雅 drain 或进程崩溃后的审计补偿。
- Aether 固定提交仅用于比对可观察行为，不复制源码、Schema、Header、依赖或运行时。
