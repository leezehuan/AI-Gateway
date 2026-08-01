# C++ AI Gateway Phase 2 实施计划

## Summary

在现有 Boost.Beast 入站适配器和共享 `libcurl multi` 基础上实现 OpenAI Responses SSE。保留 Phase 1 非流式行为及旧聊天目标，不加入多 Provider、重试、Redis、MySQL 多租户或协议转换。

核心验收 seam 继续使用：

```text
真实 HTTP 客户端 -> AiGateway -> localhost Mock Provider
```

## 核心实现

- `AiGateway` 内部增加 SSE 解析、提交门和流生命周期状态；HTTP Handler 不参与协议判断。
- `stream=true` 请求改写上游模型并发送 `Accept: text/event-stream`。
- 上游必须返回 2xx 和 `text/event-stream`；提交前缓冲到首个完整、合法、非错误 SSE 事件。
- SSE 支持 LF、CRLF、跨 chunk 事件、注释帧、多行 `data:`、`[DONE]`，合法事件按原始字节转发。
- 首事件为 `error`、伪装 JSON 错误、无效 SSE、错误 Content-Type 或预读超限时，在尚未提交响应的情况下返回脱敏 `502` JSON。
- 首个业务事件通过后提交 `200 text/event-stream`，加入 `x-request-id`、`Cache-Control: no-store` 和 `X-Accel-Buffering: no`。
- 提交后若上游超时、断开、超限或产生无效事件，发送官方 Responses `error` SSE：
  ```text
  event: error
  data: {"type":"error","code":"upstream_stream_error","message":"Upstream provider stream failed","param":null,"sequence_number":N}
  ```
  随后结束当前 HTTP 响应，不追加 `[DONE]`，也不再次请求 Provider。
- 正常流原样保留事件顺序及 `response.completed`、`[DONE]`；只有观察到成功终止事件才记录成功终态。

## 接口与并发

- 保留 `ResponseWriter::begin/write/end/client_connected`，新增 writable/drain 回调。`write()` 在连接存在时总是接收当前 bytes；返回 `false` 表示已达到高水位，需要暂停上游。
- Beast SSE Writer 使用 HTTP/1.1 chunked 编码、单一在途 `async_write` 和有界队列；降到低水位时触发 resume。
- `ProviderTransport` 改为事件式接口：响应头、body chunk、完成结果分别回调，并返回可取消、可恢复的 `ProviderTransfer`。
- curl easy handle 只允许 multi worker 操作。pause、resume、cancel 通过线程安全命令队列进入 worker，并使用 `curl_multi_wakeup` 立即唤醒。
- 当前 curl chunk 被下游接收后才进入暂停；恢复时不得重放已接收 chunk。
- `CancellationToken` 增加订阅机制；Beast 检测客户端 EOF、reset 或写失败后立即取消 transfer。
- `HttpSession` 在 Provider 完成并执行幂等 finalization 前保持自身生命周期，避免断开路径中的悬空 `ResponseWriter`。
- 每次 transfer 的完成回调严格执行一次；成功、上游失败、客户端取消和进程关闭都归并到同一终态日志。
- 暂停期间不计算流空闲超时，但仍计入最大流持续时间；`max_response_bytes` 继续限制流式总响应字节。

新增配置默认值：

```text
AI_GATEWAY_STREAM_PREFETCH_BYTES=65536
AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES=262144
AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES=65536
AI_GATEWAY_STREAM_IDLE_TIMEOUT_MS=60000
AI_GATEWAY_STREAM_MAX_DURATION_MS=900000
```

启动时要求 `low_water < high_water` 且 `prefetch <= high_water`。现有 `AI_GATEWAY_UPSTREAM_TIMEOUT_MS` 对非流式仍是总超时，对流式作为连接及首事件超时。

## HTTP 与部署

- Beast 分别维护普通完整响应和 SSE chunked 响应，确保 Phase 1 Content-Length、状态码和 Keep-Alive 行为不回归。
- 增加 Nginx HTTP upstream 示例，使用 HTTP/1.1 Keep-Alive，并配置：
  `proxy_buffering off`、`proxy_cache off`、`proxy_request_buffering off`、长 `proxy_read_timeout`、清除 upstream `Connection`。
- 更新 ADR、README、环境变量示例和 Codex 契约文档，记录 Boost.Beast Phase 2 验收、官方 Responses error event、缓冲上限及取消语义。

## TDD 与验收

按垂直切片逐项执行红绿循环：

1. 首个 SSE 事件在 Provider 完成前可被客户端读取，事件顺序和终止事件保持不变。
2. 上游请求仍正确改写模型、注入 Provider Key，并移除客户端 Key 和禁用 Header。
3. 提交前的 4xx、429、5xx、JSON 错误、错误 Content-Type、畸形 SSE、首事件超限返回脱敏 JSON。
4. 提交后的上游断开产生一个 `error` terminal event，不能切换响应协议或拼接第二条流。
5. 小水位配置和慢客户端触发 pause；客户端恢复读取后 transfer resume，缓冲始终有界且事件无重复、无丢失。
6. 客户端在首事件前和首事件后断开，Mock Provider 都在限定时间内观察到连接关闭，活动请求归零。
7. 流式首事件超时、空闲超时、最大时长和总响应超限均按提交状态产生 JSON 或 SSE 错误。
8. 慢流不会阻塞并发 `/healthz`，多个并发流不会相互阻塞。
9. Gateway 日志不包含 Key、Prompt 或事件正文，并包含 request ID、stream、终态、字节数、暂停次数和耗时。
10. 保留全部 Phase 1 测试，并执行完整构建、CTest、ASan/UBSan。
11. 若系统存在 Nginx，额外运行 `nginx -t` 和经 Nginx 的实时 SSE 测试；当前无 Nginx 环境时明确记录该条件测试未运行。

## 阶段边界

- Phase 2 仍为单节点、单租户、单 Provider，任何请求最多执行一次上游调用。
- 不实现提交前 failover、Provider 路由、Chat Completions、Redis 配额、MySQL Schema、Prometheus 或优雅 drain。
- 不复制或链接 Aether 源码；固定提交仅用于比对可观察行为。
