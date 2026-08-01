# C++ 集群聊天服务器向 AI API 网关迁移规格

> 状态：可交给编程 AI 执行  
> 目标：把当前项目改造成 C++ 多租户 AI API 二级网关  
> 参考项目：`fawney19/Aether`，固定参考提交 `0318808db98a342066c1c32640f29f4c1ee89cd8`  
> 约束：Aether 只用于理解代码逻辑，不得成为本项目的依赖、子模块或部署组成部分

## 给编程 AI 的任务摘要

将当前基于 Muduo 裸 TCP、私有 JSON `msgid` 协议的集群聊天服务器，逐步迁移为一个标准 HTTP/SSE AI API 网关。客户端是 Codex、Claude Code 或其他支持自定义 Base URL 和 API Key 的第三方客户端；服务端不启动、不托管任何 CLI/Agent 进程，只负责鉴权、策略检查、模型路由、Provider 凭据调度、流式转发、限流、额度、健康和用量审计。

最终的数据流是：

```text
Codex / Claude Code / 其他 AI 客户端
                 |
          HTTPS + HTTP/SSE
                 |
              Nginx
                 |
       C++ AI Gateway 集群
          |             |
       MySQL          Redis
          |             |
   配置与用量       分布式运行态
                 |
       OpenAI / Anthropic / Gemini / 兼容服务
```

本规格取代“为用户分配服务器端 CLI 终端”“保存完整 Conversation”“客户端断线后重放 Agent 输出”等旧设想。本项目的权威对象是一次次独立的 AI HTTP 请求，不是长期存在的用户连接或对话进程。

## Problem Statement

当前项目解决的是聊天用户之间的在线消息投递：服务端维护用户到 TCP 连接的映射，使用 Redis Pub/Sub 跨节点转发消息，并使用 MySQL 保存用户、好友、群组和离线消息。这个模型与 AI API 网关存在根本差异：

- 现有协议是无消息帧的裸 TCP JSON，无法被 Codex、Claude Code 等标准客户端直接调用，也不能可靠处理 TCP 粘包和拆包。
- `ChatService` 同时承担协议分发、用户状态、业务处理、Redis 和数据库逻辑，无法自然扩展出 Provider 路由、流式代理、限流和用量结算。
- `_userConnMap`、用户在线状态、好友、群组、离线消息和按用户订阅的 Redis 频道都不是二级 AI API 网关需要的状态。
- AI 流式响应可能持续较长时间，必须正确处理 SSE、客户端背压、取消、上游连接池和响应提交后的失败，不能把一次 `recv()` 当成完整请求。
- 多个网关节点必须共享配额、Provider 健康、熔断和亲和状态，但不应共享客户端 TCP 连接归属。
- 上游凭据、二级 API Key、Prompt 和模型输出都是敏感数据，当前硬编码数据库密码和字符串拼接 SQL 的做法不能延续。

## Solution

把项目定位为“尽量无状态的 C++ AI API 二级网关”：

1. 用户拿到本项目签发的二级 API Key，并把第三方客户端的 Base URL 指向本项目。
2. 每次模型调用都是独立 HTTP 请求；流式调用在该请求的 HTTP 响应体内使用 SSE。一次工具调用任务通常会产生多次独立模型请求。
3. 任意请求都可以进入任意 Gateway 节点。HTTP Keep-Alive 或 HTTP/2 只复用底层连接，不代表会话绑定到该节点。
4. Gateway 根据二级 API Key 恢复租户和策略，把逻辑模型解析成一组上游候选，并选择 Provider、Endpoint、Credential 和真实模型。
5. Gateway 删除客户端的认证信息，注入选中的上游凭据，使用成熟 HTTP 客户端连接池向 Provider 发起请求。
6. Gateway 原样或按协议 Adapter 转发普通响应与 SSE；客户端中途断开时取消当前上游请求并释放所有并发许可。
7. MySQL 保存配置和审计；Redis 保存有 TTL 的分布式运行态。默认不保存 Prompt、完整响应和对话历史。

## User Stories

1. 作为 API 使用者，我希望使用网关签发的二级 API Key 调用标准 AI 接口，从而无需接触真实 Provider 凭据。
2. 作为 Codex 使用者，我希望只修改 Base URL 和 API Key 就能调用兼容的 OpenAI Responses 接口。
3. 作为流式调用者，我希望 token 到达 Gateway 后立即通过 SSE 返回，而不是等完整回答生成后一次性返回。
4. 作为租户，我希望只能调用策略允许的模型、Provider 和接口格式，避免越权和意外费用。
5. 作为租户管理员，我希望可以为不同 API Key 配置 RPM、并发数、日/月额度和有效期。
6. 作为网关管理员，我希望把一个逻辑模型映射到多个 Provider、Endpoint、Credential 和上游模型。
7. 作为网关管理员，我希望可以选择固定顺序、负载均衡或缓存亲和调度策略。
8. 作为调用者，我希望同一可识别客户端会话尽量复用同一上游凭据，从而提高 Provider Prompt Cache 命中机会。
9. 作为调用者，我希望亲和目标不可用时网关仍能选择健康候选，而不是因强制粘滞导致请求失败。
10. 作为调用者，我希望上游在客户端尚未看到输出前失败时，网关能按策略尝试下一个候选。
11. 作为调用者，我希望已经收到部分流式输出后不会突然拼接另一个模型生成的内容。
12. 作为调用者，我希望我主动取消请求后，Gateway 立即停止消费上游流并释放并发额度。
13. 作为网关管理员，我希望看到请求量、活动流、首 token 延迟、错误率、429、候选尝试和成本等指标。
14. 作为审计人员，我希望知道某个请求使用了哪个租户、模型和候选、是否重试、消耗多少 token 和费用，但默认看不到 Prompt 正文。
15. 作为安全负责人，我希望二级 API Key 只以不可逆摘要保存，上游凭据加密或由外部 Secret 提供，日志中不出现任何密钥。
16. 作为运维人员，我希望部署多个 Gateway 节点后，全局限流、并发和熔断仍然一致。
17. 作为运维人员，我希望可以滚动重启任意 Gateway 节点，不需要迁移用户连接状态或 Conversation 状态。
18. 作为运维人员，我希望健康检查能区分“进程存活”“依赖已就绪”和“某个 Provider 当前不健康”。
19. 作为开发者，我希望通过一个模拟 Provider 从最高层验证整个请求生命周期，而不是依赖真实付费 API 运行测试。
20. 作为开发者，我希望每次迁移阶段结束后项目都能编译、测试和运行，避免一次性重写后无法定位问题。

## Implementation Decisions

### 1. 产品与状态边界

- 项目是 AI API Gateway，不是聊天平台、Agent 平台、终端托管平台或对话数据库。
- 服务器端不部署 Codex、Claude Code、Gemini CLI，不管理 PTY、工作区或 Agent Worker。
- 不维护 `user_id -> TcpConnection`、`conversation_id -> gateway_node` 或永久 WebSocket。
- 不为标准 AI 接口增加“先传哈希、再补传正文”的私有握手。未经修改的第三方客户端不会执行该协议。
- 不默认保存或压缩完整上下文。客户端按其使用的协议决定发送完整消息、`previous_response_id` 或其他延续标识。
- 可选会话亲和只影响上游候选选择，不补全消息、不代表会话所有权，也不要求进入同一个 Gateway 节点。

### 2. 默认技术路线

- 使用 C++17 或更高版本和 CMake。
- 默认采用成熟异步 HTTP 框架处理入站 HTTP/1.1、Keep-Alive、分块响应和 SSE；推荐 Drogon。不要在当前裸 TCP 回调上手写完整 HTTP 协议栈。
- 默认使用 `libcurl multi` 处理上游连接池、TLS、代理、HTTP/2 和流式回调。不得为每个请求创建一个阻塞线程或一个全新的 HTTP 客户端。
- Nginx 负责公网 TLS 和客户端侧 HTTP/2；Nginx 到 Gateway 可使用 HTTP/1.1 Keep-Alive；Gateway 到 Provider 优先使用连接池并在上游支持时启用 HTTP/2。
- MySQL 继续作为持久数据库，但必须使用连接池、预处理语句和 schema migration。
- Redis 继续用于集群运行态，但其职责从“跨节点聊天消息”改成“分布式配额、并发许可、亲和、健康、熔断和配置失效通知”。
- 如果编程 AI 想偏离上述框架选型，必须先写 ADR，证明替代方案能可靠支持增量 SSE、取消、背压和连接池。

### 3. 深模块与接口

顶层应形成一个深模块 `AiGateway`。它的外部接口只需要表达“处理一个 HTTP 请求，并通过可取消的响应写入器返回普通或流式响应”。鉴权、路由、尝试、提交、结算等复杂性应隐藏在实现内部，不要把二十多个业务步骤暴露给 HTTP Handler。

以下是 `AiGateway` 内部的模块职责，不应全部变成由 Handler 手工串联的浅层类：

| 模块 | 隐藏的复杂性 | 对外应保持的小接口 |
|---|---|---|
| Ingress Adapter | HTTP 路由、请求体上限、连接取消、响应写入 | 把框架请求转换成网关请求并调用 `AiGateway` |
| Protocol | Endpoint 识别、JSON 校验、模型提取、SSE 终止事件、协议错误格式 | 解析请求；规范化响应/错误；提取 usage 和会话提示 |
| Access Control | API Key 查找、租户策略、模型 allowlist、RPM、并发和预算预留 | 授权并返回一次可结算的许可 |
| Routing | 候选生成、过滤、优先级、负载均衡、亲和、健康与熔断 | 根据请求上下文生成有序执行计划 |
| Execution | 上游请求、候选尝试、提交门、流转发、背压、取消和错误分类 | 执行计划并产生一个下游响应 |
| Usage | token/费用归一化、尝试审计、预留结算、敏感数据策略 | 完成一次请求的成功、失败或取消记录 |

真正需要明确 Adapter 的 seam 只有会变化或需要测试替身的依赖：

- `ProviderTransport`：生产 Adapter 使用 libcurl multi，测试 Adapter 使用可编程模拟 Provider。
- `RuntimeState`：生产 Adapter 使用 Redis，单元/单节点测试可使用内存 Adapter。
- `GatewayRepository`：生产 Adapter 使用 MySQL；集成测试使用隔离测试库或兼容的本地替身。

不要为每张表创建只有一两个透传方法的浅层 Model。Repository 接口应按“鉴权快照、路由快照、记录请求结果”等用例提供高杠杆操作，并把 SQL 细节留在实现中。

### 4. 请求生命周期

每个请求严格按下列状态推进，并保证所有退出路径都会执行 finalization：

| 阶段 | 主要动作 | 失败结果 |
|---|---|---|
| 1. Ingress | 生成 request ID，限制 header/body，识别 Endpoint | 返回协议格式的 4xx，不访问 Provider |
| 2. Authenticate | 校验二级 API Key，加载租户和 Key 快照 | 401/403，不记录敏感认证正文 |
| 3. Normalize | 校验 JSON，提取逻辑模型、stream 和协议上下文 | 400/404 |
| 4. Reserve | 原子检查并预留 RPM、并发和预算 | 429 或额度错误 |
| 5. Plan | 生成并排序可用的 Provider 候选 | 无候选时返回 503 |
| 6. Pre-commit attempts | 连接上游，检查状态和首个有效响应事件 | 仅此阶段允许按策略切换候选 |
| 7. Commit and relay | 向客户端提交响应头和流式内容 | 提交后失败只能终止当前响应，禁止拼接新 Provider |
| 8. Finalize | 释放许可，结算 usage，记录每次候选尝试 | 即使取消或异常也必须执行 |

请求上下文必须携带稳定的 `request_id`。每次上游尝试另有 `attempt_id`，便于区分“一个用户请求”和“多个 Provider 尝试”。

### 5. 第一批兼容接口

为了让项目尽快出现真实的 AI 主链路，按以下顺序实现：

1. `GET /healthz`、`GET /readyz` 和 `GET /v1/models`。
2. `POST /v1/responses` 非流式原生透传。
3. `POST /v1/responses` SSE 流式透传。
4. `POST /v1/chat/completions` 非流式和 SSE。
5. Anthropic `POST /v1/messages`。
6. Gemini 接口或跨协议转换。

第一版优先 OpenAI Responses，是因为目标客户端明确包括 Codex。实现前必须捕获并记录目标 Codex 版本实际调用的 Endpoint、headers、请求字段和 SSE 事件；不能只凭通用 OpenAI 示例宣称兼容。

每增加一种协议都必须有独立 Protocol Adapter 和契约测试。第一版先做“客户端协议与上游协议相同的原生透传”，不要一开始实现完整 OpenAI、Anthropic、Gemini 三向转换。

### 6. HTTP、SSE 与连接语义

- 一个 Codex 用户任务可能包含多次模型请求：模型请求工具、Codex 本地执行工具、再发送工具结果。这些是多次独立 HTTP 请求和多次独立 SSE 响应。
- HTTP 响应体结束不等于 TCP 关闭。底层是否复用由 HTTP/1.1 Keep-Alive、HTTP/2、客户端和代理连接池共同决定。
- Nginx 应从原来的 TCP `stream` 负载均衡转成 HTTP 反向代理，关闭 SSE 响应缓冲和缓存，并配置适合长流的读取超时。
- Gateway 不依赖粘性会话。客户端连接到 Nginx 的同一条 TCP，也不保证所有 HTTP 请求进入同一 Gateway 节点。
- Gateway 必须过滤 hop-by-hop headers，删除客户端 `Authorization`、`Host` 和代理认证信息，再注入 Provider 所需凭据和版本头。
- 入站请求体第一版可以在有上限的内存中完整解析。必须配置最大请求体；禁止无界读取历史上下文。
- SSE 输出必须增量发送，不得被 Gateway 或 Nginx 聚合成完整响应。
- 上游读取与下游写入之间必须有有界缓冲区。慢客户端达到高水位时暂停上游读取，降到低水位后恢复；客户端断开时立即取消 libcurl transfer。
- 不得阻塞 Gateway 事件循环，也不得采用“每个活动流一个长期阻塞线程”的模型。

### 7. 路由模型

客户端请求的逻辑模型必须映射为一个或多个候选。候选身份至少包含：

```text
provider_id + endpoint_id + credential_id + upstream_model
```

候选选择分三步：

1. 过滤：租户/API Key 权限、接口格式、模型映射、Provider 启用状态、凭据有效期、RPM/并发、预算、健康和熔断。
2. 排序：Provider/Endpoint/Credential 优先级、健康分、当前负载，以及所选调度模式。
3. 尝试：按执行计划逐个尝试，并记录每个候选的开始、结束、状态、错误类和延迟。

第一批支持三种策略：

- `fixed_order`：按显式优先级稳定排序，适合主备。
- `load_balance`：在同优先级健康候选间稳定分散请求。
- `cache_affinity`：若存在可信会话提示，则优先使用 Redis 中之前成功的候选。

亲和键应包含 API Key 身份、协议、逻辑模型、客户端类型、会话提示的摘要和路由策略版本；亲和值只保存候选 ID，默认 TTL 可参考 Aether 的 300 秒。目标失败、熔断或不再符合策略时必须忽略并失效亲和记录。没有会话提示时直接负载均衡，不得用完整 Prompt 哈希伪造会话身份。

### 8. 重试、故障切换与提交门

核心不变量是：

```text
客户端响应提交前允许按策略故障切换；提交后禁止故障切换。
```

- 连接失败、明确的 429/可重试 5xx、首字节超时或提交门预读出的协议错误，可以在未提交下游响应时尝试下一个候选。
- 对网络超时等“上游可能已收到并执行请求”的模糊失败，必须考虑重复生成和重复计费。默认保守处理，并在允许重试时记录 `possible_duplicate_cost`。
- 流式响应只允许有界预读响应头和第一个可分类的 SSE 事件，用于识别伪装成 200 的 Provider 错误。不得为了重试能力缓冲完整回答。
- 一旦客户端看到响应头或首个业务事件，后续 Provider 读错误只能发送当前协议允许的 terminal error 并结束流；不得把另一个 Provider 的输出拼接到现有内容。
- 非流式和流式使用不同超时策略：连接/发送/首字节超时必须配置；非流式可有总超时；流式开始后使用可配置的空闲和最大时长策略。
- 每个请求限制最大候选次数和同一候选重试次数，避免重试风暴。

### 9. 多租户、配额和用量

- 二级 API Key 使用高熵随机值。数据库只保存可展示前缀和 `HMAC-SHA256(server_pepper, full_key)`，比较使用常量时间算法。
- Auth Snapshot 至少包含租户、Key 状态、有效期、允许协议、逻辑模型、Provider 限制、RPM、并发和预算策略。
- Redis 使用原子脚本实现租户、API Key 和 Provider Credential 级 RPM/并发控制；集群模式不得降级为每节点独立计数。
- 并发许可必须有 lease/TTL，并在正常完成、失败和客户端取消时释放；进程崩溃后由 TTL 兜底。
- 预算采用“请求前预留、请求后按真实 usage 结算”的模型。Provider 不返回 token 时标为估算或未知，不得伪装成精确计费。
- 价格表必须带模型和生效版本，避免历史 usage 因价格变更被重新解释。
- Usage 至少记录 request、tenant、API Key、协议、逻辑模型、实际候选、stream、状态、错误类、延迟、首 token 延迟、输入/输出/缓存 token、估算成本和 Provider request ID。
- 单独记录候选尝试，才能审计故障切换和潜在重复费用。

### 10. 数据职责

MySQL 持久化以下领域数据：

| 数据类别 | 建议实体 |
|---|---|
| 身份 | tenants、api_keys、roles/policies |
| 上游资源 | providers、provider_endpoints、provider_credentials |
| 模型与路由 | logical_models、model_mappings、route_policies、route_candidates |
| 配额和价格 | quota_policies、model_prices |
| 审计 | usage_records、request_attempts |
| 工程设施 | schema_migrations |

Redis 仅保存可重建、有 TTL 的运行态：

- RPM/TPM 计数和并发 lease；
- Provider/Credential 健康分和熔断状态；
- Session affinity；
- 短期配置缓存和失效通知；
- 可选的节点活动流计数。

明确不建立 `conversations`、`messages`、`offline_messages`、`friends` 或 `groups` 等聊天/对话表。Prompt 和 Response 正文默认不入库、不进指标标签、不写普通日志。若未来增加审计正文捕获，必须是租户显式启用、可截断、可脱敏、可设置 TTL 的独立功能。

旧聊天数据不自动迁移成 AI 网关数据。旧 `user` 表不能直接改名为 tenant，因为认证、权限和密钥语义不同。

### 11. 安全要求

- 删除源码中硬编码的 MySQL 密码，并立即轮换已经暴露的密码。
- 所有 SQL 使用预处理语句，禁止字符串拼接用户输入。
- Provider Credential 必须可解密但不能明文入库。MVP 可只从环境或挂载 Secret 读取；数据库方案应使用带认证的加密并从环境/Secret Manager 获取主密钥。
- 日志统一脱敏 `Authorization`、API Key、Cookie、Provider token 和代理凭据。
- 上游 URL 必须来自管理员配置和 allowlist，禁止普通租户提交任意 URL，避免 SSRF。
- 限制请求 header、body、JSON 深度、单流缓冲区、活动流和上游响应大小。
- 客户端断开、进程退出和异常路径都必须释放 Provider Credential 并发许可。
- 管理接口与模型代理接口使用不同权限；第一版可先通过离线管理工具/数据库种子配置，不应暴露无鉴权管理 Endpoint。
- 不向客户端返回 Credential ID、真实密钥、内部数据库 ID、内部 URL 或堆栈信息。

### 12. 可观测性与健康

结构化日志只记录元数据，使用 `request_id` 和 `attempt_id` 关联。至少提供以下指标：

- `active_streams`、`active_upstream_requests`、每节点请求数；
- 请求体字节、响应字节、流缓冲区高水位次数；
- Provider/模型/状态维度的请求量和错误率；
- 首字节/首 token 延迟、总延迟；
- Provider 429、5xx、超时、客户端取消和内部 499 记账；
- Credential 并发、RPM、熔断和候选切换次数；
- token 和成本；
- MySQL/Redis 连接池状态。

`/healthz` 只表示进程和事件循环存活；`/readyz` 表示配置、MySQL 和必需的 Redis 运行态可用；Provider 健康通过独立指标和管理查询展示，不能因单个 Provider 故障让整个 Gateway 退出负载均衡。

### 13. Aether 参考规则

编程 AI 可以克隆 Aether 并阅读固定提交，但只能参考以下行为：

- Auth Snapshot 如何合并用户/API Key 的权限和配额；
- Routing Policy 如何生成、过滤和排序 Provider/Endpoint/Credential 候选；
- `fixed_order`、`load_balance`、`cache_affinity` 的行为；
- Session affinity 的键、TTL、失效条件和跨节点共享方式；
- 流式响应的 commit policy、提交前故障切换和提交后 terminal failure；
- 客户端断开后的上游取消、许可释放和 drop-safe usage finalization；
- 分布式 RPM、并发 lease、健康分、熔断和候选审计思路。

以下内容不得照搬：

- 不引入 Aether crate、二进制、数据库 schema、管理后台或运行时；
- 不使用 `X-Aether-*` 头或让本项目假装成 Aether；
- 不复制受其许可证限制的源代码。参考逻辑后在 C++ 中独立实现，并用本项目契约测试证明行为；
- 不复制 Aether 的 Conversation continuation 作为通用对话数据库；
- 不引入 CLI/PTY/Worker，因为本规格明确不托管 Agent。

详细参考结论见同仓库的《Aether 架构与能力边界调研》。若实际 Aether 主分支与固定提交不同，以固定提交和本规格为准。

## Migration Plan

### Phase 0：冻结旧系统并建立迁移护栏

目标：先保留可回退基线，再开始新主链路。

- 为当前聊天版本保留明确 tag/分支，不在迁移过程中顺手修复旧聊天功能。
- 增加 CTest 和测试框架，建立可启动 Gateway 与模拟 Provider 的集成测试骨架。
- 引入统一配置加载、结构化日志、request ID 和 schema migration。
- 新增独立 `AiGateway` 可执行目标，暂时与旧 `ChatServer` 并存。
- 写 ADR 固定 HTTP 框架、上游客户端、错误模型和正文审计默认策略。

验收：旧目标仍可编译；新目标能启动并通过 `/healthz`；测试可在无真实 Provider 的环境运行。

### Phase 1：单节点、单租户、单 Provider 非流式主链路

目标：打通最小的真实 AI 请求，而不是先建设复杂控制面。

- 实现 OpenAI Responses 请求解析和非流式原生透传。
- 使用配置中的单个测试 Key 和单个上游 Credential。
- 实现 header allowlist、请求体上限、超时、统一错误和敏感信息脱敏。
- 使用 libcurl multi 的进程级连接池，不为每次请求重新握手。
- 增加模拟 Provider 的成功、4xx、5xx、超时和无效 JSON 测试。

验收：标准 HTTP 客户端使用二级 Key 调用 `/v1/responses`，能获得上游响应；错误不泄漏真实凭据；并发请求不阻塞事件循环。

### Phase 2：SSE、取消和背压

目标：让流式代理达到可用于 AI 客户端的正确语义。

- 增量解析/分类 SSE，但不任意改写合法事件内容。
- 实现有界缓冲、高低水位暂停/恢复、客户端断开取消和 drop-safe finalization。
- 配置 Nginx HTTP 反向代理、关闭响应缓冲和缓存。
- 增加慢客户端、长流、半途断开、上游中途错误和终止事件测试。

验收：首个事件可被客户端实时观察；内存不随慢客户端无界增长；客户端断开后活动上游和并发许可及时归零。

### Phase 3：MySQL 多租户与二级 API Key

目标：从静态测试配置迁移到持久化身份与策略。

- 建立 tenant、API Key、Provider、Endpoint、Credential、logical model 和 mapping schema。
- API Key 只存前缀和 HMAC 摘要；Provider Credential 使用 Secret/加密存储。
- 实现 Auth Snapshot 缓存及版本失效。
- 增加模型和协议 allowlist。
- 删除硬编码数据库凭据，使用连接池和预处理语句。

验收：两个租户使用不同 Key 只能访问各自允许的模型；禁用、过期和错误 Key 返回正确错误；数据库和日志中无明文 Key。

### Phase 4：多 Provider 路由、亲和、健康和故障切换

目标：实现二级网关的核心差异化能力。

- 实现候选生成、过滤和三种调度策略。
- 实现 Redis session affinity、健康分和熔断状态。
- 实现响应提交门、提交前 failover 和提交后 terminal failure。
- 持久化 request attempt 审计。

验收：主候选在提交前返回可重试错误时自动切换；客户端收到首个事件后上游失败时绝不拼接其他候选；两个 Gateway 节点能读取相同亲和结果。

### Phase 5：配额、预算、Usage 和监控

目标：补齐多租户资源治理。

- 使用 Redis 原子实现 tenant/API Key/Credential 的 RPM 和并发 lease。
- 实现预算预留、usage 归一化、价格版本和结算。
- 增加 Prometheus 指标、结构化审计和 Provider 健康探测。
- 定义 Redis 不可用时的策略：集群生产默认 fail closed；不得静默退化为每节点独立配额。

验收：两个节点并发压测不能突破全局上限；成功、失败、取消和故障切换都留下完整且不重复的终态记录；默认无 Prompt/Response 正文。

### Phase 6：集群化和运维验证

目标：把集群价值从“跨节点聊天”转换为“高可用 AI 流式代理”。

- 使用 Nginx HTTP upstream 部署至少两个 Gateway 节点。
- 验证滚动重启、节点宕机、MySQL/Redis 短暂故障和 Provider 429/5xx。
- 设置每节点 `active_streams`、请求体、缓冲区和连接池上限。
- 补充 readiness、优雅停机和 drain：停止接新请求，允许已有流在上限时间内完成，再取消剩余流。

验收：新请求能绕过失效节点；共享配额和路由状态保持一致；节点退出不需要更新用户在线状态。

### Phase 7：扩展协议并移除聊天遗留

目标：在新主链路稳定后完成项目身份切换。

- 增加 Chat Completions、Anthropic Messages，再按需求增加 Gemini 或协议转换。
- 更新项目名称、README、部署文档、配置示例和容量指标。
- 从默认构建删除旧 ChatClient、`ChatService`、私有 `msgid`、用户连接表、好友/群组/离线消息 Model 和按用户 Redis Pub/Sub。
- 旧聊天 schema 单独归档，不在新网关数据库中混用。
- 只有新网关端到端、集群、故障和安全测试全部通过后，才删除旧可执行目标。

验收：默认构建和文档中不再出现聊天业务；目标 CLI 的真实兼容测试通过；Aether 不在运行依赖或制品中。

## Testing Decisions

### 最高测试 seam

核心测试通过同一个 seam 进行：

```text
真实 HTTP 客户端
  -> 启动中的 AiGateway
  -> 可编程模拟 Provider
  -> 断言客户端响应、Provider 收到的请求、MySQL 审计和 Redis 许可
```

这是最重要的测试面。当前项目没有可复用的自动化测试先例，因此应新建 CTest 驱动的集成测试设施。测试只断言外部可观察行为，不断言 `AiGateway` 内部调用了哪些私有类。

### 必须覆盖的场景

1. 非流式成功、请求字段透传、逻辑模型改写和响应 header 过滤。
2. SSE 首事件实时到达、合法事件顺序和正常终止。
3. 请求体、header、活动流和缓冲区超限。
4. 无 Key、错误 Key、过期 Key、被禁模型和跨租户访问。
5. 客户端在首事件前和首事件后断开，均取消上游并完成结算。
6. 提交前连接失败、429、5xx、首字节超时和 Provider 内嵌错误触发正确 failover。
7. 提交后上游断开不触发 failover，只产生 terminal failure。
8. 慢客户端触发背压且内存保持有界。
9. 两个 Gateway 节点共享 RPM、并发、健康、熔断和亲和状态。
10. Gateway 进程在持有并发 lease 时崩溃，TTL 后许可可恢复。
11. Redis/MySQL 不可用时按既定 fail-closed/readiness 策略处理。
12. Usage 和 attempts 在成功、失败、取消、多候选下只有一个请求终态。
13. 日志、错误、指标和数据库中不出现二级 Key、Provider Credential 或默认正文。
14. Nginx 不缓冲 SSE，滚动重启可以 drain 活动流。

纯算法可以补充单元测试，包括候选排序、错误分类、价格计算、Key 摘要验证和协议事件分类。但这些测试不能替代最高 seam 的集成测试。

## Definition of Done

- Codex 指向 Gateway Base URL 后，目标版本使用二级 API Key 能完成至少一个非流式和一个流式 Responses 调用。
- 任意 Gateway 节点都能处理任意请求，不存在用户、会话或底层 TCP 到节点的持久映射。
- 多节点共享限流、并发、亲和、健康和熔断运行态。
- 流式代理具有有界内存、背压、取消和提交后禁止故障切换的保证。
- 所有密钥按本规格存储和脱敏；硬编码 MySQL 密码已删除并轮换。
- MySQL 不保存聊天关系或默认 Prompt/Response 正文。
- 自动化测试覆盖主链路、SSE、故障切换、取消、多租户和双节点配额。
- README、部署文档、schema 和构建目标都反映 AI Gateway，而不是聊天服务器。
- Aether 只出现在参考文档和来源说明中，不出现在二进制、容器、依赖、schema 或运行拓扑中。

## Out of Scope

- 在服务器端运行或分配 Codex、Claude Code 等 CLI。
- Agent Worker、PTY、工作区、工具执行、审批和 Agent 断线恢复。
- 完整 Conversation/Message/Event Store 和 SSE 重放。
- 对标准 AI 请求做分块哈希去重、差量上下文上传或对话压缩。
- 默认保存 Prompt、模型完整输出、工具结果或用户文件。
- 通用响应缓存。生成请求具有随机性、权限和隐私风险，未定义语义前不实现。
- 第一阶段同时完成 OpenAI、Anthropic、Gemini 的任意双向协议转换。
- 自己实现 TLS、HTTP/2 或底层 HTTP 连接池。
- 第一阶段开发完整 Web 管理后台。

## Further Notes

### 已知风险

- “兼容 OpenAI API”不等于“兼容 Codex”。必须针对指定 Codex 版本做真实契约捕获和回归测试。
- 上游在网络错误前可能已经开始生成，故障切换可能产生重复费用。审计必须记录每个候选尝试，策略必须可限制。
- Provider 的 RPM、TPM、余额和服务条款是外部硬上限；增加 Gateway 节点不能突破这些限制。
- Provider 凭据池可能涉及转售和账号共享限制，部署前必须确认各 Provider 服务条款。
- Aether 许可证可能限制代码复用。最稳妥的方式是只研究可观察行为和架构决策，然后独立实现。
- SSE 长连接会把容量瓶颈从“登录用户数”转成 `active_streams`、上游连接数、出口带宽、缓冲内存和 Provider 并发。

### 编程 AI 执行纪律

1. 先读当前源码、本规格和 Aether 调研文档，再提出 Phase 0/1 的具体文件级计划。
2. 一次只实施一个 Phase；每个 Phase 都必须保持编译通过并附测试证据。
3. 新 AI 主链路与旧 `ChatService` 并行建立，不要把旧聊天类改名后继续堆积职责。
4. 不要在没有自动化 SSE 和取消测试的情况下删除旧目标。
5. 遇到协议、密钥存储、故障切换或正文审计的歧义时，优先选择本规格中的保守默认值，并用 ADR 记录偏离。
6. 不要擅自加入 Conversation、Agent、WebSocket、语义缓存或分块哈希等超出范围的功能。

