# C++ AI Gateway Phase 3 实施计划

## Summary

将 Phase 2 的静态单租户配置替换为 MySQL 持久化身份与策略。`AiGateway` 保持现有 HTTP/SSE 接口，但通过异步 `RuntimeState` 获取不可变 `AuthSnapshot`，不在 Beast 事件循环执行阻塞 SQL。

Phase 3 使用 MySQL-only 认证、租户内逻辑模型命名、可复用 Access Policy、HMAC API Key 和 Secret 引用。暂不实现 Redis 配额、多候选路由、故障切换、Usage 或管理 HTTP 接口。

## Domain And Schema

- 新增根 `CONTEXT.md`，固定 Tenant、API Key、Access Policy、Logical Model、Provider、Endpoint、Credential、Model Mapping、Auth Snapshot 等术语；旧聊天 User 不等同于 Tenant。
- 新增版本化 migration，建立：
  - `schema_migrations`、`gateway_config_versions`
  - `tenants`、`access_policies`、`api_keys`
  - `policy_protocol_grants`、`policy_model_grants`、`policy_provider_grants`
  - `providers`、`provider_endpoints`、`provider_credentials`
  - `logical_models`、`model_mappings`
- Logical Model 按 `(tenant, protocol, name)` 唯一。Mapping schema 允许未来多个候选，但 Phase 3 要求每个请求恰好解析出一个启用的 Endpoint、Credential 和 upstream model；零个或多个均返回脱敏 `503 model_unavailable`。
- API Key 格式固定为 `aigw_<12字符前缀>_<32字节base64url随机值>`。数据库只存可展示前缀、公开 key ID 和 `BINARY(32)` HMAC-SHA256，不存完整 Key。
- Provider Credential 只存 `env:NAME` 或 `file:basename` 引用。`file:` 只能从 `AI_GATEWAY_SECRET_DIR` 读取安全文件名，拒绝路径穿越和符号链接，并限制为 64 KiB。
- 所有运行时和管理 DML 使用 MariaDB prepared statements；migration DDL 为静态 SQL。新代码不复用 Legacy MySQL wrapper。

## Runtime And Interfaces

- `AiGateway` 构造函数改为接收 `RuntimeState&` 和 `ProviderTransport&`；`GatewayRequest`、`ResponseWriter` 和 Phase 2 流接口保持不变。
- `RuntimeState` 隐藏 HMAC、常量时间比较、MySQL worker/pool、Auth Snapshot LRU、版本轮询和 Secret 解析。认证和模型解析使用回调式异步接口，固定 worker pool，不创建每请求线程。
- `GatewayRepository` 提供配置版本、按 Key 前缀加载候选及一致性 Auth Snapshot 查询；生产 Adapter 为 `MySqlGatewayRepository`。
- Auth Snapshot 合并 Tenant、Key、Policy、协议/模型/Provider grants 和 Model Mapping。`GET /v1/models` 只返回当前 Key 可访问的逻辑模型。
- 保持认证优先级：缺失 Bearer 直接 `401`；其余请求先异步验证 Key，再返回 JSON/model/stream 错误。
- 错误约定：
  - malformed、unknown、disabled、expired Key：统一 `401 invalid_api_key`
  - disabled Tenant/Policy：`403 access_disabled`
  - protocol/model/provider 未授权：`403 protocol_not_allowed` 或 `model_not_allowed`
  - DB、schema、worker queue、mapping 或 Secret 不可用：脱敏 `503`
- Auth cache 默认 TTL 30 秒、最多 10000 条；每 1 秒轮询 `gateway_config_versions`。版本变化立即清空缓存。轮询失败后不使用缓存继续服务，`/readyz` 和认证请求均返回 `503`；`/healthz` 仍为 `200`。
- 数据库不可用时进程继续监听并后台重连。正在执行的已授权请求不因后续版本变化被取消。
- 日志增加 tenant slug 和公开 API key ID，但不记录完整 Key、HMAC、Secret 引用值、SQL 参数、Prompt 或响应正文。

## Configuration And Administration

- 删除运行时静态变量：`AI_GATEWAY_API_KEY`、Provider URL/Key、logical/upstream model。
- 新增必填 `AI_GATEWAY_DB_PASSWORD`、`AI_GATEWAY_API_KEY_HMAC_PEPPER`；pepper 至少 32 字节。增加独立 Gateway DB host/port/user/name、pool、worker、queue、cache、poll 和 Secret 目录配置。
- 新增 `AiGatewayAdmin` 目标：
  - `migrate --dir migrations/gateway`
  - `apply-config --file config.json`
  - `issue-key --tenant <slug> --policy <slug> --name <name> [--expires-at <RFC3339>]`
  - `set-key-status --key-id <id> --status active|disabled`
  - `bump-version`
- `apply-config` 事务化 upsert Provider、Endpoint、Credential 引用、Tenant、Logical Model、Policy 和 Mapping；省略的记录不自动删除。有效变更只递增一次配置版本。
- `issue-key` 只在 stdout 输出一次完整 Key；数据库事务提交后递增版本。Pepper 轮换通过重新签发 Key 完成。
- 更新 setup、README、`.env.example`、migration 文档和 ADR 0003；setup 只使用显式或交互式 Gateway DB 密码，不创建固定凭据。

## Test Plan

- CTest 启动临时 MariaDB、应用真实 migration、调用 `AiGatewayAdmin` 建立两个租户，再运行现有 HTTP Client -> Gateway -> Mock Provider seam。
- 验证两个租户可使用相同逻辑模型名但得到不同 upstream model 和 Provider Secret；不能访问对方独有模型。
- 覆盖 missing、wrong、malformed、disabled、expired Key，以及 disabled Tenant/Policy 和 protocol/model/provider deny。
- 验证 `/v1/models` 按 Key/Policy 过滤，非流式和完整 SSE、背压、取消、超时及 Keep-Alive 测试全部保留。
- 通过数据库查询或 dump 确认完整 Gateway Key、Provider Key、Prompt 和响应正文均不存在；日志同样脱敏。
- 修改 Policy、Mapping 或 Key 状态并递增版本，验证无需重启且在一个轮询周期内生效。
- 停止 MariaDB 后验证 `/healthz=200`、`/readyz=503`、缓存命中也不能继续认证；重启后自动恢复。
- 覆盖 migration 重复执行、checksum 冲突、schema 版本不匹配、缺失 Secret、重复启用 Mapping、SQL 注入字符串和慢 DB 不阻塞 `/healthz`。
- 完成独立 Gateway 构建、Legacy 全目标构建、普通 CTest、ASan/UBSan；Aether 固定提交仅用于行为比对，不进入依赖或产物。

## Assumptions

- Phase 3 可持久化多个 Provider 资源，但一次请求仍只有一个合法 Mapping，不做排序、重试或 failover。
- Provider Secret 文件轮换后由管理员执行 `bump-version`；环境变量引用需要重启进程。
- 不建立 Redis、Usage、request attempts、quota、price、conversation 或聊天迁移表。
- 不保留 Phase 2 静态认证 fallback。
