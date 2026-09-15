#ifndef AI_GATEWAY_RUNTIME_HPP
#define AI_GATEWAY_RUNTIME_HPP

#include "gateway/gateway.hpp"
#include "gateway/governance.hpp"
#include "gateway/repository.hpp"

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ai_gateway
{
/*
 * Model Price：一个不可原地修改的价格版本。
 * 所有金额使用整数 micro-USD，避免把浮点误差带入预算和 Usage 结算。
 */
struct ModelPrice
{
    /* 数据库 model_prices.id，用于 Attempt/Usage 审计中指向具体价格版本。 */
    std::uint64_t id = 0;
    /* 管理员提供的版本字符串；相同 Provider/model/version 的费率不可原地覆盖。 */
    std::string version;
    /* UTC 生效时间的 Unix 秒；结算时选择不晚于当前时间的最新版本。 */
    std::int64_t effective_at_epoch = 0;
    /* 每一百万个未命中缓存的输入 token 的 micro-USD 单价。 */
    std::uint64_t input_per_million_microusd = 0;
    /* 每一百万个缓存命中的输入 token 的 micro-USD 单价。 */
    std::uint64_t cached_input_per_million_microusd = 0;
    /* 每一百万个输出 token 的 micro-USD 单价。 */
    std::uint64_t output_per_million_microusd = 0;
};

/*
 * Model Target：一次请求可以尝试的单个 Provider 候选。
 *
 * mapping/provider/endpoint/credential id 用于审计；fingerprint 用于 Redis 路由状态。
 * provider_api_key 只存在运行时内存，不应写入日志、指标或数据库。
 */
struct ModelTarget
{
    /* model_mappings.id；同一个逻辑模型的不同上游候选通过它区分。 */
    std::uint64_t mapping_id = 0;
    /* Provider 内部 ID，只用于审计、权限和治理，不向客户端暴露。 */
    std::uint64_t provider_id = 0;
    /* Provider Endpoint 内部 ID，表示 URL 和协议配置的数据库身份。 */
    std::uint64_t endpoint_id = 0;
    /* Credential 内部 ID，表示 Secret 引用的数据库身份。 */
    std::uint64_t credential_id = 0;
    /* 管理员可读的 Mapping 名称，用于固定顺序排序和日志候选标识。 */
    std::string mapping_name;
    /* 数值越小优先级越高；同优先级再按健康和调度模式决定顺序。 */
    std::uint16_t priority = 100;
    /* 不含 URL/Secret/正文的稳定候选摘要，用作 Redis health/affinity key。 */
    std::string fingerprint;
    /* 由管理员配置的 Provider URL，客户端不能覆盖。 */
    std::string provider_url;
    /* Secret 引用解析后的内存值；生命周期仅覆盖当前运行时快照。 */
    std::string provider_api_key;
    /* 发往 Provider 的 upstream model；客户端只能看到逻辑模型名称。 */
    std::string upstream_model;
    /* 脱敏的 Provider slug，用于指标和结构化日志标签。 */
    std::string provider_slug;
    /* Credential 可读名称，用于运维排障，不是 Secret 本身。 */
    std::string credential_name;
    /* Credential 层配额；空值表示该 Credential 不限制。 */
    std::optional<QuotaPolicy> credential_quota;
    /* 当前候选上可用的价格版本集合，结算时按生效时间选择。 */
    std::vector<ModelPrice> prices;
};

/*
 * Model Access：某个租户逻辑模型在一个 Auth Snapshot 中的授权结果。
 * candidates 只包含静态配置有效且 Policy 允许的候选，临时熔断由 RoutingRuntime 再排序。
 */
struct ModelAccess
{
    /* logical_models.id；用于请求级 Usage、Attempt 外键。 */
    std::uint64_t database_id = 0;
    /* 租户看到的逻辑模型名称，例如 gpt-5.6-terra。 */
    std::string name;
    /* responses、chat_completions 或 anthropic_messages。 */
    std::string protocol;
    /* fixed_order、load_balance 或 cache_affinity。 */
    std::string scheduling_mode = "fixed_order";
    /* 该请求最多可以调用多少个不同 Candidate，范围由 Admin 限制。 */
    std::size_t max_attempts = 3;
    /* Policy 是否显式授予该逻辑模型。 */
    bool model_granted = false;
    /* Policy 是否拒绝了候选所属 Provider。 */
    bool provider_denied = false;
    /* Mapping、Endpoint、Credential 或 Secret 组合是否无法形成可用候选。 */
    bool configuration_unavailable = false;
    /* 静态有效 Mapping 数；用于区分模型无授权与临时 Redis 熔断。 */
    std::size_t active_mapping_count = 0;
    /* 静态授权并通过配置校验的候选；临时健康状态不在快照中修改。 */
    std::vector<ModelTarget> candidates;
};

/*
 * Auth Snapshot：一次鉴权和模型解析的不可变快照。
 *
 * 请求开始后使用同一份快照，即使后台配置版本发生变化，也不会中途改变该请求的授权结论。
 */
struct AuthSnapshot
{
    /* 读取快照时的 gateway_config_versions.version。 */
    std::uint64_t config_version = 0;
    /* 数据库 Tenant ID。 */
    std::uint64_t database_tenant_id = 0;
    /* 数据库 API Key ID，而不是客户端携带的完整 Key。 */
    std::uint64_t database_api_key_id = 0;
    /* 脱敏租户 slug，可用于日志和指标标签。 */
    std::string tenant_slug;
    /* 公开 API Key ID；完整 Key/HMAC 不进入该快照之外的可观测输出。 */
    std::string public_api_key_id;
    /* Policy 允许的协议名称集合。 */
    std::unordered_set<std::string> protocols;
    /* 按 protocol + logical model name 建立的授权模型索引。 */
    std::unordered_map<std::string, ModelAccess> models;
    /* Tenant 层请求 RPM/并发/预算策略。 */
    std::optional<QuotaPolicy> tenant_quota;
    /* API Key 层请求 RPM/并发/预算策略。 */
    std::optional<QuotaPolicy> api_key_quota;
};

enum class AuthStatus
{
    authorized,
    invalid_api_key,
    access_disabled,
    unavailable
};

struct AuthResult
{
    /* authorized 时 snapshot 非空；其他状态只携带失败分类。 */
    AuthStatus status = AuthStatus::unavailable;
    /* 认证成功后的 immutable shared snapshot，多个异步回调可安全共享。 */
    std::shared_ptr<const AuthSnapshot> snapshot;
};

/* 显式配置的非计费 Provider 健康探测目标。 */
struct HealthCheckTarget
{
    /* provider_health_checks.id，用于跨节点 probe lease。 */
    std::uint64_t id = 0;
    /* 探测配置读取时的版本；版本变化后旧目标不再继续执行。 */
    std::uint64_t config_version = 0;
    /* 探测使用的 Credential ID。 */
    std::uint64_t credential_id = 0;
    /* 仅用于日志和指标的 Provider 名称。 */
    std::string provider_slug;
    /* 仅用于治理和指标的 Credential 名称。 */
    std::string credential_name;
    /* 目前管理员只能配置 GET 或 HEAD，不能用计费 POST 作为探测。 */
    std::string method;
    /* 显式配置的健康 URL，不由客户端请求影响。 */
    std::string url;
    /* Secret 引用解析出的内存值。 */
    std::string provider_api_key;
    /* 两次探测的最小间隔。 */
    long interval_ms = 30000;
    /* 单次探测总超时。 */
    long timeout_ms = 2000;
    /* 探测同样受 Credential RPM/并发约束。 */
    std::optional<QuotaPolicy> credential_quota;
    /* 该 Provider/Endpoint/Credential 下需要同步健康状态的候选摘要。 */
    std::vector<std::string> candidate_fingerprints;
};

/* 健康探测线程加载配置后的结果；available=false 时不应继续使用旧探测目标。 */
struct HealthCheckResult
{
    /* 数据库和 Secret 解析均成功时为 true；false 时调用方必须丢弃旧目标。 */
    bool available = false;
    /* 当前一次配置快照中的探测目标。 */
    std::vector<HealthCheckTarget> targets;
};

/*
 * RuntimeState：数据库访问、HMAC 鉴权、Auth Snapshot 缓存和配置版本轮询的异步外观。
 *
 * Drogon 事件循环只提交回调，不直接执行阻塞 SQL；具体线程池和缓存细节隐藏在 Impl 中。
 */
class RuntimeState
{
public:
    using AuthCallback = std::function<void(AuthResult)>;
    using AuditCallback = std::function<void(bool)>;
    using AdmissionCallback = std::function<void(RequestAdmissionStatus)>;
    using HealthCheckCallback = std::function<void(HealthCheckResult)>;

    RuntimeState(GatewayConfig config, GatewayRepository &repository);
    ~RuntimeState();

    RuntimeState(const RuntimeState &) = delete;
    RuntimeState &operator=(const RuntimeState &) = delete;

    /* 异步验证 Gateway API Key，并返回不可变 Auth Snapshot。 */
    void authenticate(std::string api_key, AuthCallback callback);
    /* 异步创建请求级 Usage/预算准入记录。 */
    void admit_request(RequestAdmission request, AdmissionCallback callback);
    /* 异步幂等结算请求级 Usage、预算和租户/API Key lease。 */
    void finish_request(RequestFinish request, AuditCallback callback);
    /* 异步加载健康探测配置；数据库不可用时返回 unavailable。 */
    void load_health_checks(HealthCheckCallback callback);
    /* 在调用 Provider 前写入 started Attempt 审计。 */
    void begin_attempt(AttemptStart attempt, AuditCallback callback);
    /* 写入 Attempt 终态；失败时由 RuntimeState 负责有限重试并 fail closed。 */
    void finish_attempt(AttemptFinish attempt, AuditCallback callback);
    /* 返回数据库、版本轮询和 worker 队列是否满足 readiness。 */
    bool ready() const;
    /* 返回启动时解析出的只读配置。 */
    const GatewayConfig &config() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
