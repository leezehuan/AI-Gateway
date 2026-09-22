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

    std::uint64_t id = 0;
    /* 管理员提供的版本字符串；相同 Provider/model/version 的费率不可原地覆盖。 */
    std::string version;

    std::int64_t effective_at_epoch = 0;

    std::uint64_t input_per_million_microusd = 0;

    std::uint64_t cached_input_per_million_microusd = 0;

    std::uint64_t output_per_million_microusd = 0;
};

struct ModelTarget
{

    std::uint64_t mapping_id = 0;
    /* Provider 内部 ID，只用于审计、权限和治理，不向客户端暴露。 */
    std::uint64_t provider_id = 0;
    /* Provider Endpoint 内部 ID，表示 URL 和协议配置的数据库身份。 */
    std::uint64_t endpoint_id = 0;
    /* Credential 内部 ID，表示 Secret 引用的数据库身份。 */
    std::uint64_t credential_id = 0;

    std::string mapping_name;

    std::uint16_t priority = 100;
    /* 不含 URL/Secret/正文的稳定候选摘要，用作 Redis health/affinity key。 */
    std::string fingerprint;
    /* 由管理员配置的 Provider URL，客户端不能覆盖。 */
    std::string provider_url;
    /* Secret 仅驻留当前运行时快照，不写入日志、指标或数据库。 */
    std::string provider_api_key;
    /* 发往 Provider 的 upstream model；客户端只能看到逻辑模型名称。 */
    std::string upstream_model;
    /* 脱敏的 Provider slug，用于指标和结构化日志标签。 */
    std::string provider_slug;
    /* Credential 可读名称，用于运维排障，不是 Secret 本身。 */
    std::string credential_name;
    /* Credential 层配额；空值表示该 Credential 不限制。 */
    std::optional<QuotaPolicy> credential_quota;

    std::vector<ModelPrice> prices;
};

struct ModelAccess
{

    std::uint64_t database_id = 0;

    std::string name;

    std::string protocol;

    std::string scheduling_mode = "fixed_order";

    std::size_t max_attempts = 3;

    bool model_granted = false;
    /* Policy 是否拒绝了候选所属 Provider。 */
    bool provider_denied = false;
    /* Mapping、Endpoint、Credential 或 Secret 组合是否无法形成可用候选。 */
    bool configuration_unavailable = false;
    /* 静态有效 Mapping 数；用于区分模型无授权与临时 Redis 熔断。 */
    std::size_t active_mapping_count = 0;

    std::vector<ModelTarget> candidates;
};

struct AuthSnapshot
{

    std::uint64_t config_version = 0;

    std::uint64_t database_tenant_id = 0;

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

    AuthStatus status = AuthStatus::unavailable;
    /* 认证成功后的 immutable shared snapshot，多个异步回调可安全共享。 */
    std::shared_ptr<const AuthSnapshot> snapshot;
};

/* 显式配置的非计费 Provider 健康探测目标。 */
struct HealthCheckTarget
{
    /* provider_health_checks.id，用于跨节点 probe lease。 */
    std::uint64_t id = 0;

    std::uint64_t config_version = 0;
    /* 探测使用的 Credential ID。 */
    std::uint64_t credential_id = 0;
    /* 仅用于日志和指标的 Provider 名称。 */
    std::string provider_slug;
    /* 仅用于治理和指标的 Credential 名称。 */
    std::string credential_name;

    std::string method;

    std::string url;
    /* Secret 引用解析出的内存值。 */
    std::string provider_api_key;

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

    std::vector<HealthCheckTarget> targets;
};

/*
 * RuntimeState：数据库访问、HMAC 鉴权、Auth Snapshot 缓存和配置版本轮询的异步外观。
 *
 * Beast 事件循环只提交回调，不直接执行阻塞 SQL；具体线程池和缓存细节隐藏在 Impl 中。
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


    void authenticate(std::string api_key, AuthCallback callback);
    /* 异步创建请求级 Usage/预算准入记录。 */
    void admit_request(RequestAdmission request, AdmissionCallback callback);
    /* 异步幂等结算请求级 Usage、预算和租户/API Key lease。 */
    void finish_request(RequestFinish request, AuditCallback callback);

    void load_health_checks(HealthCheckCallback callback);
    /* 在调用 Provider 前写入 started Attempt 审计。 */
    void begin_attempt(AttemptStart attempt, AuditCallback callback);
    /* 写入 Attempt 终态；失败时由 RuntimeState 负责有限重试并 fail closed。 */
    void finish_attempt(AttemptFinish attempt, AuditCallback callback);

    bool ready() const;

    const GatewayConfig &config() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}

#endif
