#ifndef AI_GATEWAY_REPOSITORY_HPP
#define AI_GATEWAY_REPOSITORY_HPP

#include "gateway/gateway.hpp"
#include "gateway/governance.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace ai_gateway
{
/* MySQL 查询出的逻辑模型及其 Policy 授权状态。 */
struct RepositoryModel
{
    std::uint64_t id = 0;
    std::string protocol;
    std::string name;
    std::string status;
    bool granted = false;
    std::string scheduling_mode = "fixed_order";
    std::size_t max_attempts = 3;
};

/* MySQL 查询出的 Model Mapping、Provider/Endpoint/Credential 状态和价格信息。 */
struct RepositoryMapping
{
    std::uint64_t mapping_id = 0;
    std::uint64_t logical_model_id = 0;
    std::string mapping_name;
    std::uint64_t provider_id = 0;
    std::uint64_t endpoint_id = 0;
    std::uint64_t credential_id = 0;
    std::uint64_t endpoint_provider_id = 0;
    std::uint64_t credential_provider_id = 0;
    std::string provider_status;
    std::string endpoint_protocol;
    std::string endpoint_url;
    std::string endpoint_status;
    std::string secret_ref;
    std::string credential_status;
    std::string upstream_model;
    std::uint16_t priority = 100;
    std::string mapping_status;
    std::string provider_slug;
    std::string credential_name;
    std::optional<QuotaPolicy> credential_quota;
};

/* 当前生效的 Provider/upstream model 价格版本。 */
struct RepositoryPrice
{
    std::uint64_t id = 0;
    std::uint64_t provider_id = 0;
    std::string upstream_model;
    std::string version;
    std::int64_t effective_at_epoch = 0;
    std::uint64_t input_per_million_microusd = 0;
    std::uint64_t cached_input_per_million_microusd = 0;
    std::uint64_t output_per_million_microusd = 0;
};

/*
 * 一次一致性 Auth Snapshot 查询的扁平结果。
 * Repository 负责关联表，RuntimeState 负责 HMAC 匹配、Secret 解析和领域对象合并。
 */
struct RepositoryAccessRecord
{
    std::uint64_t config_version = 0;
    std::uint64_t database_key_id = 0;
    std::uint64_t database_tenant_id = 0;
    std::string public_key_id;
    std::string key_hmac;
    std::string key_status;
    std::int64_t expires_at_epoch = 0;
    bool has_expiry = false;
    std::string tenant_slug;
    std::string tenant_status;
    std::string policy_status;
    std::unordered_set<std::string> protocols;
    std::unordered_set<std::uint64_t> providers;
    std::vector<RepositoryModel> models;
    std::vector<RepositoryMapping> mappings;
    std::optional<QuotaPolicy> tenant_quota;
    std::optional<QuotaPolicy> api_key_quota;
    std::vector<RepositoryPrice> prices;
};

/* request_attempts.started 的内部 ID 关联和候选资源身份。 */
struct AttemptStart
{
    std::string attempt_id;
    std::string request_id;
    std::size_t attempt_number = 0;
    std::uint64_t tenant_id = 0;
    std::uint64_t api_key_id = 0;
    std::uint64_t logical_model_id = 0;
    std::uint64_t mapping_id = 0;
    std::uint64_t provider_id = 0;
    std::uint64_t endpoint_id = 0;
    std::uint64_t credential_id = 0;
    bool stream = false;
};

/* Provider Usage、价格和计费质量的统一表示。 */
struct UsageAccounting
{
    std::optional<std::uint64_t> input_tokens;
    std::optional<std::uint64_t> cached_input_tokens;
    std::optional<std::uint64_t> output_tokens;
    std::string usage_quality = "unknown";
    std::uint64_t model_price_id = 0;
    std::optional<std::uint64_t> cost_microusd;
    std::string cost_quality = "unknown";
};

/* request_attempts 的幂等终态更新内容。 */
struct AttemptFinish
{
    std::string attempt_id;
    std::string state;
    long provider_status = 0;
    std::string error_class;
    bool retryable = false;
    bool possible_duplicate_cost = false;
    std::size_t response_bytes = 0;
    std::uint64_t duration_ms = 0;
    std::string provider_request_id;
    std::optional<std::uint64_t> first_byte_ms;
    UsageAccounting usage;
};

/* 请求级 Redis RPM/并发与 MySQL 日/月预算准入参数。 */
struct RequestAdmission
{
    std::string request_id;
    std::uint64_t tenant_id = 0;
    std::uint64_t api_key_id = 0;
    std::uint64_t logical_model_id = 0;
    std::string protocol;
    bool stream = false;
    std::size_t request_bytes = 0;
    std::size_t max_attempts = 1;
    std::optional<QuotaPolicy> tenant_quota;
    std::optional<QuotaPolicy> api_key_quota;
};

enum class RequestAdmissionStatus
{
    admitted,
    budget_exceeded,
    unavailable
};

struct RequestFinish
{
    std::string request_id;
    std::string state;
    std::uint64_t final_mapping_id = 0;
    std::uint64_t final_provider_id = 0;
    std::uint64_t final_endpoint_id = 0;
    std::uint64_t final_credential_id = 0;
    std::size_t attempt_count = 0;
    std::size_t billable_attempt_count = 0;
    std::size_t max_attempts = 1;
    int http_status = 0;
    std::string error_class;
    std::size_t response_bytes = 0;
    std::uint64_t duration_ms = 0;
    UsageAccounting usage;
};

/* 非计费健康探测的数据库配置及其关联候选。 */
struct RepositoryHealthCheck
{
    std::uint64_t id = 0;
    std::uint64_t config_version = 0;
    std::uint64_t tenant_id = 0;
    std::uint64_t provider_id = 0;
    std::uint64_t endpoint_id = 0;
    std::uint64_t credential_id = 0;
    std::string provider_slug;
    std::string credential_name;
    std::string method;
    std::string url;
    long interval_ms = 30000;
    long timeout_ms = 2000;
    std::string secret_ref;
    std::optional<QuotaPolicy> credential_quota;
    std::vector<RepositoryMapping> mappings;
};

class GatewayRepository
{
public:
    /* Repository 是 RuntimeState 的阻塞 SQL seam，生产实现使用 prepared statements。 */
    virtual ~GatewayRepository() = default;
    /* 读取 gateway_config_versions 当前版本。 */
    virtual std::uint64_t config_version() = 0;
    /* 按 Key display prefix 加载一致性认证候选。 */
    virtual std::vector<RepositoryAccessRecord> load_access_candidates(
        const std::string &display_prefix) = 0;
    /* 创建 Usage started 并预留预算。 */
    virtual RequestAdmissionStatus admit_request(const RequestAdmission &request) = 0;
    /* 幂等结算 Usage、预算和最终候选。 */
    virtual void finish_request(const RequestFinish &request) = 0;
    /* 加载显式健康探测配置。 */
    virtual std::vector<RepositoryHealthCheck> load_health_checks() = 0;
    /* 协调超时 started Usage 为 abandoned/unknown。 */
    virtual void reconcile_abandoned_requests() = 0;
    /* 写入唯一 Attempt started。 */
    virtual void begin_attempt(const AttemptStart &attempt) = 0;
    /* 更新 Attempt 终态和 Usage。 */
    virtual void finish_attempt(const AttemptFinish &attempt) = 0;
};

class MySqlGatewayRepository final : public GatewayRepository
{
public:
    explicit MySqlGatewayRepository(const GatewayConfig &config);
    ~MySqlGatewayRepository() override;

    std::uint64_t config_version() override;
    std::vector<RepositoryAccessRecord> load_access_candidates(
        const std::string &display_prefix) override;
    RequestAdmissionStatus admit_request(const RequestAdmission &request) override;
    void finish_request(const RequestFinish &request) override;
    std::vector<RepositoryHealthCheck> load_health_checks() override;
    void reconcile_abandoned_requests() override;
    void begin_attempt(const AttemptStart &attempt) override;
    void finish_attempt(const AttemptFinish &attempt) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
