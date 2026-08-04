#ifndef AI_GATEWAY_GOVERNANCE_HPP
#define AI_GATEWAY_GOVERNANCE_HPP

#include "gateway/gateway.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace ai_gateway
{
/*
 * Quota Policy：可绑定到 Tenant、API Key 或 Credential 的限制集合。
 * 空的 optional 表示该层不限制；金额使用整数 micro-USD。
 */
struct QuotaPolicy
{
    /* quota_policies.id，用于审计绑定关系。 */
    std::uint64_t id = 0;
    /* active/disabled；Runtime 读取后仍会检查状态。 */
    std::string status;
    /* 60 秒滚动窗口内允许的请求/Attempt 数；空值表示无限制。 */
    std::optional<std::uint64_t> rpm;
    /* 同时持有的 Redis 并发 lease 数；空值表示无限制。 */
    std::optional<std::uint64_t> concurrency;
    /* UTC 自然日预算上限，单位 micro-USD。 */
    std::optional<std::uint64_t> daily_budget_microusd;
    /* UTC 自然月预算上限，单位 micro-USD。 */
    std::optional<std::uint64_t> monthly_budget_microusd;
    /* 每个候选 Attempt 的兼容性预算预留额。 */
    std::uint64_t reservation_per_attempt_microusd = 0;
};

/* Redis 原子脚本需要同时检查的一个租户、Key 或 Credential 范围。 */
struct GovernanceScope
{
    /* tenant、api_key 或 credential；决定 Redis key namespace。 */
    std::string kind;
    /* 对应数据库内部 ID。 */
    std::uint64_t id = 0;
    /* 该作用域的滚动 RPM 限制。 */
    std::optional<std::uint64_t> rpm;
    /* 该作用域的并发 lease 限制。 */
    std::optional<std::uint64_t> concurrency;
};

enum class GovernanceStatus
{
    admitted,
    rate_limited,
    concurrency_limited,
    budget_exceeded,
    credential_limited,
    unavailable
};

struct GovernancePermit
{
    /* 一次请求准入生成的随机 lease ID，Redis 用它关联成员和续租。 */
    std::string id;
    /* 本次 permit 同时覆盖的 Tenant/API Key/Credential 作用域快照。 */
    std::vector<GovernanceScope> scopes;
};

struct GovernanceResult
{
    /* admitted 表示所有 scope 都成功；失败状态用于选择 429/503 错误。 */
    GovernanceStatus status = GovernanceStatus::unavailable;
    /* Redis 脚本计算出的建议等待时间，客户端可收到有限的 Retry-After。 */
    long retry_after_ms = 0;
    /* 只有 admitted 时存在；其他状态不可用于 release/renew。 */
    std::shared_ptr<const GovernancePermit> permit;
};

struct GovernanceStoreResult
{
    GovernanceStatus status = GovernanceStatus::unavailable;
    long retry_after_ms = 0;
};

class GovernanceStore
{
public:
    /* Redis 是跨节点治理的外部 seam，失败时请求必须 fail closed。 */
    virtual ~GovernanceStore() = default;
    virtual bool ping() = 0;
    virtual GovernanceStoreResult reserve(const std::string &permit_id,
                                          const std::vector<GovernanceScope> &scopes,
                                          long lease_ttl_ms) = 0;
    virtual bool rollback(const std::string &permit_id,
                          const std::vector<GovernanceScope> &scopes) = 0;
    virtual bool release(const std::string &permit_id,
                         const std::vector<GovernanceScope> &scopes) = 0;
    virtual bool renew(const std::string &permit_id,
                       const std::vector<GovernanceScope> &scopes,
                       long lease_ttl_ms) = 0;
    virtual std::optional<bool> acquire_probe_lease(const std::string &probe_id,
                                                    long lease_ttl_ms) = 0;
};

class HiredisGovernanceStore final : public GovernanceStore
{
public:
    explicit HiredisGovernanceStore(GatewayConfig config);
    ~HiredisGovernanceStore() override;

    bool ping() override;
    GovernanceStoreResult reserve(const std::string &permit_id,
                                  const std::vector<GovernanceScope> &scopes,
                                  long lease_ttl_ms) override;
    bool rollback(const std::string &permit_id,
                  const std::vector<GovernanceScope> &scopes) override;
    bool release(const std::string &permit_id,
                 const std::vector<GovernanceScope> &scopes) override;
    bool renew(const std::string &permit_id,
               const std::vector<GovernanceScope> &scopes,
               long lease_ttl_ms) override;
    std::optional<bool> acquire_probe_lease(const std::string &probe_id,
                                            long lease_ttl_ms) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class GovernanceRuntime
{
public:
    using AdmissionCallback = std::function<void(GovernanceResult)>;
    using CompletionCallback = std::function<void(bool)>;
    using ProbeLeaseCallback = std::function<void(std::optional<bool>)>;

    GovernanceRuntime(GatewayConfig config, GovernanceStore &store);
    ~GovernanceRuntime();

    GovernanceRuntime(const GovernanceRuntime &) = delete;
    GovernanceRuntime &operator=(const GovernanceRuntime &) = delete;

    /* 异步执行滚动 RPM 和并发 lease 的原子准入。 */
    void admit(std::vector<GovernanceScope> scopes,
               std::function<void()> lease_lost,
               AdmissionCallback callback);
    /* Provider 尚未调用时补偿删除本次准入成员。 */
    void rollback(std::shared_ptr<const GovernancePermit> permit,
                  CompletionCallback callback = {});
    /* 正常终态释放并发 lease；RPM 记录自然过期。 */
    void release(std::shared_ptr<const GovernancePermit> permit,
                 CompletionCallback callback = {});
    /* 为跨节点健康探测竞争一个短 TTL 的独占 lease。 */
    void acquire_probe_lease(std::string probe_id,
                             long lease_ttl_ms,
                             ProbeLeaseCallback callback);
    /* 返回 Redis worker、连接和队列是否允许继续接收治理请求。 */
    bool ready() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
