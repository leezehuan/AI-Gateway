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
struct QuotaPolicy
{
    std::uint64_t id = 0;
    std::string status;
    std::optional<std::uint64_t> rpm;
    std::optional<std::uint64_t> concurrency;
    std::optional<std::uint64_t> daily_budget_microusd;
    std::optional<std::uint64_t> monthly_budget_microusd;
    std::uint64_t reservation_per_attempt_microusd = 0;
};

struct GovernanceScope
{
    std::string kind;
    std::uint64_t id = 0;
    std::optional<std::uint64_t> rpm;
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
    std::string id;
    std::vector<GovernanceScope> scopes;
};

struct GovernanceResult
{
    GovernanceStatus status = GovernanceStatus::unavailable;
    long retry_after_ms = 0;
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

    void admit(std::vector<GovernanceScope> scopes,
               std::function<void()> lease_lost,
               AdmissionCallback callback);
    void rollback(std::shared_ptr<const GovernancePermit> permit,
                  CompletionCallback callback = {});
    void release(std::shared_ptr<const GovernancePermit> permit,
                 CompletionCallback callback = {});
    void acquire_probe_lease(std::string probe_id,
                             long lease_ttl_ms,
                             ProbeLeaseCallback callback);
    bool ready() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
