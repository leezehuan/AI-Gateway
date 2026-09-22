#ifndef AI_GATEWAY_ROUTING_HPP
#define AI_GATEWAY_ROUTING_HPP

#include "gateway/runtime.hpp"

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace ai_gateway
{
/* Redis 中保存的单个候选健康分、熔断和 half-open probe 状态。 */
struct CandidateRoutingState
{
    int health_score = 100;
    bool circuit_open = false;
    bool half_open_probe = false;
};

struct RoutingSnapshot
{
    std::optional<std::string> affinity_target;
    std::unordered_map<std::string, CandidateRoutingState> candidates;
};

class RoutingStore
{
public:
    /* RoutingStore 是 Redis 路由状态 seam；不可用时不应退化到节点本地状态。 */
    virtual ~RoutingStore() = default;
    virtual bool ping() = 0;
    virtual std::optional<RoutingSnapshot> snapshot(
        const std::string &affinity_key,
        const std::vector<std::string> &candidate_fingerprints,
        std::int64_t now_ms,
        long probe_lease_ms) = 0;
    virtual bool record(const std::string &candidate_fingerprint,
                        const std::string &affinity_key,
                        bool success,
                        bool retryable_failure,
                        long retry_after_ms,
                        std::int64_t now_ms,
                        unsigned failure_threshold = 0) = 0;
};

class HiredisRoutingStore final : public RoutingStore
{
public:
    explicit HiredisRoutingStore(GatewayConfig config);
    ~HiredisRoutingStore() override;

    bool ping() override;
    std::optional<RoutingSnapshot> snapshot(
        const std::string &affinity_key,
        const std::vector<std::string> &candidate_fingerprints,
        std::int64_t now_ms,
        long probe_lease_ms) override;
    bool record(const std::string &candidate_fingerprint,
                const std::string &affinity_key,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                std::int64_t now_ms,
                unsigned failure_threshold = 0) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

struct RouteRequest
{
    std::string request_id;
    std::uint64_t config_version = 0;
    std::string public_api_key_id;
    std::string protocol;
    std::string logical_model;
    std::string scheduling_mode;
    std::size_t max_attempts = 3;
    std::string client_family;
    std::string session_hint;
    std::vector<ModelTarget> candidates;
};

enum class RouteStatus
{
    ready,
    unavailable,
    no_candidates
};

struct RoutePlan
{
    RouteStatus status = RouteStatus::unavailable;
    std::string affinity_key;
    std::vector<ModelTarget> candidates;
};

class RoutingRuntime
{
public:
    using PlanCallback = std::function<void(RoutePlan)>;
    using FeedbackCallback = std::function<void(bool)>;

    RoutingRuntime(GatewayConfig config, RoutingStore &store);
    ~RoutingRuntime();

    RoutingRuntime(const RoutingRuntime &) = delete;
    RoutingRuntime &operator=(const RoutingRuntime &) = delete;


    void plan(RouteRequest request, PlanCallback callback);
    /* 返回路由 Redis 是否 ready。 */
    void record(std::string candidate_fingerprint,
                std::string affinity_key,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                FeedbackCallback callback);
    void record_health_probe(std::string candidate_fingerprint,
                             bool success,
                             FeedbackCallback callback);
    bool ready() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}

#endif
