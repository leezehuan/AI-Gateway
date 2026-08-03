#ifndef AI_GATEWAY_METRICS_HPP
#define AI_GATEWAY_METRICS_HPP

#include "gateway/repository.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace ai_gateway
{
class MetricsRegistry
{
public:
    MetricsRegistry();
    ~MetricsRegistry();

    MetricsRegistry(const MetricsRegistry &) = delete;
    MetricsRegistry &operator=(const MetricsRegistry &) = delete;

    void request_completed(const std::string &tenant,
                           const std::string &protocol,
                           const std::string &logical_model,
                           int status,
                           const std::string &error_class,
                           std::uint64_t duration_ms,
                           std::size_t failovers,
                           std::size_t backpressure_pauses);
    void attempt_completed(const std::string &tenant,
                           const std::string &protocol,
                           const std::string &logical_model,
                           const std::string &provider,
                           const std::string &credential,
                           const std::string &state,
                           const std::string &error_class,
                           const UsageAccounting &usage,
                           std::optional<std::uint64_t> first_byte_ms,
                           std::uint64_t duration_ms);
    void governance_rejected(const std::string &reason);
    void health_probe_completed(const std::string &provider,
                                const std::string &credential,
                                bool success);
    void upstream_started();
    void upstream_finished();
    void stream_started();
    void stream_finished();
    void set_dependency_readiness(bool mysql_ready, bool redis_ready);
    std::string render() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
