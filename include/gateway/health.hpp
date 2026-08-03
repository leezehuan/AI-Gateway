#ifndef AI_GATEWAY_HEALTH_HPP
#define AI_GATEWAY_HEALTH_HPP

#include <memory>

namespace ai_gateway
{
class RuntimeState;
class RoutingRuntime;
class GovernanceRuntime;
class ProviderTransport;
class MetricsRegistry;

class HealthProbeRuntime
{
public:
    HealthProbeRuntime(RuntimeState &runtime,
                       RoutingRuntime &routing,
                       GovernanceRuntime &governance,
                       ProviderTransport &transport,
                       MetricsRegistry &metrics);
    ~HealthProbeRuntime();

    HealthProbeRuntime(const HealthProbeRuntime &) = delete;
    HealthProbeRuntime &operator=(const HealthProbeRuntime &) = delete;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
