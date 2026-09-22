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
    /*
     * 启动进程级主动健康探测器。
     * 探测器从 RuntimeState 读取管理员显式配置，使用 Governance 的 Credential 配额，
     * 通过 ProviderTransport 发出非计费 GET/HEAD，并把结果反馈给共享 RoutingRuntime。
     */
    HealthProbeRuntime(RuntimeState &runtime,
                       RoutingRuntime &routing,
                       GovernanceRuntime &governance,
                       ProviderTransport &transport,
                       MetricsRegistry &metrics);
    /* 停止探测线程并等待其完成，保证引用的外部模块仍存活到线程退出。 */
    ~HealthProbeRuntime();

    HealthProbeRuntime(const HealthProbeRuntime &) = delete;
    HealthProbeRuntime &operator=(const HealthProbeRuntime &) = delete;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
}

#endif
