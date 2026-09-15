#include "gateway/curl_transport.hpp"
#include "gateway/gateway.hpp"
#include "gateway/governance.hpp"
#include "gateway/health.hpp"
#include "gateway/http_server.hpp"
#include "gateway/lifecycle.hpp"
#include "gateway/metrics.hpp"
#include "gateway/repository.hpp"
#include "gateway/runtime.hpp"
#include "gateway/routing.hpp"

#include <exception>
#include <iostream>

/*
 * 函数名直译：主函数。
 *
 * 通俗说：这是 AiGateway 进程的装配入口。它读取环境配置，把数据库、Redis、curl、治理、路由、
 * 健康探测和 HTTP 服务按依赖顺序连接起来，然后进入 Drogon 事件循环。
 *
 * 专业说法：main 是 composition root，不承载 HTTP 业务逻辑。对象声明顺序也决定销毁逆序，
 * 因此 HttpServer 先停止接入，随后 Gateway 相关运行时析构，最后 curl transport 关闭 worker。
 *
 * 返回值：正常停止返回 0；配置或启动过程中抛出异常时向 stderr 输出脱敏概要并返回 1。
 *
 * 实现方法：
 * 1. 从环境加载并校验不可变 GatewayConfig。
 * 2. 构造 MySQL Repository、认证 Runtime、Redis 路由/治理运行时、节点生命周期和指标。
 * 3. 构造唯一的 curl multi transport、健康探测和核心 AiGateway。
 * 4. 记录启动事件，运行 HTTP 服务器；服务器退出后显式关闭 transport。
 *
 * 注意：Provider 密钥、数据库密码和 Redis 密码绝不写入启动日志或异常输出。
 */
int main()
{
    try
    {
        const ai_gateway::GatewayConfig config = ai_gateway::GatewayConfig::from_env();
        config.validate();
        ai_gateway::MySqlGatewayRepository repository(config);
        ai_gateway::RuntimeState runtime(config, repository);
        ai_gateway::HiredisRoutingStore routing_store(config);
        ai_gateway::RoutingRuntime routing(config, routing_store);
        ai_gateway::HiredisGovernanceStore governance_store(config);
        ai_gateway::GovernanceRuntime governance(config, governance_store);
        ai_gateway::NodeLifecycle lifecycle(config);
        ai_gateway::MetricsRegistry metrics;
        ai_gateway::CurlMultiProviderTransport transport(config);
        ai_gateway::HealthProbeRuntime health_probes(
            runtime, routing, governance, transport, metrics);
        ai_gateway::AiGateway gateway(
            runtime, routing, governance, lifecycle, metrics, transport);
        ai_gateway::structured_log(
            "gateway_started",
            {{"listen_address", config.listen_address},
             {"listen_port", std::to_string(config.listen_port)},
             {"ready", gateway.ready() ? "true" : "false"}});
        ai_gateway::HttpServer server(config, gateway, lifecycle);
        server.run();
        transport.shutdown();
        return 0;
    }
    catch (const std::exception &error)
    {
        std::cerr << "AiGateway startup failed: " << error.what() << std::endl;
        return 1;
    }
}
