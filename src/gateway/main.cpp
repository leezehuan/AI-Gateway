#include "gateway/curl_transport.hpp"
#include "gateway/gateway.hpp"
#include "gateway/http_server.hpp"

#include <exception>
#include <iostream>

int main()
{
    try
    {
        const ai_gateway::GatewayConfig config = ai_gateway::GatewayConfig::from_env();
        ai_gateway::CurlMultiProviderTransport transport;
        ai_gateway::AiGateway gateway(config, transport);
        ai_gateway::structured_log(
            "gateway_started",
            {{"listen_address", config.listen_address},
             {"listen_port", std::to_string(config.listen_port)},
             {"ready", gateway.ready() ? "true" : "false"}});
        ai_gateway::HttpServer server(config, gateway);
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
