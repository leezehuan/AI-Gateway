#ifndef AI_GATEWAY_HTTP_SERVER_HPP
#define AI_GATEWAY_HTTP_SERVER_HPP

#include "gateway/gateway.hpp"
#include "gateway/lifecycle.hpp"

#include <memory>

namespace ai_gateway
{
class HttpServer
{
public:
    HttpServer(const GatewayConfig &config, AiGateway &gateway, NodeLifecycle &lifecycle);
    ~HttpServer();

    HttpServer(const HttpServer &) = delete;
    HttpServer &operator=(const HttpServer &) = delete;

    void run();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
