#ifndef AI_GATEWAY_HTTP_SERVER_HPP
#define AI_GATEWAY_HTTP_SERVER_HPP

#include "gateway/gateway.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>

#include <memory>
#include <thread>
#include <vector>

namespace ai_gateway
{
class HttpServer
{
public:
    HttpServer(const GatewayConfig &config, AiGateway &gateway);
    void run();

private:
    void accept();

    const GatewayConfig &config_;
    AiGateway &gateway_;
    boost::asio::io_context io_;
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::signal_set signals_;
    std::vector<std::thread> workers_;
};
} // namespace ai_gateway

#endif
