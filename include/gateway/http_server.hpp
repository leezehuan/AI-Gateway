#ifndef AI_GATEWAY_HTTP_SERVER_HPP
#define AI_GATEWAY_HTTP_SERVER_HPP

#include "gateway/gateway.hpp"
#include "gateway/lifecycle.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace ai_gateway
{
/*
 * Beast 入站服务器。
 *
 * 每个 HttpSession 实现 ResponseWriter，把普通 JSON 作为 Content-Length 响应，把 SSE
 * 作为 HTTP/1.1 chunked 响应；HttpServer 只负责 listener、signal、drain 和 io_context 生命周期。
 */
class HttpServer
{
public:
    HttpServer(const GatewayConfig &config, AiGateway &gateway, NodeLifecycle &lifecycle);
    /* 启动 accept、signal handler 和配置数量的 Asio worker。 */
    void run();

private:
    void accept();
    void wait_for_signal();
    void begin_drain();
    void cancel_remaining();
    void stop(bool forced);

    const GatewayConfig &config_;
    AiGateway &gateway_;
    NodeLifecycle &lifecycle_;
    boost::asio::io_context io_;
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::signal_set signals_;
    boost::asio::steady_timer drain_timer_;
    boost::asio::steady_timer cancel_timer_;
    std::vector<std::thread> workers_;
    std::chrono::steady_clock::time_point drain_started_;
    bool draining_ = false;
    bool cancelling_ = false;
    bool stopping_ = false;
};
} // namespace ai_gateway

#endif
