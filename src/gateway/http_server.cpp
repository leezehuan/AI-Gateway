#include "gateway/http_server.hpp"

#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <array>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

/*
 * Boost.Beast 入站 HTTP 适配器。
 *
 * 本文件不决定认证、路由或 Provider 协议，它只把 Beast request 转成 GatewayRequest，让 HttpSession
 * 实现 ResponseWriter，并在普通 Content-Length 响应与 chunked SSE 响应之间选择正确的写法。
 * socket EOF、写失败、SIGTERM 和 drain 会被转换为取消/完成通知。
 *
 * Gateway 调用 ResponseWriter::end 仅表示“不会再写业务字节”。最后一个 async_write 完成或连接断开后
 * 才触发 completion callback，所以 NodeRequestLease 不会在慢客户端仍有积压数据时过早释放容量。
 */
namespace ai_gateway
{
namespace
{
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

/* 将入站 Header 名称统一为小写，Gateway HeaderMap 采用大小写无关查找。 */
std::string lower(std::string value)
{
    for (char &character : value)
    {
        character = static_cast<char>(std::tolower(static_cast<unsigned char>(character)));
    }
    return value;
}

/* 判断响应是否应使用 Beast HTTP/1.1 chunked SSE writer。 */
bool event_stream_headers(const HeaderMap &headers)
{
    const auto found = headers.find("content-type");
    return found != headers.end() && lower(found->second).find("text/event-stream") == 0;
}

class HttpSession final : public ResponseWriter,
                          public std::enable_shared_from_this<HttpSession>
{
public:
    /* 创建单个 TCP 连接的 HTTP 会话，并保存 body/SSE 队列上限。 */
    HttpSession(tcp::socket socket, AiGateway &gateway, const GatewayConfig &config)
        : socket_(std::move(socket)),
          gateway_(gateway),
          max_body_bytes_(config.max_body_bytes),
          high_water_bytes_(config.stream_buffer_high_water_bytes),
          low_water_bytes_(config.stream_buffer_low_water_bytes)
    {
        parser_.emplace();
        parser_->body_limit(max_body_bytes_);
        parser_->header_limit(64 * 1024);
    }

    /* 开始异步读取第一条 HTTP 请求。 */
    void start()
    {
        read_request();
    }

    /*
     * ResponseWriter begin：初始化普通响应或 SSE chunked 响应状态。
     * SSE begin 只记录 Header，真正写出 Header 由 pump_stream 串行执行。
     */
    void begin(int status, const HeaderMap &headers) override
    {
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            response_status_ = status;
            response_headers_ = headers;
            response_body_.clear();
            stream_mode_ = event_stream_headers(headers);
            end_called_ = false;
            response_started_ = true;
            stream_header_started_ = false;
            stream_header_sent_ = false;
            stream_write_in_progress_ = false;
            stream_last_sent_ = false;
            backpressured_ = false;
            queued_bytes_ = 0;
            stream_queue_.clear();
        }
        if (event_stream_headers(headers))
        {
            post_stream_pump();
        }
    }

    /*
     * ResponseWriter write：普通响应追加 body；SSE 追加有界队列。
     * 到达 high water 时返回 false，让上游 transport 暂停；当前 bytes 已经被队列接收。
     */
    bool write(std::string_view bytes) override
    {
        bool stream = false;
        bool writable = true;
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            if (!connected_.load())
            {
                return false;
            }
            stream = stream_mode_;
            if (!stream)
            {
                response_body_.append(bytes.data(), bytes.size());
                return true;
            }
            auto chunk = std::make_shared<std::string>(bytes.data(), bytes.size());
            stream_queue_.push_back(std::move(chunk));
            queued_bytes_ += bytes.size();
            if (!backpressured_ && queued_bytes_ >= high_water_bytes_)
            {
                backpressured_ = true;
            }
            writable = !backpressured_;
        }
        if (stream)
        {
            post_stream_pump();
        }
        return writable;
    }

    /* 严格只接受一次 end；异步泵送完队列后才写 chunked last。 */
    void end() override
    {
        bool stream = false;
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            if (end_called_)
            {
                return;
            }
            end_called_ = true;
            stream = stream_mode_;
        }
        auto self = shared_from_this();
        if (stream)
        {
            asio::post(socket_.get_executor(), [self] { self->pump_stream(); });
        }
        else
        {
            asio::post(socket_.get_executor(), [self] { self->post_response(); });
        }
    }

    /* 返回 socket 是否仍可写。 */
    bool client_connected() const override
    {
        return connected_.load();
    }

    /* 注册低水位恢复回调，通常由 RequestExecution 调用 ProviderTransfer::resume。 */
    void set_writable_callback(std::function<void()> callback) override
    {
        std::lock_guard<std::mutex> lock(response_mutex_);
        writable_callback_ = std::move(callback);
    }

    /* 注册“实际 HTTP 写完或连接断开”的严格一次回调，用于释放 NodeRequestLease。 */
    void set_completion_callback(std::function<void()> callback) override
    {
        bool call_now = false;
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            call_now = completion_called_;
            if (!call_now)
            {
                completion_callback_ = std::move(callback);
            }
        }
        if (call_now && callback)
        {
            callback();
        }
    }

private:
    /* 使用 Beast parser 读取完整请求，并再次设置 body/header 硬上限。 */
    void read_request()
    {
        if (!connected_.load())
        {
            return;
        }
        parser_.emplace();
        parser_->body_limit(max_body_bytes_);
        parser_->header_limit(64 * 1024);
        auto self = shared_from_this();
        http::async_read(socket_, buffer_, *parser_,
                         [self](beast::error_code error, std::size_t) {
                             self->on_read(error);
                         });
    }

    /* 把 Beast 请求转换成 GatewayRequest；解析错误在进入 Gateway 前直接返回 413/关闭。 */
    void on_read(beast::error_code error)
    {
        if (error == http::error::body_limit || error == http::error::header_limit)
        {
            send_direct(413, openai_error_body(
                                 "Request is too large", "invalid_request_error", "request_too_large"));
            return;
        }
        if (error)
        {
            close();
            return;
        }

        request_id_ = generate_request_id();
        GatewayRequest request;
        request.method = std::string(parser_->get().method_string());
        request.path = std::string(parser_->get().target());
        request.request_id = request_id_;
        request.body = parser_->get().body();
        for (const auto &header : parser_->get())
        {
            request.headers[lower(std::string(header.name_string()))] =
                std::string(header.value());
        }

        request_version_ = parser_->get().version();
        request_keep_alive_ = parser_->get().keep_alive();
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            completion_callback_ = {};
            completion_called_ = false;
        }
        cancellation_source_ = CancellationSource();
        in_flight_hold_ = shared_from_this();
        monitor_disconnect();
        gateway_.handle(request, *this, cancellation_source_.token());
    }

    /* 用 peek receive 观察 EOF/reset，客户端断开时触发 CancellationSource。 */
    void monitor_disconnect()
    {
        auto self = shared_from_this();
        socket_.async_receive(
            asio::buffer(disconnect_probe_), asio::socket_base::message_peek,
            [self](beast::error_code error, std::size_t bytes) {
                if (error == asio::error::operation_aborted)
                {
                    return;
                }
                if (error == asio::error::eof || error == asio::error::connection_reset ||
                    error == asio::error::broken_pipe || (!error && bytes == 0))
                {
                    self->close();
                }
            });
    }

    /* 发送普通 Content-Length 响应；写完后按 Keep-Alive 决定复用或关闭连接。 */
    void post_response()
    {
        if (!connected_.load())
        {
            release_request(false);
            return;
        }

        HeaderMap headers;
        std::string body;
        int status = 500;
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            headers = response_headers_;
            body = response_body_;
            status = response_status_;
        }

        response_.version(request_version_);
        response_.result(valid_status(status));
        response_.keep_alive(request_keep_alive_);
        response_.set(http::field::server, "ai-gateway");
        apply_headers(response_, headers);
        response_.body() = std::move(body);
        response_.prepare_payload();

        auto self = shared_from_this();
        http::async_write(socket_, response_, [self](beast::error_code error, std::size_t) {
            if (error)
            {
                self->close();
                self->release_request(false);
                return;
            }
            self->release_request(self->response_.keep_alive());
        });
    }

    /* 把 stream pump 投递到 socket executor，保证只有一个在途 async_write。 */
    void post_stream_pump()
    {
        auto self = shared_from_this();
        asio::post(socket_.get_executor(), [self] { self->pump_stream(); });
    }

    /*
     * SSE chunked 写出状态机：先写 Header，再逐块写队列，最后写 chunk_last。
     * 任何时刻只允许一个异步写操作，写完降到 low water 才回调上游恢复。
     */
    void pump_stream()
    {
        if (!connected_.load())
        {
            release_request(false);
            return;
        }

        HeaderMap headers;
        int status = 500;
        bool start_header = false;
        std::shared_ptr<std::string> chunk;
        bool send_last = false;
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            if (!stream_mode_ || stream_write_in_progress_ || stream_last_sent_)
            {
                return;
            }
            if (!stream_header_started_)
            {
                stream_header_started_ = true;
                stream_write_in_progress_ = true;
                headers = response_headers_;
                status = response_status_;
                start_header = true;
            }
            else if (!stream_header_sent_)
            {
                return;
            }
            else if (!stream_queue_.empty())
            {
                stream_write_in_progress_ = true;
                chunk = stream_queue_.front();
            }
            else if (end_called_)
            {
                stream_write_in_progress_ = true;
                stream_last_sent_ = true;
                send_last = true;
            }
            else
            {
                return;
            }
        }

        if (start_header)
        {
            stream_response_ = {};
            stream_response_.version(request_version_);
            stream_response_.result(valid_status(status));
            stream_response_.keep_alive(request_keep_alive_);
            stream_response_.set(http::field::server, "ai-gateway");
            apply_headers(stream_response_, headers);
            stream_response_.chunked(true);
            stream_serializer_.emplace(stream_response_);
            auto self = shared_from_this();
            http::async_write_header(
                socket_, *stream_serializer_,
                [self](beast::error_code error, std::size_t) {
                    if (error)
                    {
                        self->close();
                        self->release_request(false);
                        return;
                    }
                    {
                        std::lock_guard<std::mutex> lock(self->response_mutex_);
                        self->stream_header_sent_ = true;
                        self->stream_write_in_progress_ = false;
                    }
                    self->pump_stream();
                });
            return;
        }

        if (chunk)
        {
            auto self = shared_from_this();
            asio::async_write(
                socket_, http::make_chunk(asio::buffer(*chunk)),
                [self, chunk](beast::error_code error, std::size_t) {
                    if (error)
                    {
                        self->close();
                        self->release_request(false);
                        return;
                    }
                    std::function<void()> writable;
                    {
                        std::lock_guard<std::mutex> lock(self->response_mutex_);
                        if (!self->stream_queue_.empty())
                        {
                            self->queued_bytes_ -= self->stream_queue_.front()->size();
                            self->stream_queue_.pop_front();
                        }
                        self->stream_write_in_progress_ = false;
                        if (self->backpressured_ &&
                            self->queued_bytes_ <= self->low_water_bytes_)
                        {
                            self->backpressured_ = false;
                            writable = self->writable_callback_;
                        }
                    }
                    if (writable)
                    {
                        writable();
                    }
                    self->pump_stream();
                });
            return;
        }

        if (send_last)
        {
            auto self = shared_from_this();
            asio::async_write(
                socket_, http::make_chunk_last(),
                [self](beast::error_code error, std::size_t) {
                    if (error)
                    {
                        self->close();
                        self->release_request(false);
                        return;
                    }
                    self->release_request(self->request_keep_alive_);
                });
        }
    }

    /* parser 阶段失败时绕过 Gateway，构造一个最小脱敏 JSON 响应。 */
    void send_direct(int status, std::string body)
    {
        request_id_ = generate_request_id();
        begin(status, {{"content-type", "application/json"},
                       {"x-request-id", request_id_},
                       {"cache-control", "no-store"}});
        write(body);
        end();
    }

    /* 清理当前请求资源、触发 completion callback，并按 Keep-Alive 开始下一次读取。 */
    void release_request(bool keep_alive)
    {
        beast::error_code ignored;
        socket_.cancel(ignored);
        notify_completion();
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            response_started_ = false;
            stream_mode_ = false;
            stream_queue_.clear();
            queued_bytes_ = 0;
            writable_callback_ = {};
        }
        response_ = {};
        stream_serializer_.reset();
        in_flight_hold_.reset();
        if (connected_.load() && keep_alive)
        {
            read_request();
        }
        else if (connected_.load())
        {
            close();
        }
    }

    /* 幂等关闭 socket 并广播 CancellationToken。 */
    void close()
    {
        if (!connected_.exchange(false))
        {
            return;
        }
        cancellation_source_.cancel();
        notify_completion();
        beast::error_code ignored;
        socket_.shutdown(tcp::socket::shutdown_both, ignored);
        socket_.close(ignored);
    }

    /* 严格一次通知下游写入完成，保护 NodeRequestLease 不提前释放。 */
    void notify_completion()
    {
        std::function<void()> callback;
        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            if (completion_called_)
            {
                return;
            }
            completion_called_ = true;
            callback = std::move(completion_callback_);
        }
        if (callback)
        {
            callback();
        }
    }

    /* 把任意内部状态限制在合法 HTTP 状态码范围。 */
    static http::status valid_status(int status)
    {
        return status >= 100 && status <= 599
                   ? static_cast<http::status>(status)
                   : http::status::internal_server_error;
    }

    /* 过滤 hop-by-hop 和由 Beast 自己计算的 Header，避免响应 framing 被伪造。 */
    template <typename Body>
    static void apply_headers(http::response<Body> &message, const HeaderMap &headers)
    {
        for (const auto &header : headers)
        {
            if (header.first == "connection" || header.first == "transfer-encoding" ||
                header.first == "content-length")
            {
                continue;
            }
            message.set(header.first, header.second);
        }
    }

    tcp::socket socket_;
    AiGateway &gateway_;
    std::size_t max_body_bytes_;
    std::size_t high_water_bytes_;
    std::size_t low_water_bytes_;
    beast::flat_buffer buffer_;
    std::optional<http::request_parser<http::string_body>> parser_;
    http::response<http::string_body> response_;
    http::response<http::empty_body> stream_response_;
    std::optional<http::response_serializer<http::empty_body>> stream_serializer_;
    std::shared_ptr<HttpSession> in_flight_hold_;
    CancellationSource cancellation_source_;
    std::array<char, 1> disconnect_probe_{};
    std::atomic_bool connected_{true};
    mutable std::mutex response_mutex_;
    HeaderMap response_headers_;
    std::string response_body_;
    std::deque<std::shared_ptr<std::string>> stream_queue_;
    std::function<void()> writable_callback_;
    std::function<void()> completion_callback_;
    std::string request_id_;
    std::size_t queued_bytes_ = 0;
    int response_status_ = 500;
    unsigned request_version_ = 11;
    bool request_keep_alive_ = false;
    bool response_started_ = false;
    bool stream_mode_ = false;
    bool end_called_ = false;
    bool stream_header_started_ = false;
    bool stream_header_sent_ = false;
    bool stream_write_in_progress_ = false;
    bool stream_last_sent_ = false;
    bool backpressured_ = false;
    bool completion_called_ = false;
};
} // namespace

/*
 * 函数名直译：创建 HTTP 服务器。
 *
 * 通俗说：解析监听地址，打开 TCP socket、绑定端口并进入 listen；任何失败都在启动阶段抛出，
 * 不让进程看似运行但实际上无法接收请求。
 */
HttpServer::HttpServer(const GatewayConfig &config,
                       AiGateway &gateway,
                       NodeLifecycle &lifecycle)
    : config_(config),
      gateway_(gateway),
      lifecycle_(lifecycle),
      io_(),
      acceptor_(io_),
      signals_(io_, SIGINT, SIGTERM),
      drain_timer_(io_),
      cancel_timer_(io_)
{
    const auto address = asio::ip::make_address(config_.listen_address);
    tcp::endpoint endpoint(address, config_.listen_port);
    beast::error_code error;
    acceptor_.open(endpoint.protocol(), error);
    if (error)
    {
        throw std::runtime_error("failed to open gateway listener: " + error.message());
    }
    acceptor_.set_option(asio::socket_base::reuse_address(true), error);
    acceptor_.bind(endpoint, error);
    if (error)
    {
        throw std::runtime_error("failed to bind gateway listener: " + error.message());
    }
    acceptor_.listen(asio::socket_base::max_listen_connections, error);
    if (error)
    {
        throw std::runtime_error("failed to listen on gateway port: " + error.message());
    }
}

/*
 * 函数名直译：运行 HTTP 服务器。
 *
 * 通俗说：启动 accept、注册节点空闲回调和终止信号，再用配置数量的 Asio worker 处理会话。
 * 活动请求归零后 listener 和 io_context 才正常停止。
 */
void HttpServer::run()
{
    accept();
    lifecycle_.on_idle([this] {
        asio::post(io_, [this] {
            if (draining_)
            {
                stop(false);
            }
        });
    });
    wait_for_signal();

    const std::size_t thread_count = std::max<std::size_t>(1, config_.io_threads);
    for (std::size_t index = 1; index < thread_count; ++index)
    {
        workers_.emplace_back([this] { io_.run(); });
    }
    io_.run();
    for (auto &worker : workers_)
    {
        worker.join();
    }
}

/* 第一次 SIGINT/SIGTERM 进入自然 drain；第二次信号直接强制退出。 */
void HttpServer::wait_for_signal()
{
    signals_.async_wait([this](const beast::error_code &error, int) {
        if (error)
        {
            return;
        }
        if (draining_)
        {
            stop(true);
            return;
        }
        begin_drain();
        wait_for_signal();
    });
}

/*
 * 开始双阶段优雅停机：节点先标记 draining，停止新请求；drain_timeout 到期后取消存量上游。
 */
void HttpServer::begin_drain()
{
    draining_ = true;
    drain_started_ = std::chrono::steady_clock::now();
    lifecycle_.begin_drain();
    const NodeSnapshot state = lifecycle_.snapshot();
    structured_log("gateway_draining",
                   {{"active_requests", std::to_string(state.active_requests)},
                    {"active_streams", std::to_string(state.active_streams)},
                    {"drain_timeout_ms", std::to_string(config_.drain_timeout_ms)}});
    drain_timer_.expires_after(std::chrono::milliseconds(config_.drain_timeout_ms));
    drain_timer_.async_wait([this](const beast::error_code &error) {
        if (!error && !stopping_)
        {
            cancel_remaining();
        }
    });
}

/* 进入 shutdown cancel grace 窗口，广播取消并安排最终强制退出。 */
void HttpServer::cancel_remaining()
{
    if (cancelling_)
    {
        return;
    }
    cancelling_ = true;
    lifecycle_.cancel_remaining();
    const NodeSnapshot state = lifecycle_.snapshot();
    structured_log("gateway_shutdown_cancelling",
                   {{"active_requests", std::to_string(state.active_requests)},
                    {"active_streams", std::to_string(state.active_streams)},
                    {"cancelled_requests", std::to_string(state.shutdown_cancellations)}});
    cancel_timer_.expires_after(
        std::chrono::milliseconds(config_.shutdown_cancel_grace_ms));
    cancel_timer_.async_wait([this](const beast::error_code &error) {
        if (!error && !stopping_)
        {
            stop(true);
        }
    });
}

/* 关闭 listener、timer 和 io_context；forced 路径使用 _Exit 避免悬挂依赖阻塞退出。 */
void HttpServer::stop(bool forced)
{
    if (stopping_)
    {
        return;
    }
    stopping_ = true;
    beast::error_code ignored;
    drain_timer_.cancel();
    cancel_timer_.cancel();
    signals_.cancel(ignored);
    acceptor_.close(ignored);
    const NodeSnapshot state = lifecycle_.snapshot();
    const auto duration = draining_
                              ? std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - drain_started_).count()
                              : 0;
    structured_log("gateway_stopped",
                   {{"active_requests", std::to_string(state.active_requests)},
                    {"active_streams", std::to_string(state.active_streams)},
                    {"duration_ms", std::to_string(duration)},
                    {"forced", forced ? "true" : "false"}});
    if (forced)
    {
        std::_Exit(EXIT_SUCCESS);
    }
    io_.stop();
}

/* 异步接受连接；listener 仍打开时递归安排下一次 accept。 */
void HttpServer::accept()
{
    acceptor_.async_accept([this](beast::error_code error, tcp::socket socket) {
        if (!error)
        {
            std::make_shared<HttpSession>(std::move(socket), gateway_, config_)->start();
        }
        if (acceptor_.is_open())
        {
            accept();
        }
    });
}
} // namespace ai_gateway
