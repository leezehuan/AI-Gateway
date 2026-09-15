#include "gateway/http_server.hpp"

#include <drogon/drogon.h>
#include <trantor/net/EventLoop.h>
#include <trantor/net/TcpConnection.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ai_gateway
{
namespace
{
std::string lower(std::string value)
{
    for (char &character : value)
    {
        character = static_cast<char>(std::tolower(static_cast<unsigned char>(character)));
    }
    return value;
}

bool event_stream_headers(const HeaderMap &headers)
{
    const auto found = headers.find("content-type");
    return found != headers.end() && lower(found->second).rfind("text/event-stream", 0) == 0;
}

std::size_t parser_body_limit(std::size_t gateway_limit)
{
    constexpr std::size_t error_envelope_headroom = 64 * 1024;
    if (gateway_limit > std::numeric_limits<std::size_t>::max() - error_envelope_headroom)
    {
        return gateway_limit;
    }
    return gateway_limit + error_envelope_headroom;
}

std::string connection_key(const trantor::InetAddress &local,
                           const trantor::InetAddress &peer)
{
    return local.toIpPort() + "|" + peer.toIpPort();
}

class DrogonResponseWriter;

class ConnectionRegistry
{
public:
    void connection_changed(const trantor::TcpConnectionPtr &connection);
    trantor::TcpConnectionPtr attach(const drogon::HttpRequestPtr &request,
                                    const std::shared_ptr<DrogonResponseWriter> &writer);
    void write_complete(const std::string &key);

private:
    struct Entry
    {
        std::weak_ptr<trantor::TcpConnection> connection;
        std::vector<std::weak_ptr<DrogonResponseWriter>> writers;
    };

    std::mutex mutex_;
    std::unordered_map<std::string, Entry> connections_;
};

class DrogonResponseWriter final : public ResponseWriter,
                                   public std::enable_shared_from_this<DrogonResponseWriter>
{
public:
    using ResponseCallback = std::function<void(const drogon::HttpResponsePtr &)>;

    DrogonResponseWriter(ResponseCallback callback, const GatewayConfig &config)
        : response_callback_(std::move(callback)),
          high_water_bytes_(config.stream_buffer_high_water_bytes),
          low_water_bytes_(config.stream_buffer_low_water_bytes)
    {
    }

    void attach_connection(trantor::TcpConnectionPtr connection)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connection_ = std::move(connection);
        self_hold_ = shared_from_this();
    }

    CancellationToken cancellation_token() const
    {
        return cancellation_source_.token();
    }

    void begin(int status, const HeaderMap &headers) override
    {
        bool dispatch_stream = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (response_started_)
            {
                return;
            }
            response_started_ = true;
            response_status_ = status;
            response_headers_ = headers;
            stream_mode_ = event_stream_headers(headers);
            dispatch_stream = stream_mode_;
        }
        if (dispatch_stream)
        {
            dispatch_stream_response();
        }
    }

    bool write(std::string_view bytes) override
    {
        bool stream = false;
        bool writable = true;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!connected_ || end_called_)
            {
                return false;
            }
            stream = stream_mode_;
            if (!stream)
            {
                response_body_.append(bytes.data(), bytes.size());
                return true;
            }
            stream_queue_.emplace_back(bytes);
            queued_bytes_ += bytes.size();
            if (queued_bytes_ >= high_water_bytes_)
            {
                backpressured_ = true;
            }
            writable = !backpressured_;
        }
        post_stream_pump();
        return writable;
    }

    void end() override
    {
        bool stream = false;
        bool connected = true;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (end_called_)
            {
                return;
            }
            end_called_ = true;
            stream = stream_mode_;
            connected = connected_;
        }
        if (stream)
        {
            if (connected)
            {
                post_stream_pump();
            }
            else
            {
                notify_completion();
            }
        }
        else
        {
            dispatch_buffered_response();
        }
    }

    bool client_connected() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return connected_;
    }

    void set_writable_callback(std::function<void()> callback) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        writable_callback_ = std::move(callback);
    }

    void set_completion_callback(std::function<void()> callback) override
    {
        bool call_now = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
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

    void disconnected()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!connected_)
            {
                return;
            }
            connected_ = false;
        }
        cancellation_source_.cancel();
        bool complete_now = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            complete_now = end_called_;
        }
        if (complete_now)
        {
            notify_completion();
        }
    }

    void transport_write_complete()
    {
        bool complete = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            complete = end_called_ && (stream_mode_ ? stream_closed_ : response_dispatched_);
        }
        if (complete)
        {
            notify_completion();
        }
    }

private:
    void dispatch_stream_response()
    {
        const auto self = shared_from_this();
        auto response = drogon::HttpResponse::newAsyncStreamResponse(
            [self](drogon::ResponseStreamPtr stream) {
                {
                    std::lock_guard<std::mutex> lock(self->mutex_);
                    self->stream_ = std::move(stream);
                }
                self->post_stream_pump();
            },
            true);
        apply_response_metadata(response);
        dispatch(std::move(response));
    }

    void dispatch_buffered_response()
    {
        std::string body;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            body = std::move(response_body_);
        }
        auto response = drogon::HttpResponse::newHttpResponse();
        apply_response_metadata(response);
        response->setBody(std::move(body));
        dispatch(std::move(response));
        bool connected = true;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            connected = connected_;
        }
        if (!connected)
        {
            notify_completion();
        }
    }

    void apply_response_metadata(const drogon::HttpResponsePtr &response)
    {
        int status = 500;
        HeaderMap headers;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            status = response_status_;
            headers = response_headers_;
        }
        response->setStatusCode(status >= 100 && status <= 599
                                    ? static_cast<drogon::HttpStatusCode>(status)
                                    : drogon::k500InternalServerError);
        response->addHeader("server", "ai-gateway");
        for (const auto &header : headers)
        {
            if (header.first == "connection" || header.first == "transfer-encoding" ||
                header.first == "content-length")
            {
                continue;
            }
            if (header.first == "content-type")
            {
                response->setContentTypeString(header.second);
            }
            else
            {
                response->addHeader(header.first, header.second);
            }
        }
    }

    void dispatch(drogon::HttpResponsePtr response)
    {
        ResponseCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (response_dispatched_)
            {
                return;
            }
            response_dispatched_ = true;
            callback = std::move(response_callback_);
        }
        if (callback)
        {
            callback(response);
        }
    }

    void post_stream_pump()
    {
        trantor::EventLoop *loop = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (pump_scheduled_)
            {
                return;
            }
            pump_scheduled_ = true;
            if (connection_)
            {
                loop = connection_->getLoop();
            }
        }
        const auto self = shared_from_this();
        if (loop)
        {
            loop->queueInLoop([self] { self->pump_stream(); });
        }
        else
        {
            drogon::app().getLoop()->queueInLoop([self] { self->pump_stream(); });
        }
    }

    void pump_stream()
    {
        std::string chunk;
        std::function<void()> writable;
        bool close_stream = false;
        drogon::ResponseStream *stream = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pump_scheduled_ = false;
            if (!connected_ || !stream_)
            {
                return;
            }
            stream = stream_.get();
            if (!stream_queue_.empty())
            {
                chunk = std::move(stream_queue_.front());
                stream_queue_.pop_front();
                queued_bytes_ -= chunk.size();
                if (backpressured_ && queued_bytes_ <= low_water_bytes_)
                {
                    backpressured_ = false;
                    writable = writable_callback_;
                }
            }
            else if (end_called_)
            {
                close_stream = true;
            }
        }

        if (!chunk.empty() && (!stream || !stream->send(chunk)))
        {
            disconnected();
            return;
        }
        if (writable)
        {
            writable();
        }
        if (close_stream)
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (stream_)
                {
                    stream_->close();
                    stream_.reset();
                }
                stream_closed_ = true;
            }
            return;
        }
        if (!chunk.empty())
        {
            post_stream_pump();
        }
    }

    void notify_completion()
    {
        std::function<void()> callback;
        std::shared_ptr<DrogonResponseWriter> hold;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (completion_called_)
            {
                return;
            }
            completion_called_ = true;
            callback = std::move(completion_callback_);
            hold = std::move(self_hold_);
        }
        if (callback)
        {
            callback();
        }
    }

    mutable std::mutex mutex_;
    ResponseCallback response_callback_;
    HeaderMap response_headers_;
    std::string response_body_;
    std::deque<std::string> stream_queue_;
    std::function<void()> writable_callback_;
    std::function<void()> completion_callback_;
    trantor::TcpConnectionPtr connection_;
    drogon::ResponseStreamPtr stream_;
    std::shared_ptr<DrogonResponseWriter> self_hold_;
    CancellationSource cancellation_source_;
    std::size_t queued_bytes_ = 0;
    const std::size_t high_water_bytes_;
    const std::size_t low_water_bytes_;
    int response_status_ = 500;
    bool connected_ = true;
    bool response_started_ = false;
    bool response_dispatched_ = false;
    bool stream_mode_ = false;
    bool end_called_ = false;
    bool backpressured_ = false;
    bool pump_scheduled_ = false;
    bool completion_called_ = false;
    bool stream_closed_ = false;
};

void ConnectionRegistry::connection_changed(const trantor::TcpConnectionPtr &connection)
{
    const std::string key = connection_key(connection->localAddr(), connection->peerAddr());
    std::vector<std::shared_ptr<DrogonResponseWriter>> writers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (connection->connected())
        {
            connections_[key].connection = connection;
            connection->setWriteCompleteCallback(
                [this, key](const trantor::TcpConnectionPtr &) { write_complete(key); });
            return;
        }
        const auto found = connections_.find(key);
        if (found != connections_.end())
        {
            for (const auto &weak : found->second.writers)
            {
                if (auto candidate = weak.lock())
                {
                    writers.push_back(std::move(candidate));
                }
            }
            connections_.erase(found);
        }
    }
    for (const auto &writer : writers)
    {
        writer->disconnected();
    }
}

void ConnectionRegistry::write_complete(const std::string &key)
{
    std::vector<std::shared_ptr<DrogonResponseWriter>> writers;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = connections_.find(key);
        if (found == connections_.end())
        {
            return;
        }
        for (const auto &weak : found->second.writers)
        {
            if (auto writer = weak.lock())
            {
                writers.push_back(std::move(writer));
            }
        }
    }
    for (const auto &writer : writers)
    {
        writer->transport_write_complete();
    }
}

trantor::TcpConnectionPtr ConnectionRegistry::attach(
    const drogon::HttpRequestPtr &request,
    const std::shared_ptr<DrogonResponseWriter> &writer)
{
    const std::string key = connection_key(request->localAddr(), request->peerAddr());
    std::lock_guard<std::mutex> lock(mutex_);
    auto &entry = connections_[key];
    entry.writers.emplace_back(writer);
    return entry.connection.lock();
}
} // namespace

class HttpServer::Impl
{
public:
    Impl(const GatewayConfig &config, AiGateway &gateway, NodeLifecycle &lifecycle)
        : config_(config), gateway_(gateway), lifecycle_(lifecycle)
    {
    }

    void run()
    {
        auto &app = drogon::app();
        app.addListener(config_.listen_address, config_.listen_port)
            .setThreadNum(std::max<std::size_t>(1, config_.io_threads))
            // Leave bounded parser headroom so the Gateway can return its
            // protocol-specific JSON error at the configured body limit.
            .setClientMaxBodySize(parser_body_limit(config_.max_body_bytes))
            .setConnectionCallback(
                [this](const trantor::TcpConnectionPtr &connection) {
                    connections_.connection_changed(connection);
                })
            .setCustomErrorHandler([](drogon::HttpStatusCode status,
                                      const drogon::HttpRequestPtr &) {
                auto response = drogon::HttpResponse::newHttpResponse();
                response->setStatusCode(status);
                response->addHeader("cache-control", "no-store");
                response->setContentTypeString("application/json");
                response->addHeader("x-request-id", generate_request_id());
                if (status == drogon::k413RequestEntityTooLarge)
                {
                    response->setBody(openai_error_body(
                        "Request is too large", "invalid_request_error", "request_too_large"));
                }
                return response;
            });

        const auto handler =
            [this](const drogon::HttpRequestPtr &request,
                   std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
                handle(request, std::move(callback));
            };
        for (const auto *path : {"/healthz", "/readyz", "/metrics", "/v1/models",
                                 "/v1/responses", "/v1/chat/completions", "/v1/messages"})
        {
            app.registerHandler(path, handler);
        }
        app.setTermSignalHandler([this] { signal_received(); });
        app.setIntSignalHandler([this] { signal_received(); });
        lifecycle_.on_idle([this] {
            if (draining_.load())
            {
                stop(false);
            }
        });
        app.run();
    }

private:
    void handle(const drogon::HttpRequestPtr &request,
                std::function<void(const drogon::HttpResponsePtr &)> callback)
    {
        auto writer = std::make_shared<DrogonResponseWriter>(std::move(callback), config_);
        writer->attach_connection(connections_.attach(request, writer));

        GatewayRequest gateway_request;
        gateway_request.method = request->methodString();
        gateway_request.path = request->path();
        gateway_request.request_id = generate_request_id();
        gateway_request.body.assign(request->body().data(), request->body().size());
        for (const auto &header : request->headers())
        {
            gateway_request.headers[lower(header.first)] = header.second;
        }
        gateway_.handle(gateway_request, *writer, writer->cancellation_token());
    }

    void signal_received()
    {
        if (draining_.exchange(true))
        {
            stop(true);
            return;
        }
        drain_started_ = std::chrono::steady_clock::now();
        lifecycle_.begin_drain();
        const NodeSnapshot state = lifecycle_.snapshot();
        structured_log("gateway_draining",
                       {{"active_requests", std::to_string(state.active_requests)},
                        {"active_streams", std::to_string(state.active_streams)},
                        {"drain_timeout_ms", std::to_string(config_.drain_timeout_ms)}});
        drogon::app().getLoop()->runAfter(
            static_cast<double>(config_.drain_timeout_ms) / 1000.0,
            [this] { cancel_remaining(); });
    }

    void cancel_remaining()
    {
        if (stopping_ || cancelling_.exchange(true))
        {
            return;
        }
        lifecycle_.cancel_remaining();
        const NodeSnapshot state = lifecycle_.snapshot();
        structured_log("gateway_shutdown_cancelling",
                       {{"active_requests", std::to_string(state.active_requests)},
                        {"active_streams", std::to_string(state.active_streams)},
                        {"cancelled_requests", std::to_string(state.shutdown_cancellations)}});
        drogon::app().getLoop()->runAfter(
            static_cast<double>(config_.shutdown_cancel_grace_ms) / 1000.0,
            [this] { stop(true); });
    }

    void stop(bool forced)
    {
        if (stopping_.exchange(true))
        {
            return;
        }
        const NodeSnapshot state = lifecycle_.snapshot();
        const auto duration = draining_
                                  ? std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - drain_started_)
                                        .count()
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
        drogon::app().quit();
    }

    const GatewayConfig &config_;
    AiGateway &gateway_;
    NodeLifecycle &lifecycle_;
    ConnectionRegistry connections_;
    std::atomic_bool draining_{false};
    std::atomic_bool cancelling_{false};
    std::atomic_bool stopping_{false};
    std::chrono::steady_clock::time_point drain_started_;
};

HttpServer::HttpServer(const GatewayConfig &config,
                       AiGateway &gateway,
                       NodeLifecycle &lifecycle)
    : impl_(std::make_unique<Impl>(config, gateway, lifecycle))
{
}

HttpServer::~HttpServer() = default;

void HttpServer::run()
{
    impl_->run();
}
} // namespace ai_gateway
