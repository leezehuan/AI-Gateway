#ifndef AI_GATEWAY_GATEWAY_HPP
#define AI_GATEWAY_GATEWAY_HPP

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "gateway/mysql.hpp"

namespace ai_gateway
{
class RuntimeState;
struct AuthSnapshot;
using HeaderMap = std::unordered_map<std::string, std::string>;

struct GatewayRequest
{
    std::string method;
    std::string path;
    std::string request_id;
    HeaderMap headers;
    std::string body;
};

class CancellationState;

class CancellationToken
{
public:
    CancellationToken();

    bool is_cancelled() const;
    void on_cancel(std::function<void()> callback) const;

private:
    explicit CancellationToken(std::shared_ptr<CancellationState> state);
    std::shared_ptr<CancellationState> state_;

    friend class CancellationSource;
};

class CancellationSource
{
public:
    CancellationSource();

    CancellationToken token() const;
    void cancel() const;

private:
    std::shared_ptr<CancellationState> state_;
};

class ResponseWriter
{
public:
    virtual ~ResponseWriter() = default;
    virtual void begin(int status, const HeaderMap &headers) = 0;
    virtual bool write(std::string_view bytes) = 0;
    virtual void end() = 0;
    virtual bool client_connected() const = 0;
    virtual void set_writable_callback(std::function<void()> callback) = 0;
};

struct GatewayConfig
{
    std::string listen_address = "127.0.0.1";
    std::uint16_t listen_port = 8080;
    DatabaseConfig database;
    std::string api_key_hmac_pepper;
    std::string secret_dir = "/run/secrets/ai-gateway";
    std::size_t database_pool_size = 4;
    std::size_t database_workers = 4;
    std::size_t database_queue_size = 1024;
    std::size_t auth_cache_ttl_seconds = 30;
    std::size_t auth_cache_max_entries = 10000;
    long config_poll_interval_ms = 1000;
    std::size_t max_body_bytes = 1024 * 1024;
    std::size_t max_response_bytes = 16 * 1024 * 1024;
    long upstream_timeout_ms = 30000;
    std::size_t stream_prefetch_bytes = 64 * 1024;
    std::size_t stream_buffer_high_water_bytes = 256 * 1024;
    std::size_t stream_buffer_low_water_bytes = 64 * 1024;
    long stream_idle_timeout_ms = 60000;
    long stream_max_duration_ms = 900000;
    std::size_t io_threads = 2;

    static GatewayConfig from_env();
    void validate() const;
};

struct ProviderRequest
{
    std::string url;
    HeaderMap headers;
    std::string body;
    std::size_t max_response_bytes = 0;
    long timeout_ms = 0;
    bool streaming = false;
    long idle_timeout_ms = 0;
    long max_duration_ms = 0;
};

enum class ProviderError
{
    none,
    timeout,
    response_too_large,
    cancelled,
    callback_aborted,
    unavailable,
    shutdown
};

struct ProviderResponse
{
    ProviderError error = ProviderError::none;
    long status = 0;
    HeaderMap headers;
    std::string body;
};

struct ProviderResponseHead
{
    long status = 0;
    HeaderMap headers;
};

enum class ProviderChunkAction
{
    continue_transfer,
    pause_after_accept,
    cancel_transfer
};

class ProviderTransfer
{
public:
    virtual ~ProviderTransfer() = default;
    virtual void cancel() = 0;
    virtual void resume() = 0;
    virtual void mark_stream_started() = 0;
};

struct ProviderCallbacks
{
    std::function<void(const ProviderResponseHead &)> on_headers;
    std::function<ProviderChunkAction(std::string_view)> on_body;
    std::function<void(ProviderResponse)> on_complete;
};

class ProviderTransport
{
public:
    virtual ~ProviderTransport() = default;
    virtual std::shared_ptr<ProviderTransfer> execute(ProviderRequest request,
                                                      ProviderCallbacks callbacks) = 0;
    virtual bool healthy() const = 0;
};

class AiGateway
{
public:
    AiGateway(RuntimeState &runtime, ProviderTransport &transport);

    void handle(const GatewayRequest &request,
                ResponseWriter &response,
                CancellationToken cancellation);
    bool ready() const;

private:
    void handle_authorized(GatewayRequest request,
                           ResponseWriter &response,
                           CancellationToken cancellation,
                           std::chrono::steady_clock::time_point started,
                           std::shared_ptr<const AuthSnapshot> snapshot);

    RuntimeState &runtime_;
    ProviderTransport &transport_;
};

std::string generate_request_id();
std::string openai_error_body(std::string message,
                              std::string type,
                              std::string code,
                              std::string param = {});
void structured_log(const std::string &event, const HeaderMap &fields = {});
} // namespace ai_gateway

#endif
