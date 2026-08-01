#include "gateway/gateway.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <thread>

namespace ai_gateway
{
namespace
{
std::string env_string(const char *name, const char *fallback = "")
{
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' ? value : fallback;
}

template <typename T>
T env_number(const char *name, T fallback, T minimum, T maximum)
{
    const char *value = std::getenv(name);
    if (value == nullptr || value[0] == '\0')
    {
        return fallback;
    }

    errno = 0;
    char *end = nullptr;
    const unsigned long long parsed = std::strtoull(value, &end, 10);
    const auto lower_bound = static_cast<unsigned long long>(minimum);
    const auto upper_bound = static_cast<unsigned long long>(maximum);
    if (errno != 0 || end == value || *end != '\0' ||
        parsed < lower_bound || parsed > upper_bound)
    {
        throw std::runtime_error(std::string("invalid numeric environment variable: ") + name);
    }
    return static_cast<T>(parsed);
}

bool supported_url(const std::string &url)
{
    return url.rfind("https://", 0) == 0 || url.rfind("http://", 0) == 0;
}
} // namespace

GatewayConfig GatewayConfig::from_env()
{
    GatewayConfig config;
    config.listen_address = env_string("AI_GATEWAY_LISTEN_ADDRESS", "127.0.0.1");
    config.listen_port = env_number<std::uint16_t>(
        "AI_GATEWAY_LISTEN_PORT", 8080, 1, std::numeric_limits<std::uint16_t>::max());
    config.api_key = env_string("AI_GATEWAY_API_KEY");
    config.provider_responses_url = env_string("AI_GATEWAY_PROVIDER_RESPONSES_URL");
    config.provider_api_key = env_string("AI_GATEWAY_PROVIDER_API_KEY");
    config.logical_model = env_string("AI_GATEWAY_LOGICAL_MODEL");
    config.upstream_model = env_string("AI_GATEWAY_UPSTREAM_MODEL");
    config.max_body_bytes = env_number<std::size_t>(
        "AI_GATEWAY_MAX_BODY_BYTES", 1024 * 1024, 1024, 64 * 1024 * 1024);
    config.max_response_bytes = env_number<std::size_t>(
        "AI_GATEWAY_MAX_RESPONSE_BYTES", 16 * 1024 * 1024, 1024, 256 * 1024 * 1024);
    config.upstream_timeout_ms = env_number<long>(
        "AI_GATEWAY_UPSTREAM_TIMEOUT_MS", 30000, 100, 600000);
    config.stream_prefetch_bytes = env_number<std::size_t>(
        "AI_GATEWAY_STREAM_PREFETCH_BYTES", 64 * 1024, 1024, 16 * 1024 * 1024);
    config.stream_buffer_high_water_bytes = env_number<std::size_t>(
        "AI_GATEWAY_STREAM_BUFFER_HIGH_WATER_BYTES", 256 * 1024, 1024, 64 * 1024 * 1024);
    config.stream_buffer_low_water_bytes = env_number<std::size_t>(
        "AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES", 64 * 1024, 1, 64 * 1024 * 1024);
    config.stream_idle_timeout_ms = env_number<long>(
        "AI_GATEWAY_STREAM_IDLE_TIMEOUT_MS", 60000, 100, 600000);
    config.stream_max_duration_ms = env_number<long>(
        "AI_GATEWAY_STREAM_MAX_DURATION_MS", 900000, 1000, 24 * 60 * 60 * 1000L);
    if (config.stream_buffer_low_water_bytes >= config.stream_buffer_high_water_bytes)
    {
        throw std::runtime_error(
            "AI_GATEWAY_STREAM_BUFFER_LOW_WATER_BYTES must be less than the high water mark");
    }
    if (config.stream_prefetch_bytes > config.stream_buffer_high_water_bytes)
    {
        throw std::runtime_error(
            "AI_GATEWAY_STREAM_PREFETCH_BYTES must not exceed the stream high water mark");
    }
    const std::size_t default_threads = std::max<std::size_t>(
        2, std::min<std::size_t>(8, std::thread::hardware_concurrency()));
    config.io_threads = env_number<std::size_t>(
        "AI_GATEWAY_IO_THREADS", default_threads, 1, 64);
    return config;
}

bool GatewayConfig::ready() const
{
    return !api_key.empty() && !provider_api_key.empty() && !logical_model.empty() &&
           !upstream_model.empty() && supported_url(provider_responses_url);
}
} // namespace ai_gateway
