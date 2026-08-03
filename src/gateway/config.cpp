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

} // namespace

GatewayConfig GatewayConfig::from_env()
{
    GatewayConfig config;
    config.listen_address = env_string("AI_GATEWAY_LISTEN_ADDRESS", "127.0.0.1");
    config.listen_port = env_number<std::uint16_t>(
        "AI_GATEWAY_LISTEN_PORT", 8080, 1, std::numeric_limits<std::uint16_t>::max());
    config.database.host = env_string("AI_GATEWAY_DB_HOST", "127.0.0.1");
    config.database.port = env_number<std::uint16_t>(
        "AI_GATEWAY_DB_PORT", 3306, 1, std::numeric_limits<std::uint16_t>::max());
    config.database.user = env_string("AI_GATEWAY_DB_USER", "ai_gateway");
    config.database.password = env_string("AI_GATEWAY_DB_PASSWORD");
    config.database.name = env_string("AI_GATEWAY_DB_NAME", "ai_gateway");
    config.database.connect_timeout_seconds = env_number<unsigned>(
        "AI_GATEWAY_DB_CONNECT_TIMEOUT_SECONDS", 2, 1, 30);
    config.api_key_hmac_pepper = env_string("AI_GATEWAY_API_KEY_HMAC_PEPPER");
    config.secret_dir = env_string("AI_GATEWAY_SECRET_DIR", "/run/secrets/ai-gateway");
    config.database_pool_size = env_number<std::size_t>(
        "AI_GATEWAY_DB_POOL_SIZE", 4, 1, 64);
    config.database_workers = env_number<std::size_t>(
        "AI_GATEWAY_DB_WORKERS", 4, 1, 64);
    config.database_queue_size = env_number<std::size_t>(
        "AI_GATEWAY_DB_QUEUE_SIZE", 1024, 1, 100000);
    config.auth_cache_ttl_seconds = env_number<std::size_t>(
        "AI_GATEWAY_AUTH_CACHE_TTL_SECONDS", 30, 1, 3600);
    config.auth_cache_max_entries = env_number<std::size_t>(
        "AI_GATEWAY_AUTH_CACHE_MAX_ENTRIES", 10000, 1, 1000000);
    config.config_poll_interval_ms = env_number<long>(
        "AI_GATEWAY_CONFIG_POLL_INTERVAL_MS", 1000, 100, 60000);
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
    config.redis_host = env_string("AI_GATEWAY_REDIS_HOST", "127.0.0.1");
    config.redis_port = env_number<std::uint16_t>(
        "AI_GATEWAY_REDIS_PORT", 6379, 1, std::numeric_limits<std::uint16_t>::max());
    config.redis_username = env_string("AI_GATEWAY_REDIS_USERNAME");
    config.redis_password = env_string("AI_GATEWAY_REDIS_PASSWORD");
    config.redis_database = env_number<unsigned>("AI_GATEWAY_REDIS_DATABASE", 0, 0, 255);
    config.redis_workers = env_number<std::size_t>("AI_GATEWAY_REDIS_WORKERS", 2, 1, 32);
    config.redis_queue_size = env_number<std::size_t>(
        "AI_GATEWAY_REDIS_QUEUE_SIZE", 4096, 1, 100000);
    config.redis_connect_timeout_ms = env_number<long>(
        "AI_GATEWAY_REDIS_CONNECT_TIMEOUT_MS", 1000, 10, 30000);
    config.redis_command_timeout_ms = env_number<long>(
        "AI_GATEWAY_REDIS_COMMAND_TIMEOUT_MS", 500, 10, 30000);
    config.redis_key_prefix = env_string("AI_GATEWAY_REDIS_KEY_PREFIX", "aigw");
    config.affinity_ttl_seconds = env_number<std::size_t>(
        "AI_GATEWAY_AFFINITY_TTL_SECONDS", 300, 1, 86400);
    config.routing_health_ttl_seconds = env_number<std::size_t>(
        "AI_GATEWAY_ROUTING_HEALTH_TTL_SECONDS", 3600, 30, 604800);
    config.circuit_failure_threshold = env_number<unsigned>(
        "AI_GATEWAY_CIRCUIT_FAILURE_THRESHOLD", 3, 1, 100);
    config.circuit_open_ms = env_number<long>(
        "AI_GATEWAY_CIRCUIT_OPEN_MS", 30000, 100, 3600000);
    config.circuit_probe_lease_ms = env_number<long>(
        "AI_GATEWAY_CIRCUIT_PROBE_LEASE_MS", 10000, 100, 60000);
    config.governance_lease_ttl_ms = env_number<long>(
        "AI_GATEWAY_GOVERNANCE_LEASE_TTL_MS", 120000, 1000, 3600000);
    config.governance_lease_renew_ms = env_number<long>(
        "AI_GATEWAY_GOVERNANCE_LEASE_RENEW_MS", 30000, 100, 1200000);
    return config;
}

void GatewayConfig::validate() const
{
    if (database.host.empty() || database.user.empty() || database.name.empty() ||
        database.password.empty())
    {
        throw std::runtime_error("Gateway database configuration is incomplete");
    }
    if (api_key_hmac_pepper.size() < 32)
    {
        throw std::runtime_error("AI_GATEWAY_API_KEY_HMAC_PEPPER must contain at least 32 bytes");
    }
    if (redis_host.empty() || redis_key_prefix.empty() ||
        redis_key_prefix.find_first_not_of(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-") !=
            std::string::npos)
    {
        throw std::runtime_error("Gateway Redis configuration is invalid");
    }
    if (governance_lease_renew_ms * 2 >= governance_lease_ttl_ms)
    {
        throw std::runtime_error(
            "AI_GATEWAY_GOVERNANCE_LEASE_RENEW_MS must be less than half the lease TTL");
    }
}
} // namespace ai_gateway
