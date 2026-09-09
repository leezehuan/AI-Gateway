#include "gateway/gateway.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <thread>

/*
 * 环境变量到 GatewayConfig 的唯一转换入口。
 *
 * 这里不读取 .env 文件，也不连接外部服务；部署系统负责注入环境。from_env 负责逐项取值和基础数值范围，
 * validate 负责跨字段约束，例如 low water 必须小于 high water、流容量不能超过请求容量、curl host 上限
 * 不能超过总上限。把配置错误留在启动前发现，能避免 HTTP 已监听后才因 Secret/容量组合不合法发生故障。
 */
namespace ai_gateway
{
namespace
{
/*
 * 函数名直译：读取字符串环境变量。
 *
 * 通俗说：配置对象创建时，先从进程环境里取一个字符串；如果变量没有设置，
 * 就使用调用方提供的默认值。空字符串也视为“没有配置”。
 *
 * 专业说法：这是统一的环境变量读取适配器，把操作系统环境转换为网关配置字段，
 * 并集中处理可选配置的默认值。
 *
 * 参数说明：
 * - name：环境变量名称。
 * - fallback：变量缺失或为空时返回的默认字符串。
 *
 * 返回值：环境变量的有效内容，或 fallback。
 *
 * 实现方法：调用 std::getenv；只有指针非空且首字符不是 '\0' 时才采用环境值。
 */
std::string env_string(const char *name, const char *fallback = "")
{
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' ? value : fallback;
}

/*
 * 函数名直译：读取数值环境变量。
 *
 * 通俗说：把例如端口、超时时间、队列长度这样的环境变量文字转换成数字，
 * 并检查它是否落在允许范围内；写错时立即抛出异常，避免服务带着危险配置启动。
 *
 * 专业说法：这是带边界校验的无符号十进制配置解析器。模板参数让同一套逻辑可用于
 * uint16_t、size_t 和 long 等配置字段。
 *
 * 参数说明：
 * - name：环境变量名称。
 * - fallback：变量缺失时的默认数值。
 * - minimum、maximum：闭区间合法范围。
 *
 * 返回值：解析后的数值。
 *
 * 实现方法：使用 strtoull 解析十进制文本；同时检查 errno、是否读到数字、是否有尾随字符，
 * 再检查上下界，最后转换为模板类型。
 *
 * 注意：这里只接受纯十进制文本，不接受带单位的值，例如“30s”。
 */
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

/*
 * 函数名直译：从环境变量构造网关配置。
 *
 * 通俗说：启动程序时，这个函数把监听地址、MySQL、Redis、SSE、路由、配额和停机参数
 * 一次性读进一个 GatewayConfig，后续模块只依赖这个快照，不再到处读取 getenv。
 *
 * 专业说法：这是进程级静态配置 Adapter。它负责读取默认值、解析类型，并在构造末尾验证
 * 流缓冲区、节点容量、连接数和治理租约之间的关系。
 *
 * 返回值：未发现关系约束错误时返回完整 GatewayConfig；环境变量格式错误或组合非法时抛出异常。
 *
 * 实现方法：
 * 1. 读取 HTTP、数据库和 HMAC 身份配置。
 * 2. 读取请求/响应上限、SSE 预读和高低水位。
 * 3. 读取本地节点容量、libcurl 连接池和 drain 参数。
 * 4. 读取 Redis 路由、断路器和 Governance lease 参数。
 * 5. 检查 low water < high water、prefetch <= high water 等跨字段约束。
 */
GatewayConfig GatewayConfig::from_env()
{
    GatewayConfig config;
    // HTTP 监听配置；地址默认只绑定本机，避免新实例意外暴露到公网。
    config.listen_address = env_string("AI_GATEWAY_LISTEN_ADDRESS", "127.0.0.1");
    config.listen_port = env_number<std::uint16_t>(
        "AI_GATEWAY_LISTEN_PORT", 8080, 1, std::numeric_limits<std::uint16_t>::max());
    // Gateway 身份、策略和 Usage 均存放在独立数据库。
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
    // 入站和上游响应都设置硬上限，防止异常正文无限占用内存。
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
    // SSE Writer 依靠高/低水位在“暂停上游”和“恢复上游”之间形成滞回区。
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
    // 这些限制是单节点容量，不等同于 Redis 中的租户并发配额。
    config.max_active_requests = env_number<std::size_t>(
        "AI_GATEWAY_MAX_ACTIVE_REQUESTS", 256, 1, 100000);
    config.max_active_streams = env_number<std::size_t>(
        "AI_GATEWAY_MAX_ACTIVE_STREAMS", 128, 1, 100000);
    config.curl_max_total_connections = env_number<std::size_t>(
        "AI_GATEWAY_CURL_MAX_TOTAL_CONNECTIONS", 128, 1, 10000);
    config.curl_max_host_connections = env_number<std::size_t>(
        "AI_GATEWAY_CURL_MAX_HOST_CONNECTIONS", 64, 1, 10000);
    config.drain_timeout_ms = env_number<long>(
        "AI_GATEWAY_DRAIN_TIMEOUT_MS", 60000, 100, 3600000);
    config.shutdown_cancel_grace_ms = env_number<long>(
        "AI_GATEWAY_SHUTDOWN_CANCEL_GRACE_MS", 5000, 100, 60000);
    // Redis 保存跨节点 RPM、lease、affinity 和断路器状态。
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

/*
 * 函数名直译：验证网关配置。
 *
 * 通俗说：from_env 负责“读进来”，这里负责最后把必填项和相互依赖的限制再检查一遍，
 * 例如数据库密码不能缺失、流数量不能超过请求数量、每主机连接不能超过总连接数。
 *
 * 专业说法：这是启动就绪前的 fail-closed 配置不变量检查。失败时通过异常阻止进程进入监听状态，
 * 避免运行中才发现无法认证、无法写审计或容量参数互相矛盾。
 *
 * 实现方法：按数据库/HMAC、Redis、治理租约、节点容量、curl 连接池五组规则依次检查；
 * 任一规则不满足就抛出带环境变量名称的 std::runtime_error。
 */
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
    if (max_active_streams > max_active_requests)
    {
        throw std::runtime_error(
            "AI_GATEWAY_MAX_ACTIVE_STREAMS must not exceed active requests");
    }
    if (curl_max_host_connections > curl_max_total_connections)
    {
        throw std::runtime_error(
            "AI_GATEWAY_CURL_MAX_HOST_CONNECTIONS must not exceed total connections");
    }
}
} // namespace ai_gateway
