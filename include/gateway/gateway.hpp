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
class NodeLifecycle;
class NodeExecutionGuard;
class RuntimeState;
class RoutingRuntime;
class GovernanceRuntime;
class MetricsRegistry;
struct AuthSnapshot;
using HeaderMap = std::unordered_map<std::string, std::string>;

/*
 * 一次入站 HTTP 请求的最小内部表示。
 *
 * Beast/Drogon 适配层只负责把网络对象转换成这个值；后续认证、协议校验和 Provider 调用都不再依赖 Beast 类型。
 */
struct GatewayRequest
{
    /* 规范化后的 HTTP method，例如 GET/POST。 */
    std::string method;
    /* 不包含 scheme/host 的请求路径，用于协议 Adapter 匹配。 */
    std::string path;

    std::string request_id;

    HeaderMap headers;

    std::string body;
};

class CancellationState;

class CancellationToken
{
public:
    /* 创建一个尚未取消、可被多个异步参与者观察的 token。 */
    CancellationToken();

    /* 无锁读取共享取消状态。 */
    bool is_cancelled() const;
    /* 注册取消回调；若已经取消，回调会按实现立即触发。 */
    void on_cancel(std::function<void()> callback) const;

private:
    explicit CancellationToken(std::shared_ptr<CancellationState> state);
    std::shared_ptr<CancellationState> state_;

    friend class CancellationSource;
};

class CancellationSource
{
public:
    /* 创建 token 的唯一状态源；调用方把 token 交给 Gateway，把 source 留给 HTTP 断开/停机路径。 */
    CancellationSource();


    CancellationToken token() const;
    /* 广播一次取消，重复调用保持幂等。 */
    void cancel() const;

private:
    std::shared_ptr<CancellationState> state_;
};

class ResponseWriter
{
public:
    virtual ~ResponseWriter() = default;
    /* 写入状态码和响应头；必须在 write/end 之前调用且只调用一次。 */
    virtual void begin(int status, const HeaderMap &headers) = 0;

    virtual bool write(std::string_view bytes) = 0;
    /* 标记逻辑响应结束；HTTP writer 仍可能在后台完成最后一次异步写入。 */
    virtual void end() = 0;
    /* 返回 socket/HTTP session 是否仍能接收响应。 */
    virtual bool client_connected() const = 0;
    /* 下游降到低水位时通知 Provider transfer 恢复。 */
    virtual void set_writable_callback(std::function<void()> callback) = 0;

    virtual void set_completion_callback(std::function<void()> callback) = 0;
};

struct GatewayConfig
{

    std::string listen_address = "127.0.0.1";
    /* Gateway HTTP 监听端口。 */
    std::uint16_t listen_port = 8080;

    DatabaseConfig database;
    /* HMAC API Key 和 Redis affinity 摘要共用的高熵 pepper，不可记录。 */
    std::string api_key_hmac_pepper;
    /* file: Secret 引用允许读取的唯一目录。 */
    std::string secret_dir = "/run/secrets/ai-gateway";

    std::size_t database_pool_size = 4;
    /* 专门执行阻塞 Repository 调用的后台线程数。 */
    std::size_t database_workers = 4;
    /* 数据库任务有界队列长度；满时认证和审计 fail closed。 */
    std::size_t database_queue_size = 1024;

    std::size_t auth_cache_ttl_seconds = 30;

    std::size_t auth_cache_max_entries = 10000;

    long config_poll_interval_ms = 1000;

    std::size_t max_body_bytes = 1024 * 1024;
    /* 每个 Provider 响应或整个 SSE 流累计允许的最大字节数。 */
    std::size_t max_response_bytes = 16 * 1024 * 1024;
    /* 非流式请求的总超时；流式请求中主要约束连接与首事件。 */
    long upstream_timeout_ms = 30000;
    /* 提交首个 SSE 业务事件前最多可缓存的上游字节数。 */
    std::size_t stream_prefetch_bytes = 64 * 1024;

    std::size_t stream_buffer_high_water_bytes = 256 * 1024;
    /* 队列回落到此字节数时恢复 curl 读取，必须小于 high water。 */
    std::size_t stream_buffer_low_water_bytes = 64 * 1024;

    long stream_idle_timeout_ms = 60000;
    /* 从第一次 Provider Attempt 起计算的流总时长上限，不因 failover 重置。 */
    long stream_max_duration_ms = 900000;
    /* Beast io_context 的工作线程数。 */
    std::size_t io_threads = 2;
    /* 单节点同时处于完整生命周期中的代理请求上限。 */
    std::size_t max_active_requests = 256;

    std::size_t max_active_streams = 128;

    std::size_t curl_max_total_connections = 128;
    /* curl multi 对单个 Provider host 的连接上限。 */
    std::size_t curl_max_host_connections = 64;

    long drain_timeout_ms = 60000;
    /* drain 超时取消请求后，等待审计/响应 finalization 的额外窗口。 */
    long shutdown_cancel_grace_ms = 5000;
    /* Redis 共享路由和治理服务的地址。 */
    std::string redis_host = "127.0.0.1";
    std::uint16_t redis_port = 6379;
    /* Redis ACL 用户；空值使用传统 AUTH 密码形式。 */
    std::string redis_username;
    /* Redis 密码；仅用于建立连接，不进日志。 */
    std::string redis_password;
    /* Redis logical database 编号。 */
    unsigned redis_database = 0;
    /* 路由与治理 Redis 任务的固定 worker 数。 */
    std::size_t redis_workers = 2;
    /* Redis 任务有界队列上限，满时 proxy 请求 fail closed。 */
    std::size_t redis_queue_size = 4096;
    /* TCP 连接 Redis 的时间限制。 */
    long redis_connect_timeout_ms = 1000;
    /* 单条 Redis 命令的时间限制。 */
    long redis_command_timeout_ms = 500;
    /* 所有 Gateway Redis key 的管理员可配置命名空间前缀。 */
    std::string redis_key_prefix = "aigw";

    std::size_t affinity_ttl_seconds = 300;
    /* 候选健康分和熔断状态的 Redis TTL。 */
    std::size_t routing_health_ttl_seconds = 3600;

    unsigned circuit_failure_threshold = 3;

    long circuit_open_ms = 30000;

    long circuit_probe_lease_ms = 10000;
    /* Redis 并发 permit 的 TTL；进程崩溃后由它自动回收。 */
    long governance_lease_ttl_ms = 120000;
    /* 长请求续租频率，必须小于 lease TTL 的一半。 */
    long governance_lease_renew_ms = 30000;

    /* 从环境读取全部配置；不连接数据库、Redis 或 Provider。 */
    static GatewayConfig from_env();
    /* 校验跨字段关系和安全下界；无效配置应在监听前失败。 */
    void validate() const;
};

struct ProviderRequest
{

    std::string method = "POST";

    std::string url;
    /* ProtocolAdapter 构造的认证和 allowlist Header。 */
    HeaderMap headers;
    /* 已改写为 upstream model 的协议原生 JSON。 */
    std::string body;

    std::size_t max_response_bytes = 0;
    /* 非流式总超时，或流式连接/首事件超时。 */
    long timeout_ms = 0;

    bool streaming = false;

    long idle_timeout_ms = 0;

    long max_duration_ms = 0;
};

enum class ProviderError
{
    /* curl 正常完成，最终 HTTP 状态在 ProviderResponse::status 中。 */
    none,
    /* 域名解析失败，通常尚未向 Provider 发出 HTTP 请求。 */
    dns_failure,

    connection_failure,

    tls_failure,
    /* 总超时、首事件超时或已提交流的空闲/最大时长超时。 */
    timeout,

    response_too_large,
    /* 客户端断开、drain 或治理 lease 丢失引发的主动取消。 */
    cancelled,

    callback_aborted,

    unavailable,

    shutdown
};

struct ProviderResponse
{
    /* Provider Transport 的统一结果；body 只在受控上限内缓存，流式响应通过 body chunk 回调消费。 */
    ProviderError error = ProviderError::none;
    /* Provider HTTP 状态；连接级失败通常为 0。 */
    long status = 0;
    /* 规范化为小写的 Provider 响应头，供 Content-Type/Provider request ID 检查。 */
    HeaderMap headers;
    /* 非流式受限完整 body；SSE 正常情况下保持为空。 */
    std::string body;
    /* 超时/5xx 等情况下，表示请求可能已到达 Provider，failover 审计需标记潜在重复计费。 */
    bool request_may_have_been_sent = false;

    bool has_first_byte_timing = false;

    std::uint64_t dns_ms = 0;
    std::uint64_t connect_ms = 0;
    std::uint64_t tls_ms = 0;
    std::uint64_t first_byte_ms = 0;
    std::uint64_t total_ms = 0;
};

struct ProviderResponseHead
{
    /* 已收到的上游 HTTP 状态码，提交 SSE 前必须先检查 2xx。 */
    long status = 0;
    /* 已解析的上游响应头；执行状态机用它验证 JSON/SSE Content-Type。 */
    HeaderMap headers;
};

enum class ProviderChunkAction
{

    continue_transfer,
    /* 当前 chunk 已安全接收，但下游高水位已满，curl 应在本 chunk 后暂停。 */
    pause_after_accept,
    /* 当前 chunk 无法继续处理，例如客户端断开，curl 应尽快取消。 */
    cancel_transfer
};

class ProviderTransfer
{
public:
    virtual ~ProviderTransfer() = default;
    /* 取消当前 easy handle；完成回调仍会恰好收到一次终态。 */
    virtual void cancel() = 0;

    virtual void resume() = 0;
    /* 通知 transport 已越过首事件提交门，可开始按流式超时/取消语义计时。 */
    virtual void mark_stream_started() = 0;
};

struct ProviderCallbacks
{

    std::function<void(const ProviderResponseHead &)> on_headers;
    /* 每个网络 body chunk 到达时调用；返回值控制继续、暂停或取消。 */
    std::function<ProviderChunkAction(std::string_view)> on_body;
    /* easy handle 从 multi 移除后的唯一完成回调。 */
    std::function<void(ProviderResponse)> on_complete;
};

class ProviderTransport
{
public:
    virtual ~ProviderTransport() = default;
    /* 把请求交给共享 multi worker，立即返回可取消/可恢复的 transfer 句柄。 */
    virtual std::shared_ptr<ProviderTransfer> execute(ProviderRequest request,
                                                      ProviderCallbacks callbacks) = 0;
    virtual bool healthy() const = 0;
};

class AiGateway
{
public:
    /*
     * 组装 Gateway 深模块。引用的 Runtime、Routing、Governance、Lifecycle、Metrics 和 Transport
     * 均由进程 main 持有；AiGateway 只协调请求生命周期，不拥有这些外部资源。
     */
    AiGateway(RuntimeState &runtime,
              RoutingRuntime &routing,
              GovernanceRuntime &governance,
              NodeLifecycle &lifecycle,
              MetricsRegistry &metrics,
              ProviderTransport &transport);

    /* 唯一入站 seam：执行认证、协议适配、治理、路由、Provider 调用和响应写入。 */
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
    RoutingRuntime &routing_;
    GovernanceRuntime &governance_;
    NodeLifecycle &lifecycle_;
    MetricsRegistry &metrics_;
    ProviderTransport &transport_;
};

/* 生成不含凭据或正文的公开 request ID，用于响应头、日志和审计关联。 */
std::string generate_request_id();

std::string openai_error_body(std::string message,
                              std::string type,
                              std::string code,
                              std::string param = {});
/* 输出脱敏结构化终态日志；调用方只能传入允许的字段。 */
void structured_log(const std::string &event, const HeaderMap &fields = {});
}

#endif
