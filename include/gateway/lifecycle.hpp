#ifndef AI_GATEWAY_LIFECYCLE_HPP
#define AI_GATEWAY_LIFECYCLE_HPP

#include <cstddef>
#include <functional>
#include <memory>

namespace ai_gateway
{
struct GatewayConfig;

/* 本地节点准入结果；draining 和容量拒绝都不会进入分布式治理。 */
enum class NodeAdmissionStatus
{
    admitted,
    draining,
    request_capacity_exceeded,
    stream_capacity_exceeded
};

/* Prometheus 和 shutdown 日志使用的节点容量快照。 */
struct NodeSnapshot
{
    /* 已通过本节点容量门、尚未完成 execution 或 response 的请求数。 */
    std::size_t active_requests = 0;
    /* 其中 stream=true 的完整生命周期数量。 */
    std::size_t active_streams = 0;
    /* 本节点请求容量硬上限。 */
    std::size_t max_active_requests = 0;
    /* 本节点流容量硬上限，必须不大于 max_active_requests。 */
    std::size_t max_active_streams = 0;
    /* libcurl multi 的总连接限制。 */
    std::size_t max_upstream_connections = 0;
    /* 同一 Provider 主机的连接限制。 */
    std::size_t max_upstream_host_connections = 0;
    /* 因请求容量耗尽被拒绝的累计次数。 */
    std::size_t request_rejections = 0;
    /* 因流容量耗尽被拒绝的累计次数。 */
    std::size_t stream_rejections = 0;
    /* drain 超时主动取消的请求累计次数。 */
    std::size_t shutdown_cancellations = 0;
    /* true 表示停止接受新的有效代理请求，但 healthz 仍可用。 */
    bool draining = false;
};

class NodeRequestLease;

class NodeExecutionGuard
{
public:
    /* guard 析构时通知业务执行部分结束。 */
    ~NodeExecutionGuard();

private:
    explicit NodeExecutionGuard(std::shared_ptr<NodeRequestLease> lease);
    std::shared_ptr<NodeRequestLease> lease_;

    friend class NodeRequestLease;
};

class NodeRequestLease : public std::enable_shared_from_this<NodeRequestLease>
{
public:
    class State;
    ~NodeRequestLease();

    /* 创建执行完成 guard；响应完成仍由 HttpSession completion callback 单独通知。 */
    std::shared_ptr<NodeExecutionGuard> execution_guard();
    /* 通知下游 socket 已写完或已断开。 */
    void response_finished();
    /* 注册 drain 超时取消回调。 */
    void on_shutdown(std::function<void()> callback);

private:
    explicit NodeRequestLease(std::shared_ptr<State> state);
    void execution_finished();
    std::shared_ptr<State> state_;

    friend class NodeLifecycle;
    friend class NodeExecutionGuard;
};

struct NodeAdmission
{
    /* 失败状态说明是 draining 还是哪一道本地容量门拒绝。 */
    NodeAdmissionStatus status = NodeAdmissionStatus::draining;
    /* admitted 时的共享 RAII lease；失败时为空。 */
    std::shared_ptr<NodeRequestLease> lease;
};

class NodeLifecycle
{
public:
    class Impl;
    explicit NodeLifecycle(const GatewayConfig &config);
    ~NodeLifecycle();

    NodeLifecycle(const NodeLifecycle &) = delete;
    NodeLifecycle &operator=(const NodeLifecycle &) = delete;

    /* 原子申请本节点请求/流容量。 */
    NodeAdmission admit(bool stream);
    /* draining 时返回 false；容量瞬时饱和不影响 ready。 */
    bool ready() const;
    /* 第一次终止信号进入 draining。 */
    bool begin_drain();
    /* drain 超时后取消仍存活的 lease。 */
    void cancel_remaining();
    /* 活动请求归零时执行回调。 */
    void on_idle(std::function<void()> callback);
    /* 返回线程安全的本地状态副本。 */
    NodeSnapshot snapshot() const;

private:
    std::shared_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
