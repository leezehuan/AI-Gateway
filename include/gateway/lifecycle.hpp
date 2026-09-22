#ifndef AI_GATEWAY_LIFECYCLE_HPP
#define AI_GATEWAY_LIFECYCLE_HPP

#include <cstddef>
#include <functional>
#include <memory>

namespace ai_gateway
{
struct GatewayConfig;

enum class NodeAdmissionStatus
{
    admitted,
    draining,
    request_capacity_exceeded,
    stream_capacity_exceeded
};

struct NodeSnapshot
{

    std::size_t active_requests = 0;
    /* 其中 stream=true 的完整生命周期数量。 */
    std::size_t active_streams = 0;

    std::size_t max_active_requests = 0;
    /* 本节点流容量硬上限，必须不大于 max_active_requests。 */
    std::size_t max_active_streams = 0;

    std::size_t max_upstream_connections = 0;
    /* 同一 Provider 主机的连接限制。 */
    std::size_t max_upstream_host_connections = 0;

    std::size_t request_rejections = 0;

    std::size_t stream_rejections = 0;
    /* drain 超时主动取消的请求累计次数。 */
    std::size_t shutdown_cancellations = 0;

    bool draining = false;
};

class NodeRequestLease;

class NodeExecutionGuard
{
public:

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

    NodeAdmissionStatus status = NodeAdmissionStatus::draining;

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


    NodeAdmission admit(bool stream);

    bool ready() const;

    bool begin_drain();
    /* drain 超时后取消仍存活的 lease。 */
    void cancel_remaining();

    void on_idle(std::function<void()> callback);
    /* 返回线程安全的本地状态副本。 */
    NodeSnapshot snapshot() const;

private:
    std::shared_ptr<Impl> impl_;
};
}

#endif
