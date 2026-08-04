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
    std::size_t active_streams = 0;
    std::size_t max_active_requests = 0;
    std::size_t max_active_streams = 0;
    std::size_t max_upstream_connections = 0;
    std::size_t max_upstream_host_connections = 0;
    std::size_t request_rejections = 0;
    std::size_t stream_rejections = 0;
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

    std::shared_ptr<NodeExecutionGuard> execution_guard();
    void response_finished();
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
    void cancel_remaining();
    void on_idle(std::function<void()> callback);
    NodeSnapshot snapshot() const;

private:
    std::shared_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
