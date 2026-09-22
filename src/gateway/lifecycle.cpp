#include "gateway/lifecycle.hpp"

#include "gateway/gateway.hpp"

#include <mutex>
#include <utility>
#include <vector>

namespace ai_gateway
{
class NodeRequestLease::State
{
public:
    /* 保存一次节点准入的共享状态，并记录它是否对应 SSE 流。 */
    State(std::weak_ptr<NodeLifecycle::Impl> owner, bool stream)
        : owner_(std::move(owner)), stream_(stream)
    {
    }

    void execution_finished();
    void response_finished();
    void on_shutdown(std::function<void()> callback);
    bool request_shutdown();
    void abandon();

private:
    void finish(bool execution, bool response);

    std::mutex mutex_;
    std::weak_ptr<NodeLifecycle::Impl> owner_;
    std::function<void()> shutdown_callback_;
    bool stream_ = false;
    bool execution_finished_ = false;
    bool response_finished_ = false;
    bool shutdown_requested_ = false;
    bool released_ = false;
};

class NodeLifecycle::Impl : public std::enable_shared_from_this<NodeLifecycle::Impl>
{
public:


    explicit Impl(const GatewayConfig &config)
    {
        snapshot_.max_active_requests = config.max_active_requests;
        snapshot_.max_active_streams = config.max_active_streams;
        snapshot_.max_upstream_connections = config.curl_max_total_connections;
        snapshot_.max_upstream_host_connections = config.curl_max_host_connections;
    }



    NodeAdmission admit(bool stream)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (snapshot_.draining)
        {
            return {NodeAdmissionStatus::draining, {}};
        }
        if (snapshot_.active_requests >= snapshot_.max_active_requests)
        {
            ++snapshot_.request_rejections;
            return {NodeAdmissionStatus::request_capacity_exceeded, {}};
        }
        if (stream && snapshot_.active_streams >= snapshot_.max_active_streams)
        {
            ++snapshot_.stream_rejections;
            return {NodeAdmissionStatus::stream_capacity_exceeded, {}};
        }

        auto state = std::make_shared<NodeRequestLease::State>(weak_from_this(), stream);
        leases_.push_back(state);
        ++snapshot_.active_requests;
        if (stream)
        {
            ++snapshot_.active_streams;
        }
        return {NodeAdmissionStatus::admitted,
                std::shared_ptr<NodeRequestLease>(new NodeRequestLease(std::move(state)))};
    }



    bool begin_drain()
    {
        std::function<void()> idle;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (snapshot_.draining)
            {
                return false;
            }
            snapshot_.draining = true;
            if (snapshot_.active_requests == 0)
            {
                idle = idle_callback_;
            }
        }
        if (idle)
        {
            idle();
        }
        return true;
    }



    void cancel_remaining()
    {
        std::vector<std::shared_ptr<NodeRequestLease::State>> active;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto iterator = leases_.begin(); iterator != leases_.end();)
            {
                if (auto lease = iterator->lock())
                {
                    active.push_back(std::move(lease));
                    ++iterator;
                }
                else
                {
                    iterator = leases_.erase(iterator);
                }
            }
        }
        std::size_t cancelled = 0;
        for (const auto &lease : active)
        {
            if (lease->request_shutdown())
            {
                ++cancelled;
            }
        }
        if (cancelled > 0)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            snapshot_.shutdown_cancellations += cancelled;
        }
    }



    void release(bool stream)
    {
        std::function<void()> idle;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (snapshot_.active_requests > 0)
            {
                --snapshot_.active_requests;
            }
            if (stream && snapshot_.active_streams > 0)
            {
                --snapshot_.active_streams;
            }
            if (snapshot_.draining && snapshot_.active_requests == 0)
            {
                idle = idle_callback_;
            }
        }
        if (idle)
        {
            idle();
        }
    }


    void on_idle(std::function<void()> callback)
    {
        bool call_now = false;
        std::function<void()> idle;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            idle_callback_ = std::move(callback);
            call_now = snapshot_.draining && snapshot_.active_requests == 0;
            idle = idle_callback_;
        }
        if (call_now && idle)
        {
            idle();
        }
    }

    /* 返回受 mutex 保护的节点状态副本，调用方不会持有内部锁。 */
    NodeSnapshot snapshot() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return snapshot_;
    }

private:
    mutable std::mutex mutex_;
    NodeSnapshot snapshot_;
    std::vector<std::weak_ptr<NodeRequestLease::State>> leases_;
    std::function<void()> idle_callback_;
};

void NodeRequestLease::State::execution_finished() { finish(true, false); }

/* 通知 lease：下游 HTTP/SSE 实际写入已经结束或连接已经断开。 */
void NodeRequestLease::State::response_finished() { finish(false, true); }

void NodeRequestLease::State::finish(bool execution, bool response)
{
    std::shared_ptr<NodeLifecycle::Impl> owner;
    bool stream = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        execution_finished_ = execution_finished_ || execution;
        response_finished_ = response_finished_ || response;
        if (released_ || !execution_finished_ || !response_finished_)
        {
            return;
        }
        released_ = true;
        owner = owner_.lock();
        stream = stream_;
    }
    if (owner)
    {
        owner->release(stream);
    }
}

/* 保存 shutdown callback；如果取消广播先发生，则在注册时立即补调用。 */
void NodeRequestLease::State::on_shutdown(std::function<void()> callback)
{
    std::function<void()> invoke;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_callback_ = std::move(callback);
        if (shutdown_requested_)
        {
            invoke = shutdown_callback_;
        }
    }
    if (invoke)
    {
        invoke();
    }
}

bool NodeRequestLease::State::request_shutdown()
{
    std::function<void()> callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (released_ || shutdown_requested_)
        {
            return false;
        }
        shutdown_requested_ = true;
        callback = shutdown_callback_;
    }
    if (callback)
    {
        callback();
    }
    return true;
}

void NodeRequestLease::State::abandon()
{
    std::shared_ptr<NodeLifecycle::Impl> owner;
    bool stream = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (released_)
        {
            return;
        }
        released_ = true;
        owner = owner_.lock();
        stream = stream_;
    }
    if (owner)
    {
        owner->release(stream);
    }
}

NodeExecutionGuard::NodeExecutionGuard(std::shared_ptr<NodeRequestLease> lease)
    : lease_(std::move(lease))
{
}

NodeExecutionGuard::~NodeExecutionGuard()
{
    if (lease_)
    {
        lease_->execution_finished();
    }
}

/* 保存 lease 状态；shared_ptr 让异步回调可以安全延长它的生命周期。 */
NodeRequestLease::NodeRequestLease(std::shared_ptr<State> state) : state_(std::move(state)) {}

NodeRequestLease::~NodeRequestLease()
{
    if (state_)
    {
        state_->abandon();
    }
}

std::shared_ptr<NodeExecutionGuard> NodeRequestLease::execution_guard()
{
    return std::shared_ptr<NodeExecutionGuard>(new NodeExecutionGuard(shared_from_this()));
}

void NodeRequestLease::response_finished()
{
    if (state_)
    {
        state_->response_finished();
    }
}

void NodeRequestLease::execution_finished()
{
    if (state_)
    {
        state_->execution_finished();
    }
}

void NodeRequestLease::on_shutdown(std::function<void()> callback)
{
    if (state_)
    {
        state_->on_shutdown(std::move(callback));
    }
}

/* 创建带共享状态的节点生命周期对象。 */
NodeLifecycle::NodeLifecycle(const GatewayConfig &config)
    : impl_(std::make_shared<Impl>(config))
{
}

/* 生命周期析构只释放共享实现；具体 lease 会按自身 RAII 规则归还计数。 */
NodeLifecycle::~NodeLifecycle() = default;

NodeAdmission NodeLifecycle::admit(bool stream) { return impl_->admit(stream); }

/* drain 后不再 ready，但 healthz 仍由 HTTP 层保持存活。 */
bool NodeLifecycle::ready() const { return !impl_->snapshot().draining; }

bool NodeLifecycle::begin_drain() { return impl_->begin_drain(); }

/* 取消自然 drain 窗口后仍未结束的请求。 */
void NodeLifecycle::cancel_remaining() { impl_->cancel_remaining(); }

void NodeLifecycle::on_idle(std::function<void()> callback)
{
    impl_->on_idle(std::move(callback));
}

NodeSnapshot NodeLifecycle::snapshot() const { return impl_->snapshot(); }
}
