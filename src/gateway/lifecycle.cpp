#include "gateway/lifecycle.hpp"

#include "gateway/gateway.hpp"

#include <mutex>
#include <utility>
#include <vector>

/*
 * 节点本地容量和优雅停机实现。
 *
 * Redis 配额解决跨节点公平性，本模块保护单个进程不被过量 socket/流压垮。每次有效代理请求得到
 * NodeRequestLease；它必须同时等到业务 execution 完成，以及 HTTP 响应真正写完或断开，才归还容量。
 * SIGTERM 先进入 draining 并拒绝新请求；超时后经同一 lease 回调取消既有请求，促使 Provider、治理和
 * 审计仍走统一终态收束路径。
 */
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
    /*
     * 函数名直译：创建节点生命周期实现。
     *
     * 通俗说：启动时把本节点能同时处理多少请求、多少流、多少上游连接记下来，
     * 以后所有有效代理请求都从这里申请容量。
     *
     * 专业说法：初始化 NodeSnapshot 的本地容量上限；这些计数不会代替 Redis 的分布式配额。
     */
    explicit Impl(const GatewayConfig &config)
    {
        snapshot_.max_active_requests = config.max_active_requests;
        snapshot_.max_active_streams = config.max_active_streams;
        snapshot_.max_upstream_connections = config.curl_max_total_connections;
        snapshot_.max_upstream_host_connections = config.curl_max_host_connections;
    }

    /*
     * 函数名直译：申请节点容量。
     *
     * 通俗说：在访问数据库/Redis和调用 Provider 之前，原子检查节点是否正在 drain、
     * 请求数是否已满、流数是否已满；成功时返回一个 RAII lease。
     *
     * 专业说法：这是节点本地 admission gate。lease 会一直保留到执行 finalization 和实际
     * HTTP 响应都完成，避免“end 已调用但 socket 仍在写”时提前释放容量。
     *
     * 参数说明：
     * - stream：本次请求是否还要占用活动流容量。
     *
     * 返回值：包含 admitted、draining、request_capacity_exceeded 或 stream_capacity_exceeded 状态。
     *
     * 注意：mutex 保护检查和计数递增必须在同一临界区完成，否则并发请求可能突破上限。
     */
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

    /*
     * 函数名直译：开始排空节点。
     *
     * 通俗说：收到第一次 SIGTERM 后，节点不再接受新的有效代理请求，但让已经拿到 lease
     * 的请求自然完成；如果此时已没有活动请求，立即通知 HttpServer 可以关闭 listener。
     *
     * 返回值：第一次成功切换状态返回 true；重复调用返回 false。
     */
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

    /*
     * 函数名直译：取消剩余请求。
     *
     * 通俗说：自然 drain 超时后，遍历还活着的 lease，调用它们注册的 shutdown callback，
     * 让上游 transfer 被取消。弱引用已经失效的 lease 会从列表清理掉。
     *
     * 专业说法：这是第二阶段 shutdown 的广播机制；它不直接持有请求对象，避免生命周期环。
     */
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

    /*
     * 函数名直译：释放节点容量。
     *
     * 通俗说：一次请求的执行和响应都结束后，减少活动请求/活动流计数；drain 状态下如果
     * 计数降为零，就触发 idle callback，让服务器完成退出。
     *
     * 参数说明：stream 表示是否同时释放流计数。
     */
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

    /* 注册“节点进入 drain 且活动请求归零”时要执行的回调。 */
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

/* 通知 lease：持久化/上游执行部分已经最终完成。 */
void NodeRequestLease::State::execution_finished() { finish(true, false); }

/* 通知 lease：下游 HTTP/SSE 实际写入已经结束或连接已经断开。 */
void NodeRequestLease::State::response_finished() { finish(false, true); }

/*
 * 函数名直译：合并 lease 完成条件。
 *
 * 通俗说：只有“业务 finalization 完成”和“客户端响应完成”都为真，才能真正归还节点容量。
 * 任一条件重复到达都不会重复释放。
 *
 * 专业说法：这是 RAII lease 的一次性幂等终态转换，先在自身 mutex 下决定是否释放，
 * 再在锁外调用 owner，避免回调重入造成死锁。
 */
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

/*
 * 函数名直译：请求一次 shutdown。
 *
 * 通俗说：把“已请求取消”标记设为 true，并取出请求注册的回调；重复取消不会重复通知。
 *
 * 返回值：本次调用首次成功标记并通知返回 true；已经释放或已请求过返回 false。
 */
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

/*
 * 函数名直译：放弃 lease。
 *
 * 通俗说：析构时如果调用方没有正常走完两个完成条件，仍然要把节点计数归还，防止容量永久泄漏。
 *
 * 注意：正常请求应由 execution_finished/response_finished 完成；abandon 是生命周期兜底路径。
 */
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

/* 创建执行 guard；guard 析构时会自动通知执行部分完成。 */
NodeExecutionGuard::NodeExecutionGuard(std::shared_ptr<NodeRequestLease> lease)
    : lease_(std::move(lease))
{
}

/* RAII 保护：请求对象离开作用域时补交 execution_finished。 */
NodeExecutionGuard::~NodeExecutionGuard()
{
    if (lease_)
    {
        lease_->execution_finished();
    }
}

/* 保存 lease 状态；shared_ptr 让异步回调可以安全延长它的生命周期。 */
NodeRequestLease::NodeRequestLease(std::shared_ptr<State> state) : state_(std::move(state)) {}

/* lease 销毁时执行兜底释放。 */
NodeRequestLease::~NodeRequestLease()
{
    if (state_)
    {
        state_->abandon();
    }
}

/* 返回负责执行阶段完成通知的 guard。 */
std::shared_ptr<NodeExecutionGuard> NodeRequestLease::execution_guard()
{
    return std::shared_ptr<NodeExecutionGuard>(new NodeExecutionGuard(shared_from_this()));
}

/* 转发下游响应已完成通知。 */
void NodeRequestLease::response_finished()
{
    if (state_)
    {
        state_->response_finished();
    }
}

/* 转发业务执行已完成通知。 */
void NodeRequestLease::execution_finished()
{
    if (state_)
    {
        state_->execution_finished();
    }
}

/* 把 shutdown 回调挂到当前 lease。 */
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

/* 尝试为一个代理请求申请本节点容量。 */
NodeAdmission NodeLifecycle::admit(bool stream) { return impl_->admit(stream); }

/* drain 后不再 ready，但 healthz 仍由 HTTP 层保持存活。 */
bool NodeLifecycle::ready() const { return !impl_->snapshot().draining; }

/* 将节点切换到只完成存量请求的 draining 状态。 */
bool NodeLifecycle::begin_drain() { return impl_->begin_drain(); }

/* 取消自然 drain 窗口后仍未结束的请求。 */
void NodeLifecycle::cancel_remaining() { impl_->cancel_remaining(); }

/* 注册节点空闲回调。 */
void NodeLifecycle::on_idle(std::function<void()> callback)
{
    impl_->on_idle(std::move(callback));
}

/* 返回当前节点容量、拒绝和 shutdown 统计的只读快照。 */
NodeSnapshot NodeLifecycle::snapshot() const { return impl_->snapshot(); }
} // namespace ai_gateway
