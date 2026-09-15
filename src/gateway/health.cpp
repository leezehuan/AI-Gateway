#include "gateway/health.hpp"

#include "gateway/gateway.hpp"
#include "gateway/governance.hpp"
#include "gateway/metrics.hpp"
#include "gateway/routing.hpp"
#include "gateway/runtime.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

/*
 * Provider 主动健康探测控制面。
 *
 * 只有管理员显式配置的非计费 GET/HEAD URL 会被探测。后台线程先抢 Redis probe lease，保证多节点每个周期
 * 至多一个节点访问同一目标；随后申请 Credential 配额、通过共享 curl transport 调用、释放 permit，最后把
 * 结果写回 RoutingRuntime 的候选健康状态。探测不创建客户端 Usage 或 request_attempts，也不要求全部
 * Provider 健康才能让 Gateway ready，因为单个 Logical Model 仍可能有其他可路由候选。
 */
namespace ai_gateway
{
namespace
{
/*
 * 函数名直译：等待 Future。
 *
 * 通俗说：健康探测运行在自己的后台线程，但 Runtime、Governance 等模块通过异步回调返回结果。
 * 这个小循环每 50ms 检查一次结果和停机标志，避免析构时无限阻塞。
 *
 * 专业说法：把 callback API 临时桥接为 future 等待；仅供专用探测线程使用，不能放进 Drogon I/O 线程。
 *
 * 返回值：任务完成时得到值；停机时返回空值。
 */
template <typename T>
std::optional<T> await(std::future<T> &future, const std::atomic_bool &stopping)
{
    while (!stopping.load())
    {
        if (future.wait_for(std::chrono::milliseconds(50)) == std::future_status::ready)
        {
            return future.get();
        }
    }
    return std::nullopt;
}
} // namespace

class HealthProbeRuntime::Impl
{
public:
    /*
     * 函数名直译：健康探测实现构造函数。
 *
     * 通俗说：保存 Gateway 各深模块，并启动唯一后台探测线程。这个线程不会占用 HTTP 事件循环。
 *
     * 专业说法：主动健康检查独立于客户端请求，不创建 Usage 或 request_attempts，但会共用路由健康状态和 Credential 配额。
     */
    Impl(RuntimeState &runtime,
         RoutingRuntime &routing,
         GovernanceRuntime &governance,
         ProviderTransport &transport,
         MetricsRegistry &metrics)
        : runtime_(runtime), routing_(routing), governance_(governance),
          transport_(transport), metrics_(metrics), worker_([this] { run(); })
    {
    }

    /* 设置停止标志并等待探测线程退出，避免后台回调访问已经析构的模块。 */
    ~Impl()
    {
        stopping_.store(true);
        if (worker_.joinable())
        {
            worker_.join();
        }
    }

private:
    /* 异步从 RuntimeState 加载管理员显式配置的探测目标。 */
    std::optional<HealthCheckResult> load_targets()
    {
        auto promise = std::make_shared<std::promise<HealthCheckResult>>();
        auto future = promise->get_future();
        runtime_.load_health_checks([promise](HealthCheckResult result) {
            try { promise->set_value(std::move(result)); } catch (...) {}
        });
        return await(future, stopping_);
    }

    /*
     * 为“配置版本 + 探测 ID”抢占 Redis lease，同一周期多个节点中只允许一个节点实际探测。
     */
    std::optional<bool> acquire_probe(const HealthCheckTarget &target)
    {
        auto promise = std::make_shared<std::promise<std::optional<bool>>>();
        auto future = promise->get_future();
        governance_.acquire_probe_lease(
            std::to_string(target.config_version) + ":" + std::to_string(target.id),
            target.interval_ms,
            [promise](std::optional<bool> acquired) {
                try { promise->set_value(acquired); } catch (...) {}
            });
        auto result = await(future, stopping_);
        return result ? *result : std::optional<bool>();
    }

    /*
     * 健康探测不消耗租户/Key 配额，但会占用 Credential 的 RPM/并发，防止探测绕过上游自身限制。
     */
    std::shared_ptr<const GovernancePermit> acquire_credential(
        const HealthCheckTarget &target,
        const std::shared_ptr<std::atomic_bool> &lease_lost)
    {
        std::vector<GovernanceScope> scopes;
        if (target.credential_quota)
        {
            scopes.push_back({"credential", target.credential_id,
                              target.credential_quota->rpm,
                              target.credential_quota->concurrency});
        }
        auto promise = std::make_shared<std::promise<GovernanceResult>>();
        auto future = promise->get_future();
        governance_.admit(
            std::move(scopes), [lease_lost] { lease_lost->store(true); },
            [promise](GovernanceResult result) {
                try { promise->set_value(std::move(result)); } catch (...) {}
            });
        auto result = await(future, stopping_);
        if (!result || result->status != GovernanceStatus::admitted)
        {
            return {};
        }
        return std::move(result->permit);
    }

    /*
     * 函数名直译：执行探测。
 *
     * 通俗说：向管理员配置的非计费 URL 发 GET/HEAD，并等待完成；停机或 Credential lease 丢失时立即取消上游传输。
 *
     * 专业说法：ProviderTransport 仍由共享 curl multi worker 执行，future 只在本后台线程等待。
 *
     * 注意：provider_api_key 只放入请求头，不写日志、指标或持久化记录。
     */
    std::optional<ProviderResponse> execute_probe(
        const HealthCheckTarget &target,
        const std::shared_ptr<std::atomic_bool> &lease_lost)
    {
        ProviderRequest request;
        request.method = target.method;
        request.url = target.url;
        request.headers = {{"authorization", "Bearer " + target.provider_api_key},
                           {"accept", "application/json"}};
        request.max_response_bytes = 64 * 1024;
        request.timeout_ms = target.timeout_ms;
        auto promise = std::make_shared<std::promise<ProviderResponse>>();
        auto future = promise->get_future();
        auto transfer = transport_.execute(
            std::move(request),
            {{}, {}, [promise](ProviderResponse response) {
                try { promise->set_value(std::move(response)); } catch (...) {}
            }});
        while (!stopping_.load() && !lease_lost->load())
        {
            if (future.wait_for(std::chrono::milliseconds(25)) == std::future_status::ready)
            {
                return future.get();
            }
        }
        transfer->cancel();
        return std::nullopt;
    }

    /* 异步释放探测占用的 Credential governance permit，释放失败时调用方停止后续健康反馈。 */
    bool release_credential(std::shared_ptr<const GovernancePermit> permit)
    {
        auto promise = std::make_shared<std::promise<bool>>();
        auto future = promise->get_future();
        governance_.release(std::move(permit), [promise](bool released) {
            try { promise->set_value(released); } catch (...) {}
        });
        auto result = await(future, stopping_);
        return result && *result;
    }

    /* 把探测成败写入每个关联 Mapping 的共享路由健康状态。 */
    void record(const HealthCheckTarget &target, bool success)
    {
        for (const auto &fingerprint : target.candidate_fingerprints)
        {
            auto promise = std::make_shared<std::promise<bool>>();
            auto future = promise->get_future();
            routing_.record_health_probe(fingerprint, success, [promise](bool stored) {
                try { promise->set_value(stored); } catch (...) {}
            });
            if (!await(future, stopping_))
            {
                return;
            }
        }
    }

    /*
     * 函数名直译：探测一个目标。
 *
     * 通俗说：抢到跨节点 lease 后，先申请 Credential 额度，再调用 Provider，最后释放额度并更新指标与熔断分数。
 * 没抢到 lease 或额度不足均安静跳过，不把它误判为 Provider 故障。
 *
     * 专业说法：把 probe lease、GovernancePermit、ProviderTransport 与 RoutingRuntime 串成非计费控制面工作流。
     */
    void probe(const HealthCheckTarget &target)
    {
        const auto probe_lease = acquire_probe(target);
        if (!probe_lease || !*probe_lease)
        {
            return;
        }
        auto lease_lost = std::make_shared<std::atomic_bool>(false);
        auto permit = acquire_credential(target, lease_lost);
        if (!permit)
        {
            return;
        }
        metrics_.upstream_started();
        const auto response = execute_probe(target, lease_lost);
        metrics_.upstream_finished();
        if (!release_credential(std::move(permit)))
        {
            return;
        }
        const bool success = response && !lease_lost->load() &&
                             response->error == ProviderError::none &&
                             response->status >= 200 && response->status < 300;
        metrics_.health_probe_completed(target.provider_slug, target.credential_name, success);
        record(target, success);
    }

    /*
     * 函数名直译：运行探测循环。
 *
     * 通俗说：每秒刷新一次管理员配置，并按每个目标自己的 interval_ms 判断是否到期。
 *
     * 专业说法：due map 使用 steady_clock，避免系统时间调整使本地调度提前/滞后；Provider 成败只影响候选健康，
 * 不会单独把整个 Gateway 判为 not ready。
     */
    void run()
    {
        std::vector<HealthCheckTarget> targets;
        std::unordered_map<std::uint64_t, std::chrono::steady_clock::time_point> due;
        auto reload_at = std::chrono::steady_clock::now();
        while (!stopping_.load())
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= reload_at)
            {
                auto loaded = load_targets();
                if (loaded && loaded->available)
                {
                    targets = std::move(loaded->targets);
                }
                reload_at = now + std::chrono::seconds(1);
            }
            for (const auto &target : targets)
            {
                const auto found = due.find(target.id);
                if (found != due.end() && now < found->second)
                {
                    continue;
                }
                due[target.id] = now + std::chrono::milliseconds(target.interval_ms);
                probe(target);
                if (stopping_.load())
                {
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    RuntimeState &runtime_;
    RoutingRuntime &routing_;
    GovernanceRuntime &governance_;
    ProviderTransport &transport_;
    MetricsRegistry &metrics_;
    std::atomic_bool stopping_{false};
    std::thread worker_;
};

/* 创建进程级主动健康探测运行时，并立即启动其后台线程。 */
HealthProbeRuntime::HealthProbeRuntime(RuntimeState &runtime,
                                       RoutingRuntime &routing,
                                       GovernanceRuntime &governance,
                                       ProviderTransport &transport,
                                       MetricsRegistry &metrics)
    : impl_(std::make_unique<Impl>(runtime, routing, governance, transport, metrics))
{
}

/* 销毁 Pimpl，等待后台健康探测停止。 */
HealthProbeRuntime::~HealthProbeRuntime() = default;
} // namespace ai_gateway
