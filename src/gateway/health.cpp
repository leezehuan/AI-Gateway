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

namespace ai_gateway
{
namespace
{
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
    Impl(RuntimeState &runtime,
         RoutingRuntime &routing,
         GovernanceRuntime &governance,
         ProviderTransport &transport,
         MetricsRegistry &metrics)
        : runtime_(runtime), routing_(routing), governance_(governance),
          transport_(transport), metrics_(metrics), worker_([this] { run(); })
    {
    }

    ~Impl()
    {
        stopping_.store(true);
        if (worker_.joinable())
        {
            worker_.join();
        }
    }

private:
    std::optional<HealthCheckResult> load_targets()
    {
        auto promise = std::make_shared<std::promise<HealthCheckResult>>();
        auto future = promise->get_future();
        runtime_.load_health_checks([promise](HealthCheckResult result) {
            try { promise->set_value(std::move(result)); } catch (...) {}
        });
        return await(future, stopping_);
    }

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

HealthProbeRuntime::HealthProbeRuntime(RuntimeState &runtime,
                                       RoutingRuntime &routing,
                                       GovernanceRuntime &governance,
                                       ProviderTransport &transport,
                                       MetricsRegistry &metrics)
    : impl_(std::make_unique<Impl>(runtime, routing, governance, transport, metrics))
{
}

HealthProbeRuntime::~HealthProbeRuntime() = default;
} // namespace ai_gateway
