#include "gateway/metrics.hpp"

#include <algorithm>
#include <map>
#include <mutex>
#include <sstream>
#include <tuple>

namespace ai_gateway
{
namespace
{

std::string escaped(const std::string &value)
{
    std::string result;
    result.reserve(value.size());
    for (char character : value)
    {
        if (character == '\\' || character == '"')
        {
            result.push_back('\\');
            result.push_back(character);
        }
        else if (character == '\n')
        {
            result += "\\n";
        }
        else
        {
            result.push_back(character);
        }
    }
    return result;
}

void label(std::ostringstream &output, const char *name, const std::string &value, bool first)
{
    if (!first)
    {
        output << ',';
    }
    output << name << "=\"" << escaped(value) << '"';
}
}

class MetricsRegistry::Impl
{
public:
    using RequestKey = std::tuple<std::string, std::string, std::string, int, std::string>;
    using AttemptKey = std::tuple<std::string, std::string, std::string, std::string,
                                  std::string, std::string, std::string>;
    using TargetKey = std::tuple<std::string, std::string, std::string, std::string,
                                 std::string>;

    /* 在 mutex 下更新请求计数、延迟、failover 和背压累计值。 */
    void request_completed(const std::string &tenant,
                           const std::string &protocol,
                           const std::string &model,
                           int status,
                           const std::string &error,
                           std::uint64_t duration,
                           std::size_t failovers,
                           std::size_t pauses)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++requests_[{tenant, protocol, model, status, error}];
        request_duration_sum_ms_ += duration;
        ++request_duration_count_;
        failovers_ += failovers;
        backpressure_pauses_ += pauses;
    }


    void attempt_completed(const std::string &tenant,
                           const std::string &protocol,
                           const std::string &model,
                           const std::string &provider,
                           const std::string &credential,
                           const std::string &state,
                           const std::string &error,
                           const UsageAccounting &usage,
                           std::optional<std::uint64_t> first_byte,
                           std::uint64_t duration)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++attempts_[{tenant, protocol, model, provider, credential, state, error}];
        const TargetKey target{tenant, protocol, model, provider, credential};
        if (usage.input_tokens) input_tokens_[target] += *usage.input_tokens;
        if (usage.cached_input_tokens) cached_tokens_[target] += *usage.cached_input_tokens;
        if (usage.output_tokens) output_tokens_[target] += *usage.output_tokens;
        if (usage.cost_microusd) costs_[target] += *usage.cost_microusd;
        if (first_byte)
        {
            first_byte_sum_ms_[target] += *first_byte;
            ++first_byte_count_[target];
        }
        attempt_duration_sum_ms_[target] += duration;
        ++attempt_duration_count_[target];
    }

    /* 在同一把 mutex 下生成完整 Prometheus 文本，避免输出半个样本。 */
    std::string render() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ostringstream output;
        output << "# HELP ai_gateway_requests_total Completed client requests.\n"
               << "# TYPE ai_gateway_requests_total counter\n";
        for (const auto &entry : requests_)
        {
            const auto &[tenant, protocol, model, status, error] = entry.first;
            output << "ai_gateway_requests_total{";
            label(output, "tenant", tenant, true);
            label(output, "protocol", protocol, false);
            label(output, "logical_model", model, false);
            label(output, "status", std::to_string(status), false);
            label(output, "error_class", error, false);
            output << "} " << entry.second << '\n';
        }
        output << "# HELP ai_gateway_attempts_total Completed Provider attempts.\n"
               << "# TYPE ai_gateway_attempts_total counter\n";
        for (const auto &entry : attempts_)
        {
            const auto &[tenant, protocol, model, provider, credential, state, error] = entry.first;
            output << "ai_gateway_attempts_total{";
            label(output, "tenant", tenant, true);
            label(output, "protocol", protocol, false);
            label(output, "logical_model", model, false);
            label(output, "provider", provider, false);
            label(output, "credential", credential, false);
            label(output, "state", state, false);
            label(output, "error_class", error, false);
            output << "} " << entry.second << '\n';
        }
        render_target_counter(output, "ai_gateway_input_tokens_total", input_tokens_);
        render_target_counter(output, "ai_gateway_cached_input_tokens_total", cached_tokens_);
        render_target_counter(output, "ai_gateway_output_tokens_total", output_tokens_);
        render_target_counter(output, "ai_gateway_cost_microusd_total", costs_);
        render_target_summary(output, "ai_gateway_attempt_first_byte_ms",
                              first_byte_sum_ms_, first_byte_count_);
        render_target_summary(output, "ai_gateway_attempt_duration_ms",
                              attempt_duration_sum_ms_, attempt_duration_count_);
        output << "# TYPE ai_gateway_active_upstreams gauge\n"
               << "ai_gateway_active_upstreams " << active_upstreams_ << '\n'
               << "# TYPE ai_gateway_active_requests gauge\n"
               << "ai_gateway_active_requests " << node_.active_requests << '\n'
               << "# TYPE ai_gateway_active_streams gauge\n"
               << "ai_gateway_active_streams " << node_.active_streams << '\n'
               << "# TYPE ai_gateway_node_draining gauge\n"
               << "ai_gateway_node_draining " << (node_.draining ? 1 : 0) << '\n'
               << "# TYPE ai_gateway_capacity_limit gauge\n"
               << "ai_gateway_capacity_limit{resource=\"requests\"} "
               << node_.max_active_requests << '\n'
               << "ai_gateway_capacity_limit{resource=\"streams\"} "
               << node_.max_active_streams << '\n'
               << "ai_gateway_capacity_limit{resource=\"upstream_connections\"} "
               << node_.max_upstream_connections << '\n'
               << "ai_gateway_capacity_limit{resource=\"upstream_host_connections\"} "
               << node_.max_upstream_host_connections << '\n'
               << "# TYPE ai_gateway_capacity_rejections_total counter\n"
               << "ai_gateway_capacity_rejections_total{resource=\"requests\"} "
               << node_.request_rejections << '\n'
               << "ai_gateway_capacity_rejections_total{resource=\"streams\"} "
               << node_.stream_rejections << '\n'
               << "# TYPE ai_gateway_shutdown_cancellations_total counter\n"
               << "ai_gateway_shutdown_cancellations_total "
               << node_.shutdown_cancellations << '\n'
               << "# TYPE ai_gateway_failovers_total counter\n"
               << "ai_gateway_failovers_total " << failovers_ << '\n'
               << "# TYPE ai_gateway_backpressure_pauses_total counter\n"
               << "ai_gateway_backpressure_pauses_total " << backpressure_pauses_ << '\n'
               << "# TYPE ai_gateway_request_duration_ms summary\n"
               << "ai_gateway_request_duration_ms_sum " << request_duration_sum_ms_ << '\n'
               << "ai_gateway_request_duration_ms_count " << request_duration_count_ << '\n'
               << "# TYPE ai_gateway_mysql_ready gauge\n"
               << "ai_gateway_mysql_ready " << (mysql_ready_ ? 1 : 0) << '\n'
               << "# TYPE ai_gateway_redis_ready gauge\n"
               << "ai_gateway_redis_ready " << (redis_ready_ ? 1 : 0) << '\n'
               << "# TYPE ai_gateway_governance_rejections_total counter\n";
        for (const auto &entry : governance_rejections_)
        {
            output << "ai_gateway_governance_rejections_total{reason=\""
                   << escaped(entry.first) << "\"} " << entry.second << '\n';
        }
        output << "# TYPE ai_gateway_health_probes_total counter\n";
        for (const auto &entry : health_probes_)
        {
            output << "ai_gateway_health_probes_total{provider=\""
                   << escaped(std::get<0>(entry.first)) << "\",credential=\""
                   << escaped(std::get<1>(entry.first)) << "\",status=\""
                   << (std::get<2>(entry.first) ? "success" : "failure")
                   << "\"} " << entry.second << '\n';
        }
        return output.str();
    }


    static void render_labels(std::ostringstream &output, const TargetKey &key)
    {
        const auto &[tenant, protocol, model, provider, credential] = key;
        label(output, "tenant", tenant, true);
        label(output, "protocol", protocol, false);
        label(output, "logical_model", model, false);
        label(output, "provider", provider, false);
        label(output, "credential", credential, false);
    }


    static void render_target_counter(
        std::ostringstream &output,
        const char *name,
        const std::map<TargetKey, std::uint64_t> &values)
    {
        output << "# TYPE " << name << " counter\n";
        for (const auto &entry : values)
        {
            output << name << '{';
            render_labels(output, entry.first);
            output << "} " << entry.second << '\n';
        }
    }


    static void render_target_summary(
        std::ostringstream &output,
        const char *name,
        const std::map<TargetKey, std::uint64_t> &sums,
        const std::map<TargetKey, std::uint64_t> &counts)
    {
        output << "# TYPE " << name << " summary\n";
        for (const auto &entry : sums)
        {
            output << name << "_sum{";
            render_labels(output, entry.first);
            output << "} " << entry.second << '\n';
            output << name << "_count{";
            render_labels(output, entry.first);
            const auto count = counts.find(entry.first);
            output << "} " << (count == counts.end() ? 0 : count->second) << '\n';
        }
    }

    mutable std::mutex mutex_;
    std::map<RequestKey, std::uint64_t> requests_;
    std::map<AttemptKey, std::uint64_t> attempts_;
    std::map<TargetKey, std::uint64_t> input_tokens_;
    std::map<TargetKey, std::uint64_t> cached_tokens_;
    std::map<TargetKey, std::uint64_t> output_tokens_;
    std::map<TargetKey, std::uint64_t> costs_;
    std::map<TargetKey, std::uint64_t> first_byte_sum_ms_;
    std::map<TargetKey, std::uint64_t> first_byte_count_;
    std::map<TargetKey, std::uint64_t> attempt_duration_sum_ms_;
    std::map<TargetKey, std::uint64_t> attempt_duration_count_;
    std::map<std::string, std::uint64_t> governance_rejections_;
    std::map<std::tuple<std::string, std::string, bool>, std::uint64_t> health_probes_;
    std::uint64_t request_duration_sum_ms_ = 0;
    std::uint64_t request_duration_count_ = 0;
    std::uint64_t failovers_ = 0;
    std::uint64_t backpressure_pauses_ = 0;
    std::uint64_t active_upstreams_ = 0;
    NodeSnapshot node_;
    bool mysql_ready_ = false;
    bool redis_ready_ = false;
};

MetricsRegistry::MetricsRegistry() : impl_(std::make_unique<Impl>()) {}
MetricsRegistry::~MetricsRegistry() = default;

void MetricsRegistry::request_completed(const std::string &tenant,
                                        const std::string &protocol,
                                        const std::string &model,
                                        int status,
                                        const std::string &error,
                                        std::uint64_t duration,
                                        std::size_t failovers,
                                        std::size_t pauses)
{
    impl_->request_completed(tenant, protocol, model, status, error, duration, failovers, pauses);
}

void MetricsRegistry::attempt_completed(const std::string &tenant,
                                        const std::string &protocol,
                                        const std::string &model,
                                        const std::string &provider,
                                        const std::string &credential,
                                        const std::string &state,
                                        const std::string &error,
                                        const UsageAccounting &usage,
                                        std::optional<std::uint64_t> first_byte,
                                        std::uint64_t duration)
{
    impl_->attempt_completed(tenant, protocol, model, provider, credential, state, error,
                             usage, first_byte, duration);
}

void MetricsRegistry::governance_rejected(const std::string &reason)
{
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    ++impl_->governance_rejections_[reason];
}

void MetricsRegistry::health_probe_completed(const std::string &provider,
                                             const std::string &credential,
                                             bool success)
{
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    ++impl_->health_probes_[{provider, credential, success}];
}

void MetricsRegistry::upstream_started()
{
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    ++impl_->active_upstreams_;
}

/* 幂等减少活动上游连接计数。 */
void MetricsRegistry::upstream_finished()
{
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    if (impl_->active_upstreams_ > 0) --impl_->active_upstreams_;
}

/* 流生命周期由 NodeSnapshot 统一记录；此接口保留给指标 seam。 */
void MetricsRegistry::stream_started()
{
}

void MetricsRegistry::stream_finished()
{
}

/* 更新 MySQL/Redis readiness gauge。 */
void MetricsRegistry::set_dependency_readiness(bool mysql_ready, bool redis_ready)
{
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    impl_->mysql_ready_ = mysql_ready;
    impl_->redis_ready_ = redis_ready;
}

void MetricsRegistry::set_node_state(const NodeSnapshot &snapshot)
{
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    impl_->node_ = snapshot;
}

std::string MetricsRegistry::render() const { return impl_->render(); }
}
