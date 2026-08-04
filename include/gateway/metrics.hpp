#ifndef AI_GATEWAY_METRICS_HPP
#define AI_GATEWAY_METRICS_HPP

#include "gateway/repository.hpp"
#include "gateway/lifecycle.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace ai_gateway
{
/*
 * Prometheus 指标注册表。
 * 标签只使用租户、协议、逻辑模型、Provider、Credential、状态和错误分类，
 * 明确排除 request ID、API Key、Prompt、URL 与响应正文。
 */
class MetricsRegistry
{
public:
    /* 创建进程内原子指标集合；不连接外部 Prometheus。 */
    MetricsRegistry();
    /* 销毁指标实现；不会影响 Gateway 请求或数据库状态。 */
    ~MetricsRegistry();

    MetricsRegistry(const MetricsRegistry &) = delete;
    MetricsRegistry &operator=(const MetricsRegistry &) = delete;

    /* 记录请求级终态、延迟、failover 和背压次数。 */
    void request_completed(const std::string &tenant,
                           const std::string &protocol,
                           const std::string &logical_model,
                           int status,
                           const std::string &error_class,
                           std::uint64_t duration_ms,
                           std::size_t failovers,
                           std::size_t backpressure_pauses);
    /* 记录每次 Provider Attempt 的状态、Usage、首字节和总耗时。 */
    void attempt_completed(const std::string &tenant,
                           const std::string &protocol,
                           const std::string &logical_model,
                           const std::string &provider,
                           const std::string &credential,
                           const std::string &state,
                           const std::string &error_class,
                           const UsageAccounting &usage,
                           std::optional<std::uint64_t> first_byte_ms,
                           std::uint64_t duration_ms);
    /* 累加 RPM、并发、预算或 Credential 配额拒绝。 */
    void governance_rejected(const std::string &reason);
    /* 记录显式 Provider 健康探测成功/失败，不创建请求 Usage。 */
    void health_probe_completed(const std::string &provider,
                                const std::string &credential,
                                bool success);
    /* 活动 Provider transfer 数，用于区分上游压力与客户端流数量。 */
    void upstream_started();
    void upstream_finished();
    /* 从节点准入到响应完全写完的流生命周期计数。 */
    void stream_started();
    void stream_finished();
    void set_dependency_readiness(bool mysql_ready, bool redis_ready);
    void set_node_state(const NodeSnapshot &snapshot);
    /* 输出 Prometheus text exposition。 */
    std::string render() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
