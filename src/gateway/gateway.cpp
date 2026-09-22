#include "gateway/gateway.hpp"
#include "gateway/governance.hpp"
#include "gateway/lifecycle.hpp"
#include "gateway/metrics.hpp"
#include "gateway/protocol.hpp"
#include "gateway/runtime.hpp"
#include "gateway/routing.hpp"

#include "json.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

namespace ai_gateway
{
class CancellationState
{
public:


    void cancel()
    {
        std::vector<std::function<void()>> callbacks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (cancelled_.exchange(true))
            {
                return;
            }
            callbacks.swap(callbacks_);
        }
        for (auto &callback : callbacks)
        {
            try
            {
                callback();
            }
            catch (...)
            {
            }
        }
    }

    /* 读取原子取消标志，不需要持有回调列表的锁。 */
    bool cancelled() const
    {
        return cancelled_.load();
    }

    /* 注册取消回调；如果取消已经发生，则在锁外立即执行，避免回调重入死锁。 */
    void add(std::function<void()> callback)
    {
        bool call_now = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (cancelled_.load())
            {
                call_now = true;
            }
            else
            {
                callbacks_.push_back(std::move(callback));
            }
        }
        if (call_now)
        {
            try
            {
                callback();
            }
            catch (...)
            {
            }
        }
    }

private:
    mutable std::mutex mutex_;
    std::atomic_bool cancelled_{false};
    std::vector<std::function<void()>> callbacks_;
};

namespace
{
using json = nlohmann::json;

std::mutex log_mutex;

/* 将 HTTP Header 名称/值规范化为小写，便于大小写无关比较。 */
std::string lower(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string trim(std::string value)
{
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())))
    {
        value.pop_back();
    }
    std::size_t offset = 0;
    while (offset < value.size() && std::isspace(static_cast<unsigned char>(value[offset])))
    {
        ++offset;
    }
    return value.substr(offset);
}

bool bearer_token(const GatewayRequest &request, std::string &token)
{
    const auto found = request.headers.find("authorization");
    if (found == request.headers.end())
    {
        return false;
    }

    const std::string &value = found->second;
    const std::size_t separator = value.find(' ');
    if (separator == std::string::npos || lower(value.substr(0, separator)) != "bearer")
    {
        return false;
    }
    token = trim(value.substr(separator + 1));
    return !token.empty();
}

void write_response(ResponseWriter &writer,
                    int status,
                    const std::string &request_id,
                    std::string body,
                    HeaderMap extra_headers = {})
{
    HeaderMap headers{{"content-type", "application/json"},
                      {"x-request-id", request_id},
                      {"cache-control", "no-store"}};
    for (auto &entry : extra_headers)
    {
        headers[entry.first] = std::move(entry.second);
    }
    writer.begin(status, headers);
    writer.write(body);
    writer.end();
}

void log_completion(const GatewayRequest &request,
                    int status,
                    std::chrono::steady_clock::time_point started,
                    const std::string &logical_model = {},
                    const std::string &provider_result = "not_attempted",
                    std::size_t response_bytes = 0,
                    bool stream = false,
                    std::size_t pauses = 0,
                    const std::string &tenant_slug = {},
                    const std::string &public_api_key_id = {},
                    std::size_t failovers = 0)
{
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started);
    std::string safe_path = request.path.substr(0, request.path.find('?'));
    if (safe_path.size() > 128)
    {
        safe_path.resize(128);
    }
    HeaderMap fields{{"request_id", request.request_id},
                     {"method", request.method},
                     {"path", safe_path},
                     {"status", std::to_string(status)},
                     {"duration_ms", std::to_string(elapsed.count())},
                     {"provider_result", provider_result},
                     {"response_bytes", std::to_string(response_bytes)},
                     {"stream", stream ? "true" : "false"},
                     {"backpressure_pauses", std::to_string(pauses)},
                     {"failover_count", std::to_string(failovers)}};
    if (!logical_model.empty())
    {
        fields["logical_model"] = logical_model;
    }
    if (!tenant_slug.empty())
    {
        fields["tenant_slug"] = tenant_slug;
    }
    if (!public_api_key_id.empty())
    {
        fields["api_key_id"] = public_api_key_id;
    }
    structured_log("request_completed", fields);
}

void finish_without_attempt(RuntimeState &runtime,
                            GovernanceRuntime &governance,
                            std::shared_ptr<const GovernancePermit> permit,
                            GatewayRequest request,
                            int status,
                            std::chrono::steady_clock::time_point started,
                            std::string logical_model,
                            std::string provider_result,
                            std::size_t response_bytes,
                            bool stream,
                            std::string tenant_slug,
                            std::string public_api_key_id,
                            std::size_t max_attempts,
                            std::shared_ptr<NodeExecutionGuard> node_guard)
{
    RequestFinish finish;
    finish.request_id = request.request_id;
    finish.state = status == 499 ? "cancelled" : "failed";
    finish.max_attempts = max_attempts;
    finish.http_status = status;
    finish.error_class = provider_result;
    finish.response_bytes = response_bytes;
    finish.duration_ms = static_cast<std::uint64_t>(std::max<long long>(
        0, std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - started).count()));
    runtime.finish_request(
        std::move(finish),
        [&governance, permit = std::move(permit), request = std::move(request), status,
         started, logical_model = std::move(logical_model),
         provider_result = std::move(provider_result), response_bytes, stream,
         tenant_slug = std::move(tenant_slug),
         public_api_key_id = std::move(public_api_key_id),
         node_guard = std::move(node_guard)](bool stored) mutable {
            governance.release(
                std::move(permit),
                [request = std::move(request), status, started,
                 logical_model = std::move(logical_model),
                 provider_result = stored ? std::move(provider_result)
                                          : std::string("usage_unavailable"),
                 response_bytes, stream, tenant_slug = std::move(tenant_slug),
                 public_api_key_id = std::move(public_api_key_id),
                 node_guard = std::move(node_guard)](bool) mutable {
                    log_completion(request, status, started, logical_model, provider_result,
                                   response_bytes, stream, 0, tenant_slug,
                                   public_api_key_id);
                });
        });
}

/* 只接受短的十进制 Retry-After，拒绝把任意 Provider Header 原样转给客户端。 */
bool valid_retry_after(const std::string &value)
{
    return !value.empty() && value.size() <= 10 &&
           std::all_of(value.begin(), value.end(), [](unsigned char character) {
               return std::isdigit(character) != 0;
           });
}

enum class SseRecordKind
{
    comment,
    business,
    success_terminal,
    provider_error,
    invalid
};

struct SseRecord
{
    std::string raw;
    SseRecordKind kind = SseRecordKind::invalid;
    long sequence_number = -1;
    json payload;
    UsageAccounting usage;
};

std::size_t sse_record_end(const std::string &buffer)
{
    std::size_t cursor = 0;
    while (cursor < buffer.size())
    {
        const std::size_t line_end = buffer.find_first_of("\r\n", cursor);
        if (line_end == std::string::npos)
        {
            return std::string::npos;
        }
        const std::size_t ending_length =
            buffer[line_end] == '\r' && line_end + 1 < buffer.size() &&
                    buffer[line_end + 1] == '\n'
                ? 2
                : 1;


        if (buffer[line_end] == '\r' && line_end + 1 == buffer.size())
        {
            return std::string::npos;
        }
        if (line_end == cursor)
        {
            return line_end + ending_length;
        }
        cursor = line_end + ending_length;
    }
    return std::string::npos;
}

SseRecord classify_sse_record(std::string raw)
{
    SseRecord result;
    result.raw = std::move(raw);
    std::string event_name;
    std::string data;
    bool saw_data = false;
    std::size_t cursor = 0;
    while (cursor < result.raw.size())
    {
        const std::size_t line_end = result.raw.find_first_of("\r\n", cursor);
        if (line_end == std::string::npos)
        {
            break;
        }
        const std::size_t ending_length =
            result.raw[line_end] == '\r' && line_end + 1 < result.raw.size() &&
                    result.raw[line_end + 1] == '\n'
                ? 2
                : 1;
        const std::string line = result.raw.substr(cursor, line_end - cursor);
        cursor = line_end + ending_length;
        if (line.empty())
        {
            break;
        }
        if (line[0] == ':')
        {
            continue;
        }
        const std::size_t separator = line.find(':');
        const std::string field = separator == std::string::npos
                                      ? line
                                      : line.substr(0, separator);
        std::string value = separator == std::string::npos ? "" : line.substr(separator + 1);
        if (!value.empty() && value.front() == ' ')
        {
            value.erase(value.begin());
        }
        if (field == "event")
        {
            event_name = value;
        }
        else if (field == "data")
        {
            if (saw_data)
            {
                data.push_back('\n');
            }
            data += value;
            saw_data = true;
        }
        else
        {
            // SSE extension fields are ignored by clients but remain part of

            continue;
        }
    }

    if (!saw_data || data.empty())
    {
        result.kind = SseRecordKind::comment;
        return result;
    }
    if (data == "[DONE]")
    {
        result.kind = SseRecordKind::success_terminal;
        return result;
    }

    json payload;
    try
    {
        payload = json::parse(data);
    }
    catch (const json::exception &)
    {
        return result;
    }
    if (!payload.is_object() || !payload.contains("type") || !payload["type"].is_string())
    {
        return result;
    }
    const std::string type = payload["type"].get<std::string>();
    result.payload = payload;
    if (!event_name.empty() && event_name != type)
    {
        return result;
    }
    if (payload.contains("sequence_number") && payload["sequence_number"].is_number_integer())
    {
        result.sequence_number = payload["sequence_number"].get<long>();
    }
    if (type == "error" || type == "response.failed" || type == "response.incomplete" ||
        event_name == "error")
    {
        result.kind = SseRecordKind::provider_error;
    }
    else if (type == "response.completed")
    {
        result.kind = SseRecordKind::success_terminal;
    }
    else
    {
        result.kind = SseRecordKind::business;
    }
    return result;
}

class SseDecoder
{
public:
    /* 创建有界 SSE 解码器；prices 用于把事件里的 Usage 转换为 accounting。 */
    explicit SseDecoder(const ProtocolAdapter *adapter = nullptr,
                        const std::vector<ModelPrice> *prices = nullptr)
        : adapter_(adapter), prices_(prices)
    {
    }



    std::vector<SseRecord> push(std::string_view bytes)
    {
        buffer_.append(bytes.data(), bytes.size());
        std::vector<SseRecord> records;
        while (true)
        {
            const std::size_t end = sse_record_end(buffer_);
            if (end == std::string::npos)
            {
                break;
            }
            const std::string raw = buffer_.substr(0, end);
            if (adapter_ != nullptr && prices_ != nullptr)
            {
                const ProtocolEvent event = adapter_->classify_event(raw, *prices_);
                SseRecord record;
                record.raw = raw;
                record.kind = static_cast<SseRecordKind>(event.kind);
                record.sequence_number = event.sequence_number;
                record.payload = event.payload;
                record.usage = event.usage;
                records.push_back(std::move(record));
            }
            else
            {
                records.push_back(classify_sse_record(raw));
            }
            buffer_.erase(0, end);
        }
        return records;
    }

    /* 上游结束时检查是否还残留半个 SSE 帧。 */
    bool finish() const
    {
        return trim(buffer_).empty();
    }


    std::size_t buffered_bytes() const
    {
        return buffer_.size();
    }

private:
    std::string buffer_;
    const ProtocolAdapter *adapter_ = nullptr;
    const std::vector<ModelPrice> *prices_ = nullptr;
};

/* 判断 Provider Content-Type 是否明确是 text/event-stream。 */
bool is_event_stream(const HeaderMap &headers)
{
    const auto found = headers.find("content-type");
    return found != headers.end() && lower(found->second).find("text/event-stream") == 0;
}

std::optional<std::uint64_t> unsigned_json_value(const json &object, const char *name)
{
    if (!object.is_object() || !object.contains(name) ||
        !(object[name].is_number_unsigned() || object[name].is_number_integer()))
    {
        return std::nullopt;
    }
    try
    {
        const auto value = object[name].get<std::int64_t>();
        return value < 0 ? std::nullopt
                         : std::optional<std::uint64_t>(static_cast<std::uint64_t>(value));
    }
    catch (const json::exception &)
    {
        try
        {
            return object[name].get<std::uint64_t>();
        }
        catch (const json::exception &)
        {
            return std::nullopt;
        }
    }
}

UsageAccounting usage_accounting(const json &usage, const std::vector<ModelPrice> &prices)
{
    UsageAccounting result;
    const auto input = unsigned_json_value(usage, "input_tokens");
    const auto output = unsigned_json_value(usage, "output_tokens");
    if (!input || !output)
    {
        return result;
    }
    std::uint64_t cached = 0;
    if (usage.contains("input_tokens_details"))
    {
        const auto parsed = unsigned_json_value(usage["input_tokens_details"],
                                                "cached_tokens");
        if (!parsed || *parsed > *input)
        {
            return result;
        }
        cached = *parsed;
    }
    result.input_tokens = *input;
    result.cached_input_tokens = cached;
    result.output_tokens = *output;
    result.usage_quality = "exact";

    const auto now = static_cast<std::int64_t>(std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::now()));
    const ModelPrice *selected = nullptr;
    for (const auto &price : prices)
    {
        if (price.effective_at_epoch <= now &&
            (selected == nullptr || price.effective_at_epoch > selected->effective_at_epoch ||
             (price.effective_at_epoch == selected->effective_at_epoch &&
              price.id > selected->id)))
        {
            selected = &price;
        }
    }
    if (selected == nullptr)
    {
        return result;
    }

    __extension__ typedef unsigned __int128 Wide;
    const Wide uncached_cost = static_cast<Wide>(*input - cached) *
                               selected->input_per_million_microusd;
    const Wide cached_cost = static_cast<Wide>(cached) *
                             selected->cached_input_per_million_microusd;
    const Wide output_cost = static_cast<Wide>(*output) *
                             selected->output_per_million_microusd;
    const Wide total = uncached_cost + cached_cost + output_cost;
    const Wide rounded = (total + 999999U) / 1000000U;
    if (rounded > std::numeric_limits<std::uint64_t>::max())
    {
        return result;
    }
    result.model_price_id = selected->id;
    result.cost_microusd = static_cast<std::uint64_t>(rounded);
    result.cost_quality = "exact";
    return result;
}

/* 从 Provider 响应头提取有限长度的公开 request id，供 Attempt 审计使用。 */
std::string provider_request_id(const HeaderMap &headers)
{
    for (const char *name : {"x-request-id", "openai-request-id"})
    {
        const auto found = headers.find(name);
        if (found != headers.end() && !found->second.empty() && found->second.size() <= 255)
        {
            return found->second;
        }
    }
    return {};
}

/* 将 curl/生命周期错误枚举映射为脱敏稳定错误码。 */
std::string provider_error_code(ProviderError error)
{
    switch (error)
    {
    case ProviderError::dns_failure:
        return "upstream_dns_failure";
    case ProviderError::connection_failure:
        return "upstream_connection_failure";
    case ProviderError::tls_failure:
        return "upstream_tls_failure";
    case ProviderError::timeout:
        return "upstream_timeout";
    case ProviderError::response_too_large:
        return "upstream_response_too_large";
    case ProviderError::callback_aborted:
        return "upstream_stream_error";
    case ProviderError::unavailable:
        return "upstream_unavailable";
    case ProviderError::shutdown:
        return "gateway_shutdown";
    case ProviderError::cancelled:
        return "client_cancelled";
    case ProviderError::none:
        return "success";
    }
    return "upstream_unavailable";
}

class TransferBinding : public std::enable_shared_from_this<TransferBinding>
{
public:


    void attach(std::shared_ptr<ProviderTransfer> transfer)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        transfer_ = std::move(transfer);
        if (cancel_requested_ && transfer_)
        {
            transfer_->cancel();
        }
        if (resume_requested_ && transfer_)
        {
            transfer_->resume();
            resume_requested_ = false;
        }
        if (stream_started_ && transfer_)
        {
            transfer_->mark_stream_started();
        }
    }

    /* 线程安全地请求取消；transfer 尚未创建时先记录意图。 */
    void cancel()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cancel_requested_ = true;
        if (transfer_)
        {
            transfer_->cancel();
        }
    }

    /* 线程安全地恢复被高水位暂停的 transfer。 */
    void resume()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (transfer_)
        {
            transfer_->resume();
        }
        else
        {
            resume_requested_ = true;
        }
    }


    void mark_stream_started()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stream_started_ = true;
        if (transfer_)
        {
            transfer_->mark_stream_started();
        }
    }

private:
    std::mutex mutex_;
    std::shared_ptr<ProviderTransfer> transfer_;
    bool cancel_requested_ = false;
    bool resume_requested_ = false;
    bool stream_started_ = false;
};

class LeaseLossSignal
{
public:
    /*
     * 函数名直译：触发 lease 丢失。
     *
     * 通俗说：Redis 治理租约续不上时，通知当前请求取消 Provider；同一 lease 只通知一次。
     */
    void trigger()
    {
        std::function<void()> callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (lost_)
            {
                return;
            }
            lost_ = true;
            callback = callback_;
        }
        if (callback)
        {
            callback();
        }
    }


    void subscribe(std::function<void()> callback)
    {
        bool lost;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback_ = callback;
            lost = lost_;
        }
        if (lost && callback)
        {
            callback();
        }
    }

private:
    std::mutex mutex_;
    bool lost_ = false;
    std::function<void()> callback_;
};

struct AttemptDecision
{
    std::string state = "failed";
    std::string error_class = "upstream_unavailable";
    long provider_status = 0;
    bool retryable = false;
    bool possible_duplicate_cost = false;
    bool success = false;
    std::size_t upstream_bytes = 0;
    long retry_after_ms = 0;
    int client_status = 502;
    std::string client_body;
    HeaderMap client_headers;
    std::string provider_request_id;
    std::optional<std::uint64_t> first_byte_ms;
    UsageAccounting usage;
};

class RequestExecution final : public std::enable_shared_from_this<RequestExecution>
{
public:


    RequestExecution(RuntimeState &runtime,
                     RoutingRuntime &routing,
                     GovernanceRuntime &governance,
                     MetricsRegistry &metrics,
                     ProviderTransport &transport,
                     GatewayRequest request,
                     ResponseWriter &response,
                     CancellationToken cancellation,
                     GatewayConfig config,
                     const ProtocolAdapter &adapter,
                     json payload,
                     std::vector<ModelTarget> candidates,
                     std::string affinity_key,
                     std::string logical_model,
                     std::string tenant_slug,
                     std::string public_api_key_id,
                     std::uint64_t tenant_id,
                     std::uint64_t api_key_id,
                     std::uint64_t logical_model_id,
                     std::size_t max_attempts,
                     std::shared_ptr<const GovernancePermit> governance_permit,
                     std::shared_ptr<LeaseLossSignal> lease_loss,
                     std::shared_ptr<NodeRequestLease> node_lease,
                     std::shared_ptr<NodeExecutionGuard> node_guard,
                     bool stream,
                     std::chrono::steady_clock::time_point started)
        : runtime_(runtime),
          routing_(routing),
          governance_(governance),
          metrics_(metrics),
          transport_(transport),
          adapter_(&adapter),
          request_(std::move(request)),
          response_(response),
          cancellation_(std::move(cancellation)),
          config_(std::move(config)),
          payload_(std::move(payload)),
          candidates_(std::move(candidates)),
          affinity_key_(std::move(affinity_key)),
          logical_model_(std::move(logical_model)),
          tenant_slug_(std::move(tenant_slug)),
          public_api_key_id_(std::move(public_api_key_id)),
          tenant_id_(tenant_id),
          api_key_id_(api_key_id),
          logical_model_id_(logical_model_id),
          max_attempts_(max_attempts),
          governance_permit_(std::move(governance_permit)),
          lease_loss_(std::move(lease_loss)),
          node_lease_(std::move(node_lease)),
          node_guard_(std::move(node_guard)),
          stream_(stream),
          started_(started)
    {
    }



    void start()
    {
        const auto weak = weak_from_this();
        node_lease_->on_shutdown([weak] {
            if (auto self = weak.lock())
            {
                self->shutdown();
            }
        });
        lease_loss_->subscribe([weak] {
            if (auto self = weak.lock())
            {
                self->on_governance_lost();
            }
        });
        cancellation_.on_cancel([weak] {
            if (auto self = weak.lock())
            {
                self->cancel();
            }
        });
        if (stream_)
        {
            response_.set_writable_callback([weak] {
                if (auto self = weak.lock())
                {
                    self->resume();
                }
            });
        }
        start_next_attempt();
    }

private:
    /* drain 超时或第二次信号要求取消当前 Provider；已提交流不会 failover。 */
    void shutdown()
    {
        if (shutdown_requested_.exchange(true))
        {
            return;
        }
        if (binding_)
        {
            binding_->cancel();
        }
        else
        {
            finalize_shutdown();
        }
    }

    /* Redis lease 丢失时立即取消上游并按提交状态生成 JSON/SSE 终态。 */
    void on_governance_lost()
    {
        governance_lost_.store(true);
        if (binding_)
        {
            binding_->cancel();
        }
        else
        {
            finalize_gateway_error("Gateway governance is unavailable",
                                   "governance_unavailable");
        }
    }

    /* 客户端 EOF、reset 或 CancellationToken 取消时停止当前 transfer。 */
    void cancel()
    {
        client_cancelled_.store(true);
        if (binding_)
        {
            binding_->cancel();
        }
    }


    void resume()
    {
        paused_.store(false);
        if (binding_)
        {
            binding_->resume();
        }
    }



    void start_next_attempt()
    {
        if (finalized_)
        {
            return;
        }
        if (governance_lost_.load())
        {
            finalize_gateway_error("Gateway governance is unavailable",
                                   "governance_unavailable");
            return;
        }
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            finalize_cancelled();
            return;
        }
        if (next_candidate_ >= candidates_.size())
        {
            if (credential_limited_ && provider_attempt_count_ == 0)
            {
                finalize_credential_limited();
                return;
            }
            finalize_failure(last_decision_);
            return;
        }

        current_target_ = candidates_[next_candidate_++];
        if (current_target_.credential_quota)
        {
            const auto quota = *current_target_.credential_quota;
            governance_.admit(
                {{"credential", current_target_.credential_id, quota.rpm, quota.concurrency}},
                [weak = weak_from_this()] {
                    if (auto self = weak.lock())
                    {
                        self->on_governance_lost();
                    }
                },
                [self = shared_from_this()](GovernanceResult result) mutable {
                    self->on_credential_admitted(std::move(result));
                });
            return;
        }
        begin_current_attempt();
    }

    /* 处理单个 Credential 的 RPM/并发结果；受限候选跳过，Redis 故障则停止 failover。 */
    void on_credential_admitted(GovernanceResult admission)
    {
        if (finalized_)
        {
            governance_.rollback(std::move(admission.permit));
            return;
        }
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            governance_.rollback(std::move(admission.permit));
            finalize_cancelled();
            return;
        }
        if (admission.status == GovernanceStatus::rate_limited ||
            admission.status == GovernanceStatus::concurrency_limited ||
            admission.status == GovernanceStatus::credential_limited)
        {
            credential_limited_ = true;
            metrics_.governance_rejected("credential_quota_exceeded");
            if (admission.retry_after_ms > 0 &&
                (credential_retry_after_ms_ == 0 ||
                 admission.retry_after_ms < credential_retry_after_ms_))
            {
                credential_retry_after_ms_ = admission.retry_after_ms;
            }
            start_next_attempt();
            return;
        }
        if (admission.status != GovernanceStatus::admitted || !admission.permit)
        {
            finalize_gateway_error("Gateway governance is unavailable",
                                   "governance_unavailable");
            return;
        }
        credential_permit_ = std::move(admission.permit);
        begin_current_attempt();
    }


    void begin_current_attempt()
    {
        ++attempt_number_;
        current_attempt_id_ = "att_" + generate_request_id().substr(4);
        attempt_started_ = std::chrono::steady_clock::now();
        reset_stream_attempt();

        AttemptStart start;
        start.attempt_id = current_attempt_id_;
        start.request_id = request_.request_id;
        start.attempt_number = attempt_number_;
        start.tenant_id = tenant_id_;
        start.api_key_id = api_key_id_;
        start.logical_model_id = logical_model_id_;
        start.mapping_id = current_target_.mapping_id;
        start.provider_id = current_target_.provider_id;
        start.endpoint_id = current_target_.endpoint_id;
        start.credential_id = current_target_.credential_id;
        start.stream = stream_;
        runtime_.begin_attempt(std::move(start),
            [self = shared_from_this()](bool stored) { self->on_attempt_started(stored); });
    }

    /* 审计写入成功后克隆原始 JSON，仅改写 model、URL、Provider Headers 并启动 curl transfer。 */
    void on_attempt_started(bool stored)
    {
        if (finalized_)
        {
            return;
        }
        if (!stored)
        {
            auto permit = std::move(credential_permit_);
            governance_.rollback(std::move(permit), [self = shared_from_this()](bool rolled_back) {
                if (rolled_back)
                {
                    self->finalize_gateway_error("Gateway audit is unavailable",
                                                 "audit_unavailable");
                }
                else
                {
                    self->finalize_gateway_error("Gateway governance is unavailable",
                                                 "governance_unavailable");
                }
            });
            return;
        }
        attempt_started_in_database_ = true;
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            AttemptDecision decision;
            decision.state = "cancelled";
            decision.error_class = "client_cancelled";
            finish_attempt(std::move(decision));
            return;
        }

        ++provider_attempt_count_;

        json upstream_payload = payload_;
        upstream_payload["model"] = current_target_.upstream_model;
        ProviderRequest provider_request;
        provider_request.url = current_target_.provider_url;
        provider_request.headers = adapter_->provider_headers(request_, current_target_, stream_);
        provider_request.body = upstream_payload.dump();
        provider_request.max_response_bytes = remaining_response_bytes();
        provider_request.timeout_ms = config_.upstream_timeout_ms;
        provider_request.streaming = stream_;
        provider_request.idle_timeout_ms = config_.stream_idle_timeout_ms;
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started_).count();
        provider_request.max_duration_ms = std::max<long>(
            1, config_.stream_max_duration_ms - static_cast<long>(elapsed));

        binding_ = std::make_shared<TransferBinding>();
        ProviderCallbacks callbacks;
        if (stream_)
        {
            callbacks.on_headers = [self = shared_from_this()](const ProviderResponseHead &head) {
                self->on_stream_headers(head);
            };
            callbacks.on_body = [self = shared_from_this()](std::string_view bytes) {
                return self->on_stream_body(bytes);
            };
            callbacks.on_complete = [self = shared_from_this()](ProviderResponse response) {
                self->on_stream_complete(std::move(response));
            };
        }
        else
        {
            callbacks.on_complete = [self = shared_from_this()](ProviderResponse response) {
                self->on_basic_complete(std::move(response));
            };
        }
        metrics_.upstream_started();
        upstream_active_ = true;
        binding_->attach(transport_.execute(std::move(provider_request), std::move(callbacks)));
    }

    /* 返回所有候选共享的剩余响应预算，failover 不会重置 max_response_bytes。 */
    std::size_t remaining_response_bytes() const
    {
        return aggregate_upstream_bytes_ < config_.max_response_bytes
                   ? config_.max_response_bytes - aggregate_upstream_bytes_
                   : 1;
    }

    /* 只有协议约定的 429/5xx 允许提交前切换候选。 */
    static bool retryable_status(long status)
    {
        return status == 429 || status == 500 || status == 502 || status == 503 ||
               status == 504;
    }

    /* 解析并限制 Provider Retry-After，最多用于延长断路器 300 秒。 */
    static long retry_after_ms(const HeaderMap &headers)
    {
        const auto found = headers.find("retry-after");
        if (found == headers.end() || !valid_retry_after(found->second))
        {
            return 0;
        }
        try
        {
            return std::min<long>(300000, std::stol(found->second) * 1000);
        }
        catch (...)
        {
            return 0;
        }
    }

    /* 将 DNS/connect/TLS/timeout/取消/超限统一归类，并标记潜在重复计费风险。 */
    AttemptDecision transport_failure(const ProviderResponse &upstream,
                                      std::string local_failure = {}) const
    {
        AttemptDecision decision;
        decision.provider_status = upstream.status;
        decision.error_class = local_failure.empty() ? provider_error_code(upstream.error)
                                                     : std::move(local_failure);
        decision.upstream_bytes = attempt_upstream_bytes_;
        if (upstream.error == ProviderError::cancelled ||
            upstream.error == ProviderError::shutdown)
        {
            decision.state = upstream.error == ProviderError::cancelled
                                 ? "cancelled"
                                 : "failed";
            decision.retryable = false;
            decision.possible_duplicate_cost = false;
            return decision;
        }
        decision.retryable = true;
        decision.possible_duplicate_cost =
            upstream.error != ProviderError::dns_failure &&
            upstream.error != ProviderError::connection_failure &&
            upstream.error != ProviderError::tls_failure;
        if (upstream.error == ProviderError::timeout ||
            decision.error_class == "upstream_invalid_stream" ||
            decision.error_class == "upstream_stream_error" ||
            decision.error_class == "upstream_prefetch_too_large")
        {
            decision.possible_duplicate_cost = true;
        }
        return decision;
    }

    /* 处理 Provider HTTP 状态；4xx/429 保留可理解状态，429/5xx 才可能切换。 */
    AttemptDecision http_failure(long status, const HeaderMap &headers) const
    {
        AttemptDecision decision;
        decision.provider_status = status;
        decision.upstream_bytes = attempt_upstream_bytes_;
        decision.retryable = retryable_status(status);
        decision.possible_duplicate_cost = decision.retryable && status != 429;
        decision.retry_after_ms = retry_after_ms(headers);
        decision.error_class = status == 429 ? "upstream_rate_limited"
                                             : (decision.retryable
                                                    ? "upstream_unavailable"
                                                    : "upstream_rejected_request");
        decision.client_status = status >= 400 && status < 500 ? static_cast<int>(status) : 502;
        const ProtocolError error = protocol_error(
            decision.client_status,
            status >= 400 && status < 500 ? "upstream_error" : "server_error",
            decision.error_class,
            status >= 400 && status < 500 ? "Upstream provider rejected the request"
                                          : "Upstream provider returned an invalid status");
        decision.client_body = adapter_->error_body(error);
        const auto retry_after = headers.find("retry-after");
        if (retry_after != headers.end() && valid_retry_after(retry_after->second))
        {
            decision.client_headers["retry-after"] = retry_after->second;
        }
        return decision;
    }



    void on_basic_complete(ProviderResponse upstream)
    {
        finish_active_upstream();
        if (finalized_ || !attempt_started_in_database_)
        {
            return;
        }
        attempt_upstream_bytes_ = upstream.body.size();
        aggregate_upstream_bytes_ += attempt_upstream_bytes_;
        AttemptDecision decision;
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            decision.state = "cancelled";
            decision.error_class = "client_cancelled";
            decision.upstream_bytes = attempt_upstream_bytes_;
        }
        else if (aggregate_upstream_bytes_ > config_.max_response_bytes ||
                 upstream.error == ProviderError::response_too_large)
        {
            decision = transport_failure(upstream, "upstream_response_too_large");
            decision.retryable = true;
            decision.possible_duplicate_cost = true;
        }
        else if (upstream.error != ProviderError::none)
        {
            decision = transport_failure(upstream);
        }
        else if (upstream.status < 200 || upstream.status >= 300)
        {
            decision = http_failure(upstream.status, upstream.headers);
        }
        else
        {
            try
            {
                json response_json = json::parse(upstream.body);
                decision.provider_status = upstream.status;
                decision.upstream_bytes = attempt_upstream_bytes_;
                ProtocolError protocol_failure;
                if (!adapter_->normalize_response(response_json, logical_model_,
                                                  current_target_.prices,
                                                  decision.client_body, decision.usage,
                                                  protocol_failure))
                {
                    decision.state = "failed";
                    decision.error_class = protocol_failure.code.empty()
                                               ? "upstream_invalid_response"
                                               : protocol_failure.code;
                    decision.retryable = true;
                    decision.possible_duplicate_cost = true;
                }
                else
                {
                    decision.state = "succeeded";
                    decision.error_class = "success";
                    decision.success = true;
                    decision.client_status = static_cast<int>(upstream.status);
                }
            }
            catch (const json::exception &)
            {
                decision.state = "failed";
                decision.error_class = "upstream_invalid_response";
                decision.provider_status = upstream.status;
                decision.retryable = true;
                decision.possible_duplicate_cost = true;
                decision.upstream_bytes = attempt_upstream_bytes_;
            }
        }
        apply_transport_metadata(decision, upstream);
        finish_attempt(std::move(decision));
    }

    /* 收到上游响应头后记录状态和 Content-Type；只有 2xx text/event-stream 才能走 SSE。 */
    void on_stream_headers(const ProviderResponseHead &head)
    {
        stream_status_ = head.status;
        stream_headers_ = head.headers;
        stream_is_sse_ = stream_status_ >= 200 && stream_status_ < 300 &&
                         is_event_stream(stream_headers_);
    }



    ProviderChunkAction on_stream_body(std::string_view bytes)
    {
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            cancel();
            return ProviderChunkAction::cancel_transfer;
        }
        attempt_upstream_bytes_ += bytes.size();
        aggregate_upstream_bytes_ += bytes.size();
        if (aggregate_upstream_bytes_ > config_.max_response_bytes)
        {
            stream_local_failure_ = "upstream_response_too_large";
            return ProviderChunkAction::cancel_transfer;
        }
        if (!stream_is_sse_)
        {
            const std::size_t remaining = config_.stream_prefetch_bytes > stream_error_body_.size()
                                              ? config_.stream_prefetch_bytes -
                                                    stream_error_body_.size()
                                              : 0;
            stream_error_body_.append(bytes.data(), std::min(bytes.size(), remaining));
            if (stream_status_ >= 200 && stream_status_ < 300 && remaining < bytes.size())
            {
                stream_local_failure_ = "upstream_prefetch_too_large";
                return ProviderChunkAction::cancel_transfer;
            }
            return ProviderChunkAction::continue_transfer;
        }

        for (const auto &record : stream_decoder_.push(bytes))
        {
            if (!committed_)
            {
                stream_precommit_bytes_ += record.raw.size();
                if (stream_precommit_bytes_ > config_.stream_prefetch_bytes)
                {
                    stream_local_failure_ = "upstream_prefetch_too_large";
                    return ProviderChunkAction::cancel_transfer;
                }
                if (record.kind == SseRecordKind::comment)
                {
                    stream_precommit_records_.push_back(record);
                    continue;
                }
                if (record.kind == SseRecordKind::invalid ||
                    record.kind == SseRecordKind::provider_error)
                {
                    stream_local_failure_ = record.kind == SseRecordKind::provider_error
                                                ? "upstream_stream_error"
                                                : "upstream_invalid_stream";
                    return ProviderChunkAction::cancel_transfer;
                }
                stream_precommit_records_.push_back(record);
                if (!commit_stream())
                {
                    return ProviderChunkAction::cancel_transfer;
                }
                continue;
            }
            if (record.kind == SseRecordKind::invalid ||
                record.kind == SseRecordKind::provider_error)
            {
                stream_local_failure_ = record.kind == SseRecordKind::provider_error
                                            ? "upstream_stream_error"
                                            : "upstream_invalid_stream";
                return ProviderChunkAction::cancel_transfer;
            }
            if (!write_stream_record(record))
            {
                return ProviderChunkAction::cancel_transfer;
            }
        }
        if (!committed_ && stream_precommit_bytes_ + stream_decoder_.buffered_bytes() >
                               config_.stream_prefetch_bytes)
        {
            stream_local_failure_ = "upstream_prefetch_too_large";
            return ProviderChunkAction::cancel_transfer;
        }
        return paused_.load() ? ProviderChunkAction::pause_after_accept
                              : ProviderChunkAction::continue_transfer;
    }



    void on_stream_complete(ProviderResponse upstream)
    {
        finish_active_upstream();
        if (finalized_ || !attempt_started_in_database_)
        {
            return;
        }
        AttemptDecision decision;
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            decision.state = "cancelled";
            decision.error_class = "client_cancelled";
            decision.upstream_bytes = attempt_upstream_bytes_;
            apply_transport_metadata(decision, upstream);
            finish_attempt(std::move(decision));
            return;
        }
        if (committed_ && !stream_decoder_.finish())
        {
            stream_local_failure_ = "upstream_invalid_stream";
        }
        if (committed_)
        {
            decision.provider_status = upstream.status != 0 ? upstream.status : stream_status_;
            decision.upstream_bytes = attempt_upstream_bytes_;
            if (upstream.error == ProviderError::none && stream_success_terminal_seen_ &&
                stream_local_failure_.empty())
            {
                decision.state = "succeeded";
                decision.error_class = "success";
                decision.success = true;
                decision.client_status = 200;
            }
            else
            {
                decision.state = "failed";
                decision.error_class = !stream_local_failure_.empty()
                                           ? stream_local_failure_
                                           : (upstream.error != ProviderError::none
                                                  ? provider_error_code(upstream.error)
                                                  : "upstream_stream_incomplete");
                decision.retryable = false;
                decision.possible_duplicate_cost = true;
            }
            decision.usage = stream_usage_;
            apply_transport_metadata(decision, upstream);
            finish_attempt(std::move(decision));
            return;
        }

        const long status = upstream.status != 0 ? upstream.status : stream_status_;
        if (upstream.error != ProviderError::none)
        {
            decision = transport_failure(upstream, stream_local_failure_);
        }
        else if (status < 200 || status >= 300)
        {
            decision = http_failure(status, upstream.headers);
        }
        else if (!stream_is_sse_)
        {
            decision.state = "failed";
            decision.error_class = "upstream_invalid_stream";
            decision.provider_status = status;
            decision.retryable = true;
            decision.possible_duplicate_cost = true;
            decision.upstream_bytes = attempt_upstream_bytes_;
        }
        else
        {
            decision.state = "failed";
            decision.error_class = stream_local_failure_.empty()
                                       ? "upstream_invalid_stream"
                                       : stream_local_failure_;
            decision.provider_status = status;
            decision.retryable = true;
            decision.possible_duplicate_cost = true;
            decision.upstream_bytes = attempt_upstream_bytes_;
        }
        apply_transport_metadata(decision, upstream);
        finish_attempt(std::move(decision));
    }

    /* 把 Provider request id 和首字节耗时写入 Attempt 决策，不写 URL/Secret。 */
    void apply_transport_metadata(AttemptDecision &decision,
                                  const ProviderResponse &upstream) const
    {
        decision.provider_request_id = provider_request_id(upstream.headers);
        if (upstream.has_first_byte_timing)
        {
            decision.first_byte_ms = upstream.first_byte_ms;
        }
    }

    /* 幂等减少活动上游指标，防止完成回调和取消路径重复计数。 */
    void finish_active_upstream()
    {
        if (upstream_active_)
        {
            upstream_active_ = false;
            metrics_.upstream_finished();
        }
    }

    /* 写入 Attempt 终态，释放 Credential lease，再决定成功、失败、取消或 failover。 */
    void finish_attempt(AttemptDecision decision)
    {
        if (!attempt_started_in_database_)
        {
            return;
        }
        attempt_started_in_database_ = false;
        if (governance_lost_.load())
        {
            decision.state = "failed";
            decision.error_class = "governance_unavailable";
            decision.retryable = false;
            decision.success = false;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - attempt_started_);
        AttemptFinish finish;
        finish.attempt_id = current_attempt_id_;
        finish.state = decision.state;
        finish.provider_status = decision.provider_status;
        finish.error_class = decision.error_class;
        finish.retryable = decision.retryable;
        finish.possible_duplicate_cost = decision.possible_duplicate_cost;
        finish.response_bytes = decision.upstream_bytes;
        finish.duration_ms = static_cast<std::uint64_t>(std::max<long long>(0, elapsed.count()));
        finish.provider_request_id = decision.provider_request_id;
        finish.first_byte_ms = decision.first_byte_ms;
        finish.usage = decision.usage;
        runtime_.finish_attempt(std::move(finish),
            [self = shared_from_this(), decision = std::move(decision),
             duration = elapsed.count()](bool stored) mutable {
                self->on_attempt_audited(std::move(decision), stored, duration);
            });
    }

    /* Attempt 审计回调：先释放 Credential lease，再进入 Usage 聚合和路由反馈。 */
    void on_attempt_audited(AttemptDecision decision, bool stored, long long duration_ms)
    {
        auto permit = std::move(credential_permit_);
        governance_.release(
            std::move(permit),
            [self = shared_from_this(), decision = std::move(decision), stored,
             duration_ms](bool released) mutable {
                self->after_credential_released(std::move(decision), stored, released,
                                                duration_ms);
            });
    }

    /* Credential lease 释放后的唯一分流点；释放失败时禁止继续尝试其他候选。 */
    void after_credential_released(AttemptDecision decision,
                                   bool stored,
                                   bool released,
                                   long long duration_ms)
    {
        structured_log("attempt_completed",
            {{"request_id", request_.request_id},
             {"attempt_id", current_attempt_id_},
             {"attempt_number", std::to_string(attempt_number_)},
             {"candidate", current_target_.fingerprint},
             {"tenant_slug", tenant_slug_},
             {"api_key_id", public_api_key_id_},
             {"state", decision.state},
             {"provider_status", std::to_string(decision.provider_status)},
             {"error_class", decision.error_class},
             {"retryable", decision.retryable ? "true" : "false"},
             {"possible_duplicate_cost", decision.possible_duplicate_cost ? "true" : "false"},
             {"response_bytes", std::to_string(decision.upstream_bytes)},
             {"duration_ms", std::to_string(std::max<long long>(0, duration_ms))}});
        metrics_.attempt_completed(
            tenant_slug_, adapter_->protocol(), logical_model_, current_target_.provider_slug,
            current_target_.credential_name, decision.state, decision.error_class,
            decision.usage, decision.first_byte_ms,
            static_cast<std::uint64_t>(std::max<long long>(0, duration_ms)));
        if (!released)
        {
            if (committed_)
            {
                finalize_committed_failure("governance_unavailable");
            }
            else
            {
                finalize_gateway_error("Gateway governance is unavailable",
                                       "governance_unavailable");
            }
            return;
        }
        if (!stored)
        {
            if (committed_)
            {
                finalize_committed_failure("audit_unavailable");
            }
            else
            {
                finalize_gateway_error("Gateway audit is unavailable", "audit_unavailable");
            }
            return;
        }
        if (decision.success || decision.possible_duplicate_cost)
        {
            ++billable_attempt_count_;
            if (decision.usage.usage_quality == "exact" &&
                decision.usage.input_tokens && decision.usage.cached_input_tokens &&
                decision.usage.output_tokens)
            {
                aggregate_usage_.input_tokens =
                    aggregate_usage_.input_tokens.value_or(0) +
                    *decision.usage.input_tokens;
                aggregate_usage_.cached_input_tokens =
                    aggregate_usage_.cached_input_tokens.value_or(0) +
                    *decision.usage.cached_input_tokens;
                aggregate_usage_.output_tokens =
                    aggregate_usage_.output_tokens.value_or(0) +
                    *decision.usage.output_tokens;
                ++exact_usage_attempt_count_;
            }
            if (decision.usage.cost_quality == "exact" &&
                decision.usage.cost_microusd)
            {
                aggregate_usage_.cost_microusd =
                    aggregate_usage_.cost_microusd.value_or(0) +
                    *decision.usage.cost_microusd;
                ++exact_cost_attempt_count_;
            }
        }
        if (shutdown_requested_.load())
        {
            finalize_shutdown();
            return;
        }
        if (decision.state == "cancelled")
        {
            finalize_cancelled();
            return;
        }

        const bool success = decision.success;
        routing_.record(current_target_.fingerprint, affinity_key_, success,
                        decision.retryable, decision.retry_after_ms,
            [self = shared_from_this(), decision = std::move(decision)](bool recorded) mutable {
                self->on_routing_feedback(std::move(decision), recorded);
            });
    }

    /*
     * 处理 Redis 健康/affinity 反馈。
     * 未提交且 retryable 时启动下一个候选；已提交时只结束当前响应，绝不 failover。
     */
    void on_routing_feedback(AttemptDecision decision, bool recorded)
    {
        if (!recorded)
        {
            if (committed_)
            {
                if (decision.success)
                {
                    response_.end();
                    finalize_log(200, "success");
                }
                else
                {
                    finalize_committed_failure("routing_unavailable");
                }
            }
            else
            {
                finalize_gateway_error("Gateway routing is unavailable", "routing_unavailable");
            }
            return;
        }
        last_decision_ = decision;
        if (decision.retryable && !committed_ && next_candidate_ < candidates_.size())
        {
            start_next_attempt();
            return;
        }
        if (decision.success)
        {
            if (stream_)
            {
                response_.end();
                finalize_log(200, "success");
            }
            else
            {
                write_response(response_, decision.client_status, request_.request_id,
                               decision.client_body, std::move(decision.client_headers));
                finalize_log(decision.client_status, "success", decision.client_body.size());
            }
            return;
        }
        if (committed_)
        {
            finalize_committed_failure(decision.error_class);
            return;
        }
        finalize_failure(decision);
    }

    /* 把未提交的失败编码为客户端 JSON，优先保留 Provider 4xx/429 的状态约定。 */
    void finalize_failure(const AttemptDecision &decision)
    {
        if (finalized_)
        {
            return;
        }
        if (decision.error_class == "governance_unavailable")
        {
            finalize_gateway_error("Gateway governance is unavailable",
                                   "governance_unavailable");
            return;
        }
        if (!decision.retryable && decision.client_status >= 400 &&
            decision.client_status < 500 && !decision.client_body.empty())
        {
            write_response(response_, decision.client_status, request_.request_id,
                           decision.client_body, decision.client_headers);
            finalize_log(decision.client_status, decision.error_class,
                         decision.client_body.size());
            return;
        }
        if (decision.provider_status == 429 && !decision.client_body.empty())
        {
            write_response(response_, 429, request_.request_id, decision.client_body,
                           decision.client_headers);
            finalize_log(429, decision.error_class, decision.client_body.size());
            return;
        }
        const std::string code = decision.error_class.empty()
                                     ? "upstream_unavailable"
                                     : decision.error_class;
        const std::string body = adapter_->error_body(protocol_error(
            502, "server_error", code, "Upstream provider request failed"));
        write_response(response_, 502, request_.request_id, body);
        finalize_log(502, code, body.size());
    }

    /* 已提交 SSE 的唯一失败出口：写一个协议原生 terminal error，然后 end。 */
    void finalize_committed_failure(const std::string &code)
    {
        if (finalized_)
        {
            return;
        }
        if (!terminal_error_written_)
        {
            const std::string event = adapter_->terminal_error(
                std::max<long>(stream_last_sequence_number_ + 1, 1),
                "upstream_stream_error", "Upstream provider stream failed");
            downstream_response_bytes_ += event.size();
            response_.write(event);
            terminal_error_written_ = true;
        }
        response_.end();
        finalize_log(200, code);
    }

    /* 生成脱敏 503 Gateway 错误，不暴露数据库、Redis、URL 或堆栈。 */
    void finalize_gateway_error(const std::string &message, const std::string &code)
    {
        if (finalized_)
        {
            return;
        }
        const std::string body = adapter_->error_body(
            protocol_error(503, "server_error", code, message));
        write_response(response_, 503, request_.request_id, body);
        finalize_log(503, code, body.size());
    }

    /* 所有候选 Credential 都被配额跳过时返回 429 credential_quota_exceeded。 */
    void finalize_credential_limited()
    {
        if (finalized_)
        {
            return;
        }
        const std::string body = adapter_->error_body(protocol_error(
            429, "rate_limit_error", "credential_quota_exceeded",
            "All provider credentials are currently quota limited"));
        HeaderMap headers;
        if (credential_retry_after_ms_ > 0)
        {
            headers["retry-after"] = std::to_string(
                std::max<long>(1, (credential_retry_after_ms_ + 999) / 1000));
        }
        write_response(response_, 429, request_.request_id, body, std::move(headers));
        finalize_log(429, "credential_quota_exceeded", body.size());
    }

    /* 客户端取消终态：结束响应、结算 cancelled，不进行 failover。 */
    void finalize_cancelled()
    {
        if (finalized_)
        {
            return;
        }
        response_.end();
        finalize_log(499, "client_cancelled");
    }

    /* drain 强制取消终态：未提交返回 JSON，已提交发送 gateway_shutdown SSE。 */
    void finalize_shutdown()
    {
        if (finalized_)
        {
            return;
        }
        if (committed_)
        {
            if (!terminal_error_written_)
            {
                const std::string event = adapter_->terminal_error(
                    std::max<long>(stream_last_sequence_number_ + 1, 1),
                    "gateway_shutdown", "Gateway node is shutting down");
                downstream_response_bytes_ += event.size();
                response_.write(event);
                terminal_error_written_ = true;
            }
            response_.end();
            finalize_log(200, "gateway_shutdown", 0, "cancelled");
            return;
        }
        const std::string body = adapter_->error_body(protocol_error(
            503, "server_error", "gateway_shutdown", "Gateway node is shutting down"));
        write_response(response_, 503, request_.request_id, body, {{"retry-after", "1"}});
        finalize_log(503, "gateway_shutdown", body.size(), "cancelled");
    }



    void finalize_log(int status,
                      const std::string &provider_result,
                      std::size_t bytes = 0,
                      std::string terminal_state = {})
    {
        if (finalized_)
        {
            return;
        }
        finalized_ = true;
        finish_active_upstream();
        if (stream_active_)
        {
            stream_active_ = false;
            metrics_.stream_finished();
        }
        const std::size_t response_bytes = stream_ ? downstream_response_bytes_ : bytes;
        RequestFinish finish;
        finish.request_id = request_.request_id;
        finish.state = terminal_state.empty()
                           ? (status == 200 ? "succeeded"
                                            : (status == 499 ? "cancelled" : "failed"))
                           : std::move(terminal_state);
        finish.final_mapping_id = provider_attempt_count_ == 0 ? 0 : current_target_.mapping_id;
        finish.final_provider_id = provider_attempt_count_ == 0 ? 0 : current_target_.provider_id;
        finish.final_endpoint_id = provider_attempt_count_ == 0 ? 0 : current_target_.endpoint_id;
        finish.final_credential_id = provider_attempt_count_ == 0
                                         ? 0 : current_target_.credential_id;
        finish.attempt_count = provider_attempt_count_;
        finish.billable_attempt_count = billable_attempt_count_;
        finish.max_attempts = max_attempts_;
        finish.http_status = status;
        finish.error_class = provider_result;
        finish.response_bytes = response_bytes;
        finish.duration_ms = static_cast<std::uint64_t>(std::max<long long>(
            0, std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - started_).count()));
        if (billable_attempt_count_ > 0 &&
            exact_usage_attempt_count_ == billable_attempt_count_)
        {
            aggregate_usage_.usage_quality = "exact";
        }
        else if (exact_usage_attempt_count_ > 0)
        {
            aggregate_usage_.usage_quality = "partial";
        }
        if (billable_attempt_count_ > 0 &&
            exact_cost_attempt_count_ == billable_attempt_count_)
        {
            aggregate_usage_.cost_quality = "exact";
        }
        finish.usage = aggregate_usage_;
        metrics_.request_completed(
            tenant_slug_, adapter_->protocol(), logical_model_, status, provider_result,
            finish.duration_ms,
            provider_attempt_count_ > 0 ? provider_attempt_count_ - 1 : 0, pauses_);
        auto permit = std::move(governance_permit_);
        runtime_.finish_request(
            std::move(finish),
            [self = shared_from_this(), permit = std::move(permit), status, provider_result,
             response_bytes](bool stored) mutable {
                self->governance_.release(
                    std::move(permit),
                    [self, status,
                     provider_result = stored ? provider_result
                                              : std::string("usage_unavailable"),
                     response_bytes](bool) {
                        log_completion(
                            self->request_, status, self->started_, self->logical_model_,
                            provider_result, response_bytes, self->stream_, self->pauses_,
                            self->tenant_slug_, self->public_api_key_id_,
                            self->provider_attempt_count_ > 0
                                ? self->provider_attempt_count_ - 1 : 0);
                    });
            });
    }

    /* 切换候选前清空本次 Attempt 的 SSE/预读/首事件状态，但不重置全请求响应预算。 */
    void reset_stream_attempt()
    {
        attempt_started_in_database_ = false;
        attempt_upstream_bytes_ = 0;
        stream_decoder_ = SseDecoder(adapter_, &current_target_.prices);
        stream_usage_ = UsageAccounting();
        stream_headers_.clear();
        stream_precommit_records_.clear();
        stream_error_body_.clear();
        stream_local_failure_.clear();
        stream_status_ = 0;
        stream_precommit_bytes_ = 0;
        stream_is_sse_ = false;
        stream_success_terminal_seen_ = false;
        paused_.store(false);
    }


    bool commit_stream()
    {
        committed_ = true;
        metrics_.stream_started();
        stream_active_ = true;
        binding_->mark_stream_started();
        response_.begin(200, { {"content-type", "text/event-stream; charset=utf-8"},
                               {"cache-control", "no-store"},
                               {"x-request-id", request_.request_id},
                               {"x-accel-buffering", "no"} });
        for (const auto &record : stream_precommit_records_)
        {
            if (!write_stream_record(record))
            {
                stream_precommit_records_.clear();
                return false;
            }
        }
        stream_precommit_records_.clear();
        return true;
    }

    /* 更新序号/Usage 后把单个原始 SSE 帧写给客户端；高水位时只暂停，不丢弃当前帧。 */
    bool write_stream_record(const SseRecord &record)
    {
        if (record.sequence_number >= 0)
        {
            stream_last_sequence_number_ = std::max(stream_last_sequence_number_,
                                                    record.sequence_number);
        }
        if (record.usage.input_tokens)
        {
            stream_usage_.input_tokens = record.usage.input_tokens;
        }
        if (record.usage.cached_input_tokens)
        {
            stream_usage_.cached_input_tokens = record.usage.cached_input_tokens;
        }
        if (record.usage.output_tokens)
        {
            stream_usage_.output_tokens = record.usage.output_tokens;
        }
        if (stream_usage_.input_tokens && stream_usage_.output_tokens)
        {
            json usage = {{"input_tokens", *stream_usage_.input_tokens},
                          {"output_tokens", *stream_usage_.output_tokens},
                          {"input_tokens_details",
                           {{"cached_tokens", stream_usage_.cached_input_tokens.value_or(0)}}}};
            stream_usage_ = usage_accounting(usage, current_target_.prices);
        }
        else if (stream_usage_.input_tokens || stream_usage_.output_tokens)
        {
            stream_usage_.usage_quality = "partial";
        }
        if (record.kind == SseRecordKind::success_terminal)
        {
            stream_success_terminal_seen_ = true;
        }
        downstream_response_bytes_ += record.raw.size();
        if (!response_.write(record.raw))
        {
            if (!response_.client_connected())
            {
                client_cancelled_.store(true);
                return false;
            }
            paused_.store(true);
            ++pauses_;
        }
        return true;
    }

    RuntimeState &runtime_;
    RoutingRuntime &routing_;
    GovernanceRuntime &governance_;
    MetricsRegistry &metrics_;
    ProviderTransport &transport_;
    const ProtocolAdapter *adapter_ = nullptr;
    GatewayRequest request_;
    ResponseWriter &response_;
    CancellationToken cancellation_;
    GatewayConfig config_;
    json payload_;
    std::vector<ModelTarget> candidates_;
    std::string affinity_key_;
    std::string logical_model_;
    std::string tenant_slug_;
    std::string public_api_key_id_;
    std::uint64_t tenant_id_ = 0;
    std::uint64_t api_key_id_ = 0;
    std::uint64_t logical_model_id_ = 0;
    std::size_t max_attempts_ = 1;
    std::shared_ptr<const GovernancePermit> governance_permit_;
    std::shared_ptr<const GovernancePermit> credential_permit_;
    std::shared_ptr<LeaseLossSignal> lease_loss_;
    std::shared_ptr<NodeRequestLease> node_lease_;
    std::shared_ptr<NodeExecutionGuard> node_guard_;
    bool stream_ = false;
    std::chrono::steady_clock::time_point started_;
    std::chrono::steady_clock::time_point attempt_started_;
    std::shared_ptr<TransferBinding> binding_;
    ModelTarget current_target_;
    AttemptDecision last_decision_;
    std::string current_attempt_id_;
    std::size_t next_candidate_ = 0;
    std::size_t attempt_number_ = 0;
    std::size_t provider_attempt_count_ = 0;
    std::size_t billable_attempt_count_ = 0;
    std::size_t aggregate_upstream_bytes_ = 0;
    std::size_t attempt_upstream_bytes_ = 0;
    std::size_t downstream_response_bytes_ = 0;
    std::size_t pauses_ = 0;
    SseDecoder stream_decoder_;
    UsageAccounting stream_usage_;
    UsageAccounting aggregate_usage_;
    HeaderMap stream_headers_;
    std::vector<SseRecord> stream_precommit_records_;
    std::string stream_error_body_;
    std::string stream_local_failure_;
    long stream_status_ = 0;
    long stream_last_sequence_number_ = -1;
    std::size_t stream_precommit_bytes_ = 0;
    bool stream_is_sse_ = false;
    bool stream_success_terminal_seen_ = false;
    std::size_t exact_usage_attempt_count_ = 0;
    std::size_t exact_cost_attempt_count_ = 0;
    bool committed_ = false;
    bool terminal_error_written_ = false;
    bool credential_limited_ = false;
    long credential_retry_after_ms_ = 0;
    bool attempt_started_in_database_ = false;
    bool finalized_ = false;
    bool upstream_active_ = false;
    bool stream_active_ = false;
    std::atomic_bool paused_{false};
    std::atomic_bool client_cancelled_{false};
    std::atomic_bool governance_lost_{false};
    std::atomic_bool shutdown_requested_{false};
};
}

/* 创建未取消的 Token；CancellationSource 与异步协作者共享同一 CancellationState。 */
CancellationToken::CancellationToken()
    : state_(std::make_shared<CancellationState>())
{
}

/* 用已有共享状态创建 Token 视图，Token 本身不拥有取消动作。 */
CancellationToken::CancellationToken(std::shared_ptr<CancellationState> state)
    : state_(std::move(state))
{
}

/* 查询当前请求是否已取消。 */
bool CancellationToken::is_cancelled() const
{
    return state_ != nullptr && state_->cancelled();
}

/* 订阅客户端断开、shutdown 或上游 lease 丢失等取消来源。 */
void CancellationToken::on_cancel(std::function<void()> callback) const
{
    if (state_ != nullptr)
    {
        state_->add(std::move(callback));
    }
}

/* 创建拥有取消权的 Source。 */
CancellationSource::CancellationSource()
    : state_(std::make_shared<CancellationState>())
{
}

CancellationToken CancellationSource::token() const
{
    return CancellationToken(state_);
}

/* 广播一次取消事件；重复调用没有额外效果。 */
void CancellationSource::cancel() const
{
    if (state_ != nullptr)
    {
        state_->cancel();
    }
}

AiGateway::AiGateway(RuntimeState &runtime,
                     RoutingRuntime &routing,
                     GovernanceRuntime &governance,
                     NodeLifecycle &lifecycle,
                     MetricsRegistry &metrics,
                     ProviderTransport &transport)
    : runtime_(runtime), routing_(routing), governance_(governance), lifecycle_(lifecycle),
      metrics_(metrics), transport_(transport)
{
}

bool AiGateway::ready() const
{
    return lifecycle_.ready() && runtime_.ready() && routing_.ready() && governance_.ready() &&
           transport_.healthy();
}

void AiGateway::handle(const GatewayRequest &request,
                       ResponseWriter &response,
                       CancellationToken cancellation)
{
    const auto started = std::chrono::steady_clock::now();

    // healthz 只回答进程和事件循环是否活着，即使数据库、Redis 或节点正在 drain 也保持 200。
    if (request.method == "GET" && request.path == "/healthz")
    {
        const std::string body = R"({"status":"ok"})";
        write_response(response, 200, request.request_id, body);
        log_completion(request, 200, started, {}, "not_attempted", body.size());
        return;
    }

    if (request.method == "GET" && request.path == "/readyz")
    {
        const bool is_ready = ready();
        const std::string body = is_ready ? R"({"status":"ready"})"
                                          : R"({"status":"not_ready"})";
        write_response(response, is_ready ? 200 : 503, request.request_id, body);
        log_completion(request, is_ready ? 200 : 503, started, {}, "not_attempted", body.size());
        return;
    }

    if (request.method == "GET" && request.path == "/metrics")
    {
        metrics_.set_dependency_readiness(runtime_.ready(),
                                          routing_.ready() && governance_.ready());
        metrics_.set_node_state(lifecycle_.snapshot());
        const std::string body = metrics_.render();
        response.begin(200, {{"content-type", "text/plain; version=0.0.4; charset=utf-8"},
                             {"cache-control", "no-store"},
                             {"x-request-id", request.request_id}});
        response.write(body);
        response.end();
        return;
    }
    // 先按方法/路径选协议，之后缺失 Bearer 时仍能返回该协议的原生错误结构。
    const ProtocolAdapter *request_adapter =
        find_protocol_adapter(request.method, request.path);
    const auto auth_error_body = [request_adapter](ProtocolError error) {
        return request_adapter != nullptr
                   ? request_adapter->error_body(error)
                   : openai_error_body(error.message, error.type, error.code,
                                       error.param.empty() ? std::string() : error.param);
    };
    std::string token;
    if (!bearer_token(request, token))
    {
        const std::string body = auth_error_body(protocol_error(
            401, "authentication_error", "invalid_api_key", "Invalid API key"));
        write_response(response, 401, request.request_id, body,
                       {{"www-authenticate", "Bearer"}});
        log_completion(request, 401, started, {}, "not_attempted", body.size());
        return;
    }

    // 鉴权和快照加载异步执行，当前线程只注册完成回调。
    runtime_.authenticate(
        std::move(token),
        [this, request, &response, cancellation, started, request_adapter,
         auth_error_body](AuthResult result) mutable {
            if (cancellation.is_cancelled() || !response.client_connected())
            {
                response.end();
                log_completion(request, 499, started, {}, "client_cancelled");
                return;
            }
            if (result.status == AuthStatus::invalid_api_key)
            {
                const std::string body = auth_error_body(protocol_error(
                    401, "authentication_error", "invalid_api_key", "Invalid API key"));
                write_response(response, 401, request.request_id, body,
                               {{"www-authenticate", "Bearer"}});
                log_completion(request, 401, started, {}, "not_attempted", body.size());
                return;
            }
            if (result.status == AuthStatus::access_disabled)
            {
                const std::string body = auth_error_body(protocol_error(
                    403, "permission_error", "access_disabled", "Access is disabled"));
                write_response(response, 403, request.request_id, body);
                log_completion(request, 403, started, {}, "not_attempted", body.size());
                return;
            }
            if (result.status != AuthStatus::authorized || !result.snapshot)
            {
                const std::string body = auth_error_body(protocol_error(
                    503, "server_error", "authorization_unavailable",
                    "Gateway authorization is unavailable"));
                write_response(response, 503, request.request_id, body);
                log_completion(request, 503, started, {}, "not_attempted", body.size());
                return;
            }
            handle_authorized(request, response, cancellation, started,
                              std::move(result.snapshot));
        });
}

void AiGateway::handle_authorized(GatewayRequest request,
                                  ResponseWriter &response,
                                  CancellationToken cancellation,
                                  std::chrono::steady_clock::time_point started,
                                  std::shared_ptr<const AuthSnapshot> snapshot)
{
    const GatewayConfig &config = runtime_.config();
    const std::string &tenant_slug = snapshot->tenant_slug;
    const std::string &api_key_id = snapshot->public_api_key_id;
    const ProtocolAdapter *adapter = find_protocol_adapter(request.method, request.path);
    const auto error_body = [adapter](ProtocolError error) {
        return adapter != nullptr
                   ? adapter->error_body(error)
                   : openai_error_body(error.message, error.type, error.code,
                                       error.param.empty() ? std::string() : error.param);
    };
    // models 只展示静态授权且至少有一个候选的逻辑名称，不消耗请求配额，也不暴露 Provider。
    if (request.method == "GET" && request.path == "/v1/models")
    {
        std::set<std::string> model_names;
        for (const auto &entry : snapshot->models)
        {
            const ModelAccess &access = entry.second;
            if (access.model_granted && !access.candidates.empty())
            {
                model_names.insert(access.name);
            }
        }
        json data = json::array();
        for (const auto &name : model_names)
        {
            data.push_back({{"id", name}, {"object", "model"}, {"created", 0},
                            {"owned_by", "ai-gateway"}});
        }
        json body_json = {{"object", "list"}, {"data", std::move(data)}};
        const std::string body = body_json.dump();
        write_response(response, 200, request.request_id, body);
        log_completion(request, 200, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    if (adapter == nullptr)
    {
        const std::string body = error_body(protocol_error(
            404, "invalid_request_error", "not_found", "Endpoint not found"));
        write_response(response, 404, request.request_id, body);
        log_completion(request, 404, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    if (snapshot->protocols.count(adapter->protocol()) == 0)
    {
        const std::string body = error_body(protocol_error(
            403, "permission_error", "protocol_not_allowed", "Protocol is not allowed"));
        write_response(response, 403, request.request_id, body);
        log_completion(request, 403, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }

    if (request.body.size() > config.max_body_bytes)
    {
        const std::string body = error_body(protocol_error(
            413, "invalid_request_error", "body_too_large", "Request body is too large"));
        write_response(response, 413, request.request_id, body);
        log_completion(request, 413, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    const auto content_type = request.headers.find("content-type");
    if (content_type == request.headers.end() ||
        lower(content_type->second).rfind("application/json", 0) != 0)
    {
        const std::string body = error_body(protocol_error(
            400, "invalid_request_error", "invalid_content_type",
            "Content-Type must be application/json", "content-type"));
        write_response(response, 400, request.request_id, body);
        log_completion(request, 400, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    // 只允许 JSON 对象；具体 model/messages/max_tokens/stream 由协议 Adapter 校验。
    json payload;
    try
    {
        payload = json::parse(request.body);
    }
    catch (const json::exception &)
    {
        const std::string body = error_body(protocol_error(
            400, "invalid_request_error", "invalid_json", "Request body must be valid JSON"));
        write_response(response, 400, request.request_id, body);
        log_completion(request, 400, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    ProtocolRequestContext protocol_context;
    ProtocolError validation_error;
    if (!adapter->validate_request(payload, protocol_context, validation_error))
    {
        const std::string body = error_body(validation_error);
        write_response(response, 400, request.request_id, body);
        log_completion(request, 400, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    const std::string logical_model = protocol_context.model;
    const auto model = snapshot->models.find(adapter->protocol() + "\x1f" + logical_model);
    if (model == snapshot->models.end() || !model->second.model_granted ||
        (model->second.provider_denied && model->second.candidates.empty()))
    {
        const std::string body = error_body(protocol_error(
            403, "permission_error", "model_not_allowed", "Model is not allowed", "model"));
        write_response(response, 403, request.request_id, body);
        log_completion(request, 403, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    if (model->second.candidates.empty())
    {
        const std::string body = error_body(protocol_error(
            503, "server_error", "model_unavailable",
            "Model is temporarily unavailable", "model"));
        write_response(response, 503, request.request_id, body);
        log_completion(request, 503, started, logical_model, "not_attempted", body.size(), false,
                       0, tenant_slug, api_key_id);
        return;
    }
    const bool stream = protocol_context.stream;
    // 节点容量是本机第一道治理门，成功后才进入 Redis/MySQL，失败不会消耗分布式配额。
    NodeAdmission node_admission = lifecycle_.admit(stream);
    if (node_admission.status != NodeAdmissionStatus::admitted || !node_admission.lease)
    {
        const bool draining = node_admission.status == NodeAdmissionStatus::draining;
        const std::string code = draining ? "gateway_draining" : "gateway_overloaded";
        const std::string body = error_body(protocol_error(
            503, "server_error", code,
            draining ? "Gateway node is draining" : "Gateway node is at capacity"));
        write_response(response, 503, request.request_id, body, {{"retry-after", "1"}});
        log_completion(request, 503, started, logical_model, code, body.size(), stream, 0,
                       tenant_slug, api_key_id);
        return;
    }
    auto node_lease = std::move(node_admission.lease);
    auto node_guard = node_lease->execution_guard();
    response.set_completion_callback([node_lease] { node_lease->response_finished(); });
    std::string client_family = "generic";
    const auto user_agent = request.headers.find("user-agent");
    const auto originator = request.headers.find("originator");
    if ((user_agent != request.headers.end() &&
         lower(user_agent->second).find("codex") != std::string::npos) ||
        (originator != request.headers.end() &&
         lower(originator->second).find("codex") != std::string::npos))
    {
        client_family = "codex";
    }
    std::string session_hint;
    for (const char *name : {"session-id", "thread-id"})
    {
        const auto found = request.headers.find(name);
        if (found != request.headers.end() && !found->second.empty() &&
            found->second.size() <= 1024)
        {
            session_hint = found->second;
            break;
        }
    }
    if (session_hint.empty())
    {
        session_hint = adapter->session_hint(payload);
    }

    RouteRequest route_request;
    route_request.request_id = request.request_id;
    route_request.config_version = snapshot->config_version;
    route_request.public_api_key_id = api_key_id;
    route_request.protocol = adapter->protocol();
    route_request.logical_model = logical_model;
    route_request.scheduling_mode = model->second.scheduling_mode;
    route_request.max_attempts = model->second.max_attempts;
    route_request.client_family = std::move(client_family);
    route_request.session_hint = std::move(session_hint);
    route_request.candidates = model->second.candidates;
    std::vector<GovernanceScope> governance_scopes;
    const auto add_scope = [&governance_scopes](const char *kind,
                                                 std::uint64_t id,
                                                 const std::optional<QuotaPolicy> &quota) {
        if (quota)
        {
            governance_scopes.push_back({kind, id, quota->rpm, quota->concurrency});
        }
    };
    add_scope("tenant", snapshot->database_tenant_id, snapshot->tenant_quota);
    add_scope("api_key", snapshot->database_api_key_id, snapshot->api_key_quota);
    const std::uint64_t tenant_database_id = snapshot->database_tenant_id;
    const std::uint64_t api_key_database_id = snapshot->database_api_key_id;
    const std::uint64_t model_database_id = model->second.database_id;
    const std::size_t max_attempts = model->second.max_attempts;
    RequestAdmission request_admission;
    request_admission.request_id = request.request_id;
    request_admission.tenant_id = tenant_database_id;
    request_admission.api_key_id = api_key_database_id;
    request_admission.logical_model_id = model_database_id;
    request_admission.protocol = adapter->protocol();
    request_admission.stream = stream;
    request_admission.request_bytes = request.body.size();
    request_admission.max_attempts = max_attempts;
    request_admission.tenant_quota = snapshot->tenant_quota;
    request_admission.api_key_quota = snapshot->api_key_quota;

    auto lease_loss = std::make_shared<LeaseLossSignal>();
    governance_.admit(std::move(governance_scopes),
        [lease_loss] { lease_loss->trigger(); },
        [this, request = std::move(request), &response, cancellation, config,
         logical_model, tenant_slug, api_key_id, started, stream,
         tenant_database_id, api_key_database_id, model_database_id,
         payload = std::move(payload), route_request = std::move(route_request),
         request_admission = std::move(request_admission), max_attempts,
         lease_loss, node_lease, node_guard, adapter](GovernanceResult admission) mutable {
            if (cancellation.is_cancelled() || !response.client_connected())
            {
                governance_.release(admission.permit);
                response.end();
                log_completion(request, 499, started, logical_model, "client_cancelled", 0,
                               stream, 0, tenant_slug, api_key_id);
                return;
            }
            if (admission.status != GovernanceStatus::admitted || !admission.permit)
            {
                int status = 503;
                std::string code = "governance_unavailable";
                std::string type = "server_error";
                std::string message = "Gateway governance is unavailable";
                if (admission.status == GovernanceStatus::rate_limited)
                {
                    status = 429;
                    code = "rate_limit_exceeded";
                    type = "rate_limit_error";
                    message = "Request rate limit exceeded";
                }
                else if (admission.status == GovernanceStatus::concurrency_limited)
                {
                    status = 429;
                    code = "concurrency_limit_exceeded";
                    type = "rate_limit_error";
                    message = "Concurrent request limit exceeded";
                }
                metrics_.governance_rejected(code);
                const std::string body = adapter->error_body(
                    protocol_error(status, type, code, message));
                HeaderMap headers;
                if (admission.retry_after_ms > 0)
                {
                    headers["retry-after"] = std::to_string(
                        std::max<long>(1, (admission.retry_after_ms + 999) / 1000));
                }
                write_response(response, status, request.request_id, body, std::move(headers));
                log_completion(request, status, started, logical_model, code,
                               body.size(), stream, 0, tenant_slug, api_key_id);
                return;
            }
            // 请求级 RPM、并发和日/月预算通过 RuntimeState 异步进入 MySQL/Redis。
            runtime_.admit_request(
                std::move(request_admission),
                [this, request = std::move(request), &response, cancellation, config,
                 logical_model, tenant_slug, api_key_id, started, stream,
                 tenant_database_id, api_key_database_id, model_database_id,
                 payload = std::move(payload), permit = std::move(admission.permit),
                 route_request = std::move(route_request), lease_loss,
                 max_attempts, node_lease, node_guard, adapter](RequestAdmissionStatus request_status) mutable {
                    if (request_status != RequestAdmissionStatus::admitted)
                    {
                        const bool budget = request_status ==
                                            RequestAdmissionStatus::budget_exceeded;
                        metrics_.governance_rejected(
                            budget ? "budget_exceeded" : "governance_unavailable");
                        governance_.rollback(
                            std::move(permit),
                            [request = std::move(request), &response, started,
                             logical_model, tenant_slug, api_key_id, stream,
                             budget, node_guard, adapter](bool rolled_back) mutable {
                                const bool budget_error = budget && rolled_back;
                                const int status = budget_error ? 429 : 503;
                                const std::string code = budget_error
                                                             ? "budget_exceeded"
                                                             : "governance_unavailable";
                                const std::string body = adapter->error_body(protocol_error(
                                    status,
                                    budget_error ? "insufficient_quota" : "server_error",
                                    code,
                                    budget_error ? "Request budget is exhausted"
                                                 : "Gateway governance is unavailable"));
                                write_response(response, status, request.request_id, body);
                                log_completion(request, status, started, logical_model, code,
                                               body.size(), stream, 0, tenant_slug, api_key_id);
                            });
                        return;
                    }
                    if (cancellation.is_cancelled() || !response.client_connected())
                    {
                        response.end();
                        finish_without_attempt(runtime_, governance_, std::move(permit),
                                               std::move(request), 499, started, logical_model,
                                               "client_cancelled", 0, stream, tenant_slug,
                                               api_key_id, max_attempts, node_guard);
                        return;
                    }
                    routing_.plan(
                        std::move(route_request),
                        [this, request = std::move(request), &response, cancellation, config,
                         logical_model, tenant_slug, api_key_id, started, stream,
                         tenant_database_id, api_key_database_id, model_database_id,
                         payload = std::move(payload), permit = std::move(permit),
                         lease_loss, max_attempts, node_lease,
                         node_guard, adapter](RoutePlan plan) mutable {
                            if (cancellation.is_cancelled() || !response.client_connected())
                            {
                                response.end();
                                finish_without_attempt(
                                    runtime_, governance_, std::move(permit), std::move(request),
                                    499, started, logical_model, "client_cancelled", 0, stream,
                                    tenant_slug, api_key_id, max_attempts, node_guard);
                                return;
                            }
                            if (plan.status == RouteStatus::unavailable)
                            {
                                const std::string body = adapter->error_body(protocol_error(
                                    503, "server_error", "routing_unavailable",
                                    "Gateway routing is unavailable"));
                                write_response(response, 503, request.request_id, body);
                                finish_without_attempt(
                                    runtime_, governance_, std::move(permit), std::move(request),
                                    503, started, logical_model, "routing_unavailable",
                                    body.size(), stream, tenant_slug, api_key_id, max_attempts,
                                    node_guard);
                                return;
                            }
                            if (plan.status != RouteStatus::ready || plan.candidates.empty())
                            {
                                const std::string body = adapter->error_body(protocol_error(
                                    503, "server_error", "model_unavailable",
                                    "Model is temporarily unavailable", "model"));
                                write_response(response, 503, request.request_id, body);
                                finish_without_attempt(
                                    runtime_, governance_, std::move(permit), std::move(request),
                                    503, started, logical_model, "no_candidate", body.size(),
                                    stream, tenant_slug, api_key_id, max_attempts, node_guard);
                                return;
                            }

                            auto execution = std::make_shared<RequestExecution>(
                                runtime_, routing_, governance_, metrics_, transport_,
                                std::move(request), response, cancellation, config, *adapter,
                                std::move(payload),
                                std::move(plan.candidates), std::move(plan.affinity_key),
                                logical_model, tenant_slug, api_key_id, tenant_database_id,
                                api_key_database_id, model_database_id, max_attempts,
                                std::move(permit), lease_loss, node_lease, node_guard,
                                stream, started);
                            execution->start();
                        });
                });
        });
}

std::string generate_request_id()
{
    thread_local std::mt19937_64 generator(std::random_device{}());
    std::ostringstream output;
    output << "req_" << std::hex << std::setfill('0')
           << std::setw(16) << generator() << std::setw(16) << generator();
    return output.str();
}

/* 兼容无 ProtocolAdapter 路径的 OpenAI 风格错误编码。 */
std::string openai_error_body(std::string message,
                              std::string type,
                              std::string code,
                              std::string param)
{
    json error = {{"message", std::move(message)},
                  {"type", std::move(type)},
                  {"param", param.empty() ? json(nullptr) : json(std::move(param))},
                  {"code", std::move(code)}};
    return json({{"error", std::move(error)}}).dump();
}

void structured_log(const std::string &event, const HeaderMap &fields)
{
    json record = {{"event", event}};
    for (const auto &field : fields)
    {
        record[field.first] = field.second;
    }
    std::lock_guard<std::mutex> lock(log_mutex);
    std::cout << record.dump() << std::endl;
}
}
