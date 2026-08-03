#include "gateway/gateway.hpp"
#include "gateway/runtime.hpp"
#include "gateway/routing.hpp"

#include "json.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
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

    bool cancelled() const
    {
        return cancelled_.load();
    }

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

HeaderMap upstream_headers(const GatewayRequest &request,
                           const ModelTarget &target,
                           bool stream)
{
    static const char *allowed[] = {
        "user-agent", "openai-beta", "originator", "session-id",
        "thread-id", "x-client-request-id"};

    HeaderMap headers{{"authorization", "Bearer " + target.provider_api_key},
                      {"content-type", "application/json"},
                      {"accept", stream ? "text/event-stream" : "application/json"},
                      {"x-request-id", request.request_id}};
    for (const char *name : allowed)
    {
        const auto found = request.headers.find(name);
        if (found != request.headers.end())
        {
            headers[name] = found->second;
        }
    }
    return headers;
}

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
        // A CR at the end of a body chunk may be the first half of CRLF.
        // Keep it buffered until the following chunk identifies the delimiter.
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
            // the raw event forwarded downstream.
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
            records.push_back(classify_sse_record(buffer_.substr(0, end)));
            buffer_.erase(0, end);
        }
        return records;
    }

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
};

std::string stream_terminal_error(long sequence_number, const std::string &code)
{
    json payload = {{"type", "error"},
                    {"code", code},
                    {"message", "Upstream provider stream failed"},
                    {"param", nullptr},
                    {"sequence_number", std::max<long>(0, sequence_number)}};
    return "event: error\ndata: " + payload.dump() + "\n\n";
}

bool is_event_stream(const HeaderMap &headers)
{
    const auto found = headers.find("content-type");
    return found != headers.end() && lower(found->second).find("text/event-stream") == 0;
}

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

    void cancel()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cancel_requested_ = true;
        if (transfer_)
        {
            transfer_->cancel();
        }
    }

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
};

class RequestExecution final : public std::enable_shared_from_this<RequestExecution>
{
public:
    RequestExecution(RuntimeState &runtime,
                     RoutingRuntime &routing,
                     ProviderTransport &transport,
                     GatewayRequest request,
                     ResponseWriter &response,
                     CancellationToken cancellation,
                     GatewayConfig config,
                     json payload,
                     std::vector<ModelTarget> candidates,
                     std::string affinity_key,
                     std::string logical_model,
                     std::string tenant_slug,
                     std::string public_api_key_id,
                     std::uint64_t tenant_id,
                     std::uint64_t api_key_id,
                     std::uint64_t logical_model_id,
                     bool stream,
                     std::chrono::steady_clock::time_point started)
        : runtime_(runtime),
          routing_(routing),
          transport_(transport),
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
          stream_(stream),
          started_(started)
    {
    }

    void start()
    {
        const auto weak = weak_from_this();
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
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            finalize_cancelled();
            return;
        }
        if (next_candidate_ >= candidates_.size())
        {
            finalize_failure(last_decision_);
            return;
        }

        current_target_ = candidates_[next_candidate_++];
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

    void on_attempt_started(bool stored)
    {
        if (finalized_)
        {
            return;
        }
        if (!stored)
        {
            finalize_gateway_error("Gateway audit is unavailable", "audit_unavailable");
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

        json upstream_payload = payload_;
        upstream_payload["model"] = current_target_.upstream_model;
        ProviderRequest provider_request;
        provider_request.url = current_target_.provider_url;
        provider_request.headers = upstream_headers(request_, current_target_, stream_);
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
        binding_->attach(transport_.execute(std::move(provider_request), std::move(callbacks)));
    }

    std::size_t remaining_response_bytes() const
    {
        return aggregate_upstream_bytes_ < config_.max_response_bytes
                   ? config_.max_response_bytes - aggregate_upstream_bytes_
                   : 1;
    }

    static bool retryable_status(long status)
    {
        return status == 429 || status == 500 || status == 502 || status == 503 ||
               status == 504;
    }

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
        decision.client_body = openai_error_body(
            status >= 400 && status < 500 ? "Upstream provider rejected the request"
                                          : "Upstream provider returned an invalid status",
            status >= 400 && status < 500 ? "upstream_error" : "server_error",
            decision.error_class);
        const auto retry_after = headers.find("retry-after");
        if (retry_after != headers.end() && valid_retry_after(retry_after->second))
        {
            decision.client_headers["retry-after"] = retry_after->second;
        }
        return decision;
    }

    void on_basic_complete(ProviderResponse upstream)
    {
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
                if (!response_json.is_object())
                {
                    throw json::type_error::create(302, "response is not an object");
                }
                if (response_json.contains("model"))
                {
                    response_json["model"] = logical_model_;
                }
                decision.state = "succeeded";
                decision.error_class = "success";
                decision.success = true;
                decision.provider_status = upstream.status;
                decision.client_status = static_cast<int>(upstream.status);
                decision.client_body = response_json.dump();
                decision.upstream_bytes = attempt_upstream_bytes_;
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
        finish_attempt(std::move(decision));
    }

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
        finish_attempt(std::move(decision));
    }

    void finish_attempt(AttemptDecision decision)
    {
        if (!attempt_started_in_database_)
        {
            return;
        }
        attempt_started_in_database_ = false;
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
        runtime_.finish_attempt(std::move(finish),
            [self = shared_from_this(), decision = std::move(decision),
             duration = elapsed.count()](bool stored) mutable {
                self->on_attempt_audited(std::move(decision), stored, duration);
            });
    }

    void on_attempt_audited(AttemptDecision decision, bool stored, long long duration_ms)
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

    void finalize_failure(const AttemptDecision &decision)
    {
        if (finalized_)
        {
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
        const std::string body = openai_error_body(
            "Upstream provider request failed", "server_error", code);
        write_response(response_, 502, request_.request_id, body);
        finalize_log(502, code, body.size());
    }

    void finalize_committed_failure(const std::string &code)
    {
        if (finalized_)
        {
            return;
        }
        if (!terminal_error_written_)
        {
            const std::string event = stream_terminal_error(
                std::max<long>(stream_last_sequence_number_ + 1, 1),
                "upstream_stream_error");
            downstream_response_bytes_ += event.size();
            response_.write(event);
            terminal_error_written_ = true;
        }
        response_.end();
        finalize_log(200, code);
    }

    void finalize_gateway_error(const std::string &message, const std::string &code)
    {
        if (finalized_)
        {
            return;
        }
        const std::string body = openai_error_body(message, "server_error", code);
        write_response(response_, 503, request_.request_id, body);
        finalize_log(503, code, body.size());
    }

    void finalize_cancelled()
    {
        if (finalized_)
        {
            return;
        }
        response_.end();
        finalize_log(499, "client_cancelled");
    }

    void finalize_log(int status, const std::string &provider_result, std::size_t bytes = 0)
    {
        if (finalized_)
        {
            return;
        }
        finalized_ = true;
        log_completion(request_, status, started_, logical_model_, provider_result,
                       stream_ ? downstream_response_bytes_ : bytes, stream_, pauses_,
                       tenant_slug_, public_api_key_id_,
                       attempt_number_ > 0 ? attempt_number_ - 1 : 0);
    }

    void reset_stream_attempt()
    {
        attempt_started_in_database_ = false;
        attempt_upstream_bytes_ = 0;
        stream_decoder_ = SseDecoder();
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

    bool write_stream_record(const SseRecord &record)
    {
        if (record.sequence_number >= 0)
        {
            stream_last_sequence_number_ = std::max(stream_last_sequence_number_,
                                                    record.sequence_number);
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
    ProviderTransport &transport_;
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
    bool stream_ = false;
    std::chrono::steady_clock::time_point started_;
    std::chrono::steady_clock::time_point attempt_started_;
    std::shared_ptr<TransferBinding> binding_;
    ModelTarget current_target_;
    AttemptDecision last_decision_;
    std::string current_attempt_id_;
    std::size_t next_candidate_ = 0;
    std::size_t attempt_number_ = 0;
    std::size_t aggregate_upstream_bytes_ = 0;
    std::size_t attempt_upstream_bytes_ = 0;
    std::size_t downstream_response_bytes_ = 0;
    std::size_t pauses_ = 0;
    SseDecoder stream_decoder_;
    HeaderMap stream_headers_;
    std::vector<SseRecord> stream_precommit_records_;
    std::string stream_error_body_;
    std::string stream_local_failure_;
    long stream_status_ = 0;
    long stream_last_sequence_number_ = -1;
    std::size_t stream_precommit_bytes_ = 0;
    bool stream_is_sse_ = false;
    bool stream_success_terminal_seen_ = false;
    bool committed_ = false;
    bool terminal_error_written_ = false;
    bool attempt_started_in_database_ = false;
    bool finalized_ = false;
    std::atomic_bool paused_{false};
    std::atomic_bool client_cancelled_{false};
};
} // namespace

CancellationToken::CancellationToken()
    : state_(std::make_shared<CancellationState>())
{
}

CancellationToken::CancellationToken(std::shared_ptr<CancellationState> state)
    : state_(std::move(state))
{
}

bool CancellationToken::is_cancelled() const
{
    return state_ != nullptr && state_->cancelled();
}

void CancellationToken::on_cancel(std::function<void()> callback) const
{
    if (state_ != nullptr)
    {
        state_->add(std::move(callback));
    }
}

CancellationSource::CancellationSource()
    : state_(std::make_shared<CancellationState>())
{
}

CancellationToken CancellationSource::token() const
{
    return CancellationToken(state_);
}

void CancellationSource::cancel() const
{
    if (state_ != nullptr)
    {
        state_->cancel();
    }
}

AiGateway::AiGateway(RuntimeState &runtime, RoutingRuntime &routing, ProviderTransport &transport)
    : runtime_(runtime), routing_(routing), transport_(transport)
{
}

bool AiGateway::ready() const
{
    return runtime_.ready() && routing_.ready() && transport_.healthy();
}

void AiGateway::handle(const GatewayRequest &request,
                       ResponseWriter &response,
                       CancellationToken cancellation)
{
    const auto started = std::chrono::steady_clock::now();

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
    std::string token;
    if (!bearer_token(request, token))
    {
        const std::string body = openai_error_body(
            "Invalid API key", "authentication_error", "invalid_api_key");
        write_response(response, 401, request.request_id, body,
                       {{"www-authenticate", "Bearer"}});
        log_completion(request, 401, started, {}, "not_attempted", body.size());
        return;
    }

    runtime_.authenticate(
        std::move(token),
        [this, request, &response, cancellation, started](AuthResult result) mutable {
            if (cancellation.is_cancelled() || !response.client_connected())
            {
                response.end();
                log_completion(request, 499, started, {}, "client_cancelled");
                return;
            }
            if (result.status == AuthStatus::invalid_api_key)
            {
                const std::string body = openai_error_body(
                    "Invalid API key", "authentication_error", "invalid_api_key");
                write_response(response, 401, request.request_id, body,
                               {{"www-authenticate", "Bearer"}});
                log_completion(request, 401, started, {}, "not_attempted", body.size());
                return;
            }
            if (result.status == AuthStatus::access_disabled)
            {
                const std::string body = openai_error_body(
                    "Access is disabled", "permission_error", "access_disabled");
                write_response(response, 403, request.request_id, body);
                log_completion(request, 403, started, {}, "not_attempted", body.size());
                return;
            }
            if (result.status != AuthStatus::authorized || !result.snapshot)
            {
                const std::string body = openai_error_body(
                    "Gateway authorization is unavailable", "server_error",
                    "authorization_unavailable");
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
    if (request.method == "GET" && request.path == "/v1/models")
    {
        std::vector<std::string> model_names;
        if (snapshot->protocols.count("responses") != 0)
        {
            for (const auto &entry : snapshot->models)
            {
                const ModelAccess &access = entry.second;
                if (access.protocol == "responses" && access.model_granted &&
                    !access.candidates.empty())
                {
                    model_names.push_back(access.name);
                }
            }
        }
        std::sort(model_names.begin(), model_names.end());
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
    if (request.method != "POST" || request.path != "/v1/responses")
    {
        const std::string body = openai_error_body(
            "Endpoint not found", "invalid_request_error", "not_found");
        write_response(response, 404, request.request_id, body);
        log_completion(request, 404, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    if (snapshot->protocols.count("responses") == 0)
    {
        const std::string body = openai_error_body(
            "Protocol is not allowed", "permission_error", "protocol_not_allowed");
        write_response(response, 403, request.request_id, body);
        log_completion(request, 403, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    if (request.body.size() > config.max_body_bytes)
    {
        const std::string body = openai_error_body(
            "Request body is too large", "invalid_request_error", "body_too_large");
        write_response(response, 413, request.request_id, body);
        log_completion(request, 413, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    const auto content_type = request.headers.find("content-type");
    if (content_type == request.headers.end() ||
        lower(content_type->second).rfind("application/json", 0) != 0)
    {
        const std::string body = openai_error_body(
            "Content-Type must be application/json", "invalid_request_error",
            "invalid_content_type", "content-type");
        write_response(response, 400, request.request_id, body);
        log_completion(request, 400, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    json payload;
    try
    {
        payload = json::parse(request.body);
    }
    catch (const json::exception &)
    {
        const std::string body = openai_error_body(
            "Request body must be valid JSON", "invalid_request_error", "invalid_json");
        write_response(response, 400, request.request_id, body);
        log_completion(request, 400, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    if (!payload.is_object() || !payload.contains("model") || !payload["model"].is_string() ||
        payload["model"].get<std::string>().empty())
    {
        const std::string body = openai_error_body(
            "model must be a non-empty string", "invalid_request_error", "invalid_model", "model");
        write_response(response, 400, request.request_id, body);
        log_completion(request, 400, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    const std::string logical_model = payload["model"].get<std::string>();
    const auto model = snapshot->models.find("responses\x1f" + logical_model);
    if (model == snapshot->models.end() || !model->second.model_granted ||
        (model->second.provider_denied && model->second.candidates.empty()))
    {
        const std::string body = openai_error_body(
            "Model is not allowed", "permission_error", "model_not_allowed", "model");
        write_response(response, 403, request.request_id, body);
        log_completion(request, 403, started, {}, "not_attempted", body.size(), false, 0,
                       tenant_slug, api_key_id);
        return;
    }
    if (model->second.candidates.empty())
    {
        const std::string body = openai_error_body(
            "Model is temporarily unavailable", "server_error", "model_unavailable", "model");
        write_response(response, 503, request.request_id, body);
        log_completion(request, 503, started, logical_model, "not_attempted", body.size(), false,
                       0, tenant_slug, api_key_id);
        return;
    }
    if (payload.contains("stream") && !payload["stream"].is_boolean())
    {
        const std::string body = openai_error_body(
            "stream must be a boolean", "invalid_request_error", "invalid_stream", "stream");
        write_response(response, 400, request.request_id, body);
        log_completion(request, 400, started, logical_model, "not_attempted", body.size(), false,
                       0, tenant_slug, api_key_id);
        return;
    }

    const bool stream = payload.value("stream", false);
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
    if (session_hint.empty() && payload.contains("prompt_cache_key") &&
        payload["prompt_cache_key"].is_string())
    {
        const std::string value = payload["prompt_cache_key"].get<std::string>();
        if (!value.empty() && value.size() <= 1024)
        {
            session_hint = value;
        }
    }

    RouteRequest route_request;
    route_request.request_id = request.request_id;
    route_request.config_version = snapshot->config_version;
    route_request.public_api_key_id = api_key_id;
    route_request.protocol = "responses";
    route_request.logical_model = logical_model;
    route_request.scheduling_mode = model->second.scheduling_mode;
    route_request.max_attempts = model->second.max_attempts;
    route_request.client_family = std::move(client_family);
    route_request.session_hint = std::move(session_hint);
    route_request.candidates = model->second.candidates;
    const std::uint64_t tenant_database_id = snapshot->database_tenant_id;
    const std::uint64_t api_key_database_id = snapshot->database_api_key_id;
    const std::uint64_t model_database_id = model->second.database_id;

    routing_.plan(std::move(route_request),
        [this, request = std::move(request), &response, cancellation, config,
         logical_model, tenant_slug, api_key_id, started, stream,
         tenant_database_id, api_key_database_id, model_database_id,
         payload = std::move(payload)](RoutePlan plan) mutable {
            if (cancellation.is_cancelled() || !response.client_connected())
            {
                response.end();
                log_completion(request, 499, started, logical_model, "client_cancelled", 0,
                               stream, 0, tenant_slug, api_key_id);
                return;
            }
            if (plan.status == RouteStatus::unavailable)
            {
                const std::string body = openai_error_body(
                    "Gateway routing is unavailable", "server_error", "routing_unavailable");
                write_response(response, 503, request.request_id, body);
                log_completion(request, 503, started, logical_model, "routing_unavailable",
                               body.size(), stream, 0, tenant_slug, api_key_id);
                return;
            }
            if (plan.status != RouteStatus::ready || plan.candidates.empty())
            {
                const std::string body = openai_error_body(
                    "Model is temporarily unavailable", "server_error", "model_unavailable",
                    "model");
                write_response(response, 503, request.request_id, body);
                log_completion(request, 503, started, logical_model, "no_candidate", body.size(),
                               stream, 0, tenant_slug, api_key_id);
                return;
            }

            auto execution = std::make_shared<RequestExecution>(
                runtime_, routing_, transport_, std::move(request), response, cancellation,
                config, std::move(payload), std::move(plan.candidates),
                std::move(plan.affinity_key), logical_model, tenant_slug, api_key_id,
                tenant_database_id, api_key_database_id, model_database_id, stream, started);
            execution->start();
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
} // namespace ai_gateway
