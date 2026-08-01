#include "gateway/gateway.hpp"
#include "gateway/runtime.hpp"

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
                    const std::string &public_api_key_id = {})
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
                     {"backpressure_pauses", std::to_string(pauses)}};
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

class StreamExecution final : public std::enable_shared_from_this<StreamExecution>
{
public:
    StreamExecution(const GatewayRequest &request,
                    ResponseWriter &response,
                    CancellationToken cancellation,
                    GatewayConfig config,
                    std::string logical_model,
                    std::string tenant_slug,
                    std::string public_api_key_id,
                    std::chrono::steady_clock::time_point started)
        : request_(request),
          response_(response),
          cancellation_(std::move(cancellation)),
          config_(std::move(config)),
          logical_model_(std::move(logical_model)),
          tenant_slug_(std::move(tenant_slug)),
          public_api_key_id_(std::move(public_api_key_id)),
          started_(started),
          binding_(std::make_shared<TransferBinding>())
    {
    }

    void attach(std::shared_ptr<ProviderTransfer> transfer)
    {
        binding_->attach(std::move(transfer));
    }

    void cancel()
    {
        client_cancelled_.store(true);
        binding_->cancel();
    }

    void resume()
    {
        paused_.store(false);
        binding_->resume();
    }

    void on_headers(const ProviderResponseHead &head)
    {
        status_ = head.status;
        headers_ = head.headers;
        sse_ = status_ >= 200 && status_ < 300 && is_event_stream(headers_);
    }

    ProviderChunkAction on_body(std::string_view bytes)
    {
        if (cancellation_.is_cancelled() || !response_.client_connected())
        {
            cancel();
            return ProviderChunkAction::cancel_transfer;
        }
        if (!sse_)
        {
            if (status_ < 200 || status_ >= 300)
            {
                const std::size_t remaining = config_.stream_prefetch_bytes > error_body_.size()
                                                  ? config_.stream_prefetch_bytes - error_body_.size()
                                                  : 0;
                error_body_.append(bytes.data(), std::min(bytes.size(), remaining));
                return ProviderChunkAction::continue_transfer;
            }
            if (precommit_bytes_ + bytes.size() <= config_.stream_prefetch_bytes)
            {
                error_body_.append(bytes.data(), bytes.size());
                precommit_bytes_ += bytes.size();
            }
            else
            {
                local_failure_ = "upstream_prefetch_too_large";
                return ProviderChunkAction::cancel_transfer;
            }
            return ProviderChunkAction::continue_transfer;
        }

        const auto records = decoder_.push(bytes);
        for (const auto &record : records)
        {
            if (!committed_)
            {
                precommit_bytes_ += record.raw.size();
                if (precommit_bytes_ > config_.stream_prefetch_bytes)
                {
                    local_failure_ = "upstream_prefetch_too_large";
                    return ProviderChunkAction::cancel_transfer;
                }
                if (record.kind == SseRecordKind::comment)
                {
                    precommit_records_.push_back(record);
                    continue;
                }
                if (record.kind == SseRecordKind::invalid ||
                    record.kind == SseRecordKind::provider_error)
                {
                    local_failure_ = record.kind == SseRecordKind::provider_error
                                         ? "upstream_stream_error"
                                         : "upstream_invalid_stream";
                    return ProviderChunkAction::cancel_transfer;
                }
                precommit_records_.push_back(record);
                if (!commit())
                {
                    return ProviderChunkAction::cancel_transfer;
                }
                continue;
            }
            else if (record.kind == SseRecordKind::invalid)
            {
                local_failure_ = "upstream_invalid_stream";
                return ProviderChunkAction::cancel_transfer;
            }
            else if (record.kind == SseRecordKind::provider_error)
            {
                local_failure_ = "upstream_stream_error";
                return ProviderChunkAction::cancel_transfer;
            }

            if (!write_record(record))
            {
                return ProviderChunkAction::cancel_transfer;
            }
            if (record.kind == SseRecordKind::provider_error)
            {
                return ProviderChunkAction::cancel_transfer;
            }
        }
        if (!committed_ &&
            precommit_bytes_ + decoder_.buffered_bytes() > config_.stream_prefetch_bytes)
        {
            local_failure_ = "upstream_prefetch_too_large";
            return ProviderChunkAction::cancel_transfer;
        }
        return paused_.load() ? ProviderChunkAction::pause_after_accept
                              : ProviderChunkAction::continue_transfer;
    }

    void complete(ProviderResponse upstream)
    {
        if (finalized_)
        {
            return;
        }
        finalized_ = true;
        if (cancellation_.is_cancelled() || client_cancelled_.load() ||
            !response_.client_connected())
        {
            response_.end();
            log(499, "client_cancelled", response_bytes_);
            return;
        }

        const long status = upstream.status != 0 ? upstream.status : status_;
        if (committed_ && !decoder_.finish())
        {
            local_failure_ = "upstream_invalid_stream";
        }
        if (committed_ && success_terminal_seen_ && local_failure_.empty())
        {
            response_.end();
            log(200, "success", response_bytes_);
            return;
        }
        if (upstream.error != ProviderError::none)
        {
            if (committed_)
            {
                const std::string code = local_failure_.empty()
                                             ? provider_error_code(upstream.error)
                                             : local_failure_;
                write_terminal_error();
                response_.end();
                log(200, code, response_bytes_);
            }
            else
            {
                const std::string code = local_failure_.empty()
                                             ? provider_error_code(upstream.error)
                                             : local_failure_;
                const std::string body = openai_error_body(
                    "Upstream provider request failed", "server_error", code);
                write_response(response_, 502, request_.request_id, body);
                log(502, code, body.size());
            }
            return;
        }

        if (status >= 400 && status < 500)
        {
            const int client_status = static_cast<int>(status);
            const std::string code = status == 429 ? "upstream_rate_limited"
                                                   : "upstream_rejected_request";
            HeaderMap extra;
            const auto retry_after = upstream.headers.find("retry-after");
            if (retry_after != upstream.headers.end() && valid_retry_after(retry_after->second))
            {
                extra["retry-after"] = retry_after->second;
            }
            const std::string body = openai_error_body(
                "Upstream provider rejected the request", "upstream_error", code);
            write_response(response_, client_status, request_.request_id, body, std::move(extra));
            log(client_status, code, body.size());
            return;
        }
        if (status < 200 || status >= 300)
        {
            const std::string body = openai_error_body(
                "Upstream provider returned an invalid status", "server_error",
                "upstream_unavailable");
            write_response(response_, 502, request_.request_id, body);
            log(502, "upstream_unavailable", body.size());
            return;
        }
        if (!sse_)
        {
            const std::string body = openai_error_body(
                "Upstream provider did not return an event stream", "server_error",
                "upstream_invalid_stream");
            write_response(response_, 502, request_.request_id, body);
            log(502, "upstream_invalid_stream", body.size());
            return;
        }

        if (!committed_)
        {
            const std::string body = openai_error_body(
                "Upstream provider returned no valid stream event", "server_error",
                local_failure_.empty() ? "upstream_invalid_stream" : local_failure_);
            write_response(response_, 502, request_.request_id, body);
            log(502, local_failure_.empty() ? "upstream_invalid_stream" : local_failure_,
                body.size());
            return;
        }
        if (!local_failure_.empty())
        {
            write_terminal_error();
            response_.end();
            log(200, local_failure_, response_bytes_);
            return;
        }
        write_terminal_error();
        response_.end();
        log(200, "upstream_stream_incomplete", response_bytes_);
    }

private:
    void log(int status, const std::string &provider_result, std::size_t bytes)
    {
        log_completion(request_, status, started_, logical_model_, provider_result, bytes, true,
                       pauses_, tenant_slug_, public_api_key_id_);
    }

    bool commit()
    {
        committed_ = true;
        binding_->mark_stream_started();
        response_.begin(200, {{"content-type", "text/event-stream; charset=utf-8"},
                              {"cache-control", "no-store"},
                              {"x-request-id", request_.request_id},
                              {"x-accel-buffering", "no"}});
        for (const auto &record : precommit_records_)
        {
            if (!write_record(record))
            {
                precommit_records_.clear();
                return false;
            }
        }
        precommit_records_.clear();
        return true;
    }

    bool write_record(const SseRecord &record)
    {
        if (record.sequence_number >= 0)
        {
            last_sequence_number_ = std::max(last_sequence_number_, record.sequence_number);
        }
        if (record.kind == SseRecordKind::success_terminal)
        {
            success_terminal_seen_ = true;
        }
        response_bytes_ += record.raw.size();
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

    void write_terminal_error()
    {
        const std::string event =
            stream_terminal_error(next_sequence_number(), "upstream_stream_error");
        response_bytes_ += event.size();
        response_.write(event);
    }

    long next_sequence_number() const
    {
        return std::max<long>(last_sequence_number_ + 1, 1);
    }

    GatewayRequest request_;
    ResponseWriter &response_;
    CancellationToken cancellation_;
    GatewayConfig config_;
    std::string logical_model_;
    std::string tenant_slug_;
    std::string public_api_key_id_;
    std::chrono::steady_clock::time_point started_;
    std::shared_ptr<TransferBinding> binding_;
    SseDecoder decoder_;
    HeaderMap headers_;
    std::vector<SseRecord> precommit_records_;
    std::string error_body_;
    std::string local_failure_;
    long status_ = 0;
    long last_sequence_number_ = -1;
    std::size_t precommit_bytes_ = 0;
    std::size_t response_bytes_ = 0;
    std::size_t pauses_ = 0;
    bool sse_ = false;
    bool committed_ = false;
    std::atomic_bool paused_{false};
    std::atomic_bool client_cancelled_{false};
    bool finalized_ = false;
    bool success_terminal_seen_ = false;
};

class BasicExecution final : public std::enable_shared_from_this<BasicExecution>
{
public:
    BasicExecution(const GatewayRequest &request,
                   ResponseWriter &response,
                   CancellationToken cancellation,
                   std::string logical_model,
                   std::string tenant_slug,
                   std::string public_api_key_id,
                   std::chrono::steady_clock::time_point started)
        : request_(request),
          response_(response),
          cancellation_(std::move(cancellation)),
          logical_model_(std::move(logical_model)),
          tenant_slug_(std::move(tenant_slug)),
          public_api_key_id_(std::move(public_api_key_id)),
          started_(started),
          binding_(std::make_shared<TransferBinding>())
    {
    }

    void attach(std::shared_ptr<ProviderTransfer> transfer)
    {
        binding_->attach(std::move(transfer));
    }

    void cancel()
    {
        binding_->cancel();
    }

    void complete(ProviderResponse upstream)
    {
        if (finalized_)
        {
            return;
        }
        finalized_ = true;
        if (cancellation_.is_cancelled() || !response_.client_connected())
        {
            response_.end();
            log(499, "client_cancelled");
            return;
        }
        if (upstream.error != ProviderError::none)
        {
            const std::string code = provider_error_code(upstream.error);
            const std::string body = openai_error_body(
                "Upstream provider request failed", "server_error", code);
            write_response(response_, 502, request_.request_id, body);
            log(502, code, body.size());
            return;
        }
        if (upstream.status >= 400 && upstream.status < 500)
        {
            const int status = static_cast<int>(upstream.status);
            const std::string code = status == 429 ? "upstream_rate_limited"
                                                   : "upstream_rejected_request";
            HeaderMap extra;
            const auto retry_after = upstream.headers.find("retry-after");
            if (retry_after != upstream.headers.end() && valid_retry_after(retry_after->second))
            {
                extra["retry-after"] = retry_after->second;
            }
            const std::string body = openai_error_body(
                "Upstream provider rejected the request", "upstream_error", code);
            write_response(response_, status, request_.request_id, body, std::move(extra));
            log(status, code, body.size());
            return;
        }
        if (upstream.status < 200 || upstream.status >= 300)
        {
            const std::string body = openai_error_body(
                "Upstream provider returned an invalid status", "server_error",
                "upstream_unavailable");
            write_response(response_, 502, request_.request_id, body);
            log(502, "upstream_unavailable", body.size());
            return;
        }
        json response_json;
        try
        {
            response_json = json::parse(upstream.body);
        }
        catch (const json::exception &)
        {
            const std::string body = openai_error_body(
                "Upstream provider returned invalid JSON", "server_error",
                "upstream_invalid_response");
            write_response(response_, 502, request_.request_id, body);
            log(502, "upstream_invalid_response", body.size());
            return;
        }
        if (!response_json.is_object())
        {
            const std::string body = openai_error_body(
                "Upstream provider returned an invalid response", "server_error",
                "upstream_invalid_response");
            write_response(response_, 502, request_.request_id, body);
            log(502, "upstream_invalid_response", body.size());
            return;
        }
        if (response_json.contains("model"))
        {
            response_json["model"] = logical_model_;
        }
        const std::string body = response_json.dump();
        write_response(response_, static_cast<int>(upstream.status), request_.request_id, body);
        log(static_cast<int>(upstream.status), "success", body.size());
    }

private:
    void log(int status, const std::string &provider_result, std::size_t bytes = 0)
    {
        log_completion(request_, status, started_, logical_model_, provider_result, bytes, false,
                       0, tenant_slug_, public_api_key_id_);
    }

    GatewayRequest request_;
    ResponseWriter &response_;
    CancellationToken cancellation_;
    std::string logical_model_;
    std::string tenant_slug_;
    std::string public_api_key_id_;
    std::chrono::steady_clock::time_point started_;
    std::shared_ptr<TransferBinding> binding_;
    bool finalized_ = false;
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

AiGateway::AiGateway(RuntimeState &runtime, ProviderTransport &transport)
    : runtime_(runtime), transport_(transport)
{
}

bool AiGateway::ready() const
{
    return runtime_.ready() && transport_.healthy();
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
                    !access.configuration_unavailable && access.active_mapping_count == 1 &&
                    access.candidates.size() == 1)
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
    if (model->second.configuration_unavailable || model->second.active_mapping_count != 1 ||
        model->second.candidates.size() != 1)
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
    const ModelTarget &target = model->second.candidates.front();
    payload["model"] = target.upstream_model;
    ProviderRequest provider_request;
    provider_request.url = target.provider_url;
    provider_request.headers = upstream_headers(request, target, stream);
    provider_request.body = payload.dump();
    provider_request.max_response_bytes = config.max_response_bytes;
    provider_request.timeout_ms = config.upstream_timeout_ms;
    provider_request.streaming = stream;
    provider_request.idle_timeout_ms = config.stream_idle_timeout_ms;
    provider_request.max_duration_ms = config.stream_max_duration_ms;

    if (stream)
    {
        auto state = std::make_shared<StreamExecution>(
            request, response, cancellation, config, logical_model, tenant_slug, api_key_id,
            started);
        response.set_writable_callback([weak = std::weak_ptr<StreamExecution>(state)] {
            if (auto locked = weak.lock())
            {
                locked->resume();
            }
        });
        cancellation.on_cancel([weak = std::weak_ptr<StreamExecution>(state)] {
            if (auto locked = weak.lock())
            {
                locked->cancel();
            }
        });
        ProviderCallbacks callbacks;
        callbacks.on_headers = [state](const ProviderResponseHead &head) { state->on_headers(head); };
        callbacks.on_body = [state](std::string_view bytes) { return state->on_body(bytes); };
        callbacks.on_complete = [state](ProviderResponse upstream) { state->complete(std::move(upstream)); };
        state->attach(transport_.execute(std::move(provider_request), std::move(callbacks)));
        return;
    }

    auto state = std::make_shared<BasicExecution>(
        request, response, cancellation, logical_model, tenant_slug, api_key_id, started);
    cancellation.on_cancel([weak = std::weak_ptr<BasicExecution>(state)] {
        if (auto locked = weak.lock())
        {
            locked->cancel();
        }
    });
    ProviderCallbacks callbacks;
    callbacks.on_complete = [state](ProviderResponse upstream) { state->complete(std::move(upstream)); };
    state->attach(transport_.execute(std::move(provider_request), std::move(callbacks)));
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
