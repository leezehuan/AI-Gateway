#include "gateway/protocol.hpp"
#include "gateway/runtime.hpp"

#include "json.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace ai_gateway
{
namespace
{
using json = nlohmann::json;

std::string openai_protocol_error_body(const ProtocolError &error)
{
    json body = {{"error", {{"message", error.message},
                             {"type", error.type},
                             {"param", error.param.empty() ? json(nullptr) : json(error.param)},
                             {"code", error.code}}}};
    return body.dump();
}

std::optional<std::uint64_t> unsigned_value(const json &object, const char *name)
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

UsageAccounting usage_fragment_from_object(const json &usage)
{
    UsageAccounting result;
    const char *input_name = usage.contains("input_tokens")
                                 ? "input_tokens"
                                 : (usage.contains("prompt_tokens") ? "prompt_tokens" : nullptr);
    const char *output_name = usage.contains("output_tokens")
                                  ? "output_tokens"
                                  : (usage.contains("completion_tokens")
                                         ? "completion_tokens"
                                         : nullptr);
    if (input_name != nullptr)
    {
        result.input_tokens = unsigned_value(usage, input_name);
        if (!result.input_tokens)
        {
            return UsageAccounting();
        }
    }
    if (output_name != nullptr)
    {
        result.output_tokens = unsigned_value(usage, output_name);
        if (!result.output_tokens)
        {
            return UsageAccounting();
        }
    }
    if (!result.input_tokens && !result.output_tokens)
    {
        return result;
    }

    if (result.input_tokens)
    {
        result.cached_input_tokens = 0;
    }
    if (usage.contains("input_tokens_details"))
    {
        const auto parsed = unsigned_value(usage["input_tokens_details"], "cached_tokens");
        if (!result.input_tokens || !parsed || *parsed > *result.input_tokens)
        {
            return UsageAccounting();
        }
        result.cached_input_tokens = *parsed;
    }
    else if (usage.contains("prompt_tokens_details"))
    {
        const auto parsed = unsigned_value(usage["prompt_tokens_details"], "cached_tokens");
        if (!result.input_tokens || !parsed || *parsed > *result.input_tokens)
        {
            return UsageAccounting();
        }
        result.cached_input_tokens = *parsed;
    }
    else if (usage.contains("cache_read_input_tokens"))
    {
        const auto parsed = unsigned_value(usage, "cache_read_input_tokens");
        if (!result.input_tokens || !parsed || *parsed > *result.input_tokens)
        {
            return UsageAccounting();
        }
        result.cached_input_tokens = *parsed;
    }

    result.usage_quality = result.input_tokens && result.output_tokens ? "exact" : "partial";
    return result;
}

UsageAccounting usage_from_object(const json &usage,
                                  const std::vector<ModelPrice> &prices)
{
    UsageAccounting result = usage_fragment_from_object(usage);
    if (!result.input_tokens || !result.output_tokens)
    {
        return result;
    }

    const std::uint64_t cached = result.cached_input_tokens.value_or(0);
    result.cached_input_tokens = cached;
    result.usage_quality = "exact";

    const auto now = static_cast<std::int64_t>(std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::now()));
    const ModelPrice *selected = nullptr;
    for (const auto &price : prices)
    {
        if (price.effective_at_epoch <= now &&
            (selected == nullptr || price.effective_at_epoch > selected->effective_at_epoch ||
             (price.effective_at_epoch == selected->effective_at_epoch && price.id > selected->id)))
        {
            selected = &price;
        }
    }
    if (selected == nullptr)
    {
        return result;
    }

    __extension__ typedef unsigned __int128 Wide;
    const Wide uncached_cost = static_cast<Wide>(*result.input_tokens - cached) *
                               selected->input_per_million_microusd;
    const Wide cached_cost = static_cast<Wide>(cached) *
                             selected->cached_input_per_million_microusd;
    const Wide output_cost = static_cast<Wide>(*result.output_tokens) *
                             selected->output_per_million_microusd;
    const Wide rounded = (uncached_cost + cached_cost + output_cost + 999999U) / 1000000U;
    if (rounded <= std::numeric_limits<std::uint64_t>::max())
    {
        result.model_price_id = selected->id;
        result.cost_microusd = static_cast<std::uint64_t>(rounded);
        result.cost_quality = "exact";
    }
    return result;
}

bool event_fields(std::string_view raw, std::string &event_name, std::string &data)
{
    std::size_t cursor = 0;
    bool saw_data = false;
    while (cursor < raw.size())
    {
        const std::size_t line_end = raw.find_first_of("\r\n", cursor);
        if (line_end == std::string_view::npos)
        {
            break;
        }
        const std::size_t ending_length = raw[line_end] == '\r' && line_end + 1 < raw.size() &&
                                                  raw[line_end + 1] == '\n'
                                              ? 2
                                              : 1;
        const std::string line(raw.substr(cursor, line_end - cursor));
        cursor = line_end + ending_length;
        if (line.empty())
        {
            break;
        }
        if (line.front() == ':')
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
    }
    return saw_data;
}

ProtocolEvent classify_generic(std::string_view raw,
                               const std::vector<ModelPrice> &prices,
                               const std::function<ProtocolEvent(std::string_view,
                                                                  std::string_view,
                                                                  const json &,
                                                                  const std::vector<ModelPrice> &)> &classifier)
{
    ProtocolEvent result;
    std::string event_name;
    std::string data;
    if (!event_fields(raw, event_name, data) || data.empty())
    {
        result.kind = ProtocolEventKind::comment;
        return result;
    }
    if (data == "[DONE]")
    {
        result.kind = ProtocolEventKind::success_terminal;
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
    if (!payload.is_object())
    {
        return result;
    }
    return classifier(event_name, data, payload, prices);
}

class OpenAiAdapter : public ProtocolAdapter
{
public:
    explicit OpenAiAdapter(std::string name, std::string path)
        : protocol_(std::move(name)), path_(std::move(path))
    {
    }

    const std::string &protocol() const override { return protocol_; }
    bool matches(std::string_view method, std::string_view path) const override
    {
        return method == "POST" && path == path_;
    }

    bool validate_request(const Json &payload,
                          ProtocolRequestContext &context,
                          ProtocolError &error) const override
    {
        if (!payload.is_object() || !payload.contains("model") ||
            !payload["model"].is_string() || payload["model"].get<std::string>().empty())
        {
            error = protocol_error(400, "invalid_request_error", "invalid_model",
                                    "model must be a non-empty string", "model");
            return false;
        }
        if (protocol_ == "chat_completions" &&
            (!payload.contains("messages") || !payload["messages"].is_array()))
        {
            error = protocol_error(400, "invalid_request_error", "invalid_messages",
                                    "messages must be an array", "messages");
            return false;
        }
        if (payload.contains("stream") && !payload["stream"].is_boolean())
        {
            error = protocol_error(400, "invalid_request_error", "invalid_stream",
                                    "stream must be a boolean", "stream");
            return false;
        }
        context.protocol = protocol_;
        context.model = payload["model"].get<std::string>();
        context.stream = payload.value("stream", false);
        return true;
    }

    HeaderMap provider_headers(const GatewayRequest &request,
                               const ModelTarget &target,
                               bool stream) const override
    {
        HeaderMap headers{{"authorization", "Bearer " + target.provider_api_key},
                          {"content-type", "application/json"},
                          {"accept", stream ? "text/event-stream" : "application/json"},
                          {"x-request-id", request.request_id}};
        for (const char *name : {"user-agent", "openai-beta", "originator", "session-id",
                                 "thread-id", "x-client-request-id"})
        {
            const auto found = request.headers.find(name);
            if (found != request.headers.end())
            {
                headers[name] = found->second;
            }
        }
        return headers;
    }

    ProtocolEvent classify_event(std::string_view raw,
                                 const std::vector<ModelPrice> &prices) const override
    {
        return classify_generic(raw, prices,
            [](std::string_view event_name, std::string_view, const json &payload,
               const std::vector<ModelPrice> &prices) {
                ProtocolEvent result;
                result.payload = payload;
                if (payload.contains("sequence_number") &&
                    payload["sequence_number"].is_number_integer())
                {
                    result.sequence_number = payload["sequence_number"].get<long>();
                }
                const std::string type = payload.value("type", "");
                if (type == "error" || event_name == "error")
                {
                    result.kind = ProtocolEventKind::provider_error;
                }
                else if (type == "response.completed")
                {
                    result.kind = ProtocolEventKind::success_terminal;
                    if (payload.contains("response") && payload["response"].is_object() &&
                        payload["response"].contains("usage"))
                    {
                        result.usage = usage_from_object(payload["response"]["usage"], prices);
                    }
                }
                else
                {
                    result.kind = ProtocolEventKind::business;
                }
                if (payload.contains("usage") && payload["usage"].is_object())
                {
                    result.usage = usage_from_object(payload["usage"], prices);
                }
                else if (payload.contains("response") && payload["response"].is_object() &&
                         payload["response"].contains("usage") &&
                         payload["response"]["usage"].is_object())
                {
                    result.usage = usage_from_object(payload["response"]["usage"], prices);
                }
                return result;
            });
    }

    bool normalize_response(const Json &provider_response,
                            std::string_view logical_model,
                            const std::vector<ModelPrice> &prices,
                            std::string &client_body,
                            UsageAccounting &usage,
                            ProtocolError &error) const override
    {
        if (!provider_response.is_object())
        {
            error = protocol_error(502, "server_error", "upstream_invalid_response",
                                   "Upstream provider returned invalid JSON");
            return false;
        }
        Json response = provider_response;
        if (response.contains("model"))
        {
            response["model"] = std::string(logical_model);
        }
        if (response.contains("usage"))
        {
            usage = usage_from_object(response["usage"], prices);
        }
        client_body = response.dump();
        return true;
    }

    std::string error_body(const ProtocolError &error) const override
    {
        return openai_protocol_error_body(error);
    }

    std::string terminal_error(long sequence_number,
                               std::string_view code,
                               std::string_view message) const override
    {
        if (protocol_ == "chat_completions")
        {
            Json payload = {{"error", {{"message", std::string(message)},
                                         {"type", "server_error"},
                                         {"code", std::string(code)}}}};
            return "data: " + payload.dump() + "\n\n";
        }
        Json payload = {{"type", "error"}, {"code", std::string(code)},
                        {"message", std::string(message)}, {"param", nullptr},
                        {"sequence_number", std::max<long>(0, sequence_number)}};
        return "event: error\ndata: " + payload.dump() + "\n\n";
    }

    std::string session_hint(const Json &payload) const override
    {
        if (payload.contains("prompt_cache_key") && payload["prompt_cache_key"].is_string())
        {
            const std::string value = payload["prompt_cache_key"].get<std::string>();
            return value.size() <= 1024 ? value : std::string();
        }
        return {};
    }

private:
    std::string protocol_;
    std::string path_;
};

class AnthropicAdapter final : public ProtocolAdapter
{
public:
    const std::string &protocol() const override { return protocol_; }
    bool matches(std::string_view method, std::string_view path) const override
    {
        return method == "POST" && path == "/v1/messages";
    }

    bool validate_request(const Json &payload,
                          ProtocolRequestContext &context,
                          ProtocolError &error) const override
    {
        if (!payload.is_object() || !payload.contains("model") ||
            !payload["model"].is_string() || payload["model"].get<std::string>().empty())
        {
            error = protocol_error(400, "invalid_request_error", "invalid_model",
                                    "model must be a non-empty string", "model");
            return false;
        }
        if (!payload.contains("messages") || !payload["messages"].is_array())
        {
            error = protocol_error(400, "invalid_request_error", "invalid_messages",
                                    "messages must be an array", "messages");
            return false;
        }
        bool valid_max_tokens = false;
        if (payload.contains("max_tokens"))
        {
            const auto &max_tokens = payload["max_tokens"];
            if (max_tokens.is_number_unsigned())
            {
                valid_max_tokens = max_tokens.get<std::uint64_t>() > 0;
            }
            else if (max_tokens.is_number_integer())
            {
                valid_max_tokens = max_tokens.get<std::int64_t>() > 0;
            }
        }
        if (!valid_max_tokens)
        {
            error = protocol_error(400, "invalid_request_error", "invalid_max_tokens",
                                    "max_tokens must be a positive integer", "max_tokens");
            return false;
        }
        if (payload.contains("stream") && !payload["stream"].is_boolean())
        {
            error = protocol_error(400, "invalid_request_error", "invalid_stream",
                                    "stream must be a boolean", "stream");
            return false;
        }
        context.protocol = protocol_;
        context.model = payload["model"].get<std::string>();
        context.stream = payload.value("stream", false);
        return true;
    }

    HeaderMap provider_headers(const GatewayRequest &request,
                               const ModelTarget &target,
                               bool stream) const override
    {
        HeaderMap headers{{"x-api-key", target.provider_api_key},
                          {"anthropic-version", "2023-06-01"},
                          {"content-type", "application/json"},
                          {"accept", stream ? "text/event-stream" : "application/json"},
                          {"x-request-id", request.request_id}};
        for (const char *name : {"user-agent", "anthropic-beta", "x-client-request-id"})
        {
            const auto found = request.headers.find(name);
            if (found != request.headers.end())
            {
                headers[name] = found->second;
            }
        }
        return headers;
    }

    ProtocolEvent classify_event(std::string_view raw,
                                 const std::vector<ModelPrice> &prices) const override
    {
        return classify_generic(raw, prices,
            [](std::string_view event_name, std::string_view, const json &payload,
               const std::vector<ModelPrice> &prices) {
                ProtocolEvent result;
                result.payload = payload;
                const std::string type = event_name.empty()
                                             ? payload.value("type", "")
                                             : std::string(event_name);
                if (type == "error" || payload.value("type", "") == "error")
                {
                    result.kind = ProtocolEventKind::provider_error;
                }
                else if (type == "message_stop")
                {
                    result.kind = ProtocolEventKind::success_terminal;
                    if (payload.contains("usage"))
                    {
                        result.usage = usage_from_object(payload["usage"], prices);
                    }
                }
                else
                {
                    result.kind = ProtocolEventKind::business;
                }
                if (type == "message_start" && payload.contains("message") &&
                    payload["message"].is_object() &&
                    payload["message"].contains("usage") &&
                    payload["message"]["usage"].is_object())
                {
                    result.usage = usage_from_object(payload["message"]["usage"], prices);
                }
                else if (payload.contains("usage") && payload["usage"].is_object())
                {
                    result.usage = usage_from_object(payload["usage"], prices);
                }
                return result;
            });
    }

    bool normalize_response(const Json &provider_response,
                            std::string_view logical_model,
                            const std::vector<ModelPrice> &prices,
                            std::string &client_body,
                            UsageAccounting &usage,
                            ProtocolError &error) const override
    {
        if (!provider_response.is_object())
        {
            error = protocol_error(502, "api_error", "upstream_invalid_response",
                                   "Upstream provider returned invalid JSON");
            return false;
        }
        Json response = provider_response;
        if (response.contains("model"))
        {
            response["model"] = std::string(logical_model);
        }
        if (response.contains("usage"))
        {
            usage = usage_from_object(response["usage"], prices);
        }
        client_body = response.dump();
        return true;
    }

    std::string error_body(const ProtocolError &error) const override
    {
        Json body = {{"type", "error"},
                     {"error", {{"type", error.code}, {"message", error.message}}}};
        return body.dump();
    }

    std::string terminal_error(long,
                               std::string_view code,
                               std::string_view message) const override
    {
        Json body = {{"type", "error"},
                     {"error", {{"type", std::string(code)},
                                 {"message", std::string(message)}}}};
        return "event: error\ndata: " + body.dump() + "\n\n";
    }

    std::string session_hint(const Json &payload) const override
    {
        if (payload.contains("metadata") && payload["metadata"].is_object() &&
            payload["metadata"].contains("user_id") &&
            payload["metadata"]["user_id"].is_string())
        {
            const std::string value = payload["metadata"]["user_id"].get<std::string>();
            return value.size() <= 1024 ? value : std::string();
        }
        return {};
    }

private:
    const std::string protocol_ = "anthropic_messages";
};

const OpenAiAdapter &responses_adapter()
{
    static const OpenAiAdapter adapter("responses", "/v1/responses");
    return adapter;
}

const OpenAiAdapter &chat_adapter()
{
    static const OpenAiAdapter adapter("chat_completions", "/v1/chat/completions");
    return adapter;
}

const AnthropicAdapter &anthropic_adapter()
{
    static const AnthropicAdapter adapter;
    return adapter;
}
} // namespace

ProtocolError protocol_error(int status,
                             std::string type,
                             std::string code,
                             std::string message,
                             std::string param)
{
    return {status, std::move(type), std::move(code), std::move(message), std::move(param)};
}

const ProtocolAdapter *find_protocol_adapter(std::string_view method,
                                             std::string_view path)
{
    const ProtocolAdapter *adapters[] = {&responses_adapter(), &chat_adapter(),
                                         &anthropic_adapter()};
    for (const ProtocolAdapter *adapter : adapters)
    {
        if (adapter->matches(method, path))
        {
            return adapter;
        }
    }
    return nullptr;
}

const ProtocolAdapter *find_protocol_adapter_for_name(std::string_view protocol)
{
    if (protocol == "responses") return &responses_adapter();
    if (protocol == "chat_completions") return &chat_adapter();
    if (protocol == "anthropic_messages") return &anthropic_adapter();
    return nullptr;
}

bool is_supported_protocol(std::string_view protocol)
{
    return find_protocol_adapter_for_name(protocol) != nullptr;
}
} // namespace ai_gateway
