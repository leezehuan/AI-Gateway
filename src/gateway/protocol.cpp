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

/*
 * 协议 Adapter 注册表与原生透传规则。
 *
 * Responses、Chat Completions 与 Anthropic Messages 共享 Gateway 的认证、路由、治理、failover、SSE
 * 背压和审计状态机；本文件只负责协议差异：路径/方法匹配、请求最低校验、上游认证 Header、非流式响应
 * 规范化、SSE 事件分类、Usage 提取以及提交后终止错误格式。它不做跨协议转换，因此 Provider 返回的业务
 * 字段会尽量原样经过网关，只有 model 会改回租户可见的逻辑模型名。
 */
namespace ai_gateway
{
namespace
{
using json = nlohmann::json;

/*
 * 函数名直译：生成 OpenAI 风格协议错误正文。
 *
 * 通俗说：当 Responses 或 Chat Completions 发现请求/上游错误时，客户端仍然需要收到
 * 固定的 {"error": ...} JSON，而不是 C++ 异常文字或 Provider 原始秘密。
 *
 * 专业说法：这是 OpenAI 协议 Adapter 的错误编码函数，统一序列化 message、type、param 和 code。
 *
 * 参数说明：
 * - error：已由网关状态机归类的脱敏协议错误。
 *
 * 返回值：可直接作为 HTTP JSON 响应正文的字符串。
 */
std::string openai_protocol_error_body(const ProtocolError &error)
{
    json body = {{"error", {{"message", error.message},
                             {"type", error.type},
                             {"param", error.param.empty() ? json(nullptr) : json(error.param)},
                             {"code", error.code}}}};
    return body.dump();
}

/*
 * 函数名直译：读取无符号整数。
 *
 * 通俗说：Usage 里的 token 数必须是非负整数；这个函数既接受普通整数，也接受 JSON
 * 能表示的超大无符号整数，并在类型不对、缺字段或数值为负时返回空值。
 *
 * 专业说法：这是 Provider Usage 解析的窄入口，用 optional 表示“字段不存在或不合法”，
 * 避免异常穿过 HTTP 事件循环。
 *
 * 参数说明：
 * - object：要读取的 JSON 对象。
 * - name：字段名。
 *
 * 返回值：合法的 uint64_t，或 std::nullopt。
 */
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

/*
 * 函数名直译：从 Usage 对象读取字段片段。
 *
 * 通俗说：不同 Provider 的 Usage 可能分几帧到达，例如 Anthropic 先给 input_tokens，
 * 后给 output_tokens。这里先把当前帧能读出的部分保存下来，不要求一次同时出现全部字段。
 *
 * 专业说法：这是协议无关的增量 Usage 解码器；usage_quality 为 partial 时表示只能暂存字段，
 * 不能据此宣称精确费用。
 *
 * 参数说明：
 * - usage：Provider JSON 中的 usage 对象。
 *
 * 返回值：包含可识别 token 字段的 UsageAccounting；空对象表示没有合法字段。
 *
 * 注意：缓存 token 必须不大于 input token，否则整段 Usage 被视为不可信。
 */
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

/*
 * 函数名直译：完整解析 Usage。
 *
 * 通俗说：当 input 和 output token 都已经拿到时，除了记录 token，还尝试找出当前生效的
 * Model Price，计算整数 micro-USD 费用；没有价格时仍保留 token，但费用质量不是 exact。
 *
 * 专业说法：这是 Usage accounting 的协议层实现，使用 128 位中间值和向上取整，避免浮点误差。
 *
 * 参数说明：
 * - usage：Provider 返回的 Usage JSON。
 * - prices：当前候选对应的有效价格版本集合。
 *
 * 返回值：包含 token、价格版本和费用质量的 UsageAccounting。
 */
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

/*
 * 函数名直译：拆解 SSE 事件字段。
 *
 * 通俗说：一个 SSE 帧可能有 event 行、多个 data 行和注释行；这里把它们拼成事件名和
 * JSON 数据文本，让上层 Adapter 不必重复处理 LF/CRLF 和多行 data。
 *
 * 专业说法：这是 SSE wire frame 的通用字段解析器，遵循 data 字段按换行连接的规则，
 * 并忽略以冒号开头的注释帧。
 *
 * 参数说明：
 * - raw：已经由 SseDecoder 按空行切出的单个原始帧。
 * - event_name：输出 event 字段；没有时保持为空。
 * - data：输出合并后的 data 内容。
 *
 * 返回值：至少看到一个 data 字段时返回 true；注释帧或空帧返回 false。
 */
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

/*
 * 函数名直译：通用 SSE 事件分类。
 *
 * 通俗说：先完成 SSE 帧解析和 JSON 解析，再把“这是业务事件、成功结束、Provider error
 * 还是无效帧”的判断交给具体协议 Adapter。
 *
 * 专业说法：这是模板方法式的协议执行骨架，把协议无关的 framing 与协议相关的 event
 * classification 分离，供 Responses、Chat 和 Anthropic 共用。
 *
 * 参数说明：
 * - raw：单个原始 SSE 帧。
 * - prices：用于解析事件中 Usage 的价格快照。
 * - classifier：当前协议对已解析 JSON 的分类回调。
 *
 * 返回值：带事件类别、payload、序号和可选 Usage 的 ProtocolEvent。
 */
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
    /*
     * 函数名直译：构造 OpenAI Adapter。
     *
     * 通俗说：同一套逻辑同时服务 Responses 和 Chat Completions，构造时只记录协议名称和路径。
     *
     * 专业说法：这是两个 OpenAI 原生协议实例共享的策略实现；协议差异主要体现在校验字段、
     * SSE 终止事件和错误终止格式。
     */
    explicit OpenAiAdapter(std::string name, std::string path)
        : protocol_(std::move(name)), path_(std::move(path))
    {
    }

    /* 返回注册表中的稳定协议名，供鉴权快照和指标标签使用。 */
    const std::string &protocol() const override { return protocol_; }

    /*
     * 函数名直译：匹配 HTTP 请求。
     *
     * 通俗说：只有正确的 POST 方法和对应路径才会交给这个 Adapter，避免把未知接口误当成协议请求。
     *
     * 参数说明：method 为 HTTP 方法；path 为请求路径。
     * 返回值：同时匹配时返回 true。
     */
    bool matches(std::string_view method, std::string_view path) const override
    {
        return method == "POST" && path == path_;
    }

    /*
     * 函数名直译：校验 OpenAI 请求。
     *
     * 通俗说：Responses 和 Chat 都要求非空 model；Chat 还要求 messages 数组，stream 如果出现
     * 必须是布尔值。其余字段不在这里删改，会原样交给 Provider。
     *
     * 专业说法：这是协议边界校验与请求上下文提取，负责把 JSON 转为共享执行状态机需要的
     * protocol、logical model 和 stream 标志。
     *
     * 参数说明：
     * - payload：已解析的客户端 JSON。
     * - context：输出协议上下文。
     * - error：失败时输出 OpenAI 风格错误。
     *
     * 返回值：校验成功返回 true，否则返回 false。
     */
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

    /*
     * 函数名直译：生成 Provider 请求头。
     *
     * 通俗说：把 Gateway 自己的 Provider Bearer Key、Content-Type、Accept 和 request ID
     * 放进去，并只复制协议允许的受控元数据；客户端的 Gateway Authorization 不会被转发。
     *
     * 专业说法：这是上游认证与 Header allowlist seam，stream 决定接受 JSON 还是 SSE。
     */
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

    /*
     * 函数名直译：分类一个 OpenAI SSE 事件。
     *
     * 通俗说：Responses 看到 response.completed 或 Chat 看到 [DONE] 时认为成功结束；
     * type=error/event=error 进入失败路径，其余合法 JSON 作为业务事件原样转发。
     *
     * 专业说法：这是提交门所依赖的协议分类函数，同时提取事件序号和 Usage。
     */
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

    /*
     * 函数名直译：规范化非流式响应。
     *
     * 通俗说：Provider 返回合法 JSON 后，只把其中暴露给客户端的 model 改回逻辑模型名，
     * 其他字段保持原样；若 Usage 存在则交给 accounting 解析。
     *
     * 返回值：成功写入 client_body 并返回 true；顶层不是 JSON 对象时返回 false 和 502 错误。
     */
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

    /* 将内部 ProtocolError 编码成 OpenAI 的 {error:{...}} 对象。 */
    std::string error_body(const ProtocolError &error) const override
    {
        return openai_protocol_error_body(error);
    }

    /*
     * 函数名直译：生成提交后的流错误事件。
     *
     * 通俗说：流已经开始后不能再切换成 JSON，也不能重试第二个 Provider；这里只写一个
     * 当前协议可理解的终止错误，随后由共享状态机结束 HTTP 响应。
     *
     * 注意：Chat 使用 data-only error；Responses 使用官方 event: error 形状。
     */
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

    /* 从 Responses 的 prompt_cache_key 读取安全的会话亲和提示。 */
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
    /*
     * 函数名直译：获取协议名。
     *
     * 通俗说：返回数据库、Policy 和指标里统一使用的 anthropic_messages 名称。
     *
     * 专业说法：协议注册表通过这个稳定标识把 HTTP 路径、授权 grant 和 Endpoint 关联起来。
     */
    const std::string &protocol() const override { return protocol_; }

    /*
     * 函数名直译：匹配 Anthropic Messages 请求。
     *
     * 通俗说：只接受 POST /v1/messages，其他路径不会误进入 Anthropic 处理逻辑。
     */
    bool matches(std::string_view method, std::string_view path) const override
    {
        return method == "POST" && path == "/v1/messages";
    }

    /*
     * 函数名直译：校验 Anthropic Messages 请求。
     *
     * 通俗说：Anthropic 请求必须有 model、messages 数组和大于零的 max_tokens；stream
     * 如果提供，只能是 true 或 false。通过后保留其余原生字段，不转换成 OpenAI 格式。
     *
     * 专业说法：这是 Anthropic 原生协议的输入契约检查，并把公共执行状态机所需的 model/stream
     * 抽取到 ProtocolRequestContext。
     *
     * 注意：max_tokens 按 JSON 有符号和无符号整数分别读取，避免超出 int64_t 的合法正数发生窄化。
     */
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

    /*
     * 函数名直译：生成 Anthropic Provider 请求头。
     *
     * 通俗说：Anthropic 不使用 Authorization Bearer，而是使用 x-api-key；同时必须带固定的
     * anthropic-version。只有白名单中的客户端元数据会继续向上游传递。
     *
     * 专业说法：这是协议专有认证 Header 策略，防止客户端把 Gateway Key、代理认证或任意 Header
     * 透传给 Provider。
     */
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

    /*
     * 函数名直译：分类 Anthropic SSE 事件。
     *
     * 通俗说：message_start、content_block_*、message_delta 和 ping 都是可以转发的原生事件；
     * message_stop 表示成功结束，error 表示 Provider 在提交前或提交后失败。
     *
     * 专业说法：通用 SSE framing 完成后，这里按 Anthropic 的 event/type 语义映射到共享状态机。
     * 同时兼容把 input Usage 放在 message_start、把 output Usage 放在 message_delta 的常见形式。
     */
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

    /*
     * 函数名直译：规范化 Anthropic 非流式响应。
     *
     * 通俗说：验证上游返回对象后，只把 model 改回租户看到的逻辑模型名，其他 Anthropic 字段
     * 维持原状，并从 usage 中读取 token/费用信息。
     */
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

    /* 把内部错误编码为 Anthropic 原生的 {type:"error",error:{...}} 结构。 */
    std::string error_body(const ProtocolError &error) const override
    {
        Json body = {{"type", "error"},
                     {"error", {{"type", error.code}, {"message", error.message}}}};
        return body.dump();
    }

    /*
     * 函数名直译：生成 Anthropic 流终止错误。
     *
     * 通俗说：已经向客户端输出过 SSE 后，只能再输出 event: error，不能伪造 message_stop，
     * 否则调用方会误以为流正常完成。
     */
    std::string terminal_error(long,
                               std::string_view code,
                               std::string_view message) const override
    {
        Json body = {{"type", "error"},
                     {"error", {{"type", std::string(code)},
                                 {"message", std::string(message)}}}};
        return "event: error\ndata: " + body.dump() + "\n\n";
    }

    /* 从 metadata.user_id 读取可选亲和提示；没有或过长时不参与路由亲和。 */
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

/*
 * 函数名直译：构造协议错误对象。
 *
 * 通俗说：让调用点用统一方式描述状态码、错误类型、错误码、消息和可选字段名，具体 Adapter
 * 再决定怎样序列化给客户端。
 *
 * 专业说法：这是协议无关错误值对象的工厂函数，避免 HTTP 状态机直接拼接不同协议的 JSON。
 */
ProtocolError protocol_error(int status,
                             std::string type,
                             std::string code,
                             std::string message,
                             std::string param)
{
    return {status, std::move(type), std::move(code), std::move(message), std::move(param)};
}

/*
 * 函数名直译：按 HTTP 方法和路径查找协议 Adapter。
 *
 * 通俗说：入站请求到达后，从内置的三个 Adapter 里找出真正负责它的那个；没有匹配说明
 * 该路径不是 Gateway 支持的模型调用接口。
 *
 * 返回值：匹配的 Adapter 指针；未找到返回 nullptr。返回的是进程内静态对象，不需要调用方释放。
 */
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

/*
 * 函数名直译：按协议名称查找 Adapter。
 *
 * 通俗说：Admin 导入配置时只有 protocol 字符串，没有 HTTP 请求；此函数把字符串对应到
 * 已内置的协议实现。
 *
 * 返回值：已支持协议的 Adapter；未知名称返回 nullptr。
 */
const ProtocolAdapter *find_protocol_adapter_for_name(std::string_view protocol)
{
    if (protocol == "responses") return &responses_adapter();
    if (protocol == "chat_completions") return &chat_adapter();
    if (protocol == "anthropic_messages") return &anthropic_adapter();
    return nullptr;
}

/*
 * 函数名直译：判断协议是否受支持。
 *
 * 通俗说：给配置校验使用，避免把 Gemini 或拼错的协议名字写入数据库，直到有对应 Adapter 才允许启用。
 */
bool is_supported_protocol(std::string_view protocol)
{
    return find_protocol_adapter_for_name(protocol) != nullptr;
}
} // namespace ai_gateway
