#ifndef AI_GATEWAY_PROTOCOL_HPP
#define AI_GATEWAY_PROTOCOL_HPP

#include "gateway/repository.hpp"

#include "json.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ai_gateway
{
using Json = nlohmann::json;
struct ModelPrice;
struct ModelTarget;

struct ProtocolRequestContext
{
    /* Adapter 从原始 JSON 提取的协议、逻辑模型和 stream 标志。 */
    std::string protocol;
    std::string model;
    bool stream = false;
};

struct ProtocolError
{
    /* 协议原生错误在提交响应前使用；不同 Adapter 可覆写 type/code/body 形状。 */
    int status = 400;
    std::string type = "invalid_request_error";
    std::string code = "invalid_request";
    std::string message;
    std::string param;
};

enum class ProtocolEventKind
{
    /* SSE 注释帧，不构成业务事件。 */
    comment,
    /* 可安全交给客户端的业务事件。 */
    business,
    /* 观察到协议规定的成功终止事件，例如 response.completed 或 message_stop。 */
    success_terminal,
    /* Provider 明确发送的协议错误事件。 */
    provider_error,
    /* 帧语法、JSON 或事件类型无法按当前协议解释。 */
    invalid
};

struct ProtocolEvent
{
    /* 通用 SSE 解码结果，执行状态机据 kind 决定提交、转发、failover 或 terminal error。 */
    ProtocolEventKind kind = ProtocolEventKind::invalid;
    long sequence_number = -1;
    Json payload;
    UsageAccounting usage;
};

class ProtocolAdapter
{
public:
    virtual ~ProtocolAdapter() = default;

    /* 协议注册名，对应 logical_models.protocol 和 provider_endpoints.protocol。 */
    virtual const std::string &protocol() const = 0;
    /* 判断 HTTP method/path 是否属于本 Adapter。 */
    virtual bool matches(std::string_view method, std::string_view path) const = 0;
    /* 校验协议必需字段，并提取后续状态机使用的上下文。 */
    virtual bool validate_request(const Json &payload,
                                  ProtocolRequestContext &context,
                                  ProtocolError &error) const = 0;
    /* 构造 allowlist 后的 Provider 认证与协议 Header；不复制客户端敏感 Header。 */
    virtual HeaderMap provider_headers(const GatewayRequest &request,
                                       const ModelTarget &target,
                                       bool stream) const = 0;
    /* 将一个完整 SSE 帧分类，并提取可用 Usage/sequence 信息。 */
    virtual ProtocolEvent classify_event(std::string_view raw,
                                          const std::vector<ModelPrice> &prices) const = 0;
    /* 校验非流式响应、补充逻辑模型字段并提取 Usage。 */
    virtual bool normalize_response(const Json &provider_response,
                                    std::string_view logical_model,
                                    const std::vector<ModelPrice> &prices,
                                    std::string &client_body,
                                    UsageAccounting &usage,
                                    ProtocolError &error) const = 0;
    /* 生成该协议提交前使用的错误响应正文。 */
    virtual std::string error_body(const ProtocolError &error) const = 0;
    /* 生成提交后的协议原生 terminal error SSE。 */
    virtual std::string terminal_error(long sequence_number,
                                       std::string_view code,
                                       std::string_view message) const = 0;
    /* 从受控请求元数据提取 cache affinity 提示，不读取完整 Prompt。 */
    virtual std::string session_hint(const Json &payload) const = 0;
};

/* 按 HTTP method/path 查找内置 Adapter。 */
const ProtocolAdapter *find_protocol_adapter(std::string_view method,
                                             std::string_view path);
/* 按数据库中的协议名称查找 Adapter。 */
const ProtocolAdapter *find_protocol_adapter_for_name(std::string_view protocol);
/* 判断协议是否属于当前 Phase 支持集合。 */
bool is_supported_protocol(std::string_view protocol);

/* 创建统一协议错误值，供 Adapter 生成原生错误 JSON。 */
ProtocolError protocol_error(int status,
                             std::string type,
                             std::string code,
                             std::string message,
                             std::string param = {});

} // namespace ai_gateway

#endif
