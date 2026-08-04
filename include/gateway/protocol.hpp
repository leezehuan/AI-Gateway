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
    std::string protocol;
    std::string model;
    bool stream = false;
};

struct ProtocolError
{
    int status = 400;
    std::string type = "invalid_request_error";
    std::string code = "invalid_request";
    std::string message;
    std::string param;
};

enum class ProtocolEventKind
{
    comment,
    business,
    success_terminal,
    provider_error,
    invalid
};

struct ProtocolEvent
{
    ProtocolEventKind kind = ProtocolEventKind::invalid;
    long sequence_number = -1;
    Json payload;
    UsageAccounting usage;
};

class ProtocolAdapter
{
public:
    virtual ~ProtocolAdapter() = default;

    virtual const std::string &protocol() const = 0;
    virtual bool matches(std::string_view method, std::string_view path) const = 0;
    virtual bool validate_request(const Json &payload,
                                  ProtocolRequestContext &context,
                                  ProtocolError &error) const = 0;
    virtual HeaderMap provider_headers(const GatewayRequest &request,
                                       const ModelTarget &target,
                                       bool stream) const = 0;
    virtual ProtocolEvent classify_event(std::string_view raw,
                                          const std::vector<ModelPrice> &prices) const = 0;
    virtual bool normalize_response(const Json &provider_response,
                                    std::string_view logical_model,
                                    const std::vector<ModelPrice> &prices,
                                    std::string &client_body,
                                    UsageAccounting &usage,
                                    ProtocolError &error) const = 0;
    virtual std::string error_body(const ProtocolError &error) const = 0;
    virtual std::string terminal_error(long sequence_number,
                                       std::string_view code,
                                       std::string_view message) const = 0;
    virtual std::string session_hint(const Json &payload) const = 0;
};

const ProtocolAdapter *find_protocol_adapter(std::string_view method,
                                             std::string_view path);
const ProtocolAdapter *find_protocol_adapter_for_name(std::string_view protocol);
bool is_supported_protocol(std::string_view protocol);

ProtocolError protocol_error(int status,
                             std::string type,
                             std::string code,
                             std::string message,
                             std::string param = {});

} // namespace ai_gateway

#endif
