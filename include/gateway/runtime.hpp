#ifndef AI_GATEWAY_RUNTIME_HPP
#define AI_GATEWAY_RUNTIME_HPP

#include "gateway/gateway.hpp"
#include "gateway/repository.hpp"

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ai_gateway
{
struct ModelTarget
{
    std::string provider_url;
    std::string provider_api_key;
    std::string upstream_model;
};

struct ModelAccess
{
    std::string name;
    std::string protocol;
    bool model_granted = false;
    bool provider_denied = false;
    bool configuration_unavailable = false;
    std::size_t active_mapping_count = 0;
    std::vector<ModelTarget> candidates;
};

struct AuthSnapshot
{
    std::uint64_t config_version = 0;
    std::string tenant_slug;
    std::string public_api_key_id;
    std::unordered_set<std::string> protocols;
    std::unordered_map<std::string, ModelAccess> models;
};

enum class AuthStatus
{
    authorized,
    invalid_api_key,
    access_disabled,
    unavailable
};

struct AuthResult
{
    AuthStatus status = AuthStatus::unavailable;
    std::shared_ptr<const AuthSnapshot> snapshot;
};

class RuntimeState
{
public:
    using AuthCallback = std::function<void(AuthResult)>;

    RuntimeState(GatewayConfig config, GatewayRepository &repository);
    ~RuntimeState();

    RuntimeState(const RuntimeState &) = delete;
    RuntimeState &operator=(const RuntimeState &) = delete;

    void authenticate(std::string api_key, AuthCallback callback);
    bool ready() const;
    const GatewayConfig &config() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
