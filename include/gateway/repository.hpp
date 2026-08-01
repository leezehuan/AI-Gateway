#ifndef AI_GATEWAY_REPOSITORY_HPP
#define AI_GATEWAY_REPOSITORY_HPP

#include "gateway/gateway.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace ai_gateway
{
struct RepositoryModel
{
    std::uint64_t id = 0;
    std::string protocol;
    std::string name;
    std::string status;
    bool granted = false;
};

struct RepositoryMapping
{
    std::uint64_t logical_model_id = 0;
    std::uint64_t provider_id = 0;
    std::uint64_t endpoint_provider_id = 0;
    std::uint64_t credential_provider_id = 0;
    std::string provider_status;
    std::string endpoint_protocol;
    std::string endpoint_url;
    std::string endpoint_status;
    std::string secret_ref;
    std::string credential_status;
    std::string upstream_model;
    std::string mapping_status;
};

struct RepositoryAccessRecord
{
    std::uint64_t config_version = 0;
    std::uint64_t database_key_id = 0;
    std::string public_key_id;
    std::string key_hmac;
    std::string key_status;
    std::int64_t expires_at_epoch = 0;
    bool has_expiry = false;
    std::string tenant_slug;
    std::string tenant_status;
    std::string policy_status;
    std::unordered_set<std::string> protocols;
    std::unordered_set<std::uint64_t> providers;
    std::vector<RepositoryModel> models;
    std::vector<RepositoryMapping> mappings;
};

class GatewayRepository
{
public:
    virtual ~GatewayRepository() = default;
    virtual std::uint64_t config_version() = 0;
    virtual std::vector<RepositoryAccessRecord> load_access_candidates(
        const std::string &display_prefix) = 0;
};

class MySqlGatewayRepository final : public GatewayRepository
{
public:
    explicit MySqlGatewayRepository(const GatewayConfig &config);
    ~MySqlGatewayRepository() override;

    std::uint64_t config_version() override;
    std::vector<RepositoryAccessRecord> load_access_candidates(
        const std::string &display_prefix) override;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
