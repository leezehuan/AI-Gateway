#include "gateway/repository.hpp"

#include "gateway/mysql.hpp"

#include <condition_variable>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace ai_gateway
{
namespace
{
constexpr const char *required_schema_version = "0001_phase3_identity_policy";

class ConnectionPool
{
public:
    ConnectionPool(DatabaseConfig config, std::size_t size)
    {
        connections_.reserve(size);
        for (std::size_t index = 0; index < size; ++index)
        {
            connections_.push_back(std::make_unique<MySqlConnection>(config));
            available_.push_back(connections_.back().get());
        }
    }

    class Lease
    {
    public:
        Lease(ConnectionPool &pool, MySqlConnection *connection)
            : pool_(&pool), connection_(connection)
        {
        }

        Lease(Lease &&other) noexcept
            : pool_(other.pool_), connection_(other.connection_)
        {
            other.pool_ = nullptr;
            other.connection_ = nullptr;
        }

        ~Lease()
        {
            if (pool_ != nullptr)
            {
                pool_->release(connection_);
            }
        }

        MySqlConnection &operator*() const { return *connection_; }
        MySqlConnection *operator->() const { return connection_; }

    private:
        ConnectionPool *pool_;
        MySqlConnection *connection_;
    };

    Lease acquire()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return !available_.empty(); });
        MySqlConnection *connection = available_.front();
        available_.pop_front();
        return Lease(*this, connection);
    }

private:
    void release(MySqlConnection *connection)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            available_.push_back(connection);
        }
        condition_.notify_one();
    }

    std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<std::unique_ptr<MySqlConnection>> connections_;
    std::deque<MySqlConnection *> available_;
};

std::uint64_t parse_id(const std::string &value)
{
    return static_cast<std::uint64_t>(std::stoull(value));
}

void verify_schema(MySqlConnection &connection)
{
    const auto rows = connection.query_prepared(
        "SELECT version FROM schema_migrations ORDER BY version");
    if (rows.size() != 1 || rows.front().empty() ||
        rows.front().front() != required_schema_version)
    {
        throw DatabaseError("Gateway database schema version is incompatible");
    }
}
} // namespace

class MySqlGatewayRepository::Impl
{
public:
    explicit Impl(const GatewayConfig &config)
        : pool_(config.database, config.database_pool_size)
    {
    }

    std::uint64_t config_version()
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        const auto rows = connection->query_prepared(
            "SELECT version FROM gateway_config_versions WHERE singleton_id = 1");
        if (rows.size() != 1 || rows.front().empty())
        {
            throw DatabaseError("Gateway configuration version is unavailable");
        }
        return parse_id(rows.front().front());
    }

    std::vector<RepositoryAccessRecord> load(const std::string &display_prefix)
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        connection->execute_static("START TRANSACTION WITH CONSISTENT SNAPSHOT");
        try
        {
            const auto version_rows = connection->query_prepared(
                "SELECT version FROM gateway_config_versions WHERE singleton_id = 1");
            if (version_rows.size() != 1)
            {
                throw DatabaseError("Gateway configuration version is unavailable");
            }
            const std::uint64_t version = parse_id(version_rows.front().front());
            const auto key_rows = connection->query_prepared(
                "SELECT ak.id, ak.key_id, ak.key_hmac, ak.status, "
                "UNIX_TIMESTAMP(ak.expires_at), t.slug, t.status, p.id, p.status "
                "FROM api_keys ak JOIN tenants t ON t.id=ak.tenant_id "
                "JOIN access_policies p ON p.id=ak.policy_id AND p.tenant_id=t.id "
                "WHERE ak.display_prefix=?",
                {display_prefix});

            std::vector<RepositoryAccessRecord> records;
            records.reserve(key_rows.size());
            for (const auto &row : key_rows)
            {
                RepositoryAccessRecord record;
                record.config_version = version;
                record.database_key_id = parse_id(row[0]);
                record.public_key_id = row[1];
                record.key_hmac = row[2];
                record.key_status = row[3];
                record.has_expiry = !row[4].empty();
                record.expires_at_epoch = record.has_expiry ? std::stoll(row[4]) : 0;
                record.tenant_slug = row[5];
                record.tenant_status = row[6];
                const std::uint64_t policy_id = parse_id(row[7]);
                record.policy_status = row[8];

                for (const auto &protocol : connection->query_prepared(
                         "SELECT protocol FROM policy_protocol_grants "
                         "WHERE policy_id=? AND enabled=1",
                         {std::to_string(policy_id)}))
                {
                    record.protocols.insert(protocol[0]);
                }
                for (const auto &provider : connection->query_prepared(
                         "SELECT provider_id FROM policy_provider_grants "
                         "WHERE policy_id=? AND enabled=1",
                         {std::to_string(policy_id)}))
                {
                    record.providers.insert(parse_id(provider[0]));
                }
                for (const auto &model : connection->query_prepared(
                         "SELECT lm.id, lm.protocol, lm.name, lm.status, pmg.enabled "
                         "FROM policy_model_grants pmg JOIN logical_models lm "
                         "ON lm.id=pmg.logical_model_id "
                         "WHERE pmg.policy_id=? ORDER BY lm.protocol, lm.name",
                         {std::to_string(policy_id)}))
                {
                    record.models.push_back({parse_id(model[0]), model[1], model[2], model[3],
                                             model[4] == "1"});
                }
                for (const auto &mapping : connection->query_prepared(
                         "SELECT mm.logical_model_id, p.id, pe.provider_id, pc.provider_id, "
                         "p.status, pe.protocol, pe.url, pe.status, pc.secret_ref, pc.status, "
                         "mm.upstream_model, mm.status "
                         "FROM model_mappings mm "
                         "JOIN logical_models lm ON lm.id=mm.logical_model_id "
                         "JOIN provider_endpoints pe ON pe.id=mm.provider_endpoint_id "
                         "JOIN provider_credentials pc ON pc.id=mm.provider_credential_id "
                         "JOIN providers p ON p.id=pe.provider_id "
                         "WHERE lm.tenant_id=(SELECT tenant_id FROM api_keys WHERE id=?) "
                         "AND p.tenant_id=lm.tenant_id",
                         {std::to_string(record.database_key_id)}))
                {
                    record.mappings.push_back(
                        {parse_id(mapping[0]), parse_id(mapping[1]), parse_id(mapping[2]),
                         parse_id(mapping[3]), mapping[4], mapping[5], mapping[6], mapping[7],
                         mapping[8], mapping[9], mapping[10], mapping[11]});
                }
                records.push_back(std::move(record));
            }
            connection->commit();
            return records;
        }
        catch (...)
        {
            connection->rollback();
            throw;
        }
    }

private:
    ConnectionPool pool_;
};

MySqlGatewayRepository::MySqlGatewayRepository(const GatewayConfig &config)
    : impl_(std::make_unique<Impl>(config))
{
}

MySqlGatewayRepository::~MySqlGatewayRepository() = default;

std::uint64_t MySqlGatewayRepository::config_version()
{
    return impl_->config_version();
}

std::vector<RepositoryAccessRecord> MySqlGatewayRepository::load_access_candidates(
    const std::string &display_prefix)
{
    return impl_->load(display_prefix);
}
} // namespace ai_gateway
