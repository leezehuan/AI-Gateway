#include "gateway/repository.hpp"

#include "gateway/mysql.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace ai_gateway
{
namespace
{
constexpr const char *required_schema_version = "0003_phase5_governance_usage";

/* 预算不足是事务内部的控制流信号，最终会转换为 RequestAdmissionStatus，而不是泄漏为 HTTP 异常。 */
class BudgetExceeded final : public std::exception
{
};

std::uint64_t checked_multiply(std::uint64_t value, std::size_t count)
{
    if (count != 0 && value > std::numeric_limits<std::uint64_t>::max() / count)
    {
        throw DatabaseError("budget reservation exceeds supported range");
    }
    return value * static_cast<std::uint64_t>(count);
}

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

        /* 移动借用凭证的所有权，防止同一连接被归还两次。 */
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

std::optional<std::uint64_t> parse_optional_id(const std::string &value)
{
    return value.empty() ? std::optional<std::uint64_t>()
                         : std::optional<std::uint64_t>(parse_id(value));
}

std::optional<QuotaPolicy> load_quota(MySqlConnection &connection, const std::string &id)
{
    if (id.empty())
    {
        return std::nullopt;
    }
    const auto rows = connection.query_prepared(
        "SELECT id, status, rpm_limit, concurrency_limit, daily_budget_microusd, "
        "monthly_budget_microusd, reservation_per_attempt_microusd "
        "FROM quota_policies WHERE id=?", {id});
    if (rows.size() != 1 || rows.front().size() != 7)
    {
        throw DatabaseError("assigned Quota Policy is unavailable");
    }
    QuotaPolicy policy;
    policy.id = parse_id(rows.front()[0]);
    policy.status = rows.front()[1];
    policy.rpm = parse_optional_id(rows.front()[2]);
    policy.concurrency = parse_optional_id(rows.front()[3]);
    policy.daily_budget_microusd = parse_optional_id(rows.front()[4]);
    policy.monthly_budget_microusd = parse_optional_id(rows.front()[5]);
    policy.reservation_per_attempt_microusd = parse_id(rows.front()[6]);
    return policy;
}

void verify_schema(MySqlConnection &connection)
{
    const auto rows = connection.query_prepared(
        "SELECT version FROM schema_migrations ORDER BY version");
    if (rows.empty() || rows.back().empty() || rows.back().front() != required_schema_version)
    {
        throw DatabaseError("Gateway database schema version is incompatible");
    }
}
}

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
                "UNIX_TIMESTAMP(ak.expires_at), t.id, t.slug, t.status, p.id, p.status, "
                "t.quota_policy_id, ak.quota_policy_id "
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
                record.database_tenant_id = parse_id(row[5]);
                record.tenant_slug = row[6];
                record.tenant_status = row[7];
                const std::uint64_t policy_id = parse_id(row[8]);
                record.policy_status = row[9];
                record.tenant_quota = load_quota(*connection, row[10]);
                record.api_key_quota = load_quota(*connection, row[11]);

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
                         "SELECT lm.id, lm.protocol, lm.name, lm.status, pmg.enabled, "
                         "COALESCE(rp.scheduling_mode, 'fixed_order'), "
                         "COALESCE(rp.max_attempts, 3) "
                         "FROM policy_model_grants pmg JOIN logical_models lm "
                         "ON lm.id=pmg.logical_model_id "
                         "LEFT JOIN route_policies rp ON rp.logical_model_id=lm.id "
                         "WHERE pmg.policy_id=? ORDER BY lm.protocol, lm.name",
                         {std::to_string(policy_id)}))
                {
                    record.models.push_back({parse_id(model[0]), model[1], model[2], model[3],
                                             model[4] == "1", model[5],
                                             static_cast<std::size_t>(parse_id(model[6]))});
                }
                for (const auto &mapping : connection->query_prepared(
                         "SELECT mm.id, mm.logical_model_id, mm.name, p.id, pe.id, pc.id, "
                         "pe.provider_id, pc.provider_id, p.status, pe.protocol, pe.url, "
                         "pe.status, pc.secret_ref, pc.status, mm.upstream_model, mm.priority, "
                         "mm.status, p.slug, pc.name, pc.quota_policy_id "
                         "FROM model_mappings mm "
                         "JOIN logical_models lm ON lm.id=mm.logical_model_id "
                         "JOIN provider_endpoints pe ON pe.id=mm.provider_endpoint_id "
                         "JOIN provider_credentials pc ON pc.id=mm.provider_credential_id "
                         "JOIN providers p ON p.id=pe.provider_id "
                         "WHERE lm.tenant_id=(SELECT tenant_id FROM api_keys WHERE id=?) "
                         "AND p.tenant_id=lm.tenant_id ORDER BY mm.priority, mm.name",
                         {std::to_string(record.database_key_id)}))
                {
                    RepositoryMapping value;
                    value.mapping_id = parse_id(mapping[0]);
                    value.logical_model_id = parse_id(mapping[1]);
                    value.mapping_name = mapping[2];
                    value.provider_id = parse_id(mapping[3]);
                    value.endpoint_id = parse_id(mapping[4]);
                    value.credential_id = parse_id(mapping[5]);
                    value.endpoint_provider_id = parse_id(mapping[6]);
                    value.credential_provider_id = parse_id(mapping[7]);
                    value.provider_status = mapping[8];
                    value.endpoint_protocol = mapping[9];
                    value.endpoint_url = mapping[10];
                    value.endpoint_status = mapping[11];
                    value.secret_ref = mapping[12];
                    value.credential_status = mapping[13];
                    value.upstream_model = mapping[14];
                    value.priority = static_cast<std::uint16_t>(parse_id(mapping[15]));
                    value.mapping_status = mapping[16];
                    value.provider_slug = mapping[17];
                    value.credential_name = mapping[18];
                    value.credential_quota = load_quota(*connection, mapping[19]);
                    record.mappings.push_back(std::move(value));
                }
                for (const auto &price : connection->query_prepared(
                         "SELECT mp.id, mp.provider_id, mp.upstream_model, mp.version, "
                         "UNIX_TIMESTAMP(mp.effective_at), mp.input_per_million_microusd, "
                         "mp.cached_input_per_million_microusd, "
                         "mp.output_per_million_microusd FROM model_prices mp "
                         "JOIN providers p ON p.id=mp.provider_id "
                         "WHERE p.tenant_id=? AND mp.status='active' "
                         "ORDER BY mp.effective_at, mp.id",
                         {std::to_string(record.database_tenant_id)}))
                {
                    record.prices.push_back(
                        {parse_id(price[0]), parse_id(price[1]), price[2], price[3],
                         static_cast<std::int64_t>(std::stoll(price[4])), parse_id(price[5]),
                         parse_id(price[6]), parse_id(price[7])});
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



    RequestAdmissionStatus admit_request(const RequestAdmission &request)
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        connection->begin();
        try
        {
            connection->execute_prepared(
                "INSERT INTO usage_records(request_id, tenant_id, api_key_id, logical_model_id, "
                "protocol, stream, request_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                {request.request_id, std::to_string(request.tenant_id),
                 std::to_string(request.api_key_id), std::to_string(request.logical_model_id),
                 request.protocol, request.stream ? "1" : "0",
                 std::to_string(request.request_bytes)});
            const std::uint64_t usage_id = connection->last_insert_id();
            const auto periods = connection->query_prepared(
                "SELECT DATE_FORMAT(UTC_DATE(), '%Y-%m-%d'), "
                "DATE_FORMAT(UTC_DATE(), '%Y-%m-01')");
            if (periods.size() != 1 || periods.front().size() != 2)
            {
                throw DatabaseError("UTC budget periods are unavailable");
            }
            reserve_scope(*connection, usage_id, "tenant", request.tenant_id,
                          request.tenant_quota, request.max_attempts,
                          periods.front()[0], periods.front()[1]);
            reserve_scope(*connection, usage_id, "api_key", request.api_key_id,
                          request.api_key_quota, request.max_attempts,
                          periods.front()[0], periods.front()[1]);
            connection->commit();
            return RequestAdmissionStatus::admitted;
        }
        catch (const BudgetExceeded &)
        {
            connection->rollback();
            return RequestAdmissionStatus::budget_exceeded;
        }
        catch (...)
        {
            connection->rollback();
            throw;
        }
    }



    void finish_request(const RequestFinish &request)
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        connection->begin();
        try
        {
            const auto usage = connection->query_prepared(
                "SELECT id, state FROM usage_records WHERE request_id=? FOR UPDATE",
                {request.request_id});
            if (usage.size() != 1 || usage.front().size() != 2)
            {
                throw DatabaseError("Usage Record is unavailable");
            }
            if (usage.front()[1] != "started")
            {
                connection->commit();
                return;
            }

            const std::uint64_t usage_id = parse_id(usage.front()[0]);
            const auto reservations = connection->query_prepared(
                "SELECT id, scope_type, scope_id, period_kind, "
                "DATE_FORMAT(period_start, '%Y-%m-%d'), reserved_microusd "
                "FROM budget_reservations WHERE usage_record_id=? AND state='reserved' "
                "FOR UPDATE",
                {std::to_string(usage_id)});
            std::uint64_t usage_cost = 0;
            for (const auto &reservation : reservations)
            {
                const std::uint64_t reserved = parse_id(reservation[5]);
                const std::uint64_t per_attempt = request.max_attempts == 0
                                                      ? 0
                                                      : reserved / request.max_attempts;
                const std::size_t billable = std::min(request.billable_attempt_count,
                                                      request.max_attempts);
                const std::uint64_t settled =
                    request.usage.cost_quality == "exact" && request.usage.cost_microusd
                        ? *request.usage.cost_microusd
                        : checked_multiply(per_attempt, billable);
                usage_cost = std::max(usage_cost, settled);
                const auto updated = connection->execute_prepared(
                    "UPDATE budget_periods SET reserved_microusd=reserved_microusd-?, "
                    "settled_microusd=settled_microusd+? WHERE scope_type=? AND scope_id=? "
                    "AND period_kind=? AND period_start=? AND reserved_microusd>=?",
                    {std::to_string(reserved), std::to_string(settled), reservation[1],
                     reservation[2], reservation[3], reservation[4],
                     std::to_string(reserved)});
                if (updated != 1)
                {
                    throw DatabaseError("Budget Reservation cannot be settled");
                }
                connection->execute_prepared(
                    "UPDATE budget_reservations SET settled_microusd=?, state='settled' "
                    "WHERE id=? AND state='reserved'",
                    {std::to_string(settled), reservation[0]});
            }

            std::string cost;
            std::string cost_quality = "unknown";
            if (request.usage.cost_quality == "exact" && request.usage.cost_microusd)
            {
                cost = std::to_string(*request.usage.cost_microusd);
                cost_quality = "exact";
            }
            else if (request.billable_attempt_count == 0)
            {
                cost = "0";
                cost_quality = "not_billable";
            }
            else if (!reservations.empty())
            {
                cost = std::to_string(usage_cost);
                cost_quality = "estimated";
            }
            connection->execute_prepared(
                "UPDATE usage_records SET state=?, final_mapping_id=NULLIF(?, ''), "
                "final_provider_id=NULLIF(?, ''), final_endpoint_id=NULLIF(?, ''), "
                "final_credential_id=NULLIF(?, ''), attempt_count=?, failover_count=?, "
                "http_status=NULLIF(?, ''), error_class=NULLIF(?, ''), response_bytes=?, "
                "duration_ms=?, input_tokens=NULLIF(?, ''), "
                "cached_input_tokens=NULLIF(?, ''), output_tokens=NULLIF(?, ''), "
                "usage_quality=?, cost_microusd=NULLIF(?, ''), cost_quality=?, "
                "completed_at=UTC_TIMESTAMP(6) WHERE id=? AND state='started'",
                {request.state,
                 request.final_mapping_id == 0 ? std::string()
                                               : std::to_string(request.final_mapping_id),
                 request.final_provider_id == 0 ? std::string()
                                                : std::to_string(request.final_provider_id),
                 request.final_endpoint_id == 0 ? std::string()
                                                : std::to_string(request.final_endpoint_id),
                 request.final_credential_id == 0 ? std::string()
                                                  : std::to_string(request.final_credential_id),
                 std::to_string(request.attempt_count),
                 std::to_string(request.attempt_count > 0 ? request.attempt_count - 1 : 0),
                 request.http_status == 0 ? std::string() : std::to_string(request.http_status),
                 request.error_class, std::to_string(request.response_bytes),
                 std::to_string(request.duration_ms),
                 request.usage.input_tokens
                     ? std::to_string(*request.usage.input_tokens) : std::string(),
                 request.usage.cached_input_tokens
                     ? std::to_string(*request.usage.cached_input_tokens) : std::string(),
                 request.usage.output_tokens
                     ? std::to_string(*request.usage.output_tokens) : std::string(),
                 request.usage.usage_quality, cost, cost_quality,
                 std::to_string(usage_id)});
            connection->commit();
        }
        catch (...)
        {
            connection->rollback();
            throw;
        }
    }



    std::vector<RepositoryHealthCheck> load_health_checks()
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        const auto versions = connection->query_prepared(
            "SELECT version FROM gateway_config_versions WHERE singleton_id=1");
        if (versions.size() != 1 || versions.front().empty())
        {
            throw DatabaseError("Gateway configuration version is unavailable");
        }
        const std::uint64_t version = parse_id(versions.front()[0]);
        std::vector<RepositoryHealthCheck> checks;
        for (const auto &row : connection->query_prepared(
                 "SELECT hc.id, p.tenant_id, p.id, pe.id, pc.id, p.slug, pc.name, "
                 "hc.method, hc.url, hc.interval_ms, hc.timeout_ms, pc.secret_ref, "
                 "pc.quota_policy_id FROM provider_health_checks hc "
                 "JOIN providers p ON p.id=hc.provider_id "
                 "JOIN provider_endpoints pe ON pe.id=hc.endpoint_id AND pe.provider_id=p.id "
                 "JOIN provider_credentials pc ON pc.id=hc.credential_id AND pc.provider_id=p.id "
                 "WHERE hc.status='active' AND p.status='active' AND pe.status='active' "
                 "AND pc.status='active' ORDER BY hc.id"))
        {
            RepositoryHealthCheck check;
            check.id = parse_id(row[0]);
            check.config_version = version;
            check.tenant_id = parse_id(row[1]);
            check.provider_id = parse_id(row[2]);
            check.endpoint_id = parse_id(row[3]);
            check.credential_id = parse_id(row[4]);
            check.provider_slug = row[5];
            check.credential_name = row[6];
            check.method = row[7];
            check.url = row[8];
            check.interval_ms = static_cast<long>(parse_id(row[9]));
            check.timeout_ms = static_cast<long>(parse_id(row[10]));
            check.secret_ref = row[11];
            check.credential_quota = load_quota(*connection, row[12]);
            for (const auto &mapping : connection->query_prepared(
                     "SELECT mm.id, mm.logical_model_id, mm.name, mm.upstream_model "
                     "FROM model_mappings mm JOIN logical_models lm "
                     "ON lm.id=mm.logical_model_id WHERE mm.provider_endpoint_id=? "
                     "AND mm.provider_credential_id=? AND mm.status='active' "
                     "AND lm.status='active' ORDER BY mm.id",
                     {std::to_string(check.endpoint_id),
                      std::to_string(check.credential_id)}))
            {
                RepositoryMapping value;
                value.mapping_id = parse_id(mapping[0]);
                value.logical_model_id = parse_id(mapping[1]);
                value.mapping_name = mapping[2];
                value.provider_id = check.provider_id;
                value.endpoint_id = check.endpoint_id;
                value.credential_id = check.credential_id;
                value.upstream_model = mapping[3];
                check.mappings.push_back(std::move(value));
            }
            checks.push_back(std::move(check));
        }
        return checks;
    }



    void reconcile_abandoned_requests()
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        connection->begin();
        try
        {
            const auto expired = connection->query_prepared(
                "SELECT br.id, br.usage_record_id, br.scope_type, br.scope_id, "
                "br.period_kind, DATE_FORMAT(br.period_start, '%Y-%m-%d'), "
                "br.reserved_microusd FROM budget_reservations br "
                "JOIN usage_records ur ON ur.id=br.usage_record_id "
                "WHERE br.state='reserved' AND br.lease_expires_at<UTC_TIMESTAMP(6) "
                "AND ur.state='started' FOR UPDATE");
            std::vector<std::string> usage_ids;
            for (const auto &reservation : expired)
            {
                const auto released = connection->execute_prepared(
                    "UPDATE budget_reservations SET state='released', settled_microusd=0 "
                    "WHERE id=? AND state='reserved'",
                    {reservation[0]});
                if (released != 1)
                {
                    continue;
                }
                const auto period = connection->execute_prepared(
                    "UPDATE budget_periods SET reserved_microusd=reserved_microusd-? "
                    "WHERE scope_type=? AND scope_id=? AND period_kind=? AND period_start=? "
                    "AND reserved_microusd>=?",
                    {reservation[6], reservation[2], reservation[3], reservation[4],
                     reservation[5], reservation[6]});
                if (period != 1)
                {
                    throw DatabaseError("expired Budget Reservation cannot be released");
                }
                usage_ids.push_back(reservation[1]);
            }
            std::sort(usage_ids.begin(), usage_ids.end());
            usage_ids.erase(std::unique(usage_ids.begin(), usage_ids.end()), usage_ids.end());
            for (const auto &usage_id : usage_ids)
            {
                connection->execute_prepared(
                    "UPDATE usage_records ur SET ur.state='abandoned', "
                    "ur.error_class='governance_abandoned', ur.completed_at=UTC_TIMESTAMP(6) "
                    "WHERE ur.id=? AND ur.state='started' AND NOT EXISTS "
                    "(SELECT 1 FROM budget_reservations br WHERE br.usage_record_id=ur.id "
                    "AND br.state='reserved')",
                    {usage_id});
            }
            connection->commit();
        }
        catch (...)
        {
            connection->rollback();
            throw;
        }
    }



    void begin_attempt(const AttemptStart &attempt)
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        connection->execute_prepared(
            "INSERT INTO request_attempts(attempt_id, request_id, attempt_number, tenant_id, "
            "api_key_id, logical_model_id, mapping_id, provider_id, endpoint_id, credential_id, "
            "stream) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            {attempt.attempt_id, attempt.request_id, std::to_string(attempt.attempt_number),
             std::to_string(attempt.tenant_id), std::to_string(attempt.api_key_id),
             std::to_string(attempt.logical_model_id), std::to_string(attempt.mapping_id),
             std::to_string(attempt.provider_id), std::to_string(attempt.endpoint_id),
             std::to_string(attempt.credential_id), attempt.stream ? "1" : "0"});
    }



    void finish_attempt(const AttemptFinish &attempt)
    {
        auto connection = pool_.acquire();
        verify_schema(*connection);
        connection->execute_prepared(
            "UPDATE request_attempts SET state=?, provider_status=NULLIF(?, ''), "
            "provider_request_id=NULLIF(?, ''), error_class=NULLIF(?, ''), retryable=?, "
            "possible_duplicate_cost=?, response_bytes=?, duration_ms=?, "
            "first_byte_ms=NULLIF(?, ''), input_tokens=NULLIF(?, ''), "
            "cached_input_tokens=NULLIF(?, ''), output_tokens=NULLIF(?, ''), "
            "usage_quality=?, model_price_id=NULLIF(?, ''), "
            "cost_microusd=NULLIF(?, ''), cost_quality=?, completed_at=UTC_TIMESTAMP(6) "
            "WHERE attempt_id=? AND completed_at IS NULL",
            {attempt.state,
             attempt.provider_status == 0 ? std::string() : std::to_string(attempt.provider_status),
             attempt.provider_request_id, attempt.error_class, attempt.retryable ? "1" : "0",
             attempt.possible_duplicate_cost ? "1" : "0",
             std::to_string(attempt.response_bytes), std::to_string(attempt.duration_ms),
             attempt.first_byte_ms ? std::to_string(*attempt.first_byte_ms) : std::string(),
             attempt.usage.input_tokens ? std::to_string(*attempt.usage.input_tokens)
                                        : std::string(),
             attempt.usage.cached_input_tokens
                 ? std::to_string(*attempt.usage.cached_input_tokens) : std::string(),
             attempt.usage.output_tokens ? std::to_string(*attempt.usage.output_tokens)
                                         : std::string(),
             attempt.usage.usage_quality,
             attempt.usage.model_price_id == 0
                 ? std::string() : std::to_string(attempt.usage.model_price_id),
             attempt.usage.cost_microusd
                 ? std::to_string(*attempt.usage.cost_microusd) : std::string(),
             attempt.usage.cost_quality,
             attempt.attempt_id});
    }

private:


    static void reserve_scope(MySqlConnection &connection,
                              std::uint64_t usage_id,
                              const std::string &scope_type,
                              std::uint64_t scope_id,
                              const std::optional<QuotaPolicy> &quota,
                              std::size_t max_attempts,
                              const std::string &day,
                              const std::string &month)
    {
        if (!quota)
        {
            return;
        }
        const std::uint64_t reservation = checked_multiply(
            quota->reservation_per_attempt_microusd, max_attempts);
        const auto reserve_period = [&](const char *kind,
                                        const std::string &period,
                                        const std::optional<std::uint64_t> &limit) {
            if (!limit)
            {
                return;
            }
            connection.execute_prepared(
                "INSERT INTO budget_periods(scope_type, scope_id, period_kind, period_start, "
                "limit_microusd) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE "
                "limit_microusd=VALUES(limit_microusd)",
                {scope_type, std::to_string(scope_id), kind, period,
                 std::to_string(*limit)});
            const auto updated = connection.execute_prepared(
                "UPDATE budget_periods SET reserved_microusd=reserved_microusd+? "
                "WHERE scope_type=? AND scope_id=? AND period_kind=? AND period_start=? "
                "AND settled_microusd+reserved_microusd+?<=limit_microusd",
                {std::to_string(reservation), scope_type, std::to_string(scope_id), kind,
                 period, std::to_string(reservation)});
            if (updated != 1)
            {
                throw BudgetExceeded();
            }
            connection.execute_prepared(
                "INSERT INTO budget_reservations(usage_record_id, scope_type, scope_id, "
                "period_kind, period_start, reserved_microusd, lease_expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 20 MINUTE))",
                {std::to_string(usage_id), scope_type, std::to_string(scope_id), kind,
                 period, std::to_string(reservation)});
        };
        reserve_period("day", day, quota->daily_budget_microusd);
        reserve_period("month", month, quota->monthly_budget_microusd);
    }

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

RequestAdmissionStatus MySqlGatewayRepository::admit_request(const RequestAdmission &request)
{
    return impl_->admit_request(request);
}

void MySqlGatewayRepository::finish_request(const RequestFinish &request)
{
    impl_->finish_request(request);
}

std::vector<RepositoryHealthCheck> MySqlGatewayRepository::load_health_checks()
{
    return impl_->load_health_checks();
}

void MySqlGatewayRepository::reconcile_abandoned_requests()
{
    impl_->reconcile_abandoned_requests();
}

void MySqlGatewayRepository::begin_attempt(const AttemptStart &attempt)
{
    impl_->begin_attempt(attempt);
}

void MySqlGatewayRepository::finish_attempt(const AttemptFinish &attempt)
{
    impl_->finish_attempt(attempt);
}
}
