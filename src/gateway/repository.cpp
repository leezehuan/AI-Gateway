#include "gateway/repository.hpp"

#include "gateway/mysql.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

/*
 * MySQL GatewayRepository 实现。
 *
 * Repository 的职责是把“一个用例需要的一致性数据”组织成 prepared-statement 查询或事务，
 * 而不是暴露逐表 CRUD 给 HTTP 层。RuntimeState 调用它来加载认证快照、预留和结算预算、
 * 写入 request_attempts，以及清理崩溃遗留的 started Usage。ConnectionPool 只控制连接复用，
 * 调用 Repository 的线程池由 RuntimeState 控制，两者共同保证 Drogon 事件循环不阻塞 SQL。
 */
namespace ai_gateway
{
namespace
{
constexpr const char *required_schema_version = "0003_phase5_governance_usage";

/* 预算不足是事务内部的控制流信号，最终会转换为 RequestAdmissionStatus，而不是泄漏为 HTTP 异常。 */
class BudgetExceeded final : public std::exception
{
};

/*
 * 函数名直译：检查乘法。
 *
 * 通俗说：预算按“每次尝试金额 × 最大尝试次数”预留。这个函数先确认乘法不会整数溢出，
 * 再返回安全结果，避免异常大配置绕过预算判断。
 *
 * 专业说法：使用无符号整数上界进行 checked arithmetic；溢出被转换为 DatabaseError。
 *
 * 参数说明：
 * - value：单次金额。
 * - count：次数。
 *
 * 返回值：乘积；当 count 为零时返回零。
 */
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
    /*
     * 函数名直译：连接池构造函数。
     *
     * 通俗说：提前创建固定数量的 Gateway 数据库连接，并把它们放入“可领取”队列。
     * SQL worker 领取连接后独占使用，完成后由 Lease 自动归还。
     *
     * 专业说法：这是一个有界、互斥保护的连接池；容量由 database_pool_size 决定，
     * 不会因为每个 HTTP 请求而创建新的数据库线程或连接。
     *
     * 参数说明：
     * - config：每条连接共用的数据库配置。
     * - size：池中连接数量。
     *
     * 实现方法：创建 size 个 MySqlConnection，同时把裸指针登记到 available_ 队列。
     */
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
        /* 取得连接池中一条连接的 RAII 借用凭证；析构时自动归还。 */
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

        /* 借用结束后把连接放回池中，并唤醒一个等待的 worker。 */
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

        /*
         * 函数名直译：获取连接池租约。
         *
         * 通俗说：没有空闲连接时就在条件变量上等待；有连接后取出队首并交给调用者。
         *
         * 专业说法：mutex 保护 available_，condition_variable 把数据库并发限制在池容量内，
         * Lease 的析构负责最终归还。
         */
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

/*
 * 函数名直译：解析 ID。
 *
 * 通俗说：把 SQL 返回的数字文本转换成 Gateway 使用的整数主键。
 *
 * 专业说法：集中处理数据库 ID 的字符串到 uint64_t 转换；格式异常会抛出标准转换异常并由上层处理。
 */
std::uint64_t parse_id(const std::string &value)
{
    return static_cast<std::uint64_t>(std::stoull(value));
}

/* 空字符串代表 SQL NULL 时返回空 optional，否则解析为主键。 */
std::optional<std::uint64_t> parse_optional_id(const std::string &value)
{
    return value.empty() ? std::optional<std::uint64_t>()
                         : std::optional<std::uint64_t>(parse_id(value));
}

/*
 * 函数名直译：加载配额策略。
 *
 * 通俗说：根据关联 ID 读取租户、Key 或 Credential 的 RPM、并发和预算限制；没有绑定策略时返回空。
 *
 * 专业说法：这是 Repository 将 quota_policies 行转换为不可变 QuotaPolicy 的小型查询适配器。
 *
 * 参数说明：
 * - connection：当前事务/快照使用的数据库连接。
 * - id：quota_policies 主键文本。
 *
 * 返回值：存在且唯一时返回策略；ID 有关联但行缺失或形状不符时抛出数据库错误。
 */
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

/*
 * 函数名直译：校验数据库 Schema。
 *
 * 通俗说：每次使用 Gateway 数据库前，先确认迁移已经达到本程序要求的版本；版本不匹配时 fail closed。
 *
 * 专业说法：读取 schema_migrations 的最新版本并与编译期 required_schema_version 比较，
 * 防止新代码在缺列或旧约束的数据库上部分运行。
 */
void verify_schema(MySqlConnection &connection)
{
    const auto rows = connection.query_prepared(
        "SELECT version FROM schema_migrations ORDER BY version");
    if (rows.empty() || rows.back().empty() || rows.back().front() != required_schema_version)
    {
        throw DatabaseError("Gateway database schema version is incompatible");
    }
}
} // namespace

class MySqlGatewayRepository::Impl
{
public:
    /* Repository 实现构造函数：创建固定大小的数据库连接池。 */
    explicit Impl(const GatewayConfig &config)
        : pool_(config.database, config.database_pool_size)
    {
    }

    /*
     * 函数名直译：读取配置版本。
     *
     * 通俗说：RuntimeState 用这个数字判断认证缓存和路由快照是否仍然新鲜。
     *
     * 专业说法：每次读取都先校验 Schema，再读取 singleton 配置版本；失败直接抛出，调用方进入 not-ready。
     */
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

    /*
     * 函数名直译：加载访问候选。
     *
     * 通俗说：用户携带 Key 前缀到达时，在一个数据库一致性快照内把 Key、租户、策略、模型、Mapping、价格和配额全部读出。
     * 之后 RuntimeState 才会在内存里做 HMAC 比对和 Secret 解析。
     *
     * 专业说法：这是认证与路由的 Repository seam。START TRANSACTION WITH CONSISTENT SNAPSHOT
     * 保证多个关联表来自同一版本视图，避免读到半套配置。
     *
     * 参数说明：
     * - display_prefix：Key 的可展示前缀，仅用于缩小候选范围，不是完整凭据。
     *
     * 返回值：可能匹配该前缀的候选记录集合。
     *
     * 实现方法：校验 Schema，开启一致性事务，依次读取版本、Key 主记录、协议/Provider/模型授权、
     * Mapping、价格和配额；成功提交，异常回滚。
     *
     * 注意：返回值包含内部 ID 供审计和配额使用，但不应直接写入客户端响应或日志。
     */
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

    /*
     * 函数名直译：准入请求。
     *
     * 通俗说：在真正调用 Provider 前创建 started Usage，并为租户和 API Key 的日/月预算预留最多尝试次数的金额。
     * 任一预算不足都会回滚整笔事务。
     *
     * 专业说法：这是 MySQL 预算预留事务边界，与 Redis RPM/并发准入配合构成 fail-closed 请求准入。
     *
     * 参数说明：
     * - request：请求身份、模型、协议、请求大小、候选上限和配额策略快照。
     *
     * 返回值：admitted、budget_exceeded 或数据库不可用时抛出异常。
     *
     * 实现方法：插入 usage_records，按 UTC 日/月计算周期，分别锁定并增加预算预留，成功提交。
     */
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

    /*
     * 函数名直译：完成请求。
     *
     * 通俗说：Provider 链路结束后，找到本次 started Usage，结算预算、记录最终候选、耗时、状态和费用。
     * 重复调用只看到非 started 状态并直接返回，因此不会重复扣费。
     *
     * 专业说法：这是请求级幂等 finalization 事务；精确 Usage 优先，缺失 Usage 时使用预留额估算，
     * 明确不可计费的请求记录为 not_billable。
     *
     * 参数说明：
     * - request：执行状态机汇总的终态、尝试数、候选 ID、Usage 和计费质量。
     *
     * 实现方法：锁定 Usage 与 reserved reservations，逐项把 reserved 转为 settled，更新 usage_records，提交事务。
     *
     * 注意：事务失败会回滚，后台治理协调器仍可处理过期 started 记录。
     */
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

    /*
     * 函数名直译：加载健康检查。
     *
     * 通俗说：读取管理员明确配置的非计费 Provider 探测目标，以及它们使用的 Credential 配额和候选映射。
     *
     * 专业说法：这是 HealthProbeRuntime 的配置快照查询，不创建 Usage 或 request_attempts。
     */
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

    /*
     * 函数名直译：协调废弃请求。
     *
     * 通俗说：后台发现超过租约时间仍停留在 started 的 Usage 时，释放预算预留并标记 abandoned，
     * 防止一次崩溃永久占住预算。
     *
     * 专业说法：这是基于 UTC lease_expires_at 的恢复性事务；先锁定过期 reservation，再幂等释放，
     * 最后仅在该 Usage 已无其他 reserved reservation 时更新状态。
     */
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

    /*
     * 函数名直译：开始尝试。
     *
     * 通俗说：每次真正准备调用一个 Provider Candidate 前，先写入唯一 attempt_id 的 started 审计行。
     * 写入失败时上层不会继续发出 Provider 请求。
     *
     * 专业说法：request_attempts 是一次调用一次记录的审计 seam，使用 attempt_id 作为幂等身份。
     */
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

    /*
     * 函数名直译：完成尝试。
     *
     * 通俗说：把 Provider 返回状态、错误分类、耗时、字节数、首字节时间、Usage 和可能重复计费标志写回 started attempt。
     *
     * 专业说法：WHERE completed_at IS NULL 使终态更新幂等，取消、超时和正常完成都汇聚到同一路径。
     */
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
    /*
     * 函数名直译：预留一个作用域预算。
     *
     * 通俗说：为租户或 API Key 在某个 UTC 日/月周期预留“每次尝试金额 × 最大尝试数”，
     * 只有余额足够才把 reservation 写入数据库。
     *
     * 专业说法：通过 UPDATE 条件中的 settled+reserved<=limit 实现并发事务下的预算闸门，
     * 随后插入可过期的 budget_reservations 明细。
     *
     * 参数说明：
     * - connection：当前请求准入事务连接。
     * - usage_id：usage_records 主键。
     * - scope_type/scope_id：tenant 或 api_key 及其 ID。
     * - quota：该作用域的配额，可为空。
     * - max_attempts：本次请求最多候选尝试数。
     * - day/month：UTC 周期起点。
     */
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

/* Gateway Repository 构造函数：建立内部连接池，但不执行业务查询。 */
MySqlGatewayRepository::MySqlGatewayRepository(const GatewayConfig &config)
    : impl_(std::make_unique<Impl>(config))
{
}

/* 析构时销毁 Impl 和连接池，连接池再按 RAII 关闭所有 MariaDB 句柄。 */
MySqlGatewayRepository::~MySqlGatewayRepository() = default;

/* 对外暴露当前数据库配置版本。 */
std::uint64_t MySqlGatewayRepository::config_version()
{
    return impl_->config_version();
}

/* 对外暴露按前缀加载认证与路由候选的 Repository seam。 */
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
} // namespace ai_gateway
