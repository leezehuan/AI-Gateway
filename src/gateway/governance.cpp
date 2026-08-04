#include "gateway/governance.hpp"

#include <hiredis/hiredis.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

/*
 * 分布式资源治理实现。
 *
 * Redis Lua 脚本原子维护滚动 60 秒 RPM 与并发 lease；MySQL 预算预留和 Usage 终态由 RuntimeState/Repository
 * 负责。本文件只处理 Redis，并把 hiredis 阻塞调用限制在固定 worker。准入成功得到 GovernancePermit：
 * 请求完成后释放并发成员，RPM 记录按时间自然过期；长 SSE 会续租，续租失败必须取消上游而不能依赖本地计数。
 */
namespace ai_gateway
{
namespace
{
/* hiredis 回复对象的 RAII 释放器，覆盖命令失败和异常路径。 */
struct RedisReplyDeleter
{
    void operator()(redisReply *reply) const
    {
        if (reply != nullptr)
        {
            freeReplyObject(reply);
        }
    }
};
using Reply = std::unique_ptr<redisReply, RedisReplyDeleter>;

/*
 * 函数名直译：以参数数组执行 Redis 命令。
 *
 * 通俗说：把每个 C++ string 的地址和长度分别交给 hiredis，而不是先拼成一整条命令文本。
 * 这样 permit ID、scope 名称等数据即使含空格或二进制字节，也不会被 Redis 命令解析器误拆分。
 *
 * 专业说法：redisCommandArgv 是二进制安全的 hiredis API；Reply unique_ptr 接管返回对象的释放责任。
 *
 * 注意：arguments 在 redisCommandArgv 返回前一直有效，返回后 hiredis 已完成参数复制/发送。
 */
Reply command_argv(redisContext *context, const std::vector<std::string> &arguments)
{
    std::vector<const char *> values;
    std::vector<std::size_t> lengths;
    values.reserve(arguments.size());
    lengths.reserve(arguments.size());
    for (const auto &argument : arguments)
    {
        values.push_back(argument.data());
        lengths.push_back(argument.size());
    }
    return Reply(static_cast<redisReply *>(redisCommandArgv(
        context, static_cast<int>(values.size()), values.data(), lengths.data())));
}

/* 从 Redis 数组回复读取整数；类型不符时返回保守的 fallback。 */
long long integer_at(redisReply *reply, std::size_t index, long long fallback = 0)
{
    if (reply == nullptr || reply->type != REDIS_REPLY_ARRAY || index >= reply->elements ||
        reply->element[index] == nullptr)
    {
        return fallback;
    }
    redisReply *value = reply->element[index];
    if (value->type == REDIS_REPLY_INTEGER)
    {
        return value->integer;
    }
    return fallback;
}
} // namespace

class HiredisGovernanceStore::Impl
{
public:
    /* 保存 Redis 连接参数，连接本身按需建立。 */
    explicit Impl(GatewayConfig config) : config_(std::move(config)) {}

    /* 释放 mutex 保护的 hiredis 上下文。 */
    ~Impl()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        disconnect_locked();
    }

    /* PING Redis，供 readiness 和后台重连检查使用。 */
    bool ping()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return false;
        }
        Reply reply(static_cast<redisReply *>(redisCommand(context_, "PING")));
        return valid_locked(reply.get()) && reply->type == REDIS_REPLY_STATUS;
    }

    /*
     * 函数名直译：预留治理额度。
     *
     * 通俗说：一次请求可能同时受 Tenant、API Key、Credential 三层限制。Lua 会先清理过期成员，
     * 检查所有层是否都有 RPM 和并发余量，只有全部通过才把同一个 permit ID 写入全部集合。
     * 任一层不足时，它在写入任何新成员前返回，所以不会出现“Tenant 已扣、Key 未扣”的半成功状态。
     *
     * 专业说法：Redis 脚本在单线程执行，TIME 由 Redis 服务器提供，避免多 Gateway 节点的本地时钟偏差
     * 影响滚动窗口。RPM ZSET score 是开始时间；并发 ZSET score 是 lease 到期时间。
     *
     * 参数说明：
     * - permit_id：本次准入的唯一成员 ID，release/renew 必须使用同一个值。
     * - scopes：要同时检查的分层配额集合。
     * - lease_ttl_ms：并发成员的过期时间，进程崩溃后由 Redis 自动回收。
     *
     * 返回值：admitted、rate_limited、concurrency_limited 或 unavailable，以及可建议客户端等待的时间。
     */
    GovernanceStoreResult reserve(const std::string &permit_id,
                                  const std::vector<GovernanceScope> &scopes,
                                  long lease_ttl_ms)
    {
        static const std::string script =
            "local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); "
            "local n=tonumber(ARGV[3]); "
            "for i=1,n do local r=KEYS[(i-1)*2+1]; local c=KEYS[(i-1)*2+2]; "
            "local rl=tonumber(ARGV[3+(i-1)*2+1]); local cl=tonumber(ARGV[3+(i-1)*2+2]); "
            "redis.call('ZREMRANGEBYSCORE',r,'-inf',now-60000); "
            "redis.call('ZREMRANGEBYSCORE',c,'-inf',now); "
            "if rl>=0 and redis.call('ZCARD',r)>=rl then "
            "local o=redis.call('ZRANGE',r,0,0,'WITHSCORES'); "
            "local retry=60000; if #o>1 then retry=math.max(1,60000-(now-tonumber(o[2]))); end; "
            "return {1,retry}; end; "
            "if cl>=0 and redis.call('ZCARD',c)>=cl then "
            "local o=redis.call('ZRANGE',c,0,0,'WITHSCORES'); "
            "local retry=tonumber(ARGV[2]); if #o>1 then retry=math.max(1,tonumber(o[2])-now); end; "
            "return {2,retry}; end; end; "
            "for i=1,n do local r=KEYS[(i-1)*2+1]; local c=KEYS[(i-1)*2+2]; "
            "local rl=tonumber(ARGV[3+(i-1)*2+1]); local cl=tonumber(ARGV[3+(i-1)*2+2]); "
            "if rl>=0 then redis.call('ZADD',r,now,ARGV[1]); redis.call('PEXPIRE',r,120000); end; "
            "if cl>=0 then redis.call('ZADD',c,now+tonumber(ARGV[2]),ARGV[1]); "
            "redis.call('PEXPIRE',c,tonumber(ARGV[2])*2); end; end; return {0,0};";

        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return {};
        }
        std::vector<std::string> arguments{"EVAL", script,
                                           std::to_string(scopes.size() * 2)};
        for (const auto &scope : scopes)
        {
            arguments.push_back(key("rpm", scope));
            arguments.push_back(key("lease", scope));
        }
        arguments.push_back(permit_id);
        arguments.push_back(std::to_string(lease_ttl_ms));
        arguments.push_back(std::to_string(scopes.size()));
        for (const auto &scope : scopes)
        {
            arguments.push_back(scope.rpm ? std::to_string(*scope.rpm) : "-1");
            arguments.push_back(scope.concurrency ? std::to_string(*scope.concurrency) : "-1");
        }
        Reply reply = command_argv(context_, arguments);
        if (!valid_locked(reply.get()) || reply->type != REDIS_REPLY_ARRAY)
        {
            disconnect_locked();
            return {};
        }
        const long long code = integer_at(reply.get(), 0, 3);
        const long retry = static_cast<long>(std::max<long long>(0, integer_at(reply.get(), 1)));
        if (code == 0)
        {
            return {GovernanceStatus::admitted, 0};
        }
        if (code == 1)
        {
            return {GovernanceStatus::rate_limited, retry};
        }
        if (code == 2)
        {
            return {GovernanceStatus::concurrency_limited, retry};
        }
        return {};
    }

    /*
     * Provider 尚未调用时补偿删除本次准入成员。
     * 与 release 的区别是它同时删除本次刚写入的 RPM 成员，因为这次请求还没有产生可计量的上游工作。
     */
    bool rollback(const std::string &permit_id, const std::vector<GovernanceScope> &scopes)
    {
        static const std::string script =
            "for i=1,#KEYS do redis.call('ZREM',KEYS[i],ARGV[1]); end; return 1;";
        return mutate(script, permit_id, scopes, true, 0);
    }

    /*
     * 终态释放并发 lease；RPM 成员按滚动窗口自然过期。
     * 不能在普通失败、客户端取消或 Provider 5xx 后退款 RPM，否则客户端可用大量失败请求绕过速率限制。
     */
    bool release(const std::string &permit_id, const std::vector<GovernanceScope> &scopes)
    {
        static const std::string script =
            "for i=1,#KEYS do redis.call('ZREM',KEYS[i],ARGV[1]); end; return 1;";
        return mutate(script, permit_id, scopes, false, 0);
    }

    /*
     * 续租长请求的并发成员；续租失败必须使上游 transfer 取消。
     * 脚本先确认每个 lease ZSET 仍能找到 permit ID，再原子更新到期时间；找不到说明 Redis 已过期、
     * 被错误释放或运行态不一致，继续让请求占用 Provider 会破坏并发上限。
     */
    bool renew(const std::string &permit_id,
               const std::vector<GovernanceScope> &scopes,
               long lease_ttl_ms)
    {
        static const std::string script =
            "local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); "
            "for i=1,#KEYS do if redis.call('ZSCORE',KEYS[i],ARGV[1])==false then return 0; end; "
            "redis.call('ZADD',KEYS[i],'XX',now+tonumber(ARGV[2]),ARGV[1]); "
            "redis.call('PEXPIRE',KEYS[i],tonumber(ARGV[2])*2); end; return 1;";
        return mutate(script, permit_id, scopes, false, lease_ttl_ms);
    }

    /* 用 SET NX PX 竞争跨节点健康探测的单一执行权。 */
    std::optional<bool> acquire_probe_lease(const std::string &probe_id,
                                            long lease_ttl_ms)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return std::nullopt;
        }
        const std::string probe_key = config_.redis_key_prefix +
                                      ":governance:v1:health_probe:" + probe_id;
        Reply reply(static_cast<redisReply *>(redisCommand(
            context_, "SET %b 1 NX PX %ld", probe_key.data(), probe_key.size(),
            lease_ttl_ms)));
        if (!valid_locked(reply.get()))
        {
            disconnect_locked();
            return std::nullopt;
        }
        return reply->type == REDIS_REPLY_STATUS;
    }

private:
    /*
     * 函数名直译：变更治理状态。
     *
     * 通俗说：rollback、release、renew 都是“给若干 Redis key 执行一段 Lua”的相同模板。
     * 这里统一选择需要操作的 RPM/lease key、连接 Redis、执行 EVAL 并校验整数结果。
     *
     * 注意：Redis 失败返回 false，而不是假装本地操作成功；调用方据此停止 failover 或取消请求。
     */
    bool mutate(const std::string &script,
                const std::string &permit_id,
                const std::vector<GovernanceScope> &scopes,
                bool include_rpm,
                long lease_ttl_ms)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return false;
        }
        std::vector<std::string> keys;
        for (const auto &scope : scopes)
        {
            if (include_rpm && scope.rpm)
            {
                keys.push_back(key("rpm", scope));
            }
            if (scope.concurrency)
            {
                keys.push_back(key("lease", scope));
            }
        }
        if (keys.empty())
        {
            return true;
        }
        std::vector<std::string> arguments{"EVAL", script, std::to_string(keys.size())};
        arguments.insert(arguments.end(), keys.begin(), keys.end());
        arguments.push_back(permit_id);
        if (lease_ttl_ms > 0)
        {
            arguments.push_back(std::to_string(lease_ttl_ms));
        }
        Reply reply = command_argv(context_, arguments);
        if (!valid_locked(reply.get()) || reply->type != REDIS_REPLY_INTEGER)
        {
            disconnect_locked();
            return false;
        }
        return reply->integer == 1;
    }

    /* 把 scope 类型和内部 ID 组成稳定 Redis key；不使用完整 API Key、Prompt 或 URL。 */
    std::string key(const char *kind, const GovernanceScope &scope) const
    {
        return config_.redis_key_prefix + ":governance:v1:" + kind + ":" + scope.kind + ":" +
               std::to_string(scope.id);
    }

    bool valid_locked(redisReply *reply) const
    {
        return context_ != nullptr && context_->err == 0 && reply != nullptr &&
               reply->type != REDIS_REPLY_ERROR;
    }

    /*
     * 函数名直译：确保 Redis 连接存在。
     *
     * 通俗说：现有连接健康就复用；失败则丢弃并按配置建立、认证、选择逻辑 DB。
     * 连接/认证任何一步失败都会清空 context，让下一次 worker 任务从干净状态重新尝试。
     *
     * 注意：调用者必须已经持有 mutex_；密码只传给 hiredis，绝不进入错误字符串或日志。
     */
    bool ensure_locked()
    {
        if (context_ != nullptr && context_->err == 0)
        {
            return true;
        }
        disconnect_locked();
        timeval connect_timeout{config_.redis_connect_timeout_ms / 1000,
                                (config_.redis_connect_timeout_ms % 1000) * 1000};
        context_ = redisConnectWithTimeout(config_.redis_host.c_str(), config_.redis_port,
                                           connect_timeout);
        if (context_ == nullptr || context_->err != 0)
        {
            disconnect_locked();
            return false;
        }
        timeval command_timeout{config_.redis_command_timeout_ms / 1000,
                                (config_.redis_command_timeout_ms % 1000) * 1000};
        redisSetTimeout(context_, command_timeout);
        if (!config_.redis_password.empty())
        {
            Reply auth;
            if (config_.redis_username.empty())
            {
                auth.reset(static_cast<redisReply *>(redisCommand(
                    context_, "AUTH %b", config_.redis_password.data(),
                    config_.redis_password.size())));
            }
            else
            {
                auth.reset(static_cast<redisReply *>(redisCommand(
                    context_, "AUTH %b %b", config_.redis_username.data(),
                    config_.redis_username.size(), config_.redis_password.data(),
                    config_.redis_password.size())));
            }
            if (!valid_locked(auth.get()))
            {
                disconnect_locked();
                return false;
            }
        }
        if (config_.redis_database != 0)
        {
            Reply select(static_cast<redisReply *>(
                redisCommand(context_, "SELECT %u", config_.redis_database)));
            if (!valid_locked(select.get()))
            {
                disconnect_locked();
                return false;
            }
        }
        return true;
    }

    void disconnect_locked()
    {
        if (context_ != nullptr)
        {
            redisFree(context_);
            context_ = nullptr;
        }
    }

    GatewayConfig config_;
    std::mutex mutex_;
    redisContext *context_ = nullptr;
};

/* 创建生产 Redis GovernanceStore。 */
HiredisGovernanceStore::HiredisGovernanceStore(GatewayConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}
/* Impl 析构时关闭 Redis 连接。 */
HiredisGovernanceStore::~HiredisGovernanceStore() = default;

/* 转发 Redis 连通性检查。 */
bool HiredisGovernanceStore::ping() { return impl_->ping(); }

/* 转发原子请求准入。 */
GovernanceStoreResult HiredisGovernanceStore::reserve(
    const std::string &id, const std::vector<GovernanceScope> &scopes, long ttl)
{
    return impl_->reserve(id, scopes, ttl);
}

/* 转发 Provider 调用前的补偿删除。 */
bool HiredisGovernanceStore::rollback(
    const std::string &id, const std::vector<GovernanceScope> &scopes)
{
    return impl_->rollback(id, scopes);
}

/* 转发请求终态 lease 释放。 */
bool HiredisGovernanceStore::release(
    const std::string &id, const std::vector<GovernanceScope> &scopes)
{
    return impl_->release(id, scopes);
}

/* 转发活跃请求的 lease 续租。 */
bool HiredisGovernanceStore::renew(
    const std::string &id, const std::vector<GovernanceScope> &scopes, long ttl)
{
    return impl_->renew(id, scopes, ttl);
}

/* 转发健康探测的独占 lease 竞争。 */
std::optional<bool> HiredisGovernanceStore::acquire_probe_lease(
    const std::string &probe_id, long ttl)
{
    return impl_->acquire_probe_lease(probe_id, ttl);
}

class GovernanceRuntime::Impl
{
public:
    /* 启动有界 Redis worker 队列、定时 PING 和活跃 lease 续租线程。 */
    Impl(GatewayConfig config, GovernanceStore &store)
        : config_(std::move(config)), store_(store)
    {
        for (std::size_t i = 0; i < config_.redis_workers; ++i)
        {
            workers_.emplace_back([this] { worker_loop(); });
        }
        poller_ = std::thread([this] { poll_loop(); });
        enqueue_ping();
    }

    /* 停止并等待后台线程，保证析构后不再访问 GovernanceStore。 */
    ~Impl()
    {
        stopping_.store(true);
        condition_.notify_all();
        poll_condition_.notify_all();
        if (poller_.joinable()) poller_.join();
        for (auto &worker : workers_) if (worker.joinable()) worker.join();
    }

    /*
     * 生成 permit id 后异步调用 Redis reserve；成功且包含并发限制时登记到 active_，
     * 供 poll_loop 定期续租。Redis 不可用和队列满都以 unavailable fail closed。
     */
    void admit(std::vector<GovernanceScope> scopes,
               std::function<void()> lease_lost,
               AdmissionCallback callback)
    {
        auto shared_callback = std::make_shared<AdmissionCallback>(std::move(callback));
        if (!ready_.load())
        {
            (*shared_callback)({});
            return;
        }
        const std::string permit_id = "permit_" + generate_request_id().substr(4);
        if (!enqueue([this, permit_id, scopes = std::move(scopes),
                      lease_lost = std::move(lease_lost), shared_callback]() mutable {
                const auto result = store_.reserve(
                    permit_id, scopes, config_.governance_lease_ttl_ms);
                if (result.status == GovernanceStatus::unavailable)
                {
                    ready_.store(false);
                    (*shared_callback)({});
                    return;
                }
                ready_.store(true);
                if (result.status != GovernanceStatus::admitted)
                {
                    (*shared_callback)({result.status, result.retry_after_ms, {}});
                    return;
                }
                auto permit = std::make_shared<GovernancePermit>();
                permit->id = permit_id;
                permit->scopes = std::move(scopes);
                if (std::any_of(permit->scopes.begin(), permit->scopes.end(),
                                [](const GovernanceScope &scope) { return scope.concurrency.has_value(); }))
                {
                    std::lock_guard<std::mutex> lock(active_mutex_);
                    active_[permit_id] = {permit, std::move(lease_lost)};
                }
                (*shared_callback)({GovernanceStatus::admitted, 0, std::move(permit)});
            }))
        {
            ready_.store(false);
            (*shared_callback)({});
        }
    }

    /* 正常结束时保留 RPM 计数，仅释放并发 lease。 */
    void release(std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
    {
        complete(std::move(permit), std::move(callback), true);
    }

    /* 异步获取健康探测 lease；任何 Redis 不确定结果都会置 not ready。 */
    void acquire_probe_lease(std::string probe_id,
                             long lease_ttl_ms,
                             ProbeLeaseCallback callback)
    {
        auto shared_callback = std::make_shared<ProbeLeaseCallback>(std::move(callback));
        if (!ready_.load() || !enqueue(
                [this, probe_id = std::move(probe_id), lease_ttl_ms, shared_callback] {
                    const auto acquired = store_.acquire_probe_lease(probe_id, lease_ttl_ms);
                    if (!acquired) ready_.store(false);
                    (*shared_callback)(acquired);
                }))
        {
            ready_.store(false);
            (*shared_callback)(std::nullopt);
        }
    }

    /* 请求尚未执行时移除 RPM 和 lease，恢复准入前状态。 */
    void rollback(std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
    {
        complete(std::move(permit), std::move(callback), false);
    }

    /* 公共终态处理：先从续租表移除，再异步调用 release 或 rollback。 */
    void complete(std::shared_ptr<const GovernancePermit> permit,
                  CompletionCallback callback,
                  bool keep_rpm)
    {
        if (!permit)
        {
            if (callback) callback(true);
            return;
        }
        {
            std::lock_guard<std::mutex> lock(active_mutex_);
            active_.erase(permit->id);
        }
        auto shared_callback = std::make_shared<CompletionCallback>(std::move(callback));
        if (!enqueue([this, permit = std::move(permit), shared_callback, keep_rpm] {
                const bool released = keep_rpm
                                          ? store_.release(permit->id, permit->scopes)
                                          : store_.rollback(permit->id, permit->scopes);
                if (!released) ready_.store(false);
                if (*shared_callback) (*shared_callback)(released);
            }))
        {
            ready_.store(false);
            if (*shared_callback) (*shared_callback)(false);
        }
    }

    /* Redis ping、脚本或队列失败后为 false，新的模型代理请求不能继续。 */
    bool ready() const { return ready_.load(); }

private:
    struct Active
    {
        std::shared_ptr<const GovernancePermit> permit;
        std::function<void()> lease_lost;
    };

    /* 将 Redis 工作放入有界队列，避免外部依赖变慢时无上限占用内存。 */
    bool enqueue(std::function<void()> task)
    {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (stopping_.load() || tasks_.size() >= config_.redis_queue_size) return false;
            tasks_.push_back(std::move(task));
        }
        condition_.notify_one();
        return true;
    }

    /* 固定 Redis worker 消费循环；异常意味着依赖状态不可用。 */
    void worker_loop()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                condition_.wait(lock, [this] { return stopping_.load() || !tasks_.empty(); });
                if (stopping_.load() && tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            try { task(); } catch (...) { ready_.store(false); }
        }
    }

    /* 每秒刷新 Redis readiness，并按配置间隔续租全部活跃并发 permit。 */
    void poll_loop()
    {
        std::unique_lock<std::mutex> lock(poll_mutex_);
        auto next_renew = std::chrono::steady_clock::now();
        while (!stopping_.load())
        {
            poll_condition_.wait_for(lock, std::chrono::seconds(1),
                                     [this] { return stopping_.load(); });
            if (stopping_.load()) break;
            enqueue_ping();
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_renew)
            {
                enqueue_renewals();
                next_renew = now + std::chrono::milliseconds(config_.governance_lease_renew_ms);
            }
        }
    }

    /* 投递非阻塞 PING，结果直接更新 ready_。 */
    void enqueue_ping()
    {
        enqueue([this] { ready_.store(store_.ping()); });
    }

    /* 复制 active_ 后在锁外投递续租；失败时调用请求注册的 lease_lost 取消上游。 */
    void enqueue_renewals()
    {
        std::vector<Active> entries;
        {
            std::lock_guard<std::mutex> lock(active_mutex_);
            for (const auto &entry : active_) entries.push_back(entry.second);
        }
        for (auto entry : entries)
        {
            enqueue([this, entry = std::move(entry)] {
                if (!store_.renew(entry.permit->id, entry.permit->scopes,
                                  config_.governance_lease_ttl_ms))
                {
                    ready_.store(false);
                    {
                        std::lock_guard<std::mutex> lock(active_mutex_);
                        active_.erase(entry.permit->id);
                    }
                    if (entry.lease_lost) entry.lease_lost();
                }
            });
        }
    }

    GatewayConfig config_;
    GovernanceStore &store_;
    std::atomic_bool stopping_{false};
    std::atomic_bool ready_{false};
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::deque<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    std::thread poller_;
    std::mutex poll_mutex_;
    std::condition_variable poll_condition_;
    std::mutex active_mutex_;
    std::unordered_map<std::string, Active> active_;
};

/* 创建 GovernanceRuntime 外观。 */
GovernanceRuntime::GovernanceRuntime(GatewayConfig config, GovernanceStore &store)
    : impl_(std::make_unique<Impl>(std::move(config), store)) {}
/* Impl 析构会停止 worker/poller。 */
GovernanceRuntime::~GovernanceRuntime() = default;

/* 转交异步请求准入。 */
void GovernanceRuntime::admit(std::vector<GovernanceScope> scopes,
                              std::function<void()> lease_lost,
                              AdmissionCallback callback)
{
    impl_->admit(std::move(scopes), std::move(lease_lost), std::move(callback));
}

/* 转交正常释放。 */
void GovernanceRuntime::release(
    std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
{
    impl_->release(std::move(permit), std::move(callback));
}

/* 转交健康探测 lease 获取。 */
void GovernanceRuntime::acquire_probe_lease(std::string probe_id,
                                            long lease_ttl_ms,
                                            ProbeLeaseCallback callback)
{
    impl_->acquire_probe_lease(std::move(probe_id), lease_ttl_ms, std::move(callback));
}

/* 转交准入补偿。 */
void GovernanceRuntime::rollback(
    std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
{
    impl_->rollback(std::move(permit), std::move(callback));
}

/* 查询治理 Redis 当前 readiness。 */
bool GovernanceRuntime::ready() const { return impl_->ready(); }
} // namespace ai_gateway
