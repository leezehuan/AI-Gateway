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

namespace ai_gateway
{
namespace
{
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
    explicit Impl(GatewayConfig config) : config_(std::move(config)) {}

    ~Impl()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        disconnect_locked();
    }

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

    bool rollback(const std::string &permit_id, const std::vector<GovernanceScope> &scopes)
    {
        static const std::string script =
            "for i=1,#KEYS do redis.call('ZREM',KEYS[i],ARGV[1]); end; return 1;";
        return mutate(script, permit_id, scopes, true, 0);
    }

    bool release(const std::string &permit_id, const std::vector<GovernanceScope> &scopes)
    {
        static const std::string script =
            "for i=1,#KEYS do redis.call('ZREM',KEYS[i],ARGV[1]); end; return 1;";
        return mutate(script, permit_id, scopes, false, 0);
    }

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

HiredisGovernanceStore::HiredisGovernanceStore(GatewayConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}
HiredisGovernanceStore::~HiredisGovernanceStore() = default;
bool HiredisGovernanceStore::ping() { return impl_->ping(); }
GovernanceStoreResult HiredisGovernanceStore::reserve(
    const std::string &id, const std::vector<GovernanceScope> &scopes, long ttl)
{
    return impl_->reserve(id, scopes, ttl);
}
bool HiredisGovernanceStore::rollback(
    const std::string &id, const std::vector<GovernanceScope> &scopes)
{
    return impl_->rollback(id, scopes);
}
bool HiredisGovernanceStore::release(
    const std::string &id, const std::vector<GovernanceScope> &scopes)
{
    return impl_->release(id, scopes);
}
bool HiredisGovernanceStore::renew(
    const std::string &id, const std::vector<GovernanceScope> &scopes, long ttl)
{
    return impl_->renew(id, scopes, ttl);
}
std::optional<bool> HiredisGovernanceStore::acquire_probe_lease(
    const std::string &probe_id, long ttl)
{
    return impl_->acquire_probe_lease(probe_id, ttl);
}

class GovernanceRuntime::Impl
{
public:
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

    ~Impl()
    {
        stopping_.store(true);
        condition_.notify_all();
        poll_condition_.notify_all();
        if (poller_.joinable()) poller_.join();
        for (auto &worker : workers_) if (worker.joinable()) worker.join();
    }

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

    void release(std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
    {
        complete(std::move(permit), std::move(callback), true);
    }

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

    void rollback(std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
    {
        complete(std::move(permit), std::move(callback), false);
    }

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

    bool ready() const { return ready_.load(); }

private:
    struct Active
    {
        std::shared_ptr<const GovernancePermit> permit;
        std::function<void()> lease_lost;
    };

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

    void enqueue_ping()
    {
        enqueue([this] { ready_.store(store_.ping()); });
    }

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

GovernanceRuntime::GovernanceRuntime(GatewayConfig config, GovernanceStore &store)
    : impl_(std::make_unique<Impl>(std::move(config), store)) {}
GovernanceRuntime::~GovernanceRuntime() = default;
void GovernanceRuntime::admit(std::vector<GovernanceScope> scopes,
                              std::function<void()> lease_lost,
                              AdmissionCallback callback)
{
    impl_->admit(std::move(scopes), std::move(lease_lost), std::move(callback));
}
void GovernanceRuntime::release(
    std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
{
    impl_->release(std::move(permit), std::move(callback));
}
void GovernanceRuntime::acquire_probe_lease(std::string probe_id,
                                            long lease_ttl_ms,
                                            ProbeLeaseCallback callback)
{
    impl_->acquire_probe_lease(std::move(probe_id), lease_ttl_ms, std::move(callback));
}
void GovernanceRuntime::rollback(
    std::shared_ptr<const GovernancePermit> permit, CompletionCallback callback)
{
    impl_->rollback(std::move(permit), std::move(callback));
}
bool GovernanceRuntime::ready() const { return impl_->ready(); }
} // namespace ai_gateway
