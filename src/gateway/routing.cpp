#include "gateway/routing.hpp"

#include <hiredis/hiredis.h>
#include <openssl/sha.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <thread>
#include <utility>

namespace ai_gateway
{
namespace
{
using SystemClock = std::chrono::system_clock;
using SteadyClock = std::chrono::steady_clock;

struct ReplyDeleter
{
    /* hiredis 回复对象归 unique_ptr 管理，确保 Redis 命令失败或提前 return 时也会释放。 */
    void operator()(redisReply *reply) const
    {
        if (reply != nullptr)
        {
            freeReplyObject(reply);
        }
    }
};
using Reply = std::unique_ptr<redisReply, ReplyDeleter>;

/* 用候选身份计算稳定的 rendezvous hash，不暴露请求或会话内容。 */
std::uint64_t rendezvous_hash(const std::string &seed, const std::string &candidate)
{
    const std::string input = seed + "\x1f" + candidate;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(input.data()), input.size(), digest);
    std::uint64_t value = 0;
    for (std::size_t index = 0; index < sizeof(value); ++index)
    {
        value = (value << 8U) | digest[index];
    }
    return value;
}

/* 返回 Redis 熔断时间戳使用的 Unix 毫秒，使用系统时钟以便所有 Gateway 节点共享解释。 */
std::int64_t now_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        SystemClock::now().time_since_epoch()).count();
}

/* 容忍 Redis 缺值或类型异常，将其当作调用方给定的保守默认值。 */
long long reply_integer(redisReply *reply, long long fallback)
{
    if (reply == nullptr || reply->type == REDIS_REPLY_NIL)
    {
        return fallback;
    }
    if (reply->type == REDIS_REPLY_INTEGER)
    {
        return reply->integer;
    }
    if (reply->type == REDIS_REPLY_STRING && reply->str != nullptr)
    {
        try
        {
            return std::stoll(std::string(reply->str, reply->len));
        }
        catch (...)
        {
            return fallback;
        }
    }
    return fallback;
}
}

class HiredisRoutingStore::Impl
{
public:
    /* 保存 Redis 配置；连接按需建立，避免构造阶段阻塞 Gateway 启动。 */
    explicit Impl(GatewayConfig config)
        : config_(std::move(config))
    {
    }

    /* 退出时在 mutex 保护下释放单个 hiredis 连接。 */
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
        if (!valid_locked(reply.get()) || reply->type != REDIS_REPLY_STATUS ||
            std::string(reply->str, reply->len) != "PONG")
        {
            disconnect_locked();
            return false;
        }
        return true;
    }



    std::optional<RoutingSnapshot> snapshot(const std::vector<std::string> &fingerprints,
                                            std::int64_t current_ms)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return std::nullopt;
        }

        RoutingSnapshot result;
        for (const auto &fingerprint : fingerprints)
        {
            const std::string state_key = key("circuit", fingerprint);
            Reply state_reply(static_cast<redisReply *>(redisCommand(
                context_, "HMGET %b failures open_until_ms", state_key.data(),
                state_key.size())));
            if (!valid_locked(state_reply.get()) || state_reply->type != REDIS_REPLY_ARRAY ||
                state_reply->elements != 2)
            {
                disconnect_locked();
                return std::nullopt;
            }
            CandidateRoutingState state;
            const auto open_until = reply_integer(state_reply->element[1], 0);
            if (open_until > current_ms)
            {
                state.circuit_open = true;
            }
            result.candidates.emplace(fingerprint, state);
        }
        return result;
    }



    bool record(const std::string &fingerprint,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                std::int64_t current_ms,
                unsigned failure_threshold)
    {
        static const std::string script =
            "local failures=tonumber(redis.call('HGET',KEYS[1],'failures')) or 0; "
            "local open_until=tonumber(redis.call('HGET',KEYS[1],'open_until_ms')) or 0; "
            "if ARGV[1]=='1' then failures=0; open_until=0; "
            "elseif ARGV[2]=='1' then failures=failures+1; "
            "if failures>=tonumber(ARGV[3]) then "
            "open_until=tonumber(ARGV[4])+math.max(tonumber(ARGV[5]),tonumber(ARGV[6])); end; end; "
            "redis.call('HSET',KEYS[1],'failures',failures,'open_until_ms',open_until); "
            "redis.call('PEXPIRE',KEYS[1],ARGV[7]); return 1;";

        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return false;
        }
        const std::string state_key = key("circuit", fingerprint);
        const std::string success_value = success ? "1" : "0";
        const std::string retryable_value = retryable_failure ? "1" : "0";
        const std::string threshold = std::to_string(
            failure_threshold == 0 ? config_.circuit_failure_threshold : failure_threshold);
        const std::string now = std::to_string(current_ms);
        const std::string cooldown = std::to_string(config_.circuit_open_ms);
        const std::string retry_after = std::to_string(std::min<long>(300000, retry_after_ms));
        const std::string state_ttl = std::to_string(config_.routing_state_ttl_seconds * 1000);
        Reply reply(static_cast<redisReply *>(redisCommand(
            context_, "EVAL %b 1 %b %s %s %s %s %s %s %s",
            script.data(), script.size(), state_key.data(), state_key.size(), success_value.c_str(),
            retryable_value.c_str(), threshold.c_str(), now.c_str(), cooldown.c_str(),
            retry_after.c_str(), state_ttl.c_str())));
        if (!valid_locked(reply.get()) || reply->type != REDIS_REPLY_INTEGER)
        {
            disconnect_locked();
            return false;
        }
        return true;
    }

private:
    /* 将数据按版本和类型隔离到 Gateway 专用 Redis namespace。 */
    std::string key(const std::string &kind, const std::string &suffix) const
    {
        return config_.redis_key_prefix + ":routing:v1:" + kind + ":" + suffix;
    }

    /* 同时检查 hiredis 连接和 reply 类型，避免把 Redis 错误回复当成正常状态。 */
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
        timeval connect_timeout{
            config_.redis_connect_timeout_ms / 1000,
            (config_.redis_connect_timeout_ms % 1000) * 1000};
        context_ = redisConnectWithTimeout(config_.redis_host.c_str(), config_.redis_port,
                                           connect_timeout);
        if (context_ == nullptr || context_->err != 0)
        {
            disconnect_locked();
            return false;
        }
        timeval command_timeout{
            config_.redis_command_timeout_ms / 1000,
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
            if (!valid_locked(auth.get()) || auth->type != REDIS_REPLY_STATUS)
            {
                disconnect_locked();
                return false;
            }
        }
        if (config_.redis_database != 0)
        {
            Reply select(static_cast<redisReply *>(
                redisCommand(context_, "SELECT %u", config_.redis_database)));
            if (!valid_locked(select.get()) || select->type != REDIS_REPLY_STATUS)
            {
                disconnect_locked();
                return false;
            }
        }
        return true;
    }

    /* 释放失效的 hiredis context；必须在 mutex 已锁定时调用。 */
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

/* 创建生产 Redis 路由 Store。 */
HiredisRoutingStore::HiredisRoutingStore(GatewayConfig config)
    : impl_(std::make_unique<Impl>(std::move(config)))
{
}

/* 释放 Pimpl 后关闭底层 hiredis 连接。 */
HiredisRoutingStore::~HiredisRoutingStore() = default;

/* 对外转发 Redis 连通性检查。 */
bool HiredisRoutingStore::ping() { return impl_->ping(); }

std::optional<RoutingSnapshot> HiredisRoutingStore::snapshot(
    const std::vector<std::string> &candidate_fingerprints,
    std::int64_t current_ms)
{
    return impl_->snapshot(candidate_fingerprints, current_ms);
}

bool HiredisRoutingStore::record(const std::string &candidate_fingerprint,
                                 bool success,
                                 bool retryable_failure,
                                 long retry_after_ms,
                                 std::int64_t current_ms,
                                 unsigned failure_threshold)
{
    return impl_->record(candidate_fingerprint, success, retryable_failure,
                         retry_after_ms, current_ms, failure_threshold);
}

class RoutingRuntime::Impl
{
public:


    Impl(GatewayConfig config, RoutingStore &store)
        : config_(std::move(config)), store_(store)
    {
        for (std::size_t index = 0; index < config_.redis_workers; ++index)
        {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }


    ~Impl()
    {
        stopping_.store(true);
        condition_.notify_all();
        for (auto &worker : workers_)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
    }



    void plan(RouteRequest request, PlanCallback callback)
    {
        auto shared_callback = std::make_shared<PlanCallback>(std::move(callback));
        if (!enqueue([this, request = std::move(request), shared_callback]() mutable {
                RoutePlan result;
                try
                {
                    std::vector<std::string> fingerprints;
                    fingerprints.reserve(request.candidates.size());
                    for (const auto &candidate : request.candidates)
                    {
                        fingerprints.push_back(candidate.fingerprint);
                    }
                    auto snapshot = store_.snapshot(fingerprints, now_ms());
                    if (!snapshot)
                    {
                        ready_.store(false);
                        result.status = RouteStatus::unavailable;
                    }
                    else
                    {
                        ready_.store(true);
                        for (auto &candidate : request.candidates)
                        {
                            const auto state = snapshot->candidates.find(candidate.fingerprint);
                            if (state != snapshot->candidates.end() && !state->second.circuit_open)
                            {
                                result.candidates.push_back(std::move(candidate));
                            }
                        }
                        const std::string seed = request.request_id;
                        const bool load_mode = request.scheduling_mode == "load_balance";
                        std::sort(result.candidates.begin(), result.candidates.end(),
                            [&](const ModelTarget &left, const ModelTarget &right) {
                                if (left.priority != right.priority)
                                {
                                    return left.priority < right.priority;
                                }
                                if (load_mode)
                                {
                                    const auto left_hash = rendezvous_hash(seed, left.fingerprint);
                                    const auto right_hash = rendezvous_hash(seed, right.fingerprint);
                                    if (left_hash != right_hash)
                                    {
                                        return left_hash > right_hash;
                                    }
                                }
                                if (left.mapping_name != right.mapping_name)
                                {
                                    return left.mapping_name < right.mapping_name;
                                }
                                return left.fingerprint < right.fingerprint;
                            });
                        if (result.candidates.empty())
                        {
                            result.status = RouteStatus::no_candidates;
                        }
                        else
                        {
                            result.status = RouteStatus::ready;
                            const std::size_t limit = std::min<std::size_t>(
                                {request.max_attempts, result.candidates.size(), 10});
                            result.candidates.resize(limit);
                        }
                    }
                }
                catch (...)
                {
                    ready_.store(false);
                    result = {};
                }
                try
                {
                    (*shared_callback)(std::move(result));
                }
                catch (...)
                {
                }
            }))
        {
            ready_.store(false);
            (*shared_callback)({RouteStatus::unavailable, {}});
        }
    }

    /* 将成功/可重试失败反馈异步写回 Redis；写入失败会使 Gateway routing readiness 变为 false。 */
    void record(std::string fingerprint,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                FeedbackCallback callback,
                unsigned failure_threshold = 0)
    {
        auto shared_callback = std::make_shared<FeedbackCallback>(std::move(callback));
        if (!enqueue([this, fingerprint = std::move(fingerprint), success, retryable_failure,
                      retry_after_ms, failure_threshold, shared_callback]() mutable {
                const bool stored = store_.record(fingerprint, success,
                                                  retryable_failure, retry_after_ms, now_ms(),
                                                  failure_threshold);
                ready_.store(stored);
                try
                {
                    (*shared_callback)(stored);
                }
                catch (...)
                {
                }
            }))
        {
            ready_.store(false);
            (*shared_callback)(false);
        }
    }

    bool ready() const { return ready_.load(); }

private:


    bool enqueue(std::function<void()> task)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_.load() || tasks_.size() >= config_.redis_queue_size)
            {
                return false;
            }
            tasks_.push_back(std::move(task));
        }
        condition_.notify_one();
        return true;
    }



    void worker_loop()
    {
        auto next_ping = SteadyClock::now();
        while (!stopping_.load())
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                condition_.wait_for(lock, std::chrono::milliseconds(250), [this] {
                    return stopping_.load() || !tasks_.empty();
                });
                if (stopping_.load() && tasks_.empty())
                {
                    return;
                }
                if (!tasks_.empty())
                {
                    task = std::move(tasks_.front());
                    tasks_.pop_front();
                }
            }
            if (task)
            {
                try
                {
                    task();
                }
                catch (...)
                {
                    ready_.store(false);
                }
            }
            if (SteadyClock::now() >= next_ping)
            {
                ready_.store(store_.ping());
                next_ping = SteadyClock::now() + std::chrono::seconds(1);
            }
        }
    }

    GatewayConfig config_;
    RoutingStore &store_;
    std::atomic_bool stopping_{false};
    std::atomic_bool ready_{false};
    std::mutex mutex_;
    std::condition_variable condition_;
    std::deque<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
};

RoutingRuntime::RoutingRuntime(GatewayConfig config, RoutingStore &store)
    : impl_(std::make_unique<Impl>(std::move(config), store))
{
}

RoutingRuntime::~RoutingRuntime() = default;

void RoutingRuntime::plan(RouteRequest request, PlanCallback callback)
{
    impl_->plan(std::move(request), std::move(callback));
}

/* 对外提交一次普通 Provider 调用的熔断反馈。 */
void RoutingRuntime::record(std::string candidate_fingerprint,
                            bool success,
                            bool retryable_failure,
                            long retry_after_ms,
                            FeedbackCallback callback)
{
    impl_->record(std::move(candidate_fingerprint), success,
                  retryable_failure, retry_after_ms, std::move(callback));
}

/* 返回最近一次 Redis 操作/心跳确认的可用性。 */
bool RoutingRuntime::ready() const { return impl_->ready(); }
}
