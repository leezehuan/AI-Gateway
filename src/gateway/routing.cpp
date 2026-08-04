#include "gateway/routing.hpp"

#include <hiredis/hiredis.h>
#include <openssl/hmac.h>
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

/*
 * 多候选路由的共享 Redis 运行态。
 *
 * AuthSnapshot 给出“静态允许”的 ModelTarget 集合；RoutingRuntime 再从 Redis 读取会话亲和、健康分和
 * 熔断状态，过滤不可用候选并排序。fixed_order 以 priority 为主，load_balance 用 rendezvous hash 稳定分散，
 * cache_affinity 优先复用成功目标。所有节点写入同一份 Redis 状态，所以一个节点观察到连续 5xx 后，
 * 其他节点也会跳过熔断候选；Redis 不可用时不退化到本地内存路由。
 */
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

/* 把二进制摘要编码成稳定的十六进制 Redis key 后缀，不暴露原始会话提示。 */
std::string hex(const unsigned char *bytes, std::size_t size)
{
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (std::size_t index = 0; index < size; ++index)
    {
        output << std::setw(2) << static_cast<unsigned>(bytes[index]);
    }
    return output.str();
}

/*
 * 函数名直译：亲和性摘要。
 *
 * 通俗说：把 Key ID、协议、逻辑模型、客户端类别、会话提示和配置版本混合后做 HMAC，
 * 得到一个不能反推出原始会话内容的 Redis key 后缀。
 *
 * 专业说法：使用 Gateway pepper 的 HMAC-SHA256 形成分区亲和键，配置版本参与摘要，
 * 因而管理员更新路由后旧 affinity 不会误命中新配置。
 *
 * 注意：绝不直接把 session、prompt 或完整 API Key 作为 Redis key 保存。
 */
std::string affinity_digest(const GatewayConfig &config, const RouteRequest &request)
{
    const std::string input = request.public_api_key_id + "\x1f" + request.protocol + "\x1f" +
        request.logical_model + "\x1f" + request.client_family + "\x1f" +
        request.session_hint + "\x1f" + std::to_string(request.config_version);
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned digest_size = 0;
    if (HMAC(EVP_sha256(), config.api_key_hmac_pepper.data(),
             static_cast<int>(config.api_key_hmac_pepper.size()),
             reinterpret_cast<const unsigned char *>(input.data()), input.size(), digest,
             &digest_size) == nullptr || digest_size != SHA256_DIGEST_LENGTH)
    {
        throw std::runtime_error("routing affinity digest failed");
    }
    return hex(digest, digest_size);
}

/*
 * 函数名直译：Rendezvous 哈希。
 *
 * 通俗说：同一 seed 对每个候选算一个分数，分数最高者优先。候选增减时大多数请求仍会落在原目标，
 * 比简单取模更适合多节点稳定分流。
 *
 * 专业说法：这是 Highest Random Weight hashing 的一个确定性实现。
 */
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

/* 将连续健康分压缩为三个排序桶，避免极小分数差导致路由频繁抖动。 */
int health_bucket(int score)
{
    if (score >= 80)
    {
        return 2;
    }
    if (score >= 50)
    {
        return 1;
    }
    return 0;
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
} // namespace

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

    /*
     * 函数名直译：Ping Redis。
 *
     * 通俗说：发送 PING 确认共享路由运行态可用；失败立即断开，下次操作重新连接。
 *
     * 专业说法：RoutingRuntime 每秒用它维护 readiness，不会在 Beast I/O 线程中直接执行。
     */
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

    /*
     * 函数名直译：读取路由快照。
 *
     * 通俗说：一次规划路由时，读取会话上次成功的目标，以及所有候选当前健康分和熔断状态。
 * 熔断冷却结束时，只有抢到 NX probe lease 的一个节点可以先尝试恢复目标。
 *
     * 专业说法：该函数把 affinity GET、候选 HMGET 和 half-open SET NX PX 组合成一个 worker 内的 Redis 读取过程。
 * Redis 任一操作异常即返回 nullopt，让请求 fail closed 而不是退化为节点私有判断。
     */
    std::optional<RoutingSnapshot> snapshot(const std::string &affinity_key,
                                            const std::vector<std::string> &fingerprints,
                                            std::int64_t current_ms,
                                            long probe_lease_ms)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return std::nullopt;
        }

        RoutingSnapshot result;
        if (!affinity_key.empty())
        {
            Reply affinity(static_cast<redisReply *>(
                redisCommand(context_, "GET %b", affinity_key.data(), affinity_key.size())));
            if (!valid_locked(affinity.get()))
            {
                disconnect_locked();
                return std::nullopt;
            }
            if (affinity->type == REDIS_REPLY_STRING)
            {
                result.affinity_target = std::string(affinity->str, affinity->len);
            }
        }

        for (const auto &fingerprint : fingerprints)
        {
            const std::string health_key = key("health", fingerprint);
            Reply health(static_cast<redisReply *>(redisCommand(
                context_, "HMGET %b score failures open_until_ms", health_key.data(),
                health_key.size())));
            if (!valid_locked(health.get()) || health->type != REDIS_REPLY_ARRAY ||
                health->elements != 3)
            {
                disconnect_locked();
                return std::nullopt;
            }
            CandidateRoutingState state;
            state.health_score = static_cast<int>(reply_integer(health->element[0], 100));
            const auto open_until = reply_integer(health->element[2], 0);
            if (open_until > current_ms)
            {
                state.circuit_open = true;
            }
            else if (open_until > 0)
            {
                const std::string probe_key = key("probe", fingerprint);
                Reply probe(static_cast<redisReply *>(redisCommand(
                    context_, "SET %b 1 NX PX %ld", probe_key.data(), probe_key.size(),
                    probe_lease_ms)));
                if (!valid_locked(probe.get()))
                {
                    disconnect_locked();
                    return std::nullopt;
                }
                state.half_open_probe = probe->type == REDIS_REPLY_STATUS;
                state.circuit_open = !state.half_open_probe;
            }
            result.candidates.emplace(fingerprint, state);
        }
        return result;
    }

    /*
     * 函数名直译：记录路由反馈。
 *
     * 通俗说：一次候选调用完成后，原子更新健康分、连续失败次数、熔断时间，必要时写入或删除会话亲和目标。
 *
     * 专业说法：Lua 脚本使多个 Gateway 节点对同一候选的“读改写”保持原子；429 的 Retry-After 会延长冷却，
 * 但上限为五分钟。
 *
     * 注意：只有调用方确认成功终态时才携带 affinity_key，避免失败请求把会话粘到坏目标。
     */
    bool record(const std::string &fingerprint,
                const std::string &affinity_key,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                std::int64_t current_ms,
                unsigned failure_threshold)
    {
        static const std::string script =
            "local score=tonumber(redis.call('HGET',KEYS[1],'score')) or 100; "
            "local failures=tonumber(redis.call('HGET',KEYS[1],'failures')) or 0; "
            "local open_until=tonumber(redis.call('HGET',KEYS[1],'open_until_ms')) or 0; "
            "if ARGV[1]=='1' then score=math.min(100,score+5); failures=0; open_until=0; "
            "elseif ARGV[2]=='1' then score=math.max(0,score-20); failures=failures+1; "
            "if failures>=tonumber(ARGV[3]) then "
            "open_until=tonumber(ARGV[4])+math.max(tonumber(ARGV[5]),tonumber(ARGV[6])); end; end; "
            "redis.call('HSET',KEYS[1],'score',score,'failures',failures,'open_until_ms',open_until); "
            "redis.call('PEXPIRE',KEYS[1],ARGV[7]); redis.call('DEL',KEYS[2]); "
            "if ARGV[1]=='1' and ARGV[8]=='1' then redis.call('SET',KEYS[3],ARGV[10],'PX',ARGV[9]); "
            "elseif ARGV[1]=='0' and ARGV[8]=='1' and redis.call('GET',KEYS[3])==ARGV[10] "
            "then redis.call('DEL',KEYS[3]); end; return 1;";

        std::lock_guard<std::mutex> lock(mutex_);
        if (!ensure_locked())
        {
            return false;
        }
        const std::string health_key = key("health", fingerprint);
        const std::string probe_key = key("probe", fingerprint);
        const std::string safe_affinity = affinity_key.empty()
                                              ? key("no_affinity", "unused")
                                              : affinity_key;
        const std::string success_value = success ? "1" : "0";
        const std::string retryable_value = retryable_failure ? "1" : "0";
        const std::string threshold = std::to_string(
            failure_threshold == 0 ? config_.circuit_failure_threshold : failure_threshold);
        const std::string now = std::to_string(current_ms);
        const std::string cooldown = std::to_string(config_.circuit_open_ms);
        const std::string retry_after = std::to_string(std::min<long>(300000, retry_after_ms));
        const std::string health_ttl =
            std::to_string(config_.routing_health_ttl_seconds * 1000);
        const std::string has_affinity = affinity_key.empty() ? "0" : "1";
        const std::string affinity_ttl = std::to_string(config_.affinity_ttl_seconds * 1000);
        Reply reply(static_cast<redisReply *>(redisCommand(
            context_, "EVAL %b 3 %b %b %b %s %s %s %s %s %s %s %s %s %b",
            script.data(), script.size(), health_key.data(), health_key.size(), probe_key.data(),
            probe_key.size(), safe_affinity.data(), safe_affinity.size(), success_value.c_str(),
            retryable_value.c_str(), threshold.c_str(), now.c_str(), cooldown.c_str(),
            retry_after.c_str(), health_ttl.c_str(), has_affinity.c_str(), affinity_ttl.c_str(),
            fingerprint.data(), fingerprint.size())));
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

    /*
     * 函数名直译：确保已连接。
 *
     * 通俗说：需要 Redis 时复用健康连接；断开时按超时配置重连、认证并选择 DB。
 *
     * 专业说法：调用者已持有 mutex。失败路径会清理 context，保证下一次可从干净状态重试。
 *
     * 注意：密码只作为 hiredis 二进制参数传递，绝不记录。
     */
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
    const std::string &affinity_key,
    const std::vector<std::string> &candidate_fingerprints,
    std::int64_t current_ms,
    long probe_lease_ms)
{
    return impl_->snapshot(affinity_key, candidate_fingerprints, current_ms, probe_lease_ms);
}

bool HiredisRoutingStore::record(const std::string &candidate_fingerprint,
                                 const std::string &affinity_key,
                                 bool success,
                                 bool retryable_failure,
                                 long retry_after_ms,
                                 std::int64_t current_ms,
                                 unsigned failure_threshold)
{
    return impl_->record(candidate_fingerprint, affinity_key, success, retryable_failure,
                         retry_after_ms, current_ms, failure_threshold);
}

class RoutingRuntime::Impl
{
public:
    /*
     * 函数名直译：路由运行时构造函数。
 *
     * 通俗说：启动固定数量 Redis worker。HTTP 请求只把路由任务排队，不会在 Beast 事件循环里等待网络 Redis。
 *
     * 专业说法：有界任务队列 + 固定 worker pool 是 RoutingStore 的异步隔离层。
     */
    Impl(GatewayConfig config, RoutingStore &store)
        : config_(std::move(config)), store_(store)
    {
        for (std::size_t index = 0; index < config_.redis_workers; ++index)
        {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    /* 通知 worker 停止并 join，确保没有回调仍访问已销毁的 Store。 */
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

    /*
     * 函数名直译：规划路由。
 *
     * 通俗说：从允许的候选中去掉已熔断目标，再按固定优先级、健康桶、会话亲和或稳定负载均衡排序，
 * 最后限制为本请求最多可尝试的数量。
 *
     * 专业说法：异步读取 RoutingSnapshot 后执行 pure in-memory 排序；cache_affinity 首选 Redis 记录的成功目标，
 * 未命中时退化为 rendezvous hash。回调始终在 Redis worker 线程触发，上层负责切回自己的执行上下文。
     */
    void plan(RouteRequest request, PlanCallback callback)
    {
        auto shared_callback = std::make_shared<PlanCallback>(std::move(callback));
        if (!enqueue([this, request = std::move(request), shared_callback]() mutable {
                RoutePlan result;
                try
                {
                    std::string affinity_key;
                    if (request.scheduling_mode == "cache_affinity" &&
                        !request.session_hint.empty())
                    {
                        affinity_key = config_.redis_key_prefix + ":routing:v1:affinity:" +
                            affinity_digest(config_, request);
                    }
                    std::vector<std::string> fingerprints;
                    fingerprints.reserve(request.candidates.size());
                    for (const auto &candidate : request.candidates)
                    {
                        fingerprints.push_back(candidate.fingerprint);
                    }
                    auto snapshot = store_.snapshot(affinity_key, fingerprints, now_ms(),
                                                    config_.circuit_probe_lease_ms);
                    if (!snapshot)
                    {
                        ready_.store(false);
                        result.status = RouteStatus::unavailable;
                    }
                    else
                    {
                        ready_.store(true);
                        result.affinity_key = std::move(affinity_key);
                        for (auto &candidate : request.candidates)
                        {
                            const auto state = snapshot->candidates.find(candidate.fingerprint);
                            if (state != snapshot->candidates.end() && !state->second.circuit_open)
                            {
                                result.candidates.push_back(std::move(candidate));
                            }
                        }
                        const std::string seed = !result.affinity_key.empty()
                                                     ? result.affinity_key
                                                     : request.request_id;
                        const auto affinity_target = snapshot->affinity_target;
                        const bool affinity_mode = request.scheduling_mode == "cache_affinity";
                        const bool load_mode = request.scheduling_mode == "load_balance" ||
                                               affinity_mode;
                        std::sort(result.candidates.begin(), result.candidates.end(),
                            [&](const ModelTarget &left, const ModelTarget &right) {
                                if (affinity_mode && affinity_target)
                                {
                                    const bool left_match = left.fingerprint == *affinity_target;
                                    const bool right_match = right.fingerprint == *affinity_target;
                                    if (left_match != right_match)
                                    {
                                        return left_match;
                                    }
                                }
                                if (left.priority != right.priority)
                                {
                                    return left.priority < right.priority;
                                }
                                const int left_score = snapshot->candidates.at(
                                    left.fingerprint).health_score;
                                const int right_score = snapshot->candidates.at(
                                    right.fingerprint).health_score;
                                const int left_bucket = health_bucket(left_score);
                                const int right_bucket = health_bucket(right_score);
                                if (left_bucket != right_bucket)
                                {
                                    return left_bucket > right_bucket;
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
            (*shared_callback)({RouteStatus::unavailable, {}, {}});
        }
    }

    /* 将成功/可重试失败反馈异步写回 Redis；写入失败会使 Gateway routing readiness 变为 false。 */
    void record(std::string fingerprint,
                std::string affinity_key,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                FeedbackCallback callback,
                unsigned failure_threshold = 0)
    {
        auto shared_callback = std::make_shared<FeedbackCallback>(std::move(callback));
        if (!enqueue([this, fingerprint = std::move(fingerprint),
                      affinity_key = std::move(affinity_key), success, retryable_failure,
                      retry_after_ms, failure_threshold, shared_callback]() mutable {
                const bool stored = store_.record(fingerprint, affinity_key, success,
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
    /*
     * 函数名直译：入队任务。
 *
     * 通俗说：队列已满或正在停机时拒绝新 Redis 工作，调用方据此返回 routing_unavailable。
 *
     * 专业说法：mutex 保护有界 deque，condition_variable 只唤醒一个空闲 worker。
     */
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

    /*
     * 函数名直译：工作线程循环。
 *
     * 通俗说：不断取出一个路由任务执行，并每秒 Ping Redis 更新 ready 状态。
 *
     * 专业说法：worker 捕获任务异常以避免单个坏回调杀死线程；stopping_ 使析构可以有序退出。
     */
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

/* 创建 Gateway 级异步路由模块。 */
RoutingRuntime::RoutingRuntime(GatewayConfig config, RoutingStore &store)
    : impl_(std::make_unique<Impl>(std::move(config), store))
{
}

/* 销毁内部 worker pool。 */
RoutingRuntime::~RoutingRuntime() = default;

/* 对外提交一次候选排序任务。 */
void RoutingRuntime::plan(RouteRequest request, PlanCallback callback)
{
    impl_->plan(std::move(request), std::move(callback));
}

/* 对外提交一次普通 Provider 调用的健康/亲和反馈。 */
void RoutingRuntime::record(std::string candidate_fingerprint,
                            std::string affinity_key,
                            bool success,
                            bool retryable_failure,
                            long retry_after_ms,
                            FeedbackCallback callback)
{
    impl_->record(std::move(candidate_fingerprint), std::move(affinity_key), success,
                  retryable_failure, retry_after_ms, std::move(callback));
}

/* 健康探测失败按可重试失败计入熔断，但不写会话亲和。 */
void RoutingRuntime::record_health_probe(std::string candidate_fingerprint,
                                         bool success,
                                         FeedbackCallback callback)
{
    impl_->record(std::move(candidate_fingerprint), {}, success, !success, 0,
                  std::move(callback), 3);
}

/* 返回最近一次 Redis 操作/心跳确认的可用性。 */
bool RoutingRuntime::ready() const { return impl_->ready(); }
} // namespace ai_gateway
