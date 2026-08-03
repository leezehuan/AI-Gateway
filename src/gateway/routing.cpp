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

namespace ai_gateway
{
namespace
{
using SystemClock = std::chrono::system_clock;
using SteadyClock = std::chrono::steady_clock;

struct ReplyDeleter
{
    void operator()(redisReply *reply) const
    {
        if (reply != nullptr)
        {
            freeReplyObject(reply);
        }
    }
};
using Reply = std::unique_ptr<redisReply, ReplyDeleter>;

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

std::int64_t now_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        SystemClock::now().time_since_epoch()).count();
}

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
    explicit Impl(GatewayConfig config)
        : config_(std::move(config))
    {
    }

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

    bool record(const std::string &fingerprint,
                const std::string &affinity_key,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                std::int64_t current_ms)
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
        const std::string threshold = std::to_string(config_.circuit_failure_threshold);
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
    std::string key(const std::string &kind, const std::string &suffix) const
    {
        return config_.redis_key_prefix + ":routing:v1:" + kind + ":" + suffix;
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

HiredisRoutingStore::HiredisRoutingStore(GatewayConfig config)
    : impl_(std::make_unique<Impl>(std::move(config)))
{
}

HiredisRoutingStore::~HiredisRoutingStore() = default;

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
                                 std::int64_t current_ms)
{
    return impl_->record(candidate_fingerprint, affinity_key, success, retryable_failure,
                         retry_after_ms, current_ms);
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

    void record(std::string fingerprint,
                std::string affinity_key,
                bool success,
                bool retryable_failure,
                long retry_after_ms,
                FeedbackCallback callback)
    {
        auto shared_callback = std::make_shared<FeedbackCallback>(std::move(callback));
        if (!enqueue([this, fingerprint = std::move(fingerprint),
                      affinity_key = std::move(affinity_key), success, retryable_failure,
                      retry_after_ms, shared_callback]() mutable {
                const bool stored = store_.record(fingerprint, affinity_key, success,
                                                  retryable_failure, retry_after_ms, now_ms());
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

bool RoutingRuntime::ready() const { return impl_->ready(); }
} // namespace ai_gateway
