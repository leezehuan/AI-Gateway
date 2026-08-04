#include "gateway/runtime.hpp"

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iomanip>
#include <list>
#include <mutex>
#include <regex>
#include <sstream>
#include <thread>
#include <utility>

/*
 * 身份与配置运行时。
 *
 * RuntimeState 将阻塞的 MySQL Repository 隔离到固定 worker pool：HTTP 线程只投递任务和接收回调。
 * 它组合 AuthSnapshot、验证 API Key HMAC、解析 env:/file: Secret 引用、缓存认证结果并轮询配置版本。
 * 版本轮询或数据库失败时立即 fail closed，即使本地缓存命中也不继续认证，避免禁用 Key 后出现权限窗口。
 */
namespace ai_gateway
{
namespace
{
using Clock = std::chrono::steady_clock;

/* 把二进制摘要转换成固定长度十六进制文本，供日志外的公开 fingerprint 使用。 */
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

/* 使用进程配置的 HMAC pepper 对完整 Gateway Key 做不可逆校验摘要。 */
std::string hmac_key(const std::string &pepper, const std::string &key)
{
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned digest_size = 0;
    if (HMAC(EVP_sha256(), pepper.data(), static_cast<int>(pepper.size()),
             reinterpret_cast<const unsigned char *>(key.data()), key.size(), digest,
             &digest_size) == nullptr || digest_size != 32)
    {
        throw std::runtime_error("API Key verification failed");
    }
    return std::string(reinterpret_cast<char *>(digest), digest_size);
}

/* 根据配置版本和数据库 mapping 身份计算不含 Secret 的候选指纹。 */
std::string candidate_fingerprint(std::uint64_t config_version,
                                  const RepositoryMapping &mapping)
{
    const std::string identity = std::to_string(config_version) + "\x1f" +
        std::to_string(mapping.mapping_id) + "\x1f" + std::to_string(mapping.provider_id) +
        "\x1f" + std::to_string(mapping.endpoint_id) + "\x1f" +
        std::to_string(mapping.credential_id) + "\x1f" + mapping.upstream_model;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(identity.data()), identity.size(), digest);
    return hex(digest, sizeof(digest));
}

/* RepositoryAccessRecord 版本的候选指纹重载。 */
std::string candidate_fingerprint(const RepositoryAccessRecord &record,
                                  const RepositoryMapping &mapping)
{
    return candidate_fingerprint(record.config_version, mapping);
}

/* 常量时间比较两个摘要，避免 Key 校验因首个不同字节泄漏比较进度。 */
bool constant_time_equal(std::string_view left, std::string_view right)
{
    std::size_t difference = left.size() ^ right.size();
    const std::size_t count = std::max(left.size(), right.size());
    for (std::size_t index = 0; index < count; ++index)
    {
        const unsigned char a = index < left.size() ? left[index] : 0;
        const unsigned char b = index < right.size() ? right[index] : 0;
        difference |= a ^ b;
    }
    return difference == 0;
}

/* 校验 aigw_<12字符前缀>_<43字符base64url> 格式并提取公开前缀。 */
bool parse_key(const std::string &key, std::string &prefix)
{
    static const std::regex format("^aigw_([A-Za-z0-9_-]{12})_[A-Za-z0-9_-]{43}$");
    std::smatch match;
    if (!std::regex_match(key, match, format))
    {
        return false;
    }
    prefix = match[1].str();
    return true;
}

/* Provider Endpoint 只允许管理员配置的 HTTP(S) URL。 */
bool supported_url(const std::string &url)
{
    return url.rfind("http://", 0) == 0 || url.rfind("https://", 0) == 0;
}

/* 删除 Secret 文件末尾换行，并拒绝空 Secret。 */
std::string trim_secret(std::string value)
{
    while (!value.empty() && (value.back() == '\n' || value.back() == '\r'))
    {
        value.pop_back();
    }
    if (value.empty())
    {
        throw std::runtime_error("Provider Credential is unavailable");
    }
    return value;
}

/*
 * 函数名直译：解析 Provider Secret 引用。
 *
 * 通俗说：数据库只保存 env:NAME 或 file:basename，不保存真实密钥；请求真正要发给 Provider
 * 时才从环境变量或安全目录读取，并且不允许路径穿越、符号链接和超过 64 KiB 的文件。
 *
 * 专业说法：这是 Credential Secret Adapter，使用 openat + O_NOFOLLOW 约束文件边界。
 *
 * 参数说明：config 提供 Secret 目录；reference 是数据库中的引用字符串。
 * 返回值：真实 Secret，仅供内存中的 ProviderRequest 使用；解析失败抛出脱敏异常。
 */
std::string resolve_secret(const GatewayConfig &config, const std::string &reference)
{
    if (reference.rfind("env:", 0) == 0)
    {
        const std::string name = reference.substr(4);
        static const std::regex valid_name("^[A-Za-z_][A-Za-z0-9_]*$");
        if (!std::regex_match(name, valid_name))
        {
            throw std::runtime_error("Provider Credential is unavailable");
        }
        const char *value = std::getenv(name.c_str());
        if (value == nullptr || value[0] == '\0' || std::strlen(value) > 64 * 1024)
        {
            throw std::runtime_error("Provider Credential is unavailable");
        }
        return value;
    }
    if (reference.rfind("file:", 0) != 0)
    {
        throw std::runtime_error("Provider Credential is unavailable");
    }
    const std::string basename = reference.substr(5);
    static const std::regex valid_basename("^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$");
    if (!std::regex_match(basename, valid_basename) || basename == "." || basename == "..")
    {
        throw std::runtime_error("Provider Credential is unavailable");
    }

    const int directory = open(config.secret_dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (directory < 0)
    {
        throw std::runtime_error("Provider Credential is unavailable");
    }
    const int file = openat(directory, basename.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    close(directory);
    if (file < 0)
    {
        throw std::runtime_error("Provider Credential is unavailable");
    }
    struct stat info
    {
    };
    if (fstat(file, &info) != 0 || !S_ISREG(info.st_mode) || info.st_size < 1 ||
        info.st_size > 64 * 1024)
    {
        close(file);
        throw std::runtime_error("Provider Credential is unavailable");
    }
    std::string value(static_cast<std::size_t>(info.st_size), '\0');
    std::size_t offset = 0;
    while (offset < value.size())
    {
        const ssize_t bytes = read(file, value.data() + offset, value.size() - offset);
        if (bytes <= 0)
        {
            close(file);
            throw std::runtime_error("Provider Credential is unavailable");
        }
        offset += static_cast<std::size_t>(bytes);
    }
    close(file);
    return trim_secret(std::move(value));
}

/*
 * 函数名直译：构造鉴权结果。
 *
 * 通俗说：从同一个 Key 前缀查出的候选记录中，常量时间比较 HMAC，确认 Key/Tenant/Policy
 * 未禁用且未过期，然后合并协议 grant、模型 grant、Provider grant 和 Mapping，形成一次不可变快照。
 *
 * 专业说法：这是 RuntimeState 的 Auth Snapshot builder；它在数据库线程执行，负责把多张关系表
 * 的一致性查询结果压缩为请求线程可读的领域对象。
 *
 * 注意：任何 Secret、URL 不可用或 Mapping 关联不一致只会标记配置不可用/跳过候选，不把底层异常
 * 透传给客户端。
 */
AuthResult build_result(const GatewayConfig &config,
                        const std::vector<RepositoryAccessRecord> &records,
                        const std::string &candidate_hmac)
{
    const RepositoryAccessRecord *matched = nullptr;
    for (const auto &record : records)
    {
        if (constant_time_equal(record.key_hmac, candidate_hmac))
        {
            matched = &record;
        }
    }
    if (matched == nullptr || matched->key_status != "active")
    {
        return {AuthStatus::invalid_api_key, {}};
    }
    const auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    if (matched->has_expiry && matched->expires_at_epoch <= now)
    {
        return {AuthStatus::invalid_api_key, {}};
    }
    if (matched->tenant_status != "active" || matched->policy_status != "active" ||
        (matched->tenant_quota && matched->tenant_quota->status != "active") ||
        (matched->api_key_quota && matched->api_key_quota->status != "active"))
    {
        return {AuthStatus::access_disabled, {}};
    }

    auto snapshot = std::make_shared<AuthSnapshot>();
    snapshot->config_version = matched->config_version;
    snapshot->database_tenant_id = matched->database_tenant_id;
    snapshot->database_api_key_id = matched->database_key_id;
    snapshot->tenant_slug = matched->tenant_slug;
    snapshot->public_api_key_id = matched->public_key_id;
    snapshot->tenant_quota = matched->tenant_quota;
    snapshot->api_key_quota = matched->api_key_quota;
    snapshot->protocols = matched->protocols;
    std::unordered_map<std::uint64_t, ModelAccess *> by_id;
    for (const auto &model : matched->models)
    {
        ModelAccess access;
        access.database_id = model.id;
        access.name = model.name;
        access.protocol = model.protocol;
        access.scheduling_mode = model.scheduling_mode;
        access.max_attempts = model.max_attempts;
        access.model_granted = model.granted && model.status == "active";
        auto inserted = snapshot->models.emplace(model.protocol + "\x1f" + model.name,
                                                 std::move(access));
        by_id[model.id] = &inserted.first->second;
    }
    for (const auto &mapping : matched->mappings)
    {
        const auto model = by_id.find(mapping.logical_model_id);
        if (model == by_id.end() || !model->second->model_granted ||
            mapping.mapping_status != "active" || mapping.provider_status != "active" ||
            mapping.endpoint_status != "active" || mapping.credential_status != "active" ||
            mapping.endpoint_protocol != model->second->protocol ||
            mapping.provider_id != mapping.endpoint_provider_id ||
            mapping.provider_id != mapping.credential_provider_id)
        {
            continue;
        }
        ++model->second->active_mapping_count;
        if (matched->providers.count(mapping.provider_id) == 0)
        {
            model->second->provider_denied = true;
            continue;
        }
        if (!supported_url(mapping.endpoint_url) || mapping.upstream_model.empty())
        {
            model->second->configuration_unavailable = true;
            continue;
        }
        if (mapping.credential_quota && mapping.credential_quota->status != "active")
        {
            model->second->configuration_unavailable = true;
            continue;
        }
        try
        {
            ModelTarget target;
            target.mapping_id = mapping.mapping_id;
            target.provider_id = mapping.provider_id;
            target.endpoint_id = mapping.endpoint_id;
            target.credential_id = mapping.credential_id;
            target.mapping_name = mapping.mapping_name;
            target.priority = mapping.priority;
            target.fingerprint = candidate_fingerprint(*matched, mapping);
            target.provider_url = mapping.endpoint_url;
            target.provider_api_key = resolve_secret(config, mapping.secret_ref);
            target.upstream_model = mapping.upstream_model;
            target.provider_slug = mapping.provider_slug;
            target.credential_name = mapping.credential_name;
            target.credential_quota = mapping.credential_quota;
            for (const auto &price : matched->prices)
            {
                if (price.provider_id == mapping.provider_id &&
                    price.upstream_model == mapping.upstream_model)
                {
                    target.prices.push_back(
                        {price.id, price.version, price.effective_at_epoch,
                         price.input_per_million_microusd,
                         price.cached_input_per_million_microusd,
                         price.output_per_million_microusd});
                }
            }
            model->second->candidates.push_back(std::move(target));
        }
        catch (const std::exception &)
        {
            model->second->configuration_unavailable = true;
        }
    }
    return {AuthStatus::authorized, std::move(snapshot)};
}
} // namespace

class RuntimeState::Impl
{
public:
    /* 启动固定数量数据库 worker，并启动配置版本轮询线程。 */
    Impl(GatewayConfig config, GatewayRepository &repository)
        : config_(std::move(config)), repository_(repository)
    {
        for (std::size_t index = 0; index < config_.database_workers; ++index)
        {
            workers_.emplace_back([this] { worker_loop(); });
        }
        enqueue_poll();
        poller_ = std::thread([this] { poll_loop(); });
    }

    /* 停止接收新任务，唤醒并等待 poller/worker，保证析构后没有后台线程访问成员。 */
    ~Impl()
    {
        stopping_.store(true);
        queue_condition_.notify_all();
        poll_condition_.notify_all();
        if (poller_.joinable())
        {
            poller_.join();
        }
        for (auto &worker : workers_)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
    }

    /* 在 worker 线程完成 HMAC、缓存查询和 Repository Auth Snapshot 加载。 */
    void authenticate(std::string key, AuthCallback callback)
    {
        std::string prefix;
        if (!parse_key(key, prefix))
        {
            callback({AuthStatus::invalid_api_key, {}});
            return;
        }
        std::string digest;
        try
        {
            digest = hmac_key(config_.api_key_hmac_pepper, key);
        }
        catch (...)
        {
            callback({AuthStatus::unavailable, {}});
            return;
        }
        const std::string cache_key = hex(
            reinterpret_cast<const unsigned char *>(digest.data()), digest.size());
        if (!ready_.load())
        {
            callback({AuthStatus::unavailable, {}});
            return;
        }
        if (auto cached = find_cache(cache_key))
        {
            callback(*cached);
            return;
        }
        auto shared_callback = std::make_shared<AuthCallback>(std::move(callback));
        const bool accepted = enqueue(
            [this, prefix = std::move(prefix), digest = std::move(digest),
             cache_key, shared_callback]() mutable {
                if (!ready_.load())
                {
                    (*shared_callback)({AuthStatus::unavailable, {}});
                    return;
                }
                AuthResult result;
                try
                {
                    auto records = repository_.load_access_candidates(prefix);
                    result = build_result(config_, records, digest);
                    if (!records.empty() && records.front().config_version != version_.load())
                    {
                        apply_version(records.front().config_version);
                    }
                    if (result.status != AuthStatus::unavailable)
                    {
                        put_cache(cache_key, result);
                    }
                }
                catch (...)
                {
                    fail_closed();
                    result = {AuthStatus::unavailable, {}};
                }
                try
                {
                    (*shared_callback)(std::move(result));
                }
                catch (...)
                {
                }
            });
        if (!accepted)
        {
            (*shared_callback)({AuthStatus::unavailable, {}});
        }
    }

    /* 写入一次 Provider 调用前的 started Attempt；失败时不允许真正发起请求。 */
    void begin_attempt(AttemptStart attempt, AuditCallback callback)
    {
        auto shared_callback = std::make_shared<AuditCallback>(std::move(callback));
        if (!enqueue([this, attempt = std::move(attempt), shared_callback] {
                bool stored = false;
                try
                {
                    repository_.begin_attempt(attempt);
                    stored = true;
                }
                catch (...)
                {
                    fail_closed();
                }
                try
                {
                    (*shared_callback)(stored);
                }
                catch (...)
                {
                }
            }))
        {
            fail_closed();
            (*shared_callback)(false);
        }
    }

    /* 创建请求级 Usage 和预算预留，并返回 Governance/数据库准入状态。 */
    void admit_request(RequestAdmission request, AdmissionCallback callback)
    {
        auto shared_callback = std::make_shared<AdmissionCallback>(std::move(callback));
        if (!enqueue([this, request = std::move(request), shared_callback] {
                try
                {
                    (*shared_callback)(repository_.admit_request(request));
                }
                catch (...)
                {
                    fail_closed();
                    (*shared_callback)(RequestAdmissionStatus::unavailable);
                }
            }))
        {
            fail_closed();
            (*shared_callback)(RequestAdmissionStatus::unavailable);
        }
    }

    /* 以有限重试写入请求终态；连续失败会使 RuntimeState readiness 失败。 */
    void finish_request(RequestFinish request, AuditCallback callback)
    {
        auto shared_callback = std::make_shared<AuditCallback>(std::move(callback));
        if (!enqueue([this, request = std::move(request), shared_callback] {
                bool stored = false;
                for (unsigned retry = 0; retry < 3 && !stored; ++retry)
                {
                    try
                    {
                        repository_.finish_request(request);
                        stored = true;
                    }
                    catch (...)
                    {
                        if (retry + 1 < 3)
                        {
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(25U << retry));
                        }
                    }
                }
                if (!stored)
                {
                    fail_closed();
                }
                (*shared_callback)(stored);
            }))
        {
            fail_closed();
            (*shared_callback)(false);
        }
    }

    /* 从数据库加载显式配置的非计费健康探测目标。 */
    void load_health_checks(HealthCheckCallback callback)
    {
        auto shared_callback = std::make_shared<HealthCheckCallback>(std::move(callback));
        if (!enqueue([this, shared_callback] {
                HealthCheckResult result;
                try
                {
                    for (const auto &record : repository_.load_health_checks())
                    {
                        HealthCheckTarget target;
                        target.id = record.id;
                        target.config_version = record.config_version;
                        target.credential_id = record.credential_id;
                        target.provider_slug = record.provider_slug;
                        target.credential_name = record.credential_name;
                        target.method = record.method;
                        target.url = record.url;
                        target.provider_api_key = resolve_secret(config_, record.secret_ref);
                        target.interval_ms = record.interval_ms;
                        target.timeout_ms = record.timeout_ms;
                        target.credential_quota = record.credential_quota;
                        for (const auto &mapping : record.mappings)
                        {
                            target.candidate_fingerprints.push_back(
                                candidate_fingerprint(record.config_version, mapping));
                        }
                        result.targets.push_back(std::move(target));
                    }
                    result.available = true;
                }
                catch (...)
                {
                    fail_closed();
                }
                (*shared_callback)(std::move(result));
            }))
        {
            fail_closed();
            (*shared_callback)({});
        }
    }

    /* 幂等更新 Attempt 终态和 Usage 字段，并在失败时 fail closed。 */
    void finish_attempt(AttemptFinish attempt, AuditCallback callback)
    {
        auto shared_callback = std::make_shared<AuditCallback>(std::move(callback));
        if (!enqueue([this, attempt = std::move(attempt), shared_callback] {
                bool stored = false;
                for (unsigned retry = 0; retry < 3 && !stored; ++retry)
                {
                    try
                    {
                        repository_.finish_attempt(attempt);
                        stored = true;
                    }
                    catch (...)
                    {
                        if (retry + 1 < 3)
                        {
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(25U << retry));
                        }
                    }
                }
                if (!stored)
                {
                    fail_closed();
                }
                try
                {
                    (*shared_callback)(stored);
                }
                catch (...)
                {
                }
            }))
        {
            fail_closed();
            (*shared_callback)(false);
        }
    }

    /* 数据库版本轮询成功且未发生 fail_closed 时才允许新的鉴权请求。 */
    bool ready() const { return ready_.load(); }

    /* 返回只读进程配置，不暴露 worker/缓存内部状态。 */
    const GatewayConfig &config() const { return config_; }

private:
    struct CacheEntry
    {
        AuthResult result;
        Clock::time_point inserted;
        std::list<std::string>::iterator lru;
    };

    /*
     * 把数据库相关任务放入有界队列。
     * 队列满或正在停止时返回 false，使调用方立即返回 unavailable，而不是无限堆积请求。
     */
    bool enqueue(std::function<void()> task)
    {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (stopping_.load() || tasks_.size() >= config_.database_queue_size)
            {
                return false;
            }
            tasks_.push_back(std::move(task));
        }
        queue_condition_.notify_one();
        return true;
    }

    /* 固定 worker 的消费循环；任务异常被隔离，避免一个 SQL 失败杀死整个线程。 */
    void worker_loop()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_condition_.wait(lock, [this] {
                    return stopping_.load() || !tasks_.empty();
                });
                if (stopping_.load() && tasks_.empty())
                {
                    return;
                }
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }
            try
            {
                task();
            }
            catch (...)
            {
            }
        }
    }

    /* 按配置间隔投递版本轮询任务，真正 SQL 仍由 worker 队列执行。 */
    void poll_loop()
    {
        std::unique_lock<std::mutex> lock(poll_mutex_);
        while (!stopping_.load())
        {
            poll_condition_.wait_for(
                lock, std::chrono::milliseconds(config_.config_poll_interval_ms),
                [this] { return stopping_.load(); });
            if (!stopping_.load())
            {
                enqueue_poll();
            }
        }
    }

    /*
     * 投递一次配置版本/遗留 Usage 协调任务。
     * poll_pending_ 防止慢数据库时多个定时 tick 在队列中累积；任意失败清空缓存并 fail closed。
     */
    void enqueue_poll()
    {
        bool expected = false;
        if (!poll_pending_.compare_exchange_strong(expected, true))
        {
            return;
        }
        if (!enqueue([this] {
                try
                {
                    repository_.reconcile_abandoned_requests();
                    apply_version(repository_.config_version());
                    ready_.store(true);
                }
                catch (...)
                {
                    fail_closed();
                }
                poll_pending_.store(false);
            }))
        {
            poll_pending_.store(false);
            fail_closed();
        }
    }

    /* 版本变化意味着 Policy/Mapping/Key 可能已修改，必须立刻失效所有 Auth Snapshot 缓存。 */
    void apply_version(std::uint64_t version)
    {
        const std::uint64_t previous = version_.exchange(version);
        if (previous != version)
        {
            clear_cache();
        }
    }

    /* 外部身份依赖异常时停止使用旧缓存继续鉴权，readyz 也随之变为 503。 */
    void fail_closed()
    {
        ready_.store(false);
        clear_cache();
    }

    /* 在 TTL 内读取并更新 LRU 顺序；缓存 key 是 HMAC 的十六进制形式而非完整 API Key。 */
    std::shared_ptr<AuthResult> find_cache(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        const auto found = cache_.find(key);
        if (found == cache_.end())
        {
            return {};
        }
        const auto age = std::chrono::duration_cast<std::chrono::seconds>(
            Clock::now() - found->second.inserted);
        if (age.count() >= static_cast<long>(config_.auth_cache_ttl_seconds))
        {
            lru_.erase(found->second.lru);
            cache_.erase(found);
            return {};
        }
        lru_.splice(lru_.begin(), lru_, found->second.lru);
        return std::make_shared<AuthResult>(found->second.result);
    }

    /* 写入 AuthResult，并在超过最大条目数时淘汰最久未使用项。 */
    void put_cache(const std::string &key, const AuthResult &result)
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        const auto existing = cache_.find(key);
        if (existing != cache_.end())
        {
            lru_.erase(existing->second.lru);
            cache_.erase(existing);
        }
        lru_.push_front(key);
        cache_.emplace(key, CacheEntry{result, Clock::now(), lru_.begin()});
        while (cache_.size() > config_.auth_cache_max_entries)
        {
            cache_.erase(lru_.back());
            lru_.pop_back();
        }
    }

    /* 在配置变化和依赖故障时统一清空缓存。 */
    void clear_cache()
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        cache_.clear();
        lru_.clear();
    }

    GatewayConfig config_;
    GatewayRepository &repository_;
    std::atomic_bool stopping_{false};
    std::atomic_bool ready_{false};
    std::atomic_bool poll_pending_{false};
    std::atomic<std::uint64_t> version_{0};
    std::mutex queue_mutex_;
    std::condition_variable queue_condition_;
    std::deque<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    std::mutex poll_mutex_;
    std::condition_variable poll_condition_;
    std::thread poller_;
    std::mutex cache_mutex_;
    std::list<std::string> lru_;
    std::unordered_map<std::string, CacheEntry> cache_;
};

/* 创建 RuntimeState 外观，Impl 接管线程、队列、缓存和 Repository 引用。 */
RuntimeState::RuntimeState(GatewayConfig config, GatewayRepository &repository)
    : impl_(std::make_unique<Impl>(std::move(config), repository))
{
}

/* 析构由 Impl 等待所有后台线程退出。 */
RuntimeState::~RuntimeState() = default;

/* 把 API Key 和回调转交给异步实现，调用者线程不执行 SQL。 */
void RuntimeState::authenticate(std::string api_key, AuthCallback callback)
{
    impl_->authenticate(std::move(api_key), std::move(callback));
}

/* 转交 started Attempt 审计请求。 */
void RuntimeState::begin_attempt(AttemptStart attempt, AuditCallback callback)
{
    impl_->begin_attempt(std::move(attempt), std::move(callback));
}

/* 转交 Usage/预算准入事务。 */
void RuntimeState::admit_request(RequestAdmission request, AdmissionCallback callback)
{
    impl_->admit_request(std::move(request), std::move(callback));
}

/* 转交请求终态结算。 */
void RuntimeState::finish_request(RequestFinish request, AuditCallback callback)
{
    impl_->finish_request(std::move(request), std::move(callback));
}

/* 转交健康探测配置加载。 */
void RuntimeState::load_health_checks(HealthCheckCallback callback)
{
    impl_->load_health_checks(std::move(callback));
}

/* 转交 Attempt 终态审计。 */
void RuntimeState::finish_attempt(AttemptFinish attempt, AuditCallback callback)
{
    impl_->finish_attempt(std::move(attempt), std::move(callback));
}

bool RuntimeState::ready() const
{
    return impl_->ready();
}

const GatewayConfig &RuntimeState::config() const
{
    return impl_->config();
}
} // namespace ai_gateway
