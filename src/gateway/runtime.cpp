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

namespace ai_gateway
{
namespace
{
using Clock = std::chrono::steady_clock;

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

std::string candidate_fingerprint(const RepositoryAccessRecord &record,
                                  const RepositoryMapping &mapping)
{
    const std::string identity = std::to_string(record.config_version) + "\x1f" +
        std::to_string(mapping.mapping_id) + "\x1f" + std::to_string(mapping.provider_id) +
        "\x1f" + std::to_string(mapping.endpoint_id) + "\x1f" +
        std::to_string(mapping.credential_id) + "\x1f" + mapping.upstream_model;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(identity.data()), identity.size(), digest);
    return hex(digest, sizeof(digest));
}

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

bool supported_url(const std::string &url)
{
    return url.rfind("http://", 0) == 0 || url.rfind("https://", 0) == 0;
}

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
    if (matched->tenant_status != "active" || matched->policy_status != "active")
    {
        return {AuthStatus::access_disabled, {}};
    }

    auto snapshot = std::make_shared<AuthSnapshot>();
    snapshot->config_version = matched->config_version;
    snapshot->database_tenant_id = matched->database_tenant_id;
    snapshot->database_api_key_id = matched->database_key_id;
    snapshot->tenant_slug = matched->tenant_slug;
    snapshot->public_api_key_id = matched->public_key_id;
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
        try
        {
            model->second->candidates.push_back(
                {mapping.mapping_id, mapping.provider_id, mapping.endpoint_id,
                 mapping.credential_id, mapping.mapping_name, mapping.priority,
                 candidate_fingerprint(*matched, mapping), mapping.endpoint_url,
                 resolve_secret(config, mapping.secret_ref), mapping.upstream_model});
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

    bool ready() const { return ready_.load(); }
    const GatewayConfig &config() const { return config_; }

private:
    struct CacheEntry
    {
        AuthResult result;
        Clock::time_point inserted;
        std::list<std::string>::iterator lru;
    };

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

    void apply_version(std::uint64_t version)
    {
        const std::uint64_t previous = version_.exchange(version);
        if (previous != version)
        {
            clear_cache();
        }
    }

    void fail_closed()
    {
        ready_.store(false);
        clear_cache();
    }

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

RuntimeState::RuntimeState(GatewayConfig config, GatewayRepository &repository)
    : impl_(std::make_unique<Impl>(std::move(config), repository))
{
}

RuntimeState::~RuntimeState() = default;

void RuntimeState::authenticate(std::string api_key, AuthCallback callback)
{
    impl_->authenticate(std::move(api_key), std::move(callback));
}

void RuntimeState::begin_attempt(AttemptStart attempt, AuditCallback callback)
{
    impl_->begin_attempt(std::move(attempt), std::move(callback));
}

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
