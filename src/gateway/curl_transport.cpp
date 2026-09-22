#include "gateway/curl_transport.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <sstream>
#include <utility>
#include <vector>

namespace ai_gateway
{
struct CurlMultiProviderTransport::Task
{
    /* 本次调用不可变的 URL/Header/body/超时快照。 */
    ProviderRequest request;

    ProviderCallbacks callbacks;

    ProviderResponse response;

    curl_slist *headers = nullptr;
    CURL *easy = nullptr;
    /* 其他线程只写这些原子意图位；worker 读取后真正调用 curl_easy_pause/remove_handle。 */
    std::atomic_bool cancel_requested{false};
    std::atomic_bool resume_requested{false};
    std::atomic_bool stream_started{false};
    /* worker 内部状态：当前 body callback 要求暂停，以及 curl 是否已经处于 paused 状态。 */
    bool pause_requested = false;
    bool paused = false;
    bool response_too_large = false;
    bool header_too_large = false;
    bool timed_out = false;
    bool cancelled = false;
    bool callback_aborted = false;
    /* 用于首字节超时和 SSE idle timeout 的本地观测状态。 */
    bool first_body_byte = false;
    bool headers_delivered = false;
    std::size_t response_bytes = 0;
    std::size_t header_bytes = 0;
    std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point last_body_byte = started;
};

namespace
{
class CurlProviderTransfer final : public ProviderTransfer
{
public:

    CurlProviderTransfer(std::weak_ptr<CurlMultiProviderTransport::Task> task,
                         std::function<void()> wake)
        : task_(std::move(task)), wake_(std::move(wake))
    {
    }

    /* 线程安全地标记取消，并用 curl_multi_wakeup 立即唤醒 worker。 */
    void cancel() override
    {
        if (auto task = task_.lock())
        {
            task->cancel_requested.store(true);
            wake_();
        }
    }

    /* 线程安全地请求从 CURLPAUSE_RECV 恢复。 */
    void resume() override
    {
        if (auto task = task_.lock())
        {
            task->resume_requested.store(true);
            wake_();
        }
    }

    /* 首事件提交后开始流式 idle timeout 计时。 */
    void mark_stream_started() override
    {
        if (auto task = task_.lock())
        {
            task->stream_started.store(true);
            wake_();
        }
    }

private:
    std::weak_ptr<CurlMultiProviderTransport::Task> task_;
    std::function<void()> wake_;
};

/* 小写化 Provider Header 名称。 */
std::string lower(std::string value)
{
    for (char &character : value)
    {
        character = static_cast<char>(std::tolower(static_cast<unsigned char>(character)));
    }
    return value;
}

std::string trim(std::string value)
{
    while (!value.empty() && (value.back() == '\r' || value.back() == '\n' ||
                              std::isspace(static_cast<unsigned char>(value.back()))))
    {
        value.pop_back();
    }
    std::size_t offset = 0;
    while (offset < value.size() && std::isspace(static_cast<unsigned char>(value[offset])))
    {
        ++offset;
    }
    return value.substr(offset);
}

void deliver_headers(CurlMultiProviderTransport::Task &task)
{
    if (task.headers_delivered || task.response.status < 200)
    {
        return;
    }
    task.headers_delivered = true;
    if (task.callbacks.on_headers)
    {
        try
        {
            task.callbacks.on_headers({task.response.status, task.response.headers});
        }
        catch (...)
        {
            task.callback_aborted = true;
        }
    }
}

size_t write_callback(char *data, size_t size, size_t count, void *opaque)
{
    auto *task = static_cast<CurlMultiProviderTransport::Task *>(opaque);
    const std::size_t bytes = size * count;
    if (task->cancel_requested.load())
    {
        task->cancelled = true;
        return 0;
    }
    if (task->pause_requested)
    {
        task->paused = true;
        return CURL_WRITEFUNC_PAUSE;
    }
    if (task->response_bytes + bytes > task->request.max_response_bytes)
    {
        task->response_too_large = true;
        return 0;
    }

    task->response_bytes += bytes;
    task->first_body_byte = true;
    task->last_body_byte = std::chrono::steady_clock::now();

    if (!task->callbacks.on_body)
    {
        task->response.body.append(data, bytes);
        return bytes;
    }

    ProviderChunkAction action = ProviderChunkAction::cancel_transfer;
    try
    {
        action = task->callbacks.on_body(std::string_view(data, bytes));
    }
    catch (...)
    {
        task->callback_aborted = true;
        return 0;
    }

    if (action == ProviderChunkAction::cancel_transfer)
    {
        task->callback_aborted = true;
        return 0;
    }
    if (action == ProviderChunkAction::pause_after_accept)
    {
        task->pause_requested = true;
    }
    return bytes;
}

size_t header_callback(char *data, size_t size, size_t count, void *opaque)
{
    auto *task = static_cast<CurlMultiProviderTransport::Task *>(opaque);
    const std::size_t bytes = size * count;
    task->header_bytes += bytes;
    if (task->header_bytes > 64 * 1024)
    {
        task->header_too_large = true;
        return 0;
    }

    std::string line(data, bytes);
    if (line.rfind("HTTP/", 0) == 0)
    {
        task->response.headers.clear();
        task->headers_delivered = false;
        std::istringstream status_line(line);
        std::string version;
        status_line >> version >> task->response.status;
        return bytes;
    }
    if (line == "\r\n" || line == "\n")
    {
        deliver_headers(*task);
        return task->callback_aborted ? 0 : bytes;
    }

    const std::size_t separator = line.find(':');
    if (separator != std::string::npos)
    {
        const std::string name = lower(trim(line.substr(0, separator)));
        if (!name.empty())
        {
            task->response.headers[name] = trim(line.substr(separator + 1));
        }
    }
    return bytes;
}

/* libcurl 进度回调：实现首字节、总时长、流 idle timeout 和取消检查。 */
int progress_callback(void *opaque,
                      curl_off_t,
                      curl_off_t,
                      curl_off_t,
                      curl_off_t)
{
    auto *task = static_cast<CurlMultiProviderTransport::Task *>(opaque);
    if (task->cancel_requested.load())
    {
        task->cancelled = true;
        return 1;
    }

    const auto now = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - task->started);
    const bool waiting_for_response = task->request.streaming
                                          ? !task->stream_started.load()
                                          : !task->first_body_byte;
    if (waiting_for_response && elapsed.count() > task->request.timeout_ms)
    {
        task->timed_out = true;
        return 1;
    }
    if (task->request.streaming && task->first_body_byte && !task->paused &&
        task->request.idle_timeout_ms > 0)
    {
        const auto idle = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - task->last_body_byte);
        if (idle.count() > task->request.idle_timeout_ms)
        {
            task->timed_out = true;
            return 1;
        }
    }
    if (task->request.streaming && task->request.max_duration_ms > 0 &&
        elapsed.count() > task->request.max_duration_ms)
    {
        task->timed_out = true;
        return 1;
    }
    return 0;
}

/* 把 CURLcode 和 Task 内部标志转换为 Gateway ProviderError。 */
ProviderError classify_result(const CurlMultiProviderTransport::Task &task, CURLcode result)
{
    if (task.response_too_large)
    {
        return ProviderError::response_too_large;
    }
    if (task.cancelled || task.cancel_requested.load())
    {
        return ProviderError::cancelled;
    }
    if (task.callback_aborted)
    {
        return ProviderError::callback_aborted;
    }
    if (task.timed_out || result == CURLE_OPERATION_TIMEDOUT)
    {
        return ProviderError::timeout;
    }
    if (result == CURLE_COULDNT_RESOLVE_HOST || result == CURLE_COULDNT_RESOLVE_PROXY)
    {
        return ProviderError::dns_failure;
    }
    if (result == CURLE_COULDNT_CONNECT)
    {
        return ProviderError::connection_failure;
    }
    if (result == CURLE_SSL_CONNECT_ERROR || result == CURLE_PEER_FAILED_VERIFICATION ||
        result == CURLE_SSL_CERTPROBLEM || result == CURLE_SSL_CIPHER)
    {
        return ProviderError::tls_failure;
    }
    if (task.header_too_large || result != CURLE_OK)
    {
        return ProviderError::unavailable;
    }
    return ProviderError::none;
}

void complete_task(CURLM *multi,
                   CURL *easy,
                   const std::shared_ptr<CurlMultiProviderTransport::Task> &task,
                   ProviderError error)
{
    if (easy != nullptr)
    {
        curl_multi_remove_handle(multi, easy);
        curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &task->response.status);
        curl_off_t uploaded = 0;
        if (curl_easy_getinfo(easy, CURLINFO_SIZE_UPLOAD_T, &uploaded) == CURLE_OK)
        {
            task->response.request_may_have_been_sent = uploaded > 0;
        }
        const auto timing_ms = [easy](CURLINFO info) {
            double seconds = 0;
            return curl_easy_getinfo(easy, info, &seconds) == CURLE_OK
                       ? static_cast<std::uint64_t>(std::max(0.0, seconds) * 1000.0)
                       : 0;
        };
        task->response.dns_ms = timing_ms(CURLINFO_NAMELOOKUP_TIME);
        task->response.connect_ms = timing_ms(CURLINFO_CONNECT_TIME);
        task->response.tls_ms = timing_ms(CURLINFO_APPCONNECT_TIME);
        task->response.first_byte_ms = timing_ms(CURLINFO_STARTTRANSFER_TIME);
        task->response.total_ms = timing_ms(CURLINFO_TOTAL_TIME);
        task->response.has_first_byte_timing = task->first_body_byte;
    }
    task->response.error = error;
    curl_slist_free_all(task->headers);
    task->headers = nullptr;
    if (easy != nullptr)
    {
        curl_easy_cleanup(easy);
    }
    task->easy = nullptr;
    if (task->callbacks.on_complete)
    {
        try
        {
            task->callbacks.on_complete(std::move(task->response));
        }
        catch (...)
        {
        }
    }
}
}

CurlMultiProviderTransport::CurlMultiProviderTransport(const GatewayConfig &config)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
    multi_ = curl_multi_init();
    if (multi_ != nullptr)
    {
        curl_multi_setopt(multi_, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                          static_cast<long>(config.curl_max_total_connections));
        curl_multi_setopt(multi_, CURLMOPT_MAX_HOST_CONNECTIONS,
                          static_cast<long>(config.curl_max_host_connections));
        curl_multi_setopt(multi_, CURLMOPT_MAXCONNECTS,
                          static_cast<long>(config.curl_max_total_connections));
        healthy_.store(true);
        worker_ = std::thread(&CurlMultiProviderTransport::run, this);
    }
}

CurlMultiProviderTransport::~CurlMultiProviderTransport()
{
    shutdown();
    if (multi_ != nullptr)
    {
        curl_multi_cleanup(multi_);
        multi_ = nullptr;
    }
    curl_global_cleanup();
}

/* 唤醒等待中的 worker，使 cancel/resume 不必等待 250ms 轮询。 */
void CurlMultiProviderTransport::wake()
{
    wakeup_.notify_one();
    if (multi_ != nullptr)
    {
        curl_multi_wakeup(multi_);
    }
}

/* 幂等停止 transport；worker 会把 active transfer 统一归为 shutdown。 */
void CurlMultiProviderTransport::shutdown()
{
    if (stopping_.exchange(true))
    {
        return;
    }
    wake();
    if (worker_.joinable())
    {
        worker_.join();
    }
}

std::shared_ptr<ProviderTransfer> CurlMultiProviderTransport::execute(
    ProviderRequest request,
    ProviderCallbacks callbacks)
{
    auto task = std::make_shared<Task>();
    task->request = std::move(request);
    task->callbacks = std::move(callbacks);
    auto transfer = std::make_shared<CurlProviderTransfer>(task, [this] { wake(); });

    if (stopping_.load() || multi_ == nullptr)
    {
        task->response.error = ProviderError::shutdown;
        if (task->callbacks.on_complete)
        {
            task->callbacks.on_complete(std::move(task->response));
        }
        return transfer;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push(task);
    }
    wake();
    return transfer;
}

bool CurlMultiProviderTransport::healthy() const
{
    return healthy_.load() && !stopping_.load();
}

/*
 * libcurl multi worker 主循环：领取 pending、处理 cancel/resume、perform/poll，
 * 再消费 CURLMSG_DONE。所有 easy handle 生命周期都限制在该线程。
 */
void CurlMultiProviderTransport::run()
{
    int running_handles = 0;
    while (true)
    {
        std::queue<std::shared_ptr<Task>> queued;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(queued, pending_);
        }

        while (!queued.empty())
        {
            auto task = std::move(queued.front());
            queued.pop();
            if (stopping_.load() || task->cancel_requested.load())
            {
                complete_task(multi_, nullptr, task,
                              stopping_.load() ? ProviderError::shutdown : ProviderError::cancelled);
                continue;
            }

            CURL *easy = curl_easy_init();
            if (easy == nullptr)
            {
                complete_task(multi_, nullptr, task, ProviderError::unavailable);
                continue;
            }

            task->easy = easy;
            task->started = std::chrono::steady_clock::now();
            task->last_body_byte = task->started;
            for (const auto &header : task->request.headers)
            {
                const std::string line = header.first + ": " + header.second;
                task->headers = curl_slist_append(task->headers, line.c_str());
            }

            curl_easy_setopt(easy, CURLOPT_URL, task->request.url.c_str());
            if (task->request.method == "POST")
            {
                curl_easy_setopt(easy, CURLOPT_POST, 1L);
                curl_easy_setopt(easy, CURLOPT_POSTFIELDS, task->request.body.data());
                curl_easy_setopt(easy, CURLOPT_POSTFIELDSIZE_LARGE,
                                 static_cast<curl_off_t>(task->request.body.size()));
            }
            else if (task->request.method == "HEAD")
            {
                curl_easy_setopt(easy, CURLOPT_NOBODY, 1L);
            }
            else
            {
                curl_easy_setopt(easy, CURLOPT_HTTPGET, 1L);
            }
            curl_easy_setopt(easy, CURLOPT_HTTPHEADER, task->headers);
            curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, write_callback);
            curl_easy_setopt(easy, CURLOPT_WRITEDATA, task.get());
            curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, header_callback);
            curl_easy_setopt(easy, CURLOPT_HEADERDATA, task.get());
            curl_easy_setopt(easy, CURLOPT_XFERINFOFUNCTION, progress_callback);
            curl_easy_setopt(easy, CURLOPT_XFERINFODATA, task.get());
            curl_easy_setopt(easy, CURLOPT_NOPROGRESS, 0L);
            curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
            curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 0L);
            curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
            curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS,
                             task->request.timeout_ms);
            curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS,
                             task->request.streaming ? task->request.max_duration_ms
                                                     : task->request.timeout_ms);
            curl_easy_setopt(easy, CURLOPT_PRIVATE, task.get());

            const CURLMcode added = curl_multi_add_handle(multi_, easy);
            if (added != CURLM_OK)
            {
                complete_task(multi_, easy, task, ProviderError::unavailable);
                continue;
            }
            active_.emplace(easy, std::move(task));
        }

        std::vector<CURL *> cancel_now;
        for (auto &entry : active_)
        {
            auto &task = *entry.second;
            if (task.cancel_requested.load())
            {
                cancel_now.push_back(entry.first);
                continue;
            }
            if (task.resume_requested.exchange(false))
            {
                task.pause_requested = false;
                if (task.paused)
                {
                    curl_easy_pause(entry.first, CURLPAUSE_CONT);
                    task.paused = false;
                    task.last_body_byte = std::chrono::steady_clock::now();
                }
            }
        }
        for (CURL *easy : cancel_now)
        {
            const auto found = active_.find(easy);
            if (found != active_.end())
            {
                auto task = std::move(found->second);
                active_.erase(found);
                complete_task(multi_, easy, task, ProviderError::cancelled);
            }
        }

        CURLMcode perform_result = CURLM_OK;
        do
        {
            perform_result = curl_multi_perform(multi_, &running_handles);
        } while (perform_result == CURLM_CALL_MULTI_PERFORM);

        for (auto &entry : active_)
        {
            auto &task = *entry.second;
            if (task.resume_requested.exchange(false))
            {
                task.pause_requested = false;
                if (task.paused)
                {
                    curl_easy_pause(entry.first, CURLPAUSE_CONT);
                    task.paused = false;
                    task.last_body_byte = std::chrono::steady_clock::now();
                }
            }
            else if (task.pause_requested && !task.paused)
            {
                curl_easy_pause(entry.first, CURLPAUSE_RECV);
                task.paused = true;
            }
        }

        int messages = 0;
        while (CURLMsg *message = curl_multi_info_read(multi_, &messages))
        {
            if (message->msg != CURLMSG_DONE)
            {
                continue;
            }
            const auto found = active_.find(message->easy_handle);
            if (found == active_.end())
            {
                continue;
            }
            auto task = std::move(found->second);
            active_.erase(found);
            complete_task(multi_, message->easy_handle, task,
                          classify_result(*task, message->data.result));
        }

        if (stopping_.load())
        {
            for (auto &entry : active_)
            {
                complete_task(multi_, entry.first, entry.second, ProviderError::shutdown);
            }
            active_.clear();
            healthy_.store(false);
            return;
        }

        if (active_.empty())
        {
            std::unique_lock<std::mutex> lock(mutex_);
            wakeup_.wait_for(lock, std::chrono::milliseconds(250), [this] {
                return stopping_.load() || !pending_.empty();
            });
        }
        else
        {
            int descriptors = 0;
            curl_multi_poll(multi_, nullptr, 0, 100, &descriptors);
        }
    }
}
}
