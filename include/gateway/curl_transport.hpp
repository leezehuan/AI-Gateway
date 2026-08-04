#ifndef AI_GATEWAY_CURL_TRANSPORT_HPP
#define AI_GATEWAY_CURL_TRANSPORT_HPP

#include "gateway/gateway.hpp"

#include <curl/curl.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

namespace ai_gateway
{
/*
 * 共享 libcurl multi transport。
 *
 * 所有 easy handle 只在一个 worker 线程操作，Gateway 线程通过 ProviderTransfer 的
 * cancel/resume seam 发线程安全命令；因此单个慢 Provider 不会阻塞 healthz。
 */
class CurlMultiProviderTransport final : public ProviderTransport
{
public:
    struct Task;

    explicit CurlMultiProviderTransport(const GatewayConfig &config);
    ~CurlMultiProviderTransport() override;

    CurlMultiProviderTransport(const CurlMultiProviderTransport &) = delete;
    CurlMultiProviderTransport &operator=(const CurlMultiProviderTransport &) = delete;

    /* 把一次 Provider 请求放入 multi worker 队列并立即返回可控 transfer。 */
    std::shared_ptr<ProviderTransfer> execute(ProviderRequest request,
                                              ProviderCallbacks callbacks) override;
    /* 返回 curl multi worker 是否仍可接收请求。 */
    bool healthy() const override;
    /* 取消/完成所有 active transfer 并等待 worker 退出。 */
    void shutdown();

private:
    void run();
    void wake();

    mutable std::mutex mutex_;
    std::condition_variable wakeup_;
    std::queue<std::shared_ptr<Task>> pending_;
    std::thread worker_;
    CURLM *multi_ = nullptr;
    std::unordered_map<CURL *, std::shared_ptr<Task>> active_;
    std::atomic_bool stopping_{false};
    std::atomic_bool healthy_{false};
};
} // namespace ai_gateway

#endif
