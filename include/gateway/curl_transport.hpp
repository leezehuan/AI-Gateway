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
class CurlMultiProviderTransport final : public ProviderTransport
{
public:
    struct Task;

    CurlMultiProviderTransport();
    ~CurlMultiProviderTransport() override;

    CurlMultiProviderTransport(const CurlMultiProviderTransport &) = delete;
    CurlMultiProviderTransport &operator=(const CurlMultiProviderTransport &) = delete;

    std::shared_ptr<ProviderTransfer> execute(ProviderRequest request,
                                              ProviderCallbacks callbacks) override;
    bool healthy() const override;
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
