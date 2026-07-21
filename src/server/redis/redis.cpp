#include "redis.hpp"
#include "config.hpp"
#include <iostream>
using namespace std;

/*
 * 函数名直译：Redis 构造函数。
 *
 * 通俗说：先把两个 Redis 连接指针设为空，表示还没有真正连上服务器。
 *
 * 专业说法：初始化发布上下文和订阅上下文指针，避免析构时释放野指针。
 *
 * 实现方法：使用构造函数初始化列表把 _publish_context、_subcribe_context 置为 nullptr。
 */
Redis::Redis()
    : _publish_context(nullptr), _subcribe_context(nullptr)
{
}

/*
 * 函数名直译：Redis 析构函数。
 *
 * 通俗说：Redis 对象销毁时，把发布连接和订阅连接都释放掉。
 *
 * 专业说法：释放 hiredis 的 redisContext 资源，避免连接句柄泄漏。
 *
 * 实现方法：分别判断两个上下文是否非空，非空则调用 redisFree。
 */
Redis::~Redis()
{
    if (_publish_context != nullptr)
    {
        redisFree(_publish_context);
    }

    if (_subcribe_context != nullptr)
    {
        redisFree(_subcribe_context);
    }
}

/*
 * 函数名直译：连接。
 *
 * 通俗说：连接本机 Redis，并且开一个后台线程专门等订阅消息。
 *
 * 专业说法：为发布和订阅分别创建 hiredis 同步上下文。订阅连接会被
 * redisGetReply 阻塞监听，所以不能和发布连接共用。
 *
 * 返回值：两个 Redis 连接都成功时返回 true，否则返回 false。
 *
 * 实现方法：调用 redisConnect 建立 _publish_context 和 _subcribe_context；
 * 成功后创建 detach 线程执行 observer_channel_message。
 */
bool Redis::connect()
{
    const string host = chat_config::getEnv("CHAT_REDIS_HOST", "127.0.0.1");
    const unsigned int port = chat_config::getPort("CHAT_REDIS_PORT", 6379);

    // 负责publish发布消息的上下文连接
    _publish_context = redisConnect(host.c_str(), port);
    if (nullptr == _publish_context || _publish_context->err)
    {
        cerr << "connect redis " << host << ":" << port << " failed: "
             << (_publish_context == nullptr ? "cannot allocate context" : _publish_context->errstr)
             << endl;
        if (_publish_context != nullptr)
        {
            redisFree(_publish_context);
            _publish_context = nullptr;
        }
        return false;
    }

    // 负责subscribe订阅消息的上下文连接
    _subcribe_context = redisConnect(host.c_str(), port);
    if (nullptr == _subcribe_context || _subcribe_context->err)
    {
        cerr << "connect redis " << host << ":" << port << " failed: "
             << (_subcribe_context == nullptr ? "cannot allocate context" : _subcribe_context->errstr)
             << endl;
        if (_subcribe_context != nullptr)
        {
            redisFree(_subcribe_context);
            _subcribe_context = nullptr;
        }
        redisFree(_publish_context);
        _publish_context = nullptr;
        return false;
    }

    // 在单独的线程中，监听通道上的事件，有消息给业务层进行上报
    thread t([this]() {
        observer_channel_message();
    });
    t.detach();

    cout << "connect redis-server " << host << ":" << port << " success!" << endl;

    return true;
}

/*
 * 函数名直译：发布。
 *
 * 通俗说：往某个 Redis 通道发一条消息，让订阅这个通道的服务器能收到。
 *
 * 专业说法：执行 Redis PUBLISH 命令，用于跨 ChatServer 实例转发在线消息。
 *
 * 参数说明：
 * - channel：通道编号，本项目直接使用用户 id。
 * - message：要发布的消息内容，通常是完整 JSON 字符串。
 *
 * 返回值：命令发送成功返回 true，失败返回 false。
 */
bool Redis::publish(int channel, string message)
{
    if (_publish_context == nullptr)
    {
        cerr << "publish command failed: redis is not connected" << endl;
        return false;
    }

    redisReply *reply = (redisReply *)redisCommand(_publish_context, "PUBLISH %d %s", channel, message.c_str());
    if (nullptr == reply)
    {
        cerr << "publish command failed!" << endl;
        return false;
    }
    freeReplyObject(reply);
    return true;
}

/*
 * 函数名直译：订阅。
 *
 * 通俗说：告诉 Redis：“以后这个用户 id 通道有消息，请通知本服务器。”
 *
 * 专业说法：向订阅上下文发送 SUBSCRIBE 命令，用于绑定用户和当前服务器实例。
 *
 * 参数说明：
 * - channel：要订阅的通道编号，本项目用登录用户 id。
 *
 * 返回值：订阅命令写入 Redis 连接成功返回 true。
 *
 * 实现方法：使用 redisAppendCommand + redisBufferWrite 只负责把命令写出去，
 * 不在这里读取响应；响应统一由 observer_channel_message 线程处理。
 */
bool Redis::subscribe(int channel)
{
    if (_subcribe_context == nullptr)
    {
        cerr << "subscribe command failed: redis is not connected" << endl;
        return false;
    }

    // SUBSCRIBE命令本身会造成线程阻塞等待通道里面发生消息，这里只做订阅通道，不接收通道消息
    // 通道消息的接收专门在observer_channel_message函数中的独立线程中进行
    // 只负责发送命令，不阻塞接收redis server响应消息，否则和notifyMsg线程抢占响应资源
    if (REDIS_ERR == redisAppendCommand(this->_subcribe_context, "SUBSCRIBE %d", channel))
    {
        cerr << "subscribe command failed!" << endl;
        return false;
    }
    // redisBufferWrite可以循环发送缓冲区，直到缓冲区数据发送完毕（done被置为1）
    int done = 0;
    while (!done)
    {
        if (REDIS_ERR == redisBufferWrite(this->_subcribe_context, &done))
        {
            cerr << "subscribe command failed!" << endl;
            return false;
        }
    }
    // redisGetReply

    return true;
}

/*
 * 函数名直译：取消订阅。
 *
 * 通俗说：用户下线后，告诉 Redis 不用再把这个用户通道的消息推给本服务器。
 *
 * 专业说法：向订阅上下文发送 UNSUBSCRIBE 命令，解除 channel 与当前服务器的订阅关系。
 *
 * 参数说明：
 * - channel：要取消订阅的通道编号，本项目用用户 id。
 *
 * 返回值：命令写入成功返回 true，失败返回 false。
 */
bool Redis::unsubscribe(int channel)
{
    if (_subcribe_context == nullptr)
    {
        cerr << "unsubscribe command failed: redis is not connected" << endl;
        return false;
    }

    if (REDIS_ERR == redisAppendCommand(this->_subcribe_context, "UNSUBSCRIBE %d", channel))
    {
        cerr << "unsubscribe command failed!" << endl;
        return false;
    }
    // redisBufferWrite可以循环发送缓冲区，直到缓冲区数据发送完毕（done被置为1）
    int done = 0;
    while (!done)
    {
        if (REDIS_ERR == redisBufferWrite(this->_subcribe_context, &done))
        {
            cerr << "unsubscribe command failed!" << endl;
            return false;
        }
    }
    return true;
}

/*
 * 函数名直译：观察通道消息。
 *
 * 通俗说：这个函数在后台线程里一直等 Redis 推来的消息。
 * 一旦收到，就把“哪个通道、什么内容”交给业务层处理。
 *
 * 专业说法：订阅连接的阻塞接收循环，持续调用 redisGetReply 获取 SUBSCRIBE
 * 通道事件，并通过 _notify_message_handler 上报给 ChatService。
 *
 * 实现方法：Redis 订阅消息通常是三元素数组：[message, channel, payload]。
 * 这里取 element[1] 作为 channel/userid，element[2] 作为消息正文。
 */
void Redis::observer_channel_message()
{
    redisReply *reply = nullptr;
    while (REDIS_OK == redisGetReply(this->_subcribe_context, (void **)&reply))
    {
        // 订阅收到的消息是一个带三元素的数组
        if (reply != nullptr && reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3 &&
            reply->element[1] != nullptr && reply->element[1]->str != nullptr &&
            reply->element[2] != nullptr && reply->element[2]->str != nullptr &&
            _notify_message_handler)
        {
            // 给业务层上报通道上发生的消息
            _notify_message_handler(atoi(reply->element[1]->str) , reply->element[2]->str);
        }

        freeReplyObject(reply);
    }

    cerr << ">>>>>>>>>>>>> observer_channel_message quit <<<<<<<<<<<<<" << endl;
}

/*
 * 函数名直译：初始化通知处理器。
 *
 * 通俗说：业务层把一个回调函数交给 Redis 类，之后 Redis 收到订阅消息时，
 * 就调用这个回调把消息交回业务层。
 *
 * 专业说法：注册 Redis subscribe 事件的上层通知函数，实现 Redis 工具层
 * 与 ChatService 业务层之间的回调解耦。
 *
 * 参数说明：
 * - fn：形如 void(int, string) 的函数对象，第一个参数是 channel，第二个参数是消息。
 */
void Redis::init_notify_handler(function<void(int,string)> fn)
{
    this->_notify_message_handler = fn;
}
