#include "chatserver.hpp"
#include "json.hpp"
#include "chatservice.hpp"

#include <iostream>
#include <functional>
#include <string>
using namespace std;
using namespace placeholders;
using json = nlohmann::json;

/*
 * 函数名直译：聊天服务器构造函数。
 *
 * 通俗说：创建一个真正能监听客户端连接的服务器对象，并告诉 muduo：
 * 有新连接时该找谁处理，有新消息时该找谁处理，同时准备好工作线程。
 *
 * 专业说法：这是 ChatServer 的初始化入口，负责组合 muduo::net::TcpServer，
 * 注册 connection callback 和 message callback，并配置 I/O 线程数量。
 *
 * 参数说明：
 * - loop：主事件循环，muduo 通过它监听 socket 事件。
 * - listenAddr：服务器监听的 IP 和端口。
 * - nameArg：服务器名称，主要用于日志和 muduo 内部标识。
 *
 * 实现方法：用初始化列表构造 _server 和 _loop，然后通过 std::bind
 * 把成员函数 onConnection、onMessage 绑定成 muduo 需要的回调函数。
 */
ChatServer::ChatServer(EventLoop *loop,
                       const InetAddress &listenAddr,
                       const string &nameArg)
    : _server(loop, listenAddr, nameArg), _loop(loop)
{
    // 注册链接回调
    _server.setConnectionCallback(std::bind(&ChatServer::onConnection, this, _1));

    // 注册消息回调
    _server.setMessageCallback(std::bind(&ChatServer::onMessage, this, _1, _2, _3));

    // 设置线程数量
    _server.setThreadNum(4);
}

/*
 * 函数名直译：启动。
 *
 * 通俗说：让服务器正式开始监听端口，等待客户端连进来。
 *
 * 专业说法：对 TcpServer::start 的薄封装，启动监听 socket 和线程池，
 * 真正的事件循环仍然由 main 函数中的 EventLoop::loop 驱动。
 *
 * 实现方法：直接调用 muduo TcpServer 的 start 方法。
 */
void ChatServer::start()
{
    _server.start();
}

/*
 * 函数名直译：连接事件回调。
 *
 * 通俗说：客户端上线或断线时 muduo 会调用这里；本项目重点处理断线，
 * 因为断线后要把用户从在线连接表里移除，并更新数据库中的在线状态。
 *
 * 专业说法：这是 TcpServer 的 connection callback，用于感知
 * TcpConnection 的连接状态变化，并把异常断开事件转交给业务层 ChatService。
 *
 * 参数说明：
 * - conn：当前发生状态变化的 TCP 连接智能指针。
 *
 * 实现方法：如果 conn->connected() 为 false，说明连接已经关闭，
 * 调用 ChatService::clientCloseException 清理业务状态，最后 shutdown 连接。
 */
void ChatServer::onConnection(const TcpConnectionPtr &conn)
{
    // 客户端断开链接
    if (!conn->connected())
    {
        ChatService::instance()->clientCloseException(conn);
        conn->shutdown();
    }
}

/*
 * 函数名直译：消息事件回调。
 *
 * 通俗说：客户端发来一段 JSON 字符串后，这里先把字符串解析成 JSON，
 * 再根据里面的 msgid 找到对应业务函数，比如登录、注册、一对一聊天等。
 *
 * 专业说法：这是 TcpServer 的 message callback，负责协议反序列化和
 * 网络层到业务层的分发。它不直接写业务逻辑，而是通过 ChatService 的
 * 消息处理器表完成解耦。
 *
 * 参数说明：
 * - conn：发送这条消息的客户端连接。
 * - buffer：muduo 收到的网络缓冲区。
 * - time：消息到达时间戳，业务层如果需要可以继续使用。
 *
 * 实现方法：取出 buffer 全部内容，nlohmann::json::parse 反序列化，
 * 读取 js["msgid"]，从 ChatService 获取 MsgHandler 并执行。
 */
void ChatServer::onMessage(const TcpConnectionPtr &conn,
                           Buffer *buffer,
                           Timestamp time)
{
    string buf = buffer->retrieveAllAsString();

    // 测试，添加json打印代码
    cout << buf << endl;

    // 数据的反序列化
    json js = json::parse(buf);
    // 达到的目的：完全解耦网络模块的代码和业务模块的代码
    // 通过js["msgid"] 获取=》业务handler=》conn  js  time
    auto msgHandler = ChatService::instance()->getHandler(js["msgid"].get<int>());
    // 回调消息绑定好的事件处理器，来执行相应的业务处理
    msgHandler(conn, js, time);
}
