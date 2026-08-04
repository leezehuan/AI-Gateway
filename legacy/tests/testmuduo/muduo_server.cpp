/*
muduo网络库给用户提供了两个主要的类
TcpServer ： 用于编写服务器程序的
TcpClient ： 用于编写客户端程序的

epoll + 线程池
好处：能够把网络I/O的代码和业务代码区分开
                        用户的连接和断开       用户的可读写事件
*/
#include <muduo/net/TcpServer.h>
#include <muduo/net/EventLoop.h>
#include <iostream>
#include <functional>
#include <string>
using namespace std;
using namespace muduo;
using namespace muduo::net;
using namespace placeholders;

/*基于muduo网络库开发服务器程序
1.组合TcpServer对象
2.创建EventLoop事件循环对象的指针
3.明确TcpServer构造函数需要什么参数，输出ChatServer的构造函数
4.在当前服务器类的构造函数当中，注册处理连接的回调函数和处理读写时间的回调函数
5.设置合适的服务端线程数量，muduo库会自己分配I/O线程和worker线程
*/
class ChatServer
{
public:
    /*
     * 函数名直译：聊天服务器构造函数。
     *
     * 通俗说：这是 muduo 入门示例里的服务器初始化函数，
     * 它把连接回调、消息回调和线程数量都设置好。
     *
     * 专业说法：组合 TcpServer 并注册 connection callback、message callback，
     * 用于演示 muduo 网络层的基本使用方式。
     *
     * 参数说明：
     * - loop：事件循环。
     * - listenAddr：监听地址。
     * - nameArg：服务器名称。
     */
    ChatServer(EventLoop *loop,               // 事件循环
               const InetAddress &listenAddr, // IP+Port
               const string &nameArg)
        : _server(loop, listenAddr, nameArg), _loop(loop)
    {
        // 给服务器注册用户连接的创建和断开回调
        _server.setConnectionCallback(std::bind(&ChatServer::onConnection, this, _1));

        // 给服务器注册用户读写事件回调
        _server.setMessageCallback(std::bind(&ChatServer::onMessage, this, _1, _2, _3));

        // 设置服务器端的线程数量 1个I/O线程   3个worker线程
        _server.setThreadNum(4);
    }

    /*
     * 函数名直译：启动。
     *
     * 通俗说：让这个示例服务器开始监听客户端连接。
     *
     * 专业说法：对 TcpServer::start 的简单封装。
     */
    void start()
    {
        _server.start();
    }

private:
    /*
     * 函数名直译：连接事件回调。
     *
     * 通俗说：客户端连上来时打印 online，断开时打印 offline 并关闭连接。
     *
     * 专业说法：muduo connection callback 示例，用于观察 TcpConnection 状态变化。
     *
     * 参数说明：
     * - conn：发生连接状态变化的 TCP 连接。
     */
    void onConnection(const TcpConnectionPtr &conn)
    {
        if (conn->connected())
        {
            cout << conn->peerAddress().toIpPort() << " -> " << conn->localAddress().toIpPort() << " state:online" << endl;
        }
        else
        {
            cout << conn->peerAddress().toIpPort() << " -> " << conn->localAddress().toIpPort() << " state:offline" << endl;
            conn->shutdown(); // close(fd)
            // _loop->quit();
        }
    }

    /*
     * 函数名直译：消息事件回调。
     *
     * 通俗说：客户端发什么，服务器就打印什么，并原样回发给客户端。
     *
     * 专业说法：muduo message callback 示例，从 Buffer 中取出完整字符串后执行 echo。
     *
     * 参数说明：
     * - conn：发送消息的连接。
     * - buffer：接收缓冲区。
     * - time：消息到达时间。
     */
    void onMessage(const TcpConnectionPtr &conn, // 连接
                   Buffer *buffer,               // 缓冲区
                   Timestamp time)               // 接收到数据的时间信息
    {
        string buf = buffer->retrieveAllAsString();
        cout << "recv data:" << buf << " time:" << time.toFormattedString() << endl;
        conn->send(buf);
    }

    TcpServer _server; // #1
    EventLoop *_loop;  // #2 epoll
};

/*
 * 函数名直译：主函数。
 *
 * 通俗说：启动一个固定监听 127.0.0.1:6000 的 muduo echo 示例服务器。
 *
 * 专业说法：muduo TcpServer 最小示例入口，创建 EventLoop、InetAddress 和 ChatServer，
 * 然后进入事件循环。
 */
int main()
{
    EventLoop loop; // epoll
    InetAddress addr("127.0.0.1", 6000);
    ChatServer server(&loop, addr, "ChatServer");

    server.start(); // listenfd epoll_ctl=>epoll
    loop.loop();    // epoll_wait以阻塞方式等待新用户连接，已连接用户的读写事件等
}
