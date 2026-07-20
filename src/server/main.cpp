#include "chatserver.hpp"
#include "chatservice.hpp"
#include <iostream>
#include <signal.h>
using namespace std;

/*
 * 函数名直译：重置处理器。
 *
 * 通俗说：当服务器按 Ctrl+C 退出时，先把数据库里所有在线用户改成离线，
 * 再真正结束进程。
 *
 * 专业说法：SIGINT 信号处理函数，用于服务进程退出前执行业务状态清理。
 *
 * 参数说明：
 * - int：信号编号。本项目不需要使用具体编号，所以省略形参名。
 */
void resetHandler(int)
{
    ChatService::instance()->reset();
    exit(0);
}

/*
 * 函数名直译：主函数。
 *
 * 通俗说：服务端程序从这里启动。它读取命令行里的 IP 和端口，
 * 创建 muduo 事件循环和 ChatServer，然后进入无限事件循环等待客户端。
 *
 * 专业说法：ChatServer 进程入口，负责参数校验、信号处理注册、网络服务对象构造和
 * EventLoop 启动。
 *
 * 参数说明：
 * - argc：命令行参数个数，至少需要 3 个。
 * - argv：命令行参数数组，argv[1] 是监听 IP，argv[2] 是监听端口。
 *
 * 返回值：正常情况下 EventLoop::loop 会持续运行，函数通常不会主动返回。
 */
int main(int argc, char **argv)
{
    if (argc < 3)
    {
        cerr << "command invalid! example: ./ChatServer 127.0.0.1 6000" << endl;
        exit(-1);
    }

    // 解析通过命令行参数传递的ip和port
    char *ip = argv[1];
    uint16_t port = atoi(argv[2]);

    signal(SIGINT, resetHandler);

    EventLoop loop;
    InetAddress addr(ip, port);
    ChatServer server(&loop, addr, "ChatServer");

    server.start();
    loop.loop();

    return 0;
}
