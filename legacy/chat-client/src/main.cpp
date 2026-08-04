#include "json.hpp"
#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>
#include <unordered_map>
#include <functional>
using namespace std;
using json = nlohmann::json;

#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <semaphore.h>
#include <atomic>

#include "group.hpp"
#include "user.hpp"
#include "public.hpp"

// 记录当前系统登录的用户信息
User g_currentUser;
// 记录当前登录用户的好友列表信息
vector<User> g_currentUserFriendList;
// 记录当前登录用户的群组列表信息
vector<Group> g_currentUserGroupList;

// 控制主菜单页面程序
bool isMainMenuRunning = false;

// 用于读写线程之间的通信
sem_t rwsem;
// 记录登录状态
atomic_bool g_isLoginSuccess{false};


// 接收线程
void readTaskHandler(int clientfd);
// 获取系统时间（聊天信息需要添加时间信息）
string getCurrentTime();
// 主聊天页面程序
void mainMenu(int);
// 显示当前登录成功用户的基本信息
void showCurrentUserData();

/*
 * 函数名直译：主函数。
 *
 * 通俗说：客户端程序从这里开始。它先连接服务器，然后开启一个接收线程；
 * 主线程负责显示菜单、读取用户输入并发送登录/注册等请求。
 *
 * 专业说法：ChatClient 进程入口，完成 socket 创建、connect、线程初始化、
 * 首页菜单循环和基础业务请求发送。
 *
 * 参数说明：
 * - argc：命令行参数个数，至少需要 3 个。
 * - argv：命令行参数数组，argv[1] 是服务器 IP，argv[2] 是服务器端口。
 *
 * 线程模型：main 线程负责写 socket；readTaskHandler 子线程负责读 socket。
 * 登录/注册结果通过信号量 rwsem 通知 main 线程。
 */
int main(int argc, char **argv)
{
    if (argc < 3)
    {
        cerr << "command invalid! example: ./ChatClient 127.0.0.1 6000" << endl;
        exit(-1);
    }

    // 解析通过命令行参数传递的ip和port
    char *ip = argv[1];
    uint16_t port = atoi(argv[2]);

    // 创建client端的socket
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == clientfd)
    {
        cerr << "socket create error" << endl;
        exit(-1);
    }

    // 填写client需要连接的server信息ip+port
    sockaddr_in server;
    memset(&server, 0, sizeof(sockaddr_in));

    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = inet_addr(ip);

    // client和server进行连接
    if (-1 == connect(clientfd, (sockaddr *)&server, sizeof(sockaddr_in)))
    {
        cerr << "connect server error" << endl;
        close(clientfd);
        exit(-1);
    }

    // 初始化读写线程通信用的信号量
    sem_init(&rwsem, 0, 0);

    // 连接服务器成功，启动接收子线程
    std::thread readTask(readTaskHandler, clientfd); // pthread_create
    readTask.detach();                               // pthread_detach

    // main线程用于接收用户输入，负责发送数据
    for (;;)
    {
        // 显示首页面菜单 登录、注册、退出
        cout << "========================" << endl;
        cout << "1. login" << endl;
        cout << "2. register" << endl;
        cout << "3. quit" << endl;
        cout << "========================" << endl;
        cout << "choice:";
        int choice = 0;
        cin >> choice;
        cin.get(); // 读掉缓冲区残留的回车

        switch (choice)
        {
        case 1: // login业务
        {
            int id = 0;
            char pwd[50] = {0};
            cout << "userid:";
            cin >> id;
            cin.get(); // 读掉缓冲区残留的回车
            cout << "userpassword:";
            cin.getline(pwd, 50);

            json js;
            js["msgid"] = LOGIN_MSG;
            js["id"] = id;
            js["password"] = pwd;
            string request = js.dump();

            g_isLoginSuccess = false;

            int len = send(clientfd, request.c_str(), strlen(request.c_str()) + 1, 0);
            if (len == -1)
            {
                cerr << "send login msg error:" << request << endl;
            }

            sem_wait(&rwsem); // 等待信号量，由子线程处理完登录的响应消息后，通知这里
                
            if (g_isLoginSuccess) 
            {
                // 进入聊天主菜单页面
                isMainMenuRunning = true;
                mainMenu(clientfd);
            }
        }
        break;
        case 2: // register业务
        {
            char name[50] = {0};
            char pwd[50] = {0};
            cout << "username:";
            cin.getline(name, 50);
            cout << "userpassword:";
            cin.getline(pwd, 50);

            json js;
            js["msgid"] = REG_MSG;
            js["name"] = name;
            js["password"] = pwd;
            string request = js.dump();

            int len = send(clientfd, request.c_str(), strlen(request.c_str()) + 1, 0);
            if (len == -1)
            {
                cerr << "send reg msg error:" << request << endl;
            }
            
            sem_wait(&rwsem); // 等待信号量，子线程处理完注册消息会通知
        }
        break;
        case 3: // quit业务
            close(clientfd);
            sem_destroy(&rwsem);
            exit(0);
        default:
            cerr << "invalid input!" << endl;
            break;
        }
    }

    return 0;
}

/*
 * 函数名直译：处理注册响应。
 *
 * 通俗说：服务器返回注册结果后，这里负责告诉用户注册成功还是失败。
 * 成功时会打印新生成的 userid，后续登录需要使用它。
 *
 * 专业说法：处理 REG_MSG_ACK 响应 JSON，根据 errno 判断注册业务结果。
 *
 * 参数说明：
 * - responsejs：服务端返回的 JSON，包含 msgid、errno，成功时还包含 id。
 */
void doRegResponse(json &responsejs)
{
    if (0 != responsejs["errno"].get<int>()) // 注册失败
    {
        cerr << "name is already exist, register error!" << endl;
    }
    else // 注册成功
    {
        cout << "name register success, userid is " << responsejs["id"]
                << ", do not forget it!" << endl;
    }
}

/*
 * 函数名直译：处理登录响应。
 *
 * 通俗说：登录成功后，把当前用户资料、好友列表、群组列表和离线消息都保存/显示出来；
 * 登录失败就打印服务器给出的错误信息。
 *
 * 专业说法：处理 LOGIN_MSG_ACK 响应，初始化客户端全局会话状态，
 * 包括 g_currentUser、好友缓存、群组缓存和登录成功标记。
 *
 * 参数说明：
 * - responsejs：服务端返回的 JSON，成功时包含用户信息、friends、groups、offlinemsg 等字段。
 *
 * 实现方法：先判断 errno；成功后逐项解析嵌套 JSON 字符串数组，
 * 最后设置 g_isLoginSuccess，唤醒主线程进入聊天菜单。
 */
void doLoginResponse(json &responsejs)
{
    if (0 != responsejs["errno"].get<int>()) // 登录失败
    {
        cerr << responsejs["errmsg"] << endl;
        g_isLoginSuccess = false;
    }
    else // 登录成功
    {
        // 记录当前用户的id和name
        g_currentUser.setId(responsejs["id"].get<int>());
        g_currentUser.setName(responsejs["name"]);

        // 记录当前用户的好友列表信息
        if (responsejs.contains("friends"))
        {
            // 初始化
            g_currentUserFriendList.clear();

            vector<string> vec = responsejs["friends"];
            for (string &str : vec)
            {
                json js = json::parse(str);
                User user;
                user.setId(js["id"].get<int>());
                user.setName(js["name"]);
                user.setState(js["state"]);
                g_currentUserFriendList.push_back(user);
            }
        }

        // 记录当前用户的群组列表信息
        if (responsejs.contains("groups"))
        {
            // 初始化
            g_currentUserGroupList.clear();

            vector<string> vec1 = responsejs["groups"];
            for (string &groupstr : vec1)
            {
                json grpjs = json::parse(groupstr);
                Group group;
                group.setId(grpjs["id"].get<int>());
                group.setName(grpjs["groupname"]);
                group.setDesc(grpjs["groupdesc"]);

                vector<string> vec2 = grpjs["users"];
                for (string &userstr : vec2)
                {
                    GroupUser user;
                    json js = json::parse(userstr);
                    user.setId(js["id"].get<int>());
                    user.setName(js["name"]);
                    user.setState(js["state"]);
                    user.setRole(js["role"]);
                    group.getUsers().push_back(user);
                }

                g_currentUserGroupList.push_back(group);
            }
        }

        // 显示登录用户的基本信息
        showCurrentUserData();

        // 显示当前用户的离线消息  个人聊天信息或者群组消息
        if (responsejs.contains("offlinemsg"))
        {
            vector<string> vec = responsejs["offlinemsg"];
            for (string &str : vec)
            {
                json js = json::parse(str);
                // time + [id] + name + " said: " + xxx
                if (ONE_CHAT_MSG == js["msgid"].get<int>())
                {
                    cout << js["time"].get<string>() << " [" << js["id"] << "]" << js["name"].get<string>()
                            << " said: " << js["msg"].get<string>() << endl;
                }
                else
                {
                    cout << "群消息[" << js["groupid"] << "]:" << js["time"].get<string>() << " [" << js["id"] << "]" << js["name"].get<string>()
                            << " said: " << js["msg"].get<string>() << endl;
                }
            }
        }

        g_isLoginSuccess = true;
    }
}

/*
 * 函数名直译：读任务处理器。
 *
 * 通俗说：这个函数跑在子线程里，一直等待服务器发来的消息。
 * 如果收到聊天消息就直接打印；如果收到登录/注册响应，就调用对应处理函数并通知主线程。
 *
 * 专业说法：客户端 socket 接收循环，负责协议反序列化和响应/推送消息分发。
 *
 * 参数说明：
 * - clientfd：已经连接到 ChatServer 的客户端 socket 文件描述符。
 *
 * 实现方法：阻塞调用 recv，解析 JSON 后按 msgid 分支处理。
 * LOGIN_MSG_ACK 和 REG_MSG_ACK 会 sem_post 通知 main 线程等待结束。
 */
void readTaskHandler(int clientfd)
{
    for (;;)
    {
        char buffer[1024] = {0};
        int len = recv(clientfd, buffer, 1024, 0);  // 阻塞了
        if (-1 == len || 0 == len)
        {
            close(clientfd);
            exit(-1);
        }

        // 接收ChatServer转发的数据，反序列化生成json数据对象
        json js = json::parse(buffer);
        int msgtype = js["msgid"].get<int>();
        if (ONE_CHAT_MSG == msgtype)
        {
            cout << js["time"].get<string>() << " [" << js["id"] << "]" << js["name"].get<string>()
                 << " said: " << js["msg"].get<string>() << endl;
            continue;
        }

        if (GROUP_CHAT_MSG == msgtype)
        {
            cout << "群消息[" << js["groupid"] << "]:" << js["time"].get<string>() << " [" << js["id"] << "]" << js["name"].get<string>()
                 << " said: " << js["msg"].get<string>() << endl;
            continue;
        }

        if (LOGIN_MSG_ACK == msgtype)
        {
            doLoginResponse(js); // 处理登录响应的业务逻辑
            sem_post(&rwsem);    // 通知主线程，登录结果处理完成
            continue;
        }

        if (REG_MSG_ACK == msgtype)
        {
            doRegResponse(js);
            sem_post(&rwsem);    // 通知主线程，注册结果处理完成
            continue;
        }
    }
}

/*
 * 函数名直译：显示当前用户数据。
 *
 * 通俗说：登录成功后，把自己的 id/name、好友列表、群组列表和群成员信息打印出来，
 * 方便用户知道当前账号有哪些关系数据。
 *
 * 专业说法：客户端会话缓存的调试/展示函数，读取全局用户、好友和群组容器并格式化输出。
 *
 * 实现方法：遍历 g_currentUserFriendList 和 g_currentUserGroupList，
 * 对每个 Group 继续遍历其内部的 GroupUser 列表。
 */
void showCurrentUserData()
{
    cout << "======================login user======================" << endl;
    cout << "current login user => id:" << g_currentUser.getId() << " name:" << g_currentUser.getName() << endl;
    cout << "----------------------friend list---------------------" << endl;
    if (!g_currentUserFriendList.empty())
    {
        for (User &user : g_currentUserFriendList)
        {
            cout << user.getId() << " " << user.getName() << " " << user.getState() << endl;
        }
    }
    cout << "----------------------group list----------------------" << endl;
    if (!g_currentUserGroupList.empty())
    {
        for (Group &group : g_currentUserGroupList)
        {
            cout << group.getId() << " " << group.getName() << " " << group.getDesc() << endl;
            for (GroupUser &user : group.getUsers())
            {
                cout << user.getId() << " " << user.getName() << " " << user.getState()
                     << " " << user.getRole() << endl;
            }
        }
    }
    cout << "======================================================" << endl;
}

// "help" command handler
void help(int fd = 0, string str = "");
// "chat" command handler
void chat(int, string);
// "addfriend" command handler
void addfriend(int, string);
// "creategroup" command handler
void creategroup(int, string);
// "addgroup" command handler
void addgroup(int, string);
// "groupchat" command handler
void groupchat(int, string);
// "loginout" command handler
void loginout(int, string);

// 系统支持的客户端命令列表
unordered_map<string, string> commandMap = {
    {"help", "显示所有支持的命令，格式help"},
    {"chat", "一对一聊天，格式chat:friendid:message"},
    {"addfriend", "添加好友，格式addfriend:friendid"},
    {"creategroup", "创建群组，格式creategroup:groupname:groupdesc"},
    {"addgroup", "加入群组，格式addgroup:groupid"},
    {"groupchat", "群聊，格式groupchat:groupid:message"},
    {"loginout", "注销，格式loginout"}};

// 注册系统支持的客户端命令处理
unordered_map<string, function<void(int, string)>> commandHandlerMap = {
    {"help", help},
    {"chat", chat},
    {"addfriend", addfriend},
    {"creategroup", creategroup},
    {"addgroup", addgroup},
    {"groupchat", groupchat},
    {"loginout", loginout}};

/*
 * 函数名直译：主菜单。
 *
 * 通俗说：登录成功后进入这里，用户可以输入 help、chat、addfriend 等命令。
 * 函数会解析命令名，再调用对应的命令处理函数。
 *
 * 专业说法：客户端登录态命令循环，通过 commandHandlerMap 实现命令到处理器的分发。
 *
 * 参数说明：
 * - clientfd：客户端 socket，用于各命令处理器向服务器发送 JSON 请求。
 *
 * 实现方法：读取一整行命令，用第一个冒号前的字符串作为命令名；
 * 在 commandHandlerMap 中查找并调用对应函数，剩余部分作为命令参数传入。
 */
void mainMenu(int clientfd)
{
    help();

    char buffer[1024] = {0};
    while (isMainMenuRunning)
    {
        cin.getline(buffer, 1024);
        string commandbuf(buffer);
        string command; // 存储命令
        int idx = commandbuf.find(":");
        if (-1 == idx)
        {
            command = commandbuf;
        }
        else
        {
            command = commandbuf.substr(0, idx);
        }
        auto it = commandHandlerMap.find(command);
        if (it == commandHandlerMap.end())
        {
            cerr << "invalid input command!" << endl;
            continue;
        }

        // 调用相应命令的事件处理回调，mainMenu对修改封闭，添加新功能不需要修改该函数
        it->second(clientfd, commandbuf.substr(idx + 1, commandbuf.size() - idx)); // 调用命令处理方法
    }
}

/*
 * 函数名直译：帮助。
 *
 * 通俗说：打印当前客户端支持的所有命令，以及每个命令的输入格式。
 *
 * 专业说法：help 命令处理器，遍历 commandMap 输出命令说明。
 *
 * 参数说明：为了统一命令处理器签名，保留 int 和 string 参数，但本函数不使用。
 */
void help(int, string)
{
    cout << "show command list >>> " << endl;
    for (auto &p : commandMap)
    {
        cout << p.first << " : " << p.second << endl;
    }
    cout << endl;
}
/*
 * 函数名直译：添加好友命令。
 *
 * 通俗说：用户输入 addfriend:好友id 后，这里把好友 id 打包成 JSON 发给服务器。
 *
 * 专业说法：ADD_FRIEND_MSG 请求构造器和发送器。
 *
 * 参数说明：
 * - clientfd：客户端 socket。
 * - str：命令参数，格式为 friendid。
 */
void addfriend(int clientfd, string str)
{
    int friendid = atoi(str.c_str());
    json js;
    js["msgid"] = ADD_FRIEND_MSG;
    js["id"] = g_currentUser.getId();
    js["friendid"] = friendid;
    string buffer = js.dump();

    int len = send(clientfd, buffer.c_str(), strlen(buffer.c_str()) + 1, 0);
    if (-1 == len)
    {
        cerr << "send addfriend msg error -> " << buffer << endl;
    }
}
/*
 * 函数名直译：聊天命令。
 *
 * 通俗说：用户输入 chat:好友id:消息内容 后，这里解析好友 id 和消息，
 * 加上当前用户 id、昵称、时间，然后发给服务器。
 *
 * 专业说法：ONE_CHAT_MSG 请求构造器，负责把命令行输入转换成私聊 JSON 协议包。
 *
 * 参数说明：
 * - clientfd：客户端 socket。
 * - str：命令参数，格式为 friendid:message。
 */
void chat(int clientfd, string str)
{
    int idx = str.find(":"); // friendid:message
    if (-1 == idx)
    {
        cerr << "chat command invalid!" << endl;
        return;
    }

    int friendid = atoi(str.substr(0, idx).c_str());
    string message = str.substr(idx + 1, str.size() - idx);

    json js;
    js["msgid"] = ONE_CHAT_MSG;
    js["id"] = g_currentUser.getId();
    js["name"] = g_currentUser.getName();
    js["toid"] = friendid;
    js["msg"] = message;
    js["time"] = getCurrentTime();
    string buffer = js.dump();

    int len = send(clientfd, buffer.c_str(), strlen(buffer.c_str()) + 1, 0);
    if (-1 == len)
    {
        cerr << "send chat msg error -> " << buffer << endl;
    }
}
/*
 * 函数名直译：创建群命令。
 *
 * 通俗说：用户输入 creategroup:群名:群描述 后，这里把群资料发送给服务器创建新群。
 *
 * 专业说法：CREATE_GROUP_MSG 请求构造器，负责解析 groupname:groupdesc 参数。
 *
 * 参数说明：
 * - clientfd：客户端 socket。
 * - str：命令参数，格式为 groupname:groupdesc。
 */
void creategroup(int clientfd, string str)
{
    int idx = str.find(":");
    if (-1 == idx)
    {
        cerr << "creategroup command invalid!" << endl;
        return;
    }

    string groupname = str.substr(0, idx);
    string groupdesc = str.substr(idx + 1, str.size() - idx);

    json js;
    js["msgid"] = CREATE_GROUP_MSG;
    js["id"] = g_currentUser.getId();
    js["groupname"] = groupname;
    js["groupdesc"] = groupdesc;
    string buffer = js.dump();

    int len = send(clientfd, buffer.c_str(), strlen(buffer.c_str()) + 1, 0);
    if (-1 == len)
    {
        cerr << "send creategroup msg error -> " << buffer << endl;
    }
}
/*
 * 函数名直译：加入群命令。
 *
 * 通俗说：用户输入 addgroup:群id 后，这里请求服务器把当前用户加入该群。
 *
 * 专业说法：ADD_GROUP_MSG 请求构造器。
 *
 * 参数说明：
 * - clientfd：客户端 socket。
 * - str：命令参数，格式为 groupid。
 */
void addgroup(int clientfd, string str)
{
    int groupid = atoi(str.c_str());
    json js;
    js["msgid"] = ADD_GROUP_MSG;
    js["id"] = g_currentUser.getId();
    js["groupid"] = groupid;
    string buffer = js.dump();

    int len = send(clientfd, buffer.c_str(), strlen(buffer.c_str()) + 1, 0);
    if (-1 == len)
    {
        cerr << "send addgroup msg error -> " << buffer << endl;
    }
}
/*
 * 函数名直译：群聊命令。
 *
 * 通俗说：用户输入 groupchat:群id:消息内容 后，这里打包群聊 JSON 发给服务器。
 *
 * 专业说法：GROUP_CHAT_MSG 请求构造器，负责把命令参数转换成群聊协议包。
 *
 * 参数说明：
 * - clientfd：客户端 socket。
 * - str：命令参数，格式为 groupid:message。
 */
void groupchat(int clientfd, string str)
{
    int idx = str.find(":");
    if (-1 == idx)
    {
        cerr << "groupchat command invalid!" << endl;
        return;
    }

    int groupid = atoi(str.substr(0, idx).c_str());
    string message = str.substr(idx + 1, str.size() - idx);

    json js;
    js["msgid"] = GROUP_CHAT_MSG;
    js["id"] = g_currentUser.getId();
    js["name"] = g_currentUser.getName();
    js["groupid"] = groupid;
    js["msg"] = message;
    js["time"] = getCurrentTime();
    string buffer = js.dump();

    int len = send(clientfd, buffer.c_str(), strlen(buffer.c_str()) + 1, 0);
    if (-1 == len)
    {
        cerr << "send groupchat msg error -> " << buffer << endl;
    }
}
/*
 * 函数名直译：登出命令。
 *
 * 通俗说：用户输入 loginout 后，这里通知服务器当前用户要下线，
 * 然后退出聊天主菜单，回到首页登录/注册菜单。
 *
 * 专业说法：LOGINOUT_MSG 请求构造器，并在发送成功后关闭客户端登录态菜单循环。
 *
 * 参数说明：
 * - clientfd：客户端 socket。
 * - string：为了统一命令处理器签名保留，本函数不使用。
 */
void loginout(int clientfd, string)
{
    json js;
    js["msgid"] = LOGINOUT_MSG;
    js["id"] = g_currentUser.getId();
    string buffer = js.dump();

    int len = send(clientfd, buffer.c_str(), strlen(buffer.c_str()) + 1, 0);
    if (-1 == len)
    {
        cerr << "send loginout msg error -> " << buffer << endl;
    }
    else
    {
        isMainMenuRunning = false;
    }   
}

/*
 * 函数名直译：获取当前时间。
 *
 * 通俗说：发送聊天消息时，给消息加一个当前时间字符串，方便接收方看到发送时间。
 *
 * 专业说法：将 system_clock 当前时间格式化为 yyyy-mm-dd HH:MM:SS 字符串。
 *
 * 返回值：当前本地时间字符串。
 */
string getCurrentTime()
{
    auto tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    struct tm *ptm = localtime(&tt);
    char date[60] = {0};
    sprintf(date, "%d-%02d-%02d %02d:%02d:%02d",
            (int)ptm->tm_year + 1900, (int)ptm->tm_mon + 1, (int)ptm->tm_mday,
            (int)ptm->tm_hour, (int)ptm->tm_min, (int)ptm->tm_sec);
    return std::string(date);
}
