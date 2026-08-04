#include "chatservice.hpp"
#include "public.hpp"
#include <muduo/base/Logging.h>
#include <vector>
using namespace std;
using namespace muduo;

/*
 * 函数名直译：实例。
 *
 * 通俗说：整个服务器只需要一个业务调度中心，所有网络连接收到消息后，
 * 都来找同一个 ChatService 对象处理。
 *
 * 专业说法：这是线程安全的局部静态变量单例实现，用于提供全局唯一的
 * 聊天业务服务对象，集中维护消息处理器表、在线连接表和各类数据模型。
 *
 * 实现方法：函数内定义 static ChatService service，C++11 起局部静态变量
 * 初始化是线程安全的；返回它的地址给调用方。
 */
ChatService *ChatService::instance()
{
    static ChatService service;
    return &service;
}

/*
 * 函数名直译：聊天业务服务构造函数。
 *
 * 通俗说：服务器启动时，把“消息编号”和“处理函数”提前登记好。
 * 以后客户端发来的 JSON 里只要带 msgid，就能快速找到该执行的业务。
 *
 * 专业说法：初始化消息分发路由表 _msgHandlerMap，并建立 Redis 连接。
 * Redis 订阅回调会把跨服务器投递过来的消息重新交回 ChatService。
 *
 * 实现方法：用 std::bind 把成员函数绑定成统一签名的 MsgHandler，
 * 插入 unordered_map；随后连接 Redis，连接成功后注册订阅消息上报回调。
 */
ChatService::ChatService()
{
    // 用户基本业务管理相关事件处理回调注册
    _msgHandlerMap.insert({LOGIN_MSG, std::bind(&ChatService::login, this, _1, _2, _3)});
    _msgHandlerMap.insert({LOGINOUT_MSG, std::bind(&ChatService::loginout, this, _1, _2, _3)});
    _msgHandlerMap.insert({REG_MSG, std::bind(&ChatService::reg, this, _1, _2, _3)});
    _msgHandlerMap.insert({ONE_CHAT_MSG, std::bind(&ChatService::oneChat, this, _1, _2, _3)});
    _msgHandlerMap.insert({ADD_FRIEND_MSG, std::bind(&ChatService::addFriend, this, _1, _2, _3)});

    // 群组业务管理相关事件处理回调注册
    _msgHandlerMap.insert({CREATE_GROUP_MSG, std::bind(&ChatService::createGroup, this, _1, _2, _3)});
    _msgHandlerMap.insert({ADD_GROUP_MSG, std::bind(&ChatService::addGroup, this, _1, _2, _3)});
    _msgHandlerMap.insert({GROUP_CHAT_MSG, std::bind(&ChatService::groupChat, this, _1, _2, _3)});

    // 连接redis服务器
    if (_redis.connect())
    {
        // 设置上报消息的回调
        _redis.init_notify_handler(std::bind(&ChatService::handleRedisSubscribeMessage, this, _1, _2));
    }
}

/*
 * 函数名直译：重置。
 *
 * 通俗说：服务器异常退出或按 Ctrl+C 停止时，把数据库里还标记为 online
 * 的用户全部改回 offline，避免下次启动后误以为这些人还在线。
 *
 * 专业说法：这是服务端状态恢复接口，用于清理持久化在线状态。
 * 它只重置 user 表的 state 字段，不处理 TCP 连接，因为进程即将退出。
 *
 * 实现方法：调用 UserModel::resetState 执行批量 SQL 更新。
 */
void ChatService::reset()
{
    // 把online状态的用户，设置成offline
    _userModel.resetState();
}

/*
 * 函数名直译：获取处理器。
 *
 * 通俗说：根据客户端 JSON 里的 msgid，查出“这类消息该由哪个函数处理”。
 * 如果 msgid 写错了，就返回一个只打印错误日志的空处理函数。
 *
 * 专业说法：这是业务分发路由查询函数，将协议层消息类型映射到
 * MsgHandler 回调，实现网络模块和业务模块之间的解耦。
 *
 * 参数说明：
 * - msgid：客户端请求中的消息类型编号，定义在 public.hpp 的 EnMsgType。
 *
 * 返回值：匹配成功返回对应业务回调；匹配失败返回默认 lambda，避免调用方空指针崩溃。
 *
 * 实现方法：在 _msgHandlerMap 中查找 msgid，未找到时闭包捕获 msgid 并输出日志。
 */
MsgHandler ChatService::getHandler(int msgid)
{
    // 记录错误日志，msgid没有对应的事件处理回调
    auto it = _msgHandlerMap.find(msgid);
    if (it == _msgHandlerMap.end())
    {
        // 返回一个默认的处理器，空操作
        return [=](const TcpConnectionPtr &conn, json &js, Timestamp) {
            LOG_ERROR << "msgid:" << msgid << " can not find handler!";
        };
    }
    else
    {
        return _msgHandlerMap[msgid];
    }
}

/*
 * 函数名直译：登录。
 *
 * 通俗说：客户端带着用户 id 和密码来登录。这里负责查数据库验证账号，
 * 防止重复登录，记录当前连接，并把离线消息、好友列表、群组列表一次性返回给客户端。
 *
 * 专业说法：处理 LOGIN_MSG 请求，完成身份认证、在线状态迁移、连接表维护、
 * Redis channel 订阅，以及登录初始化数据聚合。
 *
 * 参数说明：
 * - conn：当前登录客户端的 TCP 连接，成功或失败响应都通过它发送。
 * - js：客户端请求 JSON，至少包含 id 和 password。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：
 * 1. 根据 id 查询 user 表并校验密码。
 * 2. 如果用户已 online，返回重复登录错误。
 * 3. 登录成功后把 id -> conn 写入 _userConnMap，并订阅 Redis 中以 id 命名的通道。
 * 4. 更新数据库用户状态为 online。
 * 5. 查询离线消息、好友列表和群组列表，打包进 LOGIN_MSG_ACK 返回。
 *
 * 注意：_userConnMap 可能被多个工作线程访问，因此写入时必须加 _connMutex。
 */
void ChatService::login(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    int id = js["id"].get<int>();
    string pwd = js["password"];

    User user = _userModel.query(id);
    if (user.getId() == id && user.getPwd() == pwd)
    {
        if (user.getState() == "online")
        {
            // 该用户已经登录，不允许重复登录
            json response;
            response["msgid"] = LOGIN_MSG_ACK;
            response["errno"] = 2;
            response["errmsg"] = "this account is using, input another!";
            conn->send(response.dump());
        }
        else
        {
            // 登录成功，记录用户连接信息
            {
                lock_guard<mutex> lock(_connMutex);
                _userConnMap.insert({id, conn});
            }

            // id用户登录成功后，向redis订阅channel(id)
            _redis.subscribe(id); 

            // 登录成功，更新用户状态信息 state offline=>online
            user.setState("online");
            _userModel.updateState(user);

            json response;
            response["msgid"] = LOGIN_MSG_ACK;
            response["errno"] = 0;
            response["id"] = user.getId();
            response["name"] = user.getName();
            // 查询该用户是否有离线消息
            vector<string> vec = _offlineMsgModel.query(id);
            if (!vec.empty())
            {
                response["offlinemsg"] = vec;
                // 读取该用户的离线消息后，把该用户的所有离线消息删除掉
                _offlineMsgModel.remove(id);
            }

            // 查询该用户的好友信息并返回
            vector<User> userVec = _friendModel.query(id);
            if (!userVec.empty())
            {
                vector<string> vec2;
                for (User &user : userVec)
                {
                    json js;
                    js["id"] = user.getId();
                    js["name"] = user.getName();
                    js["state"] = user.getState();
                    vec2.push_back(js.dump());
                }
                response["friends"] = vec2;
            }

            // 查询用户的群组信息
            vector<Group> groupuserVec = _groupModel.queryGroups(id);
            if (!groupuserVec.empty())
            {
                // group:[{groupid:[xxx, xxx, xxx, xxx]}]
                vector<string> groupV;
                for (Group &group : groupuserVec)
                {
                    json grpjson;
                    grpjson["id"] = group.getId();
                    grpjson["groupname"] = group.getName();
                    grpjson["groupdesc"] = group.getDesc();
                    vector<string> userV;
                    for (GroupUser &user : group.getUsers())
                    {
                        json js;
                        js["id"] = user.getId();
                        js["name"] = user.getName();
                        js["state"] = user.getState();
                        js["role"] = user.getRole();
                        userV.push_back(js.dump());
                    }
                    grpjson["users"] = userV;
                    groupV.push_back(grpjson.dump());
                }

                response["groups"] = groupV;
            }

            conn->send(response.dump());
        }
    }
    else
    {
        // 该用户不存在，用户存在但是密码错误，登录失败
        json response;
        response["msgid"] = LOGIN_MSG_ACK;
        response["errno"] = 1;
        response["errmsg"] = "id or password is invalid!";
        conn->send(response.dump());
    }
}

/*
 * 函数名直译：注册。
 *
 * 通俗说：客户端提交用户名和密码，服务器把它们保存到 user 表。
 * 如果保存成功，就把数据库生成的新用户 id 返回给客户端。
 *
 * 专业说法：处理 REG_MSG 请求，调用 UserModel 插入用户记录，并生成 REG_MSG_ACK。
 *
 * 参数说明：
 * - conn：注册请求对应的 TCP 连接。
 * - js：客户端请求 JSON，包含 name 和 password。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：构造 User 对象，调用 _userModel.insert；insert 成功会回填自增 id，
 * 然后根据结果返回 errno=0 或 errno=1。
 */
void ChatService::reg(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    string name = js["name"];
    string pwd = js["password"];

    User user;
    user.setName(name);
    user.setPwd(pwd);
    bool state = _userModel.insert(user);
    if (state)
    {
        // 注册成功
        json response;
        response["msgid"] = REG_MSG_ACK;
        response["errno"] = 0;
        response["id"] = user.getId();
        conn->send(response.dump());
    }
    else
    {
        // 注册失败
        json response;
        response["msgid"] = REG_MSG_ACK;
        response["errno"] = 1;
        conn->send(response.dump());
    }
}

/*
 * 函数名直译：登出。
 *
 * 通俗说：用户主动注销时，把他从“在线连接表”里删掉，
 * 取消 Redis 订阅，并把数据库状态改成 offline。
 *
 * 专业说法：处理 LOGINOUT_MSG 请求，完成用户在线会话的正常释放。
 *
 * 参数说明：
 * - conn：发起注销的连接，本函数不需要直接使用。
 * - js：客户端请求 JSON，包含 id。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：加锁删除 _userConnMap 中的 userid，调用 Redis::unsubscribe，
 * 最后通过 UserModel::updateState 持久化离线状态。
 */
void ChatService::loginout(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    int userid = js["id"].get<int>();

    {
        lock_guard<mutex> lock(_connMutex);
        auto it = _userConnMap.find(userid);
        if (it != _userConnMap.end())
        {
            _userConnMap.erase(it);
        }
    }

    // 用户注销，相当于就是下线，在redis中取消订阅通道
    _redis.unsubscribe(userid); 

    // 更新用户的状态信息
    User user(userid, "", "", "offline");
    _userModel.updateState(user);
}

/*
 * 函数名直译：客户端关闭异常。
 *
 * 通俗说：客户端不是正常点“注销”，而是断网、关窗口、进程退出时，
 * 服务器也要把它当成下线处理，不能让连接表和数据库状态残留。
 *
 * 专业说法：处理 TCP 连接异常断开后的业务清理，根据连接反查用户 id，
 * 删除在线连接、取消 Redis 订阅并更新用户状态。
 *
 * 参数说明：
 * - conn：已经断开的客户端连接。
 *
 * 实现方法：遍历 _userConnMap 找到 value 等于 conn 的记录，保存 userid 后删除；
 * 如果找到了有效用户，再取消订阅并将 state 改为 offline。
 */
void ChatService::clientCloseException(const TcpConnectionPtr &conn)
{
    User user;
    {
        lock_guard<mutex> lock(_connMutex);
        for (auto it = _userConnMap.begin(); it != _userConnMap.end(); ++it)
        {
            if (it->second == conn)
            {
                // 从map表删除用户的链接信息
                user.setId(it->first);
                _userConnMap.erase(it);
                break;
            }
        }
    }

    // 用户注销，相当于就是下线，在redis中取消订阅通道
    _redis.unsubscribe(user.getId()); 

    // 更新用户的状态信息
    if (user.getId() != -1)
    {
        user.setState("offline");
        _userModel.updateState(user);
    }
}

/*
 * 函数名直译：一对一聊天。
 *
 * 通俗说：用户 A 给用户 B 发消息。如果 B 正连接在本服务器，就直接转发；
 * 如果 B 在线但连在别的服务器，就通过 Redis 转发；如果 B 不在线，就存成离线消息。
 *
 * 专业说法：处理 ONE_CHAT_MSG 请求，按“本机在线 -> 跨服务器在线 -> 离线持久化”
 * 的顺序完成私聊消息投递。
 *
 * 参数说明：
 * - conn：发送者连接，本函数主要用于符合统一回调签名，当前不直接使用。
 * - js：聊天消息 JSON，包含 id、name、toid、msg、time 等字段。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：先在 _userConnMap 查 toid；查不到再访问 user 表判断对方是否 online。
 * 数据库显示 online 代表对方可能在其他服务器，因此 publish 到 Redis 的 toid 通道；
 * 否则写入 offlinemessage 表。
 */
void ChatService::oneChat(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    int toid = js["toid"].get<int>();

    {
        lock_guard<mutex> lock(_connMutex);
        auto it = _userConnMap.find(toid);
        if (it != _userConnMap.end())
        {
            // toid在线，转发消息   服务器主动推送消息给toid用户
            it->second->send(js.dump());
            return;
        }
    }

    // 查询toid是否在线 
    User user = _userModel.query(toid);
    if (user.getState() == "online")
    {
        _redis.publish(toid, js.dump());
        return;
    }

    // toid不在线，存储离线消息
    _offlineMsgModel.insert(toid, js.dump());
}

/*
 * 函数名直译：添加好友。
 *
 * 通俗说：把“我”和“对方”的好友关系写进 friend 表。
 *
 * 专业说法：处理 ADD_FRIEND_MSG 请求，保存 userid -> friendid 的单向好友记录。
 *
 * 参数说明：
 * - conn：请求连接，本函数无需直接响应。
 * - js：请求 JSON，包含 id 和 friendid。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：解析 userid 和 friendid，调用 FriendModel::insert 落库。
 */
void ChatService::addFriend(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    int userid = js["id"].get<int>();
    int friendid = js["friendid"].get<int>();

    // 存储好友信息
    _friendModel.insert(userid, friendid);
}

/*
 * 函数名直译：创建群组。
 *
 * 通俗说：用户新建一个群。服务器先创建群资料，再把创建者加入群成员表，
 * 并标记为 creator。
 *
 * 专业说法：处理 CREATE_GROUP_MSG 请求，维护 allgroup 和 groupuser 两张表。
 *
 * 参数说明：
 * - conn：请求连接，本函数无需直接响应。
 * - js：请求 JSON，包含 id、groupname、groupdesc。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：先调用 GroupModel::createGroup 插入群基础信息并回填 groupid；
 * 成功后调用 GroupModel::addGroup 写入创建者成员关系。
 */
void ChatService::createGroup(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    int userid = js["id"].get<int>();
    string name = js["groupname"];
    string desc = js["groupdesc"];

    // 存储新创建的群组信息
    Group group(-1, name, desc);
    if (_groupModel.createGroup(group))
    {
        // 存储群组创建人信息
        _groupModel.addGroup(userid, group.getId(), "creator");
    }
}

/*
 * 函数名直译：加入群组。
 *
 * 通俗说：把某个用户加入某个群，普通加入者的角色是 normal。
 *
 * 专业说法：处理 ADD_GROUP_MSG 请求，向 groupuser 关系表插入成员记录。
 *
 * 参数说明：
 * - conn：请求连接，本函数无需直接响应。
 * - js：请求 JSON，包含 id 和 groupid。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：解析 userid、groupid，调用 GroupModel::addGroup 并写入 normal 角色。
 */
void ChatService::addGroup(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    int userid = js["id"].get<int>();
    int groupid = js["groupid"].get<int>();
    _groupModel.addGroup(userid, groupid, "normal");
}

/*
 * 函数名直译：群组聊天。
 *
 * 通俗说：用户在群里发一条消息，服务器查出这个群除发送者外的所有成员，
 * 然后逐个投递：本机在线直接发，别的服务器在线走 Redis，不在线就存离线消息。
 *
 * 专业说法：处理 GROUP_CHAT_MSG 请求，完成群成员枚举和多目标消息投递。
 *
 * 参数说明：
 * - conn：发送者连接，本函数无需直接使用。
 * - js：群聊消息 JSON，包含 id、name、groupid、msg、time 等字段。
 * - time：muduo 消息时间戳，本函数暂未使用。
 *
 * 实现方法：调用 GroupModel::queryGroupUsers 获取除自己外的成员 id 列表；
 * 对每个成员按一对一聊天同样的三段策略投递。
 */
void ChatService::groupChat(const TcpConnectionPtr &conn, json &js, Timestamp time)
{
    int userid = js["id"].get<int>();
    int groupid = js["groupid"].get<int>();
    vector<int> useridVec = _groupModel.queryGroupUsers(userid, groupid);

    lock_guard<mutex> lock(_connMutex);
    for (int id : useridVec)
    {
        auto it = _userConnMap.find(id);
        if (it != _userConnMap.end())
        {
            // 转发群消息
            it->second->send(js.dump());
        }
        else
        {
            // 查询toid是否在线 
            User user = _userModel.query(id);
            if (user.getState() == "online")
            {
                _redis.publish(id, js.dump());
            }
            else
            {
                // 存储离线群消息
                _offlineMsgModel.insert(id, js.dump());
            }
        }
    }
}

/*
 * 函数名直译：处理 Redis 订阅消息。
 *
 * 通俗说：如果别的服务器通过 Redis 发来“某用户的新消息”，这里负责判断
 * 这个用户现在还在不在本服务器；在就推给客户端，不在就存成离线消息。
 *
 * 专业说法：Redis subscribe 回调入口，用于接收跨服务器发布的消息并转交给
 * 本机连接或离线消息模型。
 *
 * 参数说明：
 * - userid：Redis channel 编号，本项目直接用用户 id 作为 channel。
 * - msg：Redis 中传来的完整 JSON 字符串。
 *
 * 实现方法：加锁查询 _userConnMap，找到连接则 send；找不到说明连接已迁移或断开，
 * 调用 OfflineMsgModel::insert 做兜底持久化。
 */
void ChatService::handleRedisSubscribeMessage(int userid, string msg)
{
    lock_guard<mutex> lock(_connMutex);
    auto it = _userConnMap.find(userid);
    if (it != _userConnMap.end())
    {
        it->second->send(msg);
        return;
    }

    // 存储该用户的离线消息
    _offlineMsgModel.insert(userid, msg);
}
