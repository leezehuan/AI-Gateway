#ifndef PUBLIC_H
#define PUBLIC_H

/*
 * server 和 client 共用的协议编号。
 *
 * 通俗说：客户端发 JSON 时会带一个 msgid，服务器看到这个数字就知道
 * 该执行登录、注册、聊天还是群聊。客户端收到响应时也通过 msgid 判断
 * 这是登录响应还是注册响应。
 *
 * 专业说法：EnMsgType 是应用层协议的消息类型枚举，保证客户端和服务端
 * 对同一类业务使用相同的整数编号。
 */
enum EnMsgType
{
    LOGIN_MSG = 1, // 登录消息
    LOGIN_MSG_ACK, // 登录响应消息
    LOGINOUT_MSG, // 注销消息
    REG_MSG, // 注册消息
    REG_MSG_ACK, // 注册响应消息
    ONE_CHAT_MSG, // 聊天消息
    ADD_FRIEND_MSG, // 添加好友消息

    CREATE_GROUP_MSG, // 创建群组
    ADD_GROUP_MSG, // 加入群组
    GROUP_CHAT_MSG, // 群聊天
};

#endif
