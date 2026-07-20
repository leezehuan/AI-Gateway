#include "offlinemessagemodel.hpp"
#include "db.h"

/*
 * 函数名直译：插入离线消息。
 *
 * 通俗说：目标用户不在线时，把本来要发给他的消息先存进数据库。
 * 等他下次登录，服务器再把这些消息取出来发给他。
 *
 * 专业说法：向 offlinemessage 表写入 userid 和消息正文，用于离线消息持久化。
 *
 * 参数说明：
 * - userid：接收离线消息的用户 id。
 * - msg：完整消息内容，通常是 JSON 字符串。
 */
void OfflineMsgModel::insert(int userid, string msg)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "insert into offlinemessage values(%d, '%s')", userid, msg.c_str());

    MySQL mysql;
    if (mysql.connect())
    {
        mysql.update(sql);
    }
}

/*
 * 函数名直译：移除离线消息。
 *
 * 通俗说：用户登录后已经拿到了自己的离线消息，就把这些旧消息从数据库删掉，
 * 防止下次登录重复收到。
 *
 * 专业说法：按 userid 删除 offlinemessage 表中的所有离线消息记录。
 *
 * 参数说明：
 * - userid：要清空离线消息的用户 id。
 */
void OfflineMsgModel::remove(int userid)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "delete from offlinemessage where userid=%d", userid);

    MySQL mysql;
    if (mysql.connect())
    {
        mysql.update(sql);
    }
}

/*
 * 函数名直译：查询离线消息。
 *
 * 通俗说：用户刚登录时，查出他之前没收到的所有私聊和群聊消息。
 *
 * 专业说法：按 userid 查询 offlinemessage 表，返回消息正文列表。
 *
 * 参数说明：
 * - userid：需要拉取离线消息的用户 id。
 *
 * 返回值：消息字符串数组；没有离线消息或查询失败时返回空 vector。
 */
vector<string> OfflineMsgModel::query(int userid)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "select message from offlinemessage where userid = %d", userid);

    vector<string> vec;
    MySQL mysql;
    if (mysql.connect())
    {
        MYSQL_RES *res = mysql.query(sql);
        if (res != nullptr)
        {
            // 把userid用户的所有离线消息放入vec中返回
            MYSQL_ROW row;
            while((row = mysql_fetch_row(res)) != nullptr)
            {
                vec.push_back(row[0]);
            }
            mysql_free_result(res);
            return vec;
        }
    }
    return vec;
}
