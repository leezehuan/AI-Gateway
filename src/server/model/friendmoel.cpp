#include "friendmodel.hpp"
#include "db.h"

/*
 * 函数名直译：插入好友。
 *
 * 通俗说：把 userid 添加 friendid 这件事写进 friend 表。
 *
 * 专业说法：维护好友关系表的新增接口，当前实现是单向关系：
 * userid 能查到 friendid，不会自动插入反向记录。
 *
 * 参数说明：
 * - userid：发起添加好友的用户 id。
 * - friendid：被添加为好友的用户 id。
 */
void FriendModel::insert(int userid, int friendid)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "insert into friend values(%d, %d)", userid, friendid);

    MySQL mysql;
    if (mysql.connect())
    {
        mysql.update(sql);
    }
}

/*
 * 函数名直译：查询好友。
 *
 * 通俗说：用户登录时，查出他的好友 id、昵称和在线状态，返回给客户端显示。
 *
 * 专业说法：通过 friend 表和 user 表的 inner join 查询好友详情列表。
 *
 * 参数说明：
 * - userid：要查询好友列表的用户 id。
 *
 * 返回值：好友 User 对象列表；没有好友或查询失败时返回空 vector。
 */
vector<User> FriendModel::query(int userid)
{
    // 1.组装sql语句
    char sql[1024] = {0};

    sprintf(sql, "select a.id,a.name,a.state from user a inner join friend b on b.friendid = a.id where b.userid=%d", userid);

    vector<User> vec;
    MySQL mysql;
    if (mysql.connect())
    {
        MYSQL_RES *res = mysql.query(sql);
        if (res != nullptr)
        {
            // 把 userid 用户的所有好友信息放入 vec 中返回
            MYSQL_ROW row;
            while((row = mysql_fetch_row(res)) != nullptr)
            {
                User user;
                user.setId(atoi(row[0]));
                user.setName(row[1]);
                user.setState(row[2]);
                vec.push_back(user);
            }
            mysql_free_result(res);
            return vec;
        }
    }
    return vec;
}
