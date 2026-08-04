#include "usermodel.hpp"
#include "db.h"
#include <iostream>
using namespace std;

/*
 * 函数名直译：插入用户。
 *
 * 通俗说：注册新账号时，把用户名、密码和默认状态写入 user 表。
 * 插入成功后，还会把数据库生成的用户 id 填回 user 对象。
 *
 * 专业说法：User 表的数据新增接口，封装 INSERT SQL 和自增主键回填。
 *
 * 参数说明：
 * - user：待插入的用户对象。使用引用是为了在成功后 setId 回填自增 id。
 *
 * 返回值：插入成功返回 true；数据库连接或 SQL 执行失败返回 false。
 */
bool UserModel::insert(User &user)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "insert into user(name, password, state) values('%s', '%s', '%s')",
            user.getName().c_str(), user.getPwd().c_str(), user.getState().c_str());

    MySQL mysql;
    if (mysql.connect())
    {
        if (mysql.update(sql))
        {
            // 获取插入成功的用户数据生成的主键id
            user.setId(mysql_insert_id(mysql.getConnection()));
            return true;
        }
    }

    return false;
}

/*
 * 函数名直译：查询用户。
 *
 * 通俗说：根据用户 id 去 user 表找账号资料，登录校验和判断在线状态都会用到它。
 *
 * 专业说法：User 表按主键查询接口，把 MYSQL_ROW 转换成 User ORM 对象。
 *
 * 参数说明：
 * - id：用户编号，即 user 表主键。
 *
 * 返回值：找到用户时返回填充好的 User；找不到或查询失败时返回默认 User，
 * 默认 id 为 -1，可用于判断无效用户。
 */
User UserModel::query(int id)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "select * from user where id = %d", id);

    MySQL mysql;
    if (mysql.connect())
    {
        MYSQL_RES *res = mysql.query(sql);
        if (res != nullptr)
        {
            MYSQL_ROW row = mysql_fetch_row(res);
            if (row != nullptr)
            {
                User user;
                user.setId(atoi(row[0]));
                user.setName(row[1]);
                user.setPwd(row[2]);
                user.setState(row[3]);
                mysql_free_result(res);
                return user;
            }
        }
    }

    return User();
}

/*
 * 函数名直译：更新状态。
 *
 * 通俗说：用户登录时把状态改成 online，注销或断线时改成 offline。
 *
 * 专业说法：更新 user 表 state 字段，用于维护用户在线状态。
 *
 * 参数说明：
 * - user：至少需要包含有效 id 和目标 state 的用户对象。
 *
 * 返回值：更新成功返回 true；连接或 SQL 执行失败返回 false。
 */
bool UserModel::updateState(User user)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "update user set state = '%s' where id = %d", user.getState().c_str(), user.getId());

    MySQL mysql;
    if (mysql.connect())
    {
        if (mysql.update(sql))
        {
            return true;
        }
    }
    return false;
}

/*
 * 函数名直译：重置状态。
 *
 * 通俗说：服务器启动或退出清理时，把所有还显示 online 的用户改回 offline。
 *
 * 专业说法：批量修正 user 表在线状态，避免服务异常退出后留下脏状态。
 *
 * 实现方法：执行 update user set state='offline' where state='online'。
 */
void UserModel::resetState()
{
    // 1.组装sql语句
    char sql[1024] = "update user set state = 'offline' where state = 'online'";

    MySQL mysql;
    if (mysql.connect())
    {
        mysql.update(sql);
    }
}
