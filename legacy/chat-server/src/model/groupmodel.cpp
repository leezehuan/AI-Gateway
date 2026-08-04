#include "groupmodel.hpp"
#include "db.h"

/*
 * 函数名直译：创建群组。
 *
 * 通俗说：把一个新群的名称和描述存入 allgroup 表。
 * 成功后会把数据库生成的群 id 写回 group 对象。
 *
 * 专业说法：群组基础信息新增接口，封装 allgroup 表 INSERT 和自增主键回填。
 *
 * 参数说明：
 * - group：待创建的群对象。使用引用是为了回填 groupid。
 *
 * 返回值：创建成功返回 true；数据库连接或 SQL 执行失败返回 false。
 */
bool GroupModel::createGroup(Group &group)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "insert into allgroup(groupname, groupdesc) values('%s', '%s')",
            group.getName().c_str(), group.getDesc().c_str());

    MySQL mysql;
    if (mysql.connect())
    {
        if (mysql.update(sql))
        {
            group.setId(mysql_insert_id(mysql.getConnection()));
            return true;
        }
    }

    return false;
}

/*
 * 函数名直译：加入群组。
 *
 * 通俗说：把某个用户加入某个群，并记录他在群里的角色，比如 creator 或 normal。
 *
 * 专业说法：向 groupuser 群成员关系表插入一条记录。
 *
 * 参数说明：
 * - userid：加入群的用户 id。
 * - groupid：目标群 id。
 * - role：用户在群内的角色。
 */
void GroupModel::addGroup(int userid, int groupid, string role)
{
    // 1.组装sql语句
    char sql[1024] = {0};
    sprintf(sql, "insert into groupuser values(%d, %d, '%s')",
            groupid, userid, role.c_str());

    MySQL mysql;
    if (mysql.connect())
    {
        mysql.update(sql);
    }
}

/*
 * 函数名直译：查询群组。
 *
 * 通俗说：用户登录后，需要知道自己加入了哪些群，以及每个群里有哪些成员。
 * 这个函数就把这些群资料和成员资料都查出来。
 *
 * 专业说法：按 userid 查询其所属群组，并为每个 Group 聚合对应的 GroupUser 成员列表。
 *
 * 参数说明：
 * - userid：当前登录用户 id。
 *
 * 返回值：群组对象列表；每个 Group 内部的 users 保存该群成员信息。
 *
 * 实现方法：
 * 1. 通过 allgroup 和 groupuser 联表查出 userid 所在的群基础信息。
 * 2. 遍历每个群，再通过 user 和 groupuser 联表查出该群全部成员。
 * 3. 把成员转换成 GroupUser，push 到 group.getUsers() 返回的成员列表引用中。
 */
vector<Group> GroupModel::queryGroups(int userid)
{
    /*
    1. 先根据userid在groupuser表中查询出该用户所属的群组信息
    2. 在根据群组信息，查询属于该群组的所有用户的userid，并且和user表进行多表联合查询，查出用户的详细信息
    */
    char sql[1024] = {0};
    sprintf(sql, "select a.id,a.groupname,a.groupdesc from allgroup a inner join \
         groupuser b on a.id = b.groupid where b.userid=%d",
            userid);

    vector<Group> groupVec;

    MySQL mysql;
    if (mysql.connect())
    {
        MYSQL_RES *res = mysql.query(sql);
        if (res != nullptr)
        {
            MYSQL_ROW row;
            // 查出userid所有的群组信息
            while ((row = mysql_fetch_row(res)) != nullptr)
            {
                Group group;
                group.setId(atoi(row[0]));
                group.setName(row[1]);
                group.setDesc(row[2]);
                groupVec.push_back(group);
            }
            mysql_free_result(res);
        }
    }

    // 查询群组的用户信息
    for (Group &group : groupVec)
    {
        sprintf(sql, "select a.id,a.name,a.state,b.grouprole from user a \
            inner join groupuser b on b.userid = a.id where b.groupid=%d",
                group.getId());

        MYSQL_RES *res = mysql.query(sql);
        if (res != nullptr)
        {
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(res)) != nullptr)
            {
                GroupUser user;
                user.setId(atoi(row[0]));
                user.setName(row[1]);
                user.setState(row[2]);
                user.setRole(row[3]);
                group.getUsers().push_back(user);
            }
            mysql_free_result(res);
        }
    }
    return groupVec;
}

/*
 * 函数名直译：查询群用户。
 *
 * 通俗说：群聊时，发送者不用再收到自己发出的消息，所以这里查的是
 * “这个群里除我以外的所有成员 id”。
 *
 * 专业说法：按 groupid 查询 groupuser 表中的成员 id，并排除发送者 userid。
 *
 * 参数说明：
 * - userid：发送群消息的用户 id，需要从结果中排除。
 * - groupid：群组 id。
 *
 * 返回值：其他群成员 id 列表；没有成员或查询失败时返回空 vector。
 */
vector<int> GroupModel::queryGroupUsers(int userid, int groupid)
{
    char sql[1024] = {0};
    sprintf(sql, "select userid from groupuser where groupid = %d and userid != %d", groupid, userid);

    vector<int> idVec;
    MySQL mysql;
    if (mysql.connect())
    {
        MYSQL_RES *res = mysql.query(sql);
        if (res != nullptr)
        {
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(res)) != nullptr)
            {
                idVec.push_back(atoi(row[0]));
            }
            mysql_free_result(res);
        }
    }
    return idVec;
}
