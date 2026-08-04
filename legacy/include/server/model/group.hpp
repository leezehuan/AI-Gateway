#ifndef GROUP_H
#define GROUP_H

#include "groupuser.hpp"
#include <string>
#include <vector>
using namespace std;

// 群组信息的ORM类，同时聚合该群的成员列表
class Group
{
public:
    /*
     * 函数名直译：群组构造函数。
     *
     * 通俗说：创建一个群对象，里面保存群 id、群名称和群描述。
     * 如果不传参数，就得到一个默认的空群对象。
     *
     * 专业说法：allgroup 表基础字段的 ORM 构造器，成员列表 users 会按需在查询群成员时填充。
     *
     * 参数说明：
     * - id：群 id，默认 -1 表示尚未落库或无效群。
     * - name：群名称。
     * - desc：群描述。
     */
    Group(int id = -1, string name = "", string desc = "")
    {
        this->id = id;
        this->name = name;
        this->desc = desc;
    }

    // 设置群 id，创建群成功后会回填数据库自增主键。
    void setId(int id) { this->id = id; }
    // 设置群名称。
    void setName(string name) { this->name = name; }
    // 设置群描述。
    void setDesc(string desc) { this->desc = desc; }

    // 获取群 id。
    int getId() { return this->id; }
    // 获取群名称。
    string getName() { return this->name; }
    // 获取群描述。
    string getDesc() { return this->desc; }
    // 获取群成员列表引用，调用者可以直接向其中追加 GroupUser。
    vector<GroupUser> &getUsers() { return this->users; }

private:
    int id;
    string name;
    string desc;
    vector<GroupUser> users;
};

#endif
