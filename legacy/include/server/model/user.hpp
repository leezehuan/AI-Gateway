#ifndef USER_H
#define USER_H

#include <string>
using namespace std;

// User表的ORM类
class User
{
public:
    /*
     * 函数名直译：用户构造函数。
     *
     * 通俗说：创建一个用户对象，把 id、昵称、密码、在线状态这些字段先放进去。
     * 如果什么都不传，就创建一个“无效/未登录”的离线用户。
     *
     * 专业说法：User 表的 ORM 数据对象构造器，用默认参数支持空对象和完整对象创建。
     *
     * 参数说明：
     * - id：用户 id，默认 -1，常用来表示数据库里没查到用户。
     * - name：用户昵称。
     * - pwd：用户密码。
     * - state：在线状态，默认 offline。
     */
    User(int id = -1, string name = "", string pwd = "", string state = "offline")
    {
        this->id = id;
        this->name = name;
        this->password = pwd;
        this->state = state;
    }

    // 设置用户 id，通常在数据库插入成功后回填自增主键时使用。
    void setId(int id) { this->id = id; }
    // 设置用户昵称。
    void setName(string name) { this->name = name; }
    // 设置用户密码。
    void setPwd(string pwd) { this->password = pwd; }
    // 设置用户在线状态，例如 online 或 offline。
    void setState(string state) { this->state = state; }

    // 获取用户 id。
    int getId() { return this->id; }
    // 获取用户昵称。
    string getName() { return this->name; }
    // 获取用户密码，登录校验时会用到。
    string getPwd() { return this->password; }
    // 获取用户在线状态。
    string getState() { return this->state; }

protected:
    int id;
    string name;
    string password;
    string state;
};

#endif
