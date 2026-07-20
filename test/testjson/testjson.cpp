#include "json.hpp"
using json = nlohmann::json;

#include <iostream>
#include <vector>
#include <map>
#include <string>
using namespace std;

/*
 * 函数名直译：函数一。
 *
 * 通俗说：演示最普通的 JSON 对象怎么写字段，并序列化成字符串。
 *
 * 专业说法：构造包含 msg_type、from、to、msg 的 nlohmann::json 对象，
 * 使用 dump 生成可发送的 JSON 文本。
 *
 * 返回值：序列化后的 JSON 字符串。
 */
string func1()
{
    json js;
    js["msg_type"] = 2;
    js["from"] = "zhang san";
    js["to"] = "li si";
    js["msg"] = "hello, what are you doing now?";

    string sendBuf = js.dump();
    //cout<<sendBuf.c_str()<<endl;
    return sendBuf;
}

/*
 * 函数名直译：函数二。
 *
 * 通俗说：演示 JSON 里怎么放数组、普通键值对，以及嵌套对象。
 *
 * 专业说法：展示 nlohmann::json 对数组字段、字符串字段和对象字段的构造方式。
 *
 * 返回值：序列化后的 JSON 字符串。
 */
string func2()
{
    json js;
    // 添加数组
    js["id"] = {1, 2, 3, 4, 5};
    // 添加key-value
    js["name"] = "zhang san";
    // 添加对象
    js["msg"]["zhang san"] = "hello world";
    js["msg"]["liu shuo"] = "hello china";
    // 上面等同于下面这句一次性添加数组对象
    js["msg"] = {{"zhang san", "hello world"}, {"liu shuo", "hello china"}};
    //cout << js << endl;
    return js.dump();
}

/*
 * 函数名直译：函数三。
 *
 * 通俗说：演示 C++ 标准容器 vector 和 map 可以直接放进 JSON。
 *
 * 专业说法：利用 nlohmann::json 对 STL 容器的内置转换能力，
 * 将 vector<int> 和 map<int, string> 序列化为 JSON 字段。
 *
 * 返回值：序列化后的 JSON 字符串。
 */
string func3()
{
    json js;

    // 直接序列化一个vector容器
    vector<int> vec;
    vec.push_back(1);
    vec.push_back(2);
    vec.push_back(5);

    js["list"] = vec;

    // 直接序列化一个map容器
    map<int, string> m;
    m.insert({1, "挺好的?"});
    m.insert({2, "华山"});
    m.insert({3, "泰山"});

    js["path"] = m;

    string sendBuf = js.dump(); // json数据对象 =》序列化 json字符串
    //cout<<sendBuf<<endl;
    return sendBuf;
}

/*
 * 函数名直译：主函数。
 *
 * 通俗说：调用上面的 JSON 示例函数，再把得到的字符串解析回 JSON 对象，
 * 演示如何读取字段。
 *
 * 专业说法：nlohmann::json 序列化和反序列化的最小测试入口。
 *
 * 返回值：程序正常结束返回 0。
 */
int main()
{
    string recvBuf = func1();
    // 数据的反序列化   json字符串 =》反序列化 数据对象（看作容器，方便访问）
    json jsbuf = json::parse(recvBuf);
    cout<<jsbuf["msg_type"]<<endl;
    cout<<jsbuf["from"]<<endl;
    cout<<jsbuf["to"]<<endl;
    cout<<jsbuf["msg"]<<endl;

    // cout<<jsbuf["id"]<<endl;
    // auto arr = jsbuf["id"];
    // cout<<arr[2]<<endl;

    // auto msgjs = jsbuf["msg"];
    // cout<<msgjs["zhang san"]<<endl;
    // cout<<msgjs["liu shuo"]<<endl;

    // vector<int> vec = jsbuf["list"]; // js对象里面的数组类型，直接放入vector容器当中
    // for (int &v : vec)
    // {
    //     cout << v << " ";
    // }
    // cout << endl;

    // map<int, string> mymap = jsbuf["path"];
    // for (auto &p : mymap)
    // {
    //     cout << p.first << " " << p.second << endl;
    // }
    // cout << endl;

    return 0;
}
