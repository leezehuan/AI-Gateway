#include "db.h"
#include <muduo/base/Logging.h>

// 数据库配置信息
static string server = "127.0.0.1";
static string user = "root";
static string password = "123456";
static string dbname = "chat";

/*
 * 函数名直译：MySQL 构造函数。
 *
 * 通俗说：先向 MySQL C API 申请一个连接句柄，后面真正连接数据库时会继续使用它。
 *
 * 专业说法：初始化 MYSQL 连接上下文，mysql_init 返回的指针保存到 _conn。
 *
 * 实现方法：调用 mysql_init(nullptr)。这里只创建句柄，不发起网络连接。
 */
MySQL::MySQL()
{
    _conn = mysql_init(nullptr);
}

/*
 * 函数名直译：MySQL 析构函数。
 *
 * 通俗说：MySQL 对象不用了，就把数据库连接关掉，避免资源泄漏。
 *
 * 专业说法：释放 MYSQL 连接上下文。对象生命周期结束时自动执行，符合 RAII 思路。
 *
 * 实现方法：如果 _conn 不为空，调用 mysql_close。
 */
MySQL::~MySQL()
{
    if (_conn != nullptr)
        mysql_close(_conn);
}

/*
 * 函数名直译：连接。
 *
 * 通俗说：用上面的数据库地址、用户名、密码和库名，真正连到本机 MySQL。
 *
 * 专业说法：建立到 chat 数据库的 TCP 连接，并设置连接字符集，供后续 SQL 使用。
 *
 * 返回值：连接成功返回 true，失败返回 false。
 *
 * 实现方法：调用 mysql_real_connect；成功后执行 set names gbk，随后写入日志。
 */
bool MySQL::connect()
{
    MYSQL *p = mysql_real_connect(_conn, server.c_str(), user.c_str(),
                                  password.c_str(), dbname.c_str(), 3306, nullptr, 0);
    if (p != nullptr)
    {
        // C和C++代码默认的编码字符是ASCII，如果不设置，从MySQL上拉下来的中文显示？
        mysql_query(_conn, "set names gbk");
        LOG_INFO << "connect mysql success!";
    }
    else
    {
        LOG_INFO << "connect mysql fail!";
    }

    return p;
}

/*
 * 函数名直译：更新。
 *
 * 通俗说：执行不需要返回结果集的 SQL，比如 insert、delete、update。
 *
 * 专业说法：通用写操作接口，封装 mysql_query 对非 SELECT 语句的执行结果。
 *
 * 参数说明：
 * - sql：已经拼好的 SQL 字符串。
 *
 * 返回值：执行成功返回 true；失败记录日志并返回 false。
 */
bool MySQL::update(string sql)
{
    if (mysql_query(_conn, sql.c_str()))
    {
        LOG_INFO << __FILE__ << ":" << __LINE__ << ":"
                 << sql << "更新失败!";
        return false;
    }

    return true;
}

/*
 * 函数名直译：查询。
 *
 * 通俗说：执行 select 语句，并把查询结果交给调用者继续读取。
 *
 * 专业说法：通用读操作接口，封装 mysql_query 和 mysql_use_result。
 *
 * 参数说明：
 * - sql：已经拼好的 SELECT SQL 字符串。
 *
 * 返回值：成功返回 MYSQL_RES* 结果集；失败返回 nullptr。
 *
 * 注意：调用者拿到非空结果集后，读取完必须调用 mysql_free_result 释放。
 */
MYSQL_RES *MySQL::query(string sql)
{
    if (mysql_query(_conn, sql.c_str()))
    {
        LOG_INFO << __FILE__ << ":" << __LINE__ << ":"
                 << sql << "查询失败!";
        return nullptr;
    }
    
    return mysql_use_result(_conn);
}

/*
 * 函数名直译：获取连接。
 *
 * 通俗说：把底层 MYSQL* 暴露出去，方便调用 MySQL C API 的其他函数，
 * 比如获取刚插入记录的自增 id。
 *
 * 专业说法：返回当前 MySQL 封装对象持有的原生连接句柄。
 *
 * 返回值：MYSQL*，由本对象管理生命周期，外部不要主动 close。
 */
MYSQL* MySQL::getConnection()
{
    return _conn;
}
