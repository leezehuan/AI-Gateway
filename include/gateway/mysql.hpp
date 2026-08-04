#ifndef AI_GATEWAY_MYSQL_HPP
#define AI_GATEWAY_MYSQL_HPP

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace ai_gateway
{
/* Gateway 专用数据库连接配置；不复用 Legacy 聊天数据库凭据。 */
struct DatabaseConfig
{
    std::string host = "127.0.0.1";
    std::uint16_t port = 3306;
    std::string user = "ai_gateway";
    std::string password;
    std::string name = "ai_gateway";
    unsigned connect_timeout_seconds = 2;
};

class DatabaseError : public std::runtime_error
{
public:
    explicit DatabaseError(const std::string &message);
};

using DatabaseRow = std::vector<std::string>;
using DatabaseRows = std::vector<DatabaseRow>;

class MySqlConnection
{
public:
    explicit MySqlConnection(DatabaseConfig config);
    ~MySqlConnection();

    MySqlConnection(const MySqlConnection &) = delete;
    MySqlConnection &operator=(const MySqlConnection &) = delete;

    /* 只执行迁移等管理员提供的静态 SQL，不接受运行时参数。 */
    void execute_static(const std::string &sql);
    /* 使用 MariaDB prepared statement 执行 DML，返回影响行数/插入 ID。 */
    std::uint64_t execute_prepared(const std::string &sql,
                                   const std::vector<std::string> &parameters = {});
    /* 使用 prepared statement 查询，并把每列转换成受限字符串。 */
    DatabaseRows query_prepared(const std::string &sql,
                                const std::vector<std::string> &parameters = {});
    std::uint64_t last_insert_id() const;
    /* 开启/提交/回滚一个 Gateway 管理事务。 */
    void begin();
    void commit();
    void rollback() noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
