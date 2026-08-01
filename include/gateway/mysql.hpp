#ifndef AI_GATEWAY_MYSQL_HPP
#define AI_GATEWAY_MYSQL_HPP

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace ai_gateway
{
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

    void execute_static(const std::string &sql);
    std::uint64_t execute_prepared(const std::string &sql,
                                   const std::vector<std::string> &parameters = {});
    DatabaseRows query_prepared(const std::string &sql,
                                const std::vector<std::string> &parameters = {});
    std::uint64_t last_insert_id() const;
    void begin();
    void commit();
    void rollback() noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace ai_gateway

#endif
