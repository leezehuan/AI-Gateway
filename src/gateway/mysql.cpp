#include "gateway/mysql.hpp"

#include <mysql.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

namespace ai_gateway
{

DatabaseError::DatabaseError(const std::string &message)
    : std::runtime_error(message)
{
}

namespace
{

[[noreturn]] void throw_connection_error(MYSQL *connection)
{
    const std::string detail = connection == nullptr ? "client initialization failed"
                                                     : mysql_error(connection);
    throw DatabaseError("database operation failed: " + detail);
}

[[noreturn]] void throw_statement_error(MYSQL_STMT *statement)
{
    throw DatabaseError("database operation failed: " + std::string(mysql_stmt_error(statement)));
}

struct StatementCloser
{


    void operator()(MYSQL_STMT *statement) const
    {
        if (statement != nullptr)
        {
            mysql_stmt_close(statement);
        }
    }
};

using Statement = std::unique_ptr<MYSQL_STMT, StatementCloser>;

Statement prepare(MYSQL *connection, const std::string &sql)
{
    Statement statement(mysql_stmt_init(connection));
    if (!statement)
    {
        throw_connection_error(connection);
    }
    if (mysql_stmt_prepare(statement.get(), sql.data(), sql.size()) != 0)
    {
        throw_statement_error(statement.get());
    }
    return statement;
}

class ParameterBindings
{
public:


    ParameterBindings(MYSQL_STMT *statement, const std::vector<std::string> &parameters)
        : bindings_(parameters.size()), lengths_(parameters.size())
    {
        if (mysql_stmt_param_count(statement) != parameters.size())
        {
            throw DatabaseError("database operation failed: invalid parameter count");
        }
        for (std::size_t index = 0; index < parameters.size(); ++index)
        {
            std::memset(&bindings_[index], 0, sizeof(MYSQL_BIND));
            lengths_[index] = static_cast<unsigned long>(parameters[index].size());
            bindings_[index].buffer_type = MYSQL_TYPE_STRING;
            bindings_[index].buffer = const_cast<char *>(parameters[index].data());
            bindings_[index].buffer_length = lengths_[index];
            bindings_[index].length = &lengths_[index];
        }
        if (!parameters.empty() && mysql_stmt_bind_param(statement, bindings_.data()) != 0)
        {
            throw_statement_error(statement);
        }
    }

private:
    std::vector<MYSQL_BIND> bindings_;
    std::vector<unsigned long> lengths_;
};
}

class MySqlConnection::Impl
{
public:


    explicit Impl(DatabaseConfig config)
        : config_(std::move(config))
    {
    }



    ~Impl()
    {
        if (connection_ != nullptr)
        {
            mysql_close(connection_);
        }
    }



    MYSQL *connection()
    {
        if (connection_ != nullptr && mysql_ping(connection_) == 0)
        {
            return connection_;
        }
        if (connection_ != nullptr)
        {
            mysql_close(connection_);
            connection_ = nullptr;
        }

        connection_ = mysql_init(nullptr);
        if (connection_ == nullptr)
        {
            throw_connection_error(nullptr);
        }
        mysql_options(connection_, MYSQL_OPT_CONNECT_TIMEOUT, &config_.connect_timeout_seconds);
        const unsigned read_timeout = std::max(2U, config_.connect_timeout_seconds);
        mysql_options(connection_, MYSQL_OPT_READ_TIMEOUT, &read_timeout);
        mysql_options(connection_, MYSQL_OPT_WRITE_TIMEOUT, &read_timeout);
        if (mysql_real_connect(connection_, config_.host.c_str(), config_.user.c_str(),
                               config_.password.c_str(), config_.name.c_str(), config_.port,
                               nullptr, CLIENT_MULTI_STATEMENTS) == nullptr)
        {
            const std::string detail = mysql_error(connection_);
            mysql_close(connection_);
            connection_ = nullptr;
            throw DatabaseError("database connection failed: " + detail);
        }
        mysql_set_character_set(connection_, "utf8mb4");
        if (mysql_query(connection_, "SET time_zone = '+00:00'") != 0)
        {
            const std::string detail = mysql_error(connection_);
            mysql_close(connection_);
            connection_ = nullptr;
            throw DatabaseError("database session initialization failed: " + detail);
        }
        return connection_;
    }

private:
    DatabaseConfig config_;
    MYSQL *connection_ = nullptr;
};

MySqlConnection::MySqlConnection(DatabaseConfig config)
    : impl_(std::make_unique<Impl>(std::move(config)))
{
}

MySqlConnection::~MySqlConnection() = default;

void MySqlConnection::execute_static(const std::string &sql)
{
    MYSQL *connection = impl_->connection();
    if (mysql_real_query(connection, sql.data(), sql.size()) != 0)
    {
        throw_connection_error(connection);
    }
    int next_result = 0;
    do
    {
        MYSQL_RES *result = mysql_store_result(connection);
        if (result != nullptr)
        {
            mysql_free_result(result);
        }
        else if (mysql_field_count(connection) != 0)
        {
            throw_connection_error(connection);
        }
        next_result = mysql_next_result(connection);
        if (next_result > 0)
        {
            throw_connection_error(connection);
        }
    } while (next_result == 0);
}

std::uint64_t MySqlConnection::execute_prepared(
    const std::string &sql,
    const std::vector<std::string> &parameters)
{
    Statement statement = prepare(impl_->connection(), sql);
    ParameterBindings bindings(statement.get(), parameters);
    if (mysql_stmt_execute(statement.get()) != 0)
    {
        throw_statement_error(statement.get());
    }
    return static_cast<std::uint64_t>(mysql_stmt_affected_rows(statement.get()));
}

DatabaseRows MySqlConnection::query_prepared(
    const std::string &sql,
    const std::vector<std::string> &parameters)
{
    Statement statement = prepare(impl_->connection(), sql);
    ParameterBindings parameter_bindings(statement.get(), parameters);
    my_bool update_max_length = 1;
    mysql_stmt_attr_set(statement.get(), STMT_ATTR_UPDATE_MAX_LENGTH, &update_max_length);
    if (mysql_stmt_execute(statement.get()) != 0 || mysql_stmt_store_result(statement.get()) != 0)
    {
        throw_statement_error(statement.get());
    }

    MYSQL_RES *metadata = mysql_stmt_result_metadata(statement.get());
    if (metadata == nullptr)
    {
        return {};
    }
    const std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> metadata_guard(
        metadata, mysql_free_result);
    const unsigned columns = mysql_num_fields(metadata);
    MYSQL_FIELD *fields = mysql_fetch_fields(metadata);
    std::vector<MYSQL_BIND> bindings(columns);
    std::vector<std::vector<char>> buffers(columns);
    std::vector<unsigned long> lengths(columns);
    std::vector<my_bool> nulls(columns);
    std::vector<my_bool> errors(columns);
    for (unsigned index = 0; index < columns; ++index)
    {
        const unsigned long requested = std::max<unsigned long>(1, fields[index].max_length + 1);
        if (requested > 1024UL * 1024UL)
        {
            throw DatabaseError("database operation failed: result column is too large");
        }
        buffers[index].resize(requested);
        std::memset(&bindings[index], 0, sizeof(MYSQL_BIND));
        bindings[index].buffer_type = MYSQL_TYPE_STRING;
        bindings[index].buffer = buffers[index].data();
        bindings[index].buffer_length = requested;
        bindings[index].length = &lengths[index];
        bindings[index].is_null = &nulls[index];
        bindings[index].error = &errors[index];
    }
    if (columns != 0 && mysql_stmt_bind_result(statement.get(), bindings.data()) != 0)
    {
        throw_statement_error(statement.get());
    }

    DatabaseRows rows;
    while (true)
    {
        const int result = mysql_stmt_fetch(statement.get());
        if (result == MYSQL_NO_DATA)
        {
            break;
        }
        if (result != 0)
        {
            throw_statement_error(statement.get());
        }
        DatabaseRow row;
        row.reserve(columns);
        // MariaDB 的 NULL 与空字符串语义不同；这里用空字符串表示 NULL，Repository 必须依据查询约定解释它。
        for (unsigned index = 0; index < columns; ++index)
        {
            row.emplace_back(nulls[index] ? std::string()
                                          : std::string(buffers[index].data(), lengths[index]));
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

std::uint64_t MySqlConnection::last_insert_id() const
{
    return static_cast<std::uint64_t>(mysql_insert_id(impl_->connection()));
}

void MySqlConnection::begin()
{
    execute_static("START TRANSACTION");
}

void MySqlConnection::commit()
{
    execute_static("COMMIT");
}

void MySqlConnection::rollback() noexcept
{
    try
    {
        execute_static("ROLLBACK");
    }
    catch (...)
    {
    }
}
}
