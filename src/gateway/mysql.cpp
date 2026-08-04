#include "gateway/mysql.hpp"

#include <mysql.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>

/*
 * 最小 MariaDB Connector/C 封装。
 *
 * 本文件故意不复用 legacy 聊天项目的 MySQL wrapper：Gateway 运行时的 DML/查询必须走 prepared statement，
 * 不能打印完整 SQL 或参数。MySqlConnection 负责一条连接的惰性重连、UTF-8/UTC session 和 RAII 资源释放；
 * ConnectionPool/Repository 决定并发和业务事务。静态 SQL 入口仅用于受源码控制的 migration/事务控制语句。
 */
namespace ai_gateway
{
/*
 * 函数名直译：数据库错误构造函数。
 *
 * 通俗说：把底层 MariaDB 客户端返回的失败原因包装成 Gateway 自己统一使用的异常类型。
 * 上层只需要捕获 DatabaseError，就能把数据库故障转换为脱敏的 HTTP 错误，而无需依赖 C API 的错误码细节。
 *
 * 专业说法：这是基础设施层异常边界。它继承 std::runtime_error，保留异常传播能力，
 * 同时让 RuntimeState、Repository 和管理命令能够明确区分数据库故障与一般业务校验失败。
 *
 * 参数说明：
 * - message：已经整理过的数据库错误文本。
 *
 * 实现方法：直接把 message 交给基类保存；不记录日志，避免底层错误文本在不合适的位置重复输出。
 */
DatabaseError::DatabaseError(const std::string &message)
    : std::runtime_error(message)
{
}

namespace
{
/*
 * 函数名直译：抛出连接错误。
 *
 * 通俗说：当一条直接使用 MYSQL 连接的操作失败时，立即读取当前连接的错误说明并停止本次数据库工作。
 * 若连 MYSQL 对象都没创建出来，也会给出可识别的初始化失败原因。
 *
 * 专业说法：该函数统一 C 客户端连接级 API 的错误出口，标记 [[noreturn]] 告诉编译器调用后控制流不会继续。
 *
 * 参数说明：
 * - connection：可能为空的 MariaDB C API 连接句柄。
 *
 * 实现方法：从 mysql_error 取得诊断信息，转换为 DatabaseError。
 *
 * 注意：异常内容只供受控的服务端日志和诊断使用；HTTP 响应层必须继续做脱敏处理。
 */
[[noreturn]] void throw_connection_error(MYSQL *connection)
{
    const std::string detail = connection == nullptr ? "client initialization failed"
                                                     : mysql_error(connection);
    throw DatabaseError("database operation failed: " + detail);
}

/*
 * 函数名直译：抛出语句错误。
 *
 * 通俗说：预处理 SQL 的准备、绑定、执行或取结果失败时，由这里把错误交给上层。
 *
 * 专业说法：预处理语句拥有独立于连接的诊断区，因此必须调用 mysql_stmt_error，不能误读连接级错误。
 *
 * 参数说明：
 * - statement：发生失败的预处理语句句柄。
 *
 * 实现方法：读取 statement 专属错误文本并抛出 DatabaseError。
 */
[[noreturn]] void throw_statement_error(MYSQL_STMT *statement)
{
    throw DatabaseError("database operation failed: " + std::string(mysql_stmt_error(statement)));
}

struct StatementCloser
{
    /*
     * 函数名直译：调用运算符。
     *
     * 通俗说：unique_ptr 不再持有预处理语句时，会自动调用这里关闭 MariaDB 句柄。
 *
     * 专业说法：这是自定义 deleter，使 C 风格的 MYSQL_STMT 资源具备 C++ RAII 所有权语义。
 *
     * 参数说明：
     * - statement：需要释放的语句句柄，允许为空。
 *
     * 实现方法：仅对非空句柄调用 mysql_stmt_close。
     */
    void operator()(MYSQL_STMT *statement) const
    {
        if (statement != nullptr)
        {
            mysql_stmt_close(statement);
        }
    }
};

using Statement = std::unique_ptr<MYSQL_STMT, StatementCloser>;

/*
 * 函数名直译：准备语句。
 *
 * 通俗说：把一条带占位符的 SQL 先交给数据库编译，之后才能安全地把外部数据作为参数绑定进去。
 *
 * 专业说法：该函数创建并准备 MYSQL_STMT，返回带自定义 deleter 的 unique_ptr，
 * 让任意异常路径都不会泄漏服务端或客户端的语句资源。
 *
 * 参数说明：
 * - connection：已建立的 MariaDB 连接。
 * - sql：使用 ? 占位符的 SQL 模板。
 *
 * 返回值：已成功准备、由调用方独占管理的预处理语句。
 *
 * 实现方法：初始化句柄，调用 mysql_stmt_prepare；任一步失败都转换为 DatabaseError。
 */
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
    /*
     * 函数名直译：参数绑定构造函数。
 *
     * 通俗说：把 C++ 字符串数组逐个接到 SQL 的 ? 占位符上。数据库会把这些内容当作数据，
     * 而不是重新拼接成 SQL 语法，因此用户输入中的引号和关键字不会改变查询含义。
 *
     * 专业说法：该对象在 mysql_stmt_execute 期间保存 MYSQL_BIND、长度字段和字符串内存的引用，
     * 形成参数化 DML/查询所需的绑定生命周期。
 *
     * 参数说明：
     * - statement：已 prepare 的语句。
     * - parameters：按 SQL 占位符顺序提供的文本参数。
 *
     * 实现方法：先核对占位符数量，再为每个参数填充 MYSQL_TYPE_STRING 绑定，最后一次性绑定到语句。
 *
     * 注意：parameters 必须在本对象和 mysql_stmt_execute 完成前保持有效；当前调用者满足该条件。
     */
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
} // namespace

class MySqlConnection::Impl
{
public:
    /*
     * 函数名直译：实现对象构造函数。
 *
     * 通俗说：保存这条 Gateway 数据库连接以后重连时要用的地址和凭据；此时并不急着联网。
 *
     * 专业说法：采用 Pimpl 隔离 mysql.h 的 C 类型和连接生命周期，使公开头文件保持轻量。
 *
     * 参数说明：
     * - config：该连接专属的数据库配置快照。
     *
     * 实现方法：移动保存配置，延迟到第一次数据库操作时建立真实连接。
     */
    explicit Impl(DatabaseConfig config)
        : config_(std::move(config))
    {
    }

    /*
     * 函数名直译：实现对象析构函数。
 *
     * 通俗说：连接对象销毁时，主动把仍打开的数据库连接关闭。
 *
     * 专业说法：这是连接句柄的最后一道 RAII 释放保护，避免 worker 回收后遗留 MYSQL 资源。
 *
     * 实现方法：连接非空时调用 mysql_close。
     */
    ~Impl()
    {
        if (connection_ != nullptr)
        {
            mysql_close(connection_);
        }
    }

    /*
     * 函数名直译：获取连接。
 *
     * 通俗说：需要执行 SQL 时先从这里拿可用连接。旧连接还能 ping 通就复用；已经断开就丢弃并按配置重新建立。
 *
     * 专业说法：这是单个 MySqlConnection 的惰性连接和失连恢复点，同时为每个新会话设置字符集、读写超时和 UTC 时区。
 * UTC 很重要：预算日/月边界、过期时间和版本轮询都必须在所有节点上得到一致解释。
 *
     * 返回值：可立即交给 MariaDB C API 使用的连接句柄。
 *
     * 实现方法：
     * 1. 对现有连接 mysql_ping，成功则复用。
     * 2. 关闭失效连接并 mysql_init。
     * 3. 配置连接、读和写超时后执行 mysql_real_connect。
     * 4. 设置 utf8mb4 与 UTC session；任一步失败均关闭半初始化连接并抛出异常。
 *
     * 注意：本类不提供跨线程共享同一连接的同步；连接池和数据库 worker 负责把同一实例限制在正确的执行上下文。
     */
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

/*
 * 函数名直译：MySQL 连接构造函数。
 *
 * 通俗说：创建 Gateway 专用数据库连接的 C++ 外壳，真实网络连接会等到第一次 SQL 操作时再建立。
 *
 * 专业说法：Pimpl 构造入口，避免公开 API 暴露 MYSQL 的平台相关定义。
 *
 * 参数说明：
 * - config：数据库地址、账号、密码和连接超时配置。
 *
 * 实现方法：把配置移交给 Impl 管理。
 */
MySqlConnection::MySqlConnection(DatabaseConfig config)
    : impl_(std::make_unique<Impl>(std::move(config)))
{
}

/*
 * 函数名直译：MySQL 连接析构函数。
 *
 * 通俗说：当上层连接池或命令结束时，释放内部 Impl，由它关闭尚存的数据库连接。
 *
 * 专业说法：析构函数在 cpp 文件定义，保留 Impl 的不透明实现边界。
 *
 * 实现方法：使用编译器默认析构流程销毁 unique_ptr。
 */
MySqlConnection::~MySqlConnection() = default;

/*
 * 函数名直译：执行静态 SQL。
 *
 * 通俗说：执行代码仓库中写死的管理 SQL，例如迁移 DDL、START TRANSACTION 和 COMMIT。
 * 这里绝不应该传入由 HTTP、配置文件字段或管理员自由文本拼出来的 SQL。
 *
 * 专业说法：静态 SQL 走 mysql_real_query，并完整消费 CLIENT_MULTI_STATEMENTS 可能产生的全部结果集，
 * 这样同一连接不会因未读取残留结果而影响下一条命令。
 *
 * 参数说明：
 * - sql：受源码控制的静态 SQL 文本。
 *
 * 实现方法：执行 SQL；循环 store/free 每个结果集，再以 mysql_next_result 推进直到结束。
 *
 * 注意：运行时 DML 和查询必须使用 execute_prepared/query_prepared，避免 SQL 注入和转义遗漏。
 */
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

/*
 * 函数名直译：执行预处理语句。
 *
 * 通俗说：用于 INSERT、UPDATE、DELETE 等写操作。SQL 模板和参数分开传递，返回本次实际影响了多少行。
 *
 * 专业说法：这是 Gateway 运行时 DML 的唯一数据库写入入口，使用 MariaDB server-side prepared statement 绑定参数。
 *
 * 参数说明：
 * - sql：包含 ? 占位符的固定 SQL 模板。
 * - parameters：与占位符一一对应的参数值。
 *
 * 返回值：数据库报告的受影响行数。
 *
 * 实现方法：prepare SQL，创建 ParameterBindings，在绑定仍有效时执行语句，最后读取 affected_rows。
 */
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

/*
 * 函数名直译：查询预处理语句。
 *
 * 通俗说：执行 SELECT 并把结果转换成二维字符串表。上层 Repository 再按列顺序把这些原始值解释成领域对象。
 *
 * 专业说法：函数通过结果元数据计算每列最大长度，绑定受限大小的输出缓冲区后逐行 mysql_stmt_fetch，
 * 以避免公开数据库句柄和 MYSQL_ROW 到上层。
 *
 * 参数说明：
 * - sql：包含 ? 占位符的查询模板。
 * - parameters：按占位符顺序的参数。
 *
 * 返回值：每一行是一组按 SELECT 列顺序排列的字符串；SQL 不返回列时得到空集合。
 *
 * 实现方法：准备并执行语句，读取元数据，分配并绑定每列缓冲区，循环 fetch 后复制出独立字符串。
 *
 * 注意：单列结果被硬性限制为 1 MiB，防止异常数据库值在 Gateway worker 中造成无界内存占用。
 */
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

/*
 * 函数名直译：最后插入 ID。
 *
 * 通俗说：取得本连接刚刚 INSERT 的自增主键，供管理员命令或仓储层关联后续记录。
 *
 * 专业说法：该值是 MariaDB connection-local 状态，因此必须在同一 MySqlConnection、紧随相关 INSERT 后读取。
 *
 * 返回值：数据库返回的最后一个自增 ID。
 *
 * 实现方法：调用 mysql_insert_id 并转换为固定宽度无符号整数。
 */
std::uint64_t MySqlConnection::last_insert_id() const
{
    return static_cast<std::uint64_t>(mysql_insert_id(impl_->connection()));
}

/*
 * 函数名直译：开始事务。
 *
 * 通俗说：从这里开始的一组数据库修改要么一起提交，要么稍后一起撤销，例如预算预留与 Usage 创建。
 *
 * 专业说法：事务控制复用静态 SQL 入口，保持所有 MariaDB 错误处理路径一致。
 *
 * 实现方法：执行固定的 START TRANSACTION。
 */
void MySqlConnection::begin()
{
    execute_static("START TRANSACTION");
}

/*
 * 函数名直译：提交事务。
 *
 * 通俗说：确认本事务内已经完成的数据库修改正式生效。
 *
 * 专业说法：提交失败会抛出 DatabaseError，调用方应把这次业务操作视为未可靠完成并走相应的补偿/重试路径。
 *
 * 实现方法：执行固定的 COMMIT。
 */
void MySqlConnection::commit()
{
    execute_static("COMMIT");
}

/*
 * 函数名直译：回滚事务。
 *
 * 通俗说：在中途发生错误时，尽力撤销尚未提交的数据库修改；回滚本身再失败也不会遮蔽原始异常。
 *
 * 专业说法：noexcept 回滚适用于析构式错误清理路径，确保异常展开过程中不会触发第二个异常导致进程终止。
 *
 * 实现方法：执行 ROLLBACK，并吞掉该清理操作自身的所有异常。
 */
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
} // namespace ai_gateway
