#include "gateway/gateway.hpp"
#include "gateway/mysql.hpp"

#include "json.hpp"

#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include <algorithm>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace
{
using json = nlohmann::json;
using ai_gateway::MySqlConnection;

class Transaction
{
public:
    explicit Transaction(MySqlConnection &connection)
        : connection_(connection)
    {
        connection_.begin();
    }

    ~Transaction()
    {
        if (!committed_)
        {
            connection_.rollback();
        }
    }

    void commit()
    {
        connection_.commit();
        committed_ = true;
    }

private:
    MySqlConnection &connection_;
    bool committed_ = false;
};

std::unordered_map<std::string, std::string> parse_options(int argc, char **argv, int offset)
{
    std::unordered_map<std::string, std::string> options;
    for (int index = offset; index < argc; index += 2)
    {
        const std::string name = argv[index];
        if (name.rfind("--", 0) != 0 || index + 1 >= argc)
        {
            throw std::runtime_error("invalid command arguments");
        }
        if (!options.emplace(name, argv[index + 1]).second)
        {
            throw std::runtime_error("duplicate command option: " + name);
        }
    }
    return options;
}

std::string require_option(const std::unordered_map<std::string, std::string> &options,
                           const std::string &name)
{
    const auto found = options.find(name);
    if (found == options.end() || found->second.empty())
    {
        throw std::runtime_error("missing required option: " + name);
    }
    return found->second;
}

std::string read_file(const std::filesystem::path &path, std::size_t maximum = 8 * 1024 * 1024)
{
    std::ifstream input(path, std::ios::binary);
    if (!input)
    {
        throw std::runtime_error("unable to read input file: " + path.filename().string());
    }
    std::ostringstream contents;
    contents << input.rdbuf();
    std::string result = contents.str();
    if (result.size() > maximum)
    {
        throw std::runtime_error("input file is too large: " + path.filename().string());
    }
    return result;
}

std::string hex(const unsigned char *bytes, std::size_t size)
{
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (std::size_t index = 0; index < size; ++index)
    {
        output << std::setw(2) << static_cast<unsigned>(bytes[index]);
    }
    return output.str();
}

std::string sha256(const std::string &contents)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(contents.data()), contents.size(), digest);
    return hex(digest, sizeof(digest));
}

std::string base64url(const unsigned char *bytes, std::size_t size)
{
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string result;
    result.reserve((size * 8 + 5) / 6);
    std::uint32_t accumulator = 0;
    int bits = 0;
    for (std::size_t index = 0; index < size; ++index)
    {
        accumulator = (accumulator << 8U) | bytes[index];
        bits += 8;
        while (bits >= 6)
        {
            bits -= 6;
            result.push_back(alphabet[(accumulator >> bits) & 0x3fU]);
        }
    }
    if (bits != 0)
    {
        result.push_back(alphabet[(accumulator << (6 - bits)) & 0x3fU]);
    }
    return result;
}

std::vector<unsigned char> random_bytes(std::size_t size)
{
    std::vector<unsigned char> bytes(size);
    if (RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) != 1)
    {
        throw std::runtime_error("secure random generation failed");
    }
    return bytes;
}

void validate_name(const std::string &value, const std::string &field)
{
    static const std::regex pattern("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$");
    if (!std::regex_match(value, pattern))
    {
        throw std::runtime_error("invalid " + field);
    }
}

void validate_status(const std::string &status)
{
    if (status != "active" && status != "disabled")
    {
        throw std::runtime_error("status must be active or disabled");
    }
}

void validate_url(const std::string &url)
{
    if ((url.rfind("http://", 0) != 0 && url.rfind("https://", 0) != 0) ||
        url.size() > 2048)
    {
        throw std::runtime_error("Provider endpoint URL must use http or https");
    }
}

void validate_secret_ref(const std::string &reference)
{
    static const std::regex environment("^env:[A-Za-z_][A-Za-z0-9_]*$");
    static const std::regex file("^file:[A-Za-z0-9][A-Za-z0-9._-]{0,254}$");
    if (!std::regex_match(reference, environment) && !std::regex_match(reference, file))
    {
        throw std::runtime_error("credential secret_ref must be env:NAME or file:basename");
    }
}

const json &required(const json &object, const char *field, json::value_t type)
{
    if (!object.is_object() || !object.contains(field) || object[field].type() != type)
    {
        throw std::runtime_error(std::string("configuration field is missing or invalid: ") + field);
    }
    return object[field];
}

std::string required_string(const json &object, const char *field)
{
    const std::string value = required(object, field, json::value_t::string).get<std::string>();
    if (value.empty())
    {
        throw std::runtime_error(std::string("configuration field is empty: ") + field);
    }
    return value;
}

const json &array_or_empty(const json &root, const char *field)
{
    static const json empty = json::array();
    if (!root.contains(field))
    {
        return empty;
    }
    if (!root[field].is_array())
    {
        throw std::runtime_error(std::string("configuration field must be an array: ") + field);
    }
    return root[field];
}

std::string status_of(const json &object)
{
    const std::string status = object.value("status", "active");
    validate_status(status);
    return status;
}

std::uint64_t require_id(MySqlConnection &connection,
                         const std::string &sql,
                         const std::vector<std::string> &parameters,
                         const std::string &resource)
{
    const auto rows = connection.query_prepared(sql, parameters);
    if (rows.size() != 1 || rows.front().empty())
    {
        throw std::runtime_error("unknown configuration resource: " + resource);
    }
    return std::stoull(rows.front().front());
}

std::uint64_t upsert(MySqlConnection &connection,
                     const std::string &sql,
                     const std::vector<std::string> &parameters,
                     bool &changed)
{
    changed = connection.execute_prepared(sql, parameters) != 0 || changed;
    return connection.last_insert_id();
}

std::pair<std::string, bool> grant_value(const json &value)
{
    if (value.is_string())
    {
        return {value.get<std::string>(), true};
    }
    if (value.is_object() && value.contains("name") && value["name"].is_string())
    {
        return {value["name"].get<std::string>(), value.value("enabled", true)};
    }
    throw std::runtime_error("grant must be a name or an object with name and enabled");
}

void bump_version(MySqlConnection &connection)
{
    connection.execute_prepared(
        "UPDATE gateway_config_versions SET version = version + 1 WHERE singleton_id = 1");
}

void migrate(MySqlConnection &connection, const std::filesystem::path &directory)
{
    if (!std::filesystem::is_directory(directory))
    {
        throw std::runtime_error("migration directory does not exist");
    }
    connection.execute_static(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version VARCHAR(128) NOT NULL PRIMARY KEY, checksum CHAR(64) NOT NULL, "
        "applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)) "
        "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

    std::vector<std::filesystem::path> migrations;
    for (const auto &entry : std::filesystem::directory_iterator(directory))
    {
        if (entry.is_regular_file() && entry.path().extension() == ".sql")
        {
            migrations.push_back(entry.path());
        }
    }
    std::sort(migrations.begin(), migrations.end());
    if (migrations.empty())
    {
        throw std::runtime_error("migration directory contains no SQL migrations");
    }
    for (const auto &path : migrations)
    {
        const std::string version = path.stem().string();
        if (!std::regex_match(version, std::regex("^[0-9]{4}_[A-Za-z0-9_]+$")))
        {
            throw std::runtime_error("invalid migration filename: " + path.filename().string());
        }
        const std::string contents = read_file(path);
        const std::string checksum = sha256(contents);
        const auto existing = connection.query_prepared(
            "SELECT checksum FROM schema_migrations WHERE version = ?", {version});
        if (!existing.empty())
        {
            if (existing.front().front() != checksum)
            {
                throw std::runtime_error("migration checksum mismatch: " + version);
            }
            continue;
        }
        connection.execute_static(contents);
        connection.execute_prepared(
            "INSERT INTO schema_migrations(version, checksum) VALUES (?, ?)",
            {version, checksum});
    }
}

void apply_config(MySqlConnection &connection, const json &root)
{
    if (!root.is_object())
    {
        throw std::runtime_error("configuration root must be an object");
    }
    Transaction transaction(connection);
    bool changed = false;

    for (const auto &tenant : array_or_empty(root, "tenants"))
    {
        const std::string slug = required_string(tenant, "slug");
        validate_name(slug, "tenant slug");
        const std::string name = required_string(tenant, "name");
        const std::string status = status_of(tenant);
        upsert(connection,
               "INSERT INTO tenants(slug, name, status) VALUES (?, ?, ?) "
               "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), name=VALUES(name), "
               "status=VALUES(status)",
               {slug, name, status}, changed);
    }

    for (const auto &provider : array_or_empty(root, "providers"))
    {
        const std::string tenant_slug = required_string(provider, "tenant");
        const std::string slug = required_string(provider, "slug");
        validate_name(slug, "Provider slug");
        const auto tenant_id = require_id(connection,
            "SELECT id FROM tenants WHERE slug = ?", {tenant_slug}, tenant_slug);
        const auto provider_id = upsert(connection,
            "INSERT INTO providers(tenant_id, slug, name, status) VALUES (?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), name=VALUES(name), "
            "status=VALUES(status)",
            {std::to_string(tenant_id), slug, required_string(provider, "name"),
             status_of(provider)}, changed);

        for (const auto &endpoint : array_or_empty(provider, "endpoints"))
        {
            const std::string name = required_string(endpoint, "name");
            const std::string protocol = required_string(endpoint, "protocol");
            const std::string url = required_string(endpoint, "url");
            validate_name(name, "Endpoint name");
            validate_name(protocol, "protocol");
            validate_url(url);
            upsert(connection,
                "INSERT INTO provider_endpoints(provider_id, name, protocol, url, status) "
                "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), "
                "protocol=VALUES(protocol), url=VALUES(url), status=VALUES(status)",
                {std::to_string(provider_id), name, protocol, url, status_of(endpoint)}, changed);
        }
        for (const auto &credential : array_or_empty(provider, "credentials"))
        {
            const std::string name = required_string(credential, "name");
            const std::string secret_ref = required_string(credential, "secret_ref");
            validate_name(name, "Credential name");
            validate_secret_ref(secret_ref);
            upsert(connection,
                "INSERT INTO provider_credentials(provider_id, name, secret_ref, status) "
                "VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), "
                "secret_ref=VALUES(secret_ref), status=VALUES(status)",
                {std::to_string(provider_id), name, secret_ref, status_of(credential)}, changed);
        }
    }

    for (const auto &model : array_or_empty(root, "logical_models"))
    {
        const std::string tenant_slug = required_string(model, "tenant");
        const std::string protocol = required_string(model, "protocol");
        const std::string name = required_string(model, "name");
        validate_name(protocol, "protocol");
        validate_name(name, "Logical Model name");
        const auto tenant_id = require_id(connection,
            "SELECT id FROM tenants WHERE slug = ?", {tenant_slug}, tenant_slug);
        upsert(connection,
            "INSERT INTO logical_models(tenant_id, protocol, name, status) VALUES (?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), status=VALUES(status)",
            {std::to_string(tenant_id), protocol, name, status_of(model)}, changed);
    }

    for (const auto &policy : array_or_empty(root, "policies"))
    {
        const std::string tenant_slug = required_string(policy, "tenant");
        const std::string slug = required_string(policy, "slug");
        validate_name(slug, "Access Policy slug");
        const auto tenant_id = require_id(connection,
            "SELECT id FROM tenants WHERE slug = ?", {tenant_slug}, tenant_slug);
        const auto policy_id = upsert(connection,
            "INSERT INTO access_policies(tenant_id, slug, name, status) VALUES (?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), name=VALUES(name), "
            "status=VALUES(status)",
            {std::to_string(tenant_id), slug, required_string(policy, "name"),
             status_of(policy)}, changed);

        for (const auto &entry : array_or_empty(policy, "protocols"))
        {
            const auto grant = grant_value(entry);
            validate_name(grant.first, "protocol");
            upsert(connection,
                "INSERT INTO policy_protocol_grants(policy_id, protocol, enabled) VALUES (?, ?, ?) "
                "ON DUPLICATE KEY UPDATE enabled=VALUES(enabled)",
                {std::to_string(policy_id), grant.first, grant.second ? "1" : "0"}, changed);
        }
        for (const auto &entry : array_or_empty(policy, "models"))
        {
            std::string model_name;
            std::string protocol = "responses";
            bool enabled = true;
            if (entry.is_string())
            {
                model_name = entry.get<std::string>();
            }
            else if (entry.is_object())
            {
                model_name = required_string(entry, "name");
                protocol = entry.value("protocol", "responses");
                enabled = entry.value("enabled", true);
            }
            else
            {
                throw std::runtime_error("model grant must be a name or object");
            }
            const auto model_id = require_id(connection,
                "SELECT id FROM logical_models WHERE tenant_id = ? AND protocol = ? AND name = ?",
                {std::to_string(tenant_id), protocol, model_name}, model_name);
            upsert(connection,
                "INSERT INTO policy_model_grants(policy_id, logical_model_id, enabled) "
                "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE enabled=VALUES(enabled)",
                {std::to_string(policy_id), std::to_string(model_id), enabled ? "1" : "0"}, changed);
        }
        for (const auto &entry : array_or_empty(policy, "providers"))
        {
            const auto grant = grant_value(entry);
            const auto provider_id = require_id(connection,
                "SELECT id FROM providers WHERE tenant_id = ? AND slug = ?",
                {std::to_string(tenant_id), grant.first}, grant.first);
            upsert(connection,
                "INSERT INTO policy_provider_grants(policy_id, provider_id, enabled) "
                "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE enabled=VALUES(enabled)",
                {std::to_string(policy_id), std::to_string(provider_id),
                 grant.second ? "1" : "0"}, changed);
        }
    }

    for (const auto &mapping : array_or_empty(root, "mappings"))
    {
        const std::string tenant_slug = required_string(mapping, "tenant");
        const std::string protocol = required_string(mapping, "protocol");
        const std::string model_name = required_string(mapping, "logical_model");
        const std::string provider_slug = required_string(mapping, "provider");
        const auto tenant_id = require_id(connection,
            "SELECT id FROM tenants WHERE slug = ?", {tenant_slug}, tenant_slug);
        const auto model_id = require_id(connection,
            "SELECT id FROM logical_models WHERE tenant_id = ? AND protocol = ? AND name = ?",
            {std::to_string(tenant_id), protocol, model_name}, model_name);
        const auto provider_id = require_id(connection,
            "SELECT id FROM providers WHERE tenant_id = ? AND slug = ?",
            {std::to_string(tenant_id), provider_slug}, provider_slug);
        const auto endpoint_id = require_id(connection,
            "SELECT id FROM provider_endpoints WHERE provider_id = ? AND name = ? AND protocol = ?",
            {std::to_string(provider_id), required_string(mapping, "endpoint"), protocol},
            "Provider Endpoint");
        const auto credential_id = require_id(connection,
            "SELECT id FROM provider_credentials WHERE provider_id = ? AND name = ?",
            {std::to_string(provider_id), required_string(mapping, "credential")},
            "Provider Credential");
        const std::string mapping_name = mapping.value("name", provider_slug);
        validate_name(mapping_name, "Model Mapping name");
        upsert(connection,
            "INSERT INTO model_mappings(logical_model_id, name, provider_endpoint_id, "
            "provider_credential_id, upstream_model, status) VALUES (?, ?, ?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), "
            "provider_endpoint_id=VALUES(provider_endpoint_id), "
            "provider_credential_id=VALUES(provider_credential_id), "
            "upstream_model=VALUES(upstream_model), status=VALUES(status)",
            {std::to_string(model_id), mapping_name, std::to_string(endpoint_id),
             std::to_string(credential_id), required_string(mapping, "upstream_model"),
             status_of(mapping)}, changed);
    }

    if (changed)
    {
        bump_version(connection);
    }
    transaction.commit();
}

std::string normalize_expiry(std::string value)
{
    if (value.empty())
    {
        return value;
    }
    static const std::regex rfc3339(
        "^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
        "(\\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$");
    std::smatch match;
    if (!std::regex_match(value, match, rfc3339))
    {
        throw std::runtime_error("--expires-at must be an RFC3339 timestamp");
    }
    std::tm local{};
    local.tm_year = std::stoi(match[1].str()) - 1900;
    local.tm_mon = std::stoi(match[2].str()) - 1;
    local.tm_mday = std::stoi(match[3].str());
    local.tm_hour = std::stoi(match[4].str());
    local.tm_min = std::stoi(match[5].str());
    local.tm_sec = std::stoi(match[6].str());
    const std::tm requested = local;
    std::time_t timestamp = timegm(&local);
    std::tm normalized{};
    gmtime_r(&timestamp, &normalized);
    if (normalized.tm_year != requested.tm_year || normalized.tm_mon != requested.tm_mon ||
        normalized.tm_mday != requested.tm_mday || normalized.tm_hour != requested.tm_hour ||
        normalized.tm_min != requested.tm_min || normalized.tm_sec != requested.tm_sec)
    {
        throw std::runtime_error("--expires-at must be a valid RFC3339 timestamp");
    }
    const std::string zone = match[8].str();
    if (zone != "Z")
    {
        const int hours = std::stoi(zone.substr(1, 2));
        const int minutes = std::stoi(zone.substr(4, 2));
        if (hours > 23 || minutes > 59)
        {
            throw std::runtime_error("--expires-at contains an invalid UTC offset");
        }
        const int offset = (hours * 60 + minutes) * 60;
        timestamp += zone.front() == '+' ? -offset : offset;
    }
    std::tm utc{};
    gmtime_r(&timestamp, &utc);
    char formatted[32];
    if (std::strftime(formatted, sizeof(formatted), "%Y-%m-%d %H:%M:%S", &utc) == 0)
    {
        throw std::runtime_error("--expires-at is outside the supported range");
    }
    return std::string(formatted) + match[7].str();
}

struct IssuedKey
{
    std::string key;
    std::string public_id;
};

IssuedKey issue_key(MySqlConnection &connection,
                    const ai_gateway::GatewayConfig &config,
                    const std::string &tenant_slug,
                    const std::string &policy_slug,
                    const std::string &name,
                    const std::string &expires_at)
{
    const auto rows = connection.query_prepared(
        "SELECT t.id, p.id FROM tenants t JOIN access_policies p ON p.tenant_id=t.id "
        "WHERE t.slug=? AND p.slug=?",
        {tenant_slug, policy_slug});
    if (rows.size() != 1)
    {
        throw std::runtime_error("unknown Tenant or Access Policy");
    }
    const auto prefix_bytes = random_bytes(9);
    const auto secret_bytes = random_bytes(32);
    const auto id_bytes = random_bytes(12);
    const std::string prefix = base64url(prefix_bytes.data(), prefix_bytes.size());
    const std::string key = "aigw_" + prefix + "_" +
                            base64url(secret_bytes.data(), secret_bytes.size());
    const std::string key_id = "key_" + hex(id_bytes.data(), id_bytes.size());
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned digest_size = 0;
    if (HMAC(EVP_sha256(), config.api_key_hmac_pepper.data(),
             static_cast<int>(config.api_key_hmac_pepper.size()),
             reinterpret_cast<const unsigned char *>(key.data()), key.size(),
             digest, &digest_size) == nullptr || digest_size != 32)
    {
        throw std::runtime_error("API Key HMAC failed");
    }
    const std::string binary_digest(reinterpret_cast<char *>(digest), digest_size);

    Transaction transaction(connection);
    connection.execute_prepared(
        "INSERT INTO api_keys(key_id, tenant_id, policy_id, name, display_prefix, key_hmac, "
        "status, expires_at) VALUES (?, ?, ?, ?, ?, ?, 'active', NULLIF(?, ''))",
        {key_id, rows[0][0], rows[0][1], name, prefix, binary_digest, expires_at});
    bump_version(connection);
    transaction.commit();
    return {key, key_id};
}

void set_key_status(MySqlConnection &connection,
                    const std::string &key_id,
                    const std::string &status)
{
    validate_status(status);
    Transaction transaction(connection);
    const auto changed = connection.execute_prepared(
        "UPDATE api_keys SET status=? WHERE key_id=?", {status, key_id});
    if (changed == 0)
    {
        const auto exists = connection.query_prepared(
            "SELECT key_id FROM api_keys WHERE key_id=?", {key_id});
        if (exists.empty())
        {
            throw std::runtime_error("unknown API Key ID");
        }
    }
    if (changed != 0)
    {
        bump_version(connection);
    }
    transaction.commit();
}
} // namespace

int main(int argc, char **argv)
{
    try
    {
        if (argc < 2)
        {
            throw std::runtime_error("usage: AiGatewayAdmin <command> [options]");
        }
        const std::string command = argv[1];
        const auto options = parse_options(argc, argv, 2);
        const ai_gateway::GatewayConfig config = ai_gateway::GatewayConfig::from_env();
        if (config.database.password.empty())
        {
            throw std::runtime_error("AI_GATEWAY_DB_PASSWORD is required");
        }
        MySqlConnection connection(config.database);

        if (command == "migrate")
        {
            migrate(connection, require_option(options, "--dir"));
        }
        else if (command == "apply-config")
        {
            const std::string contents = read_file(require_option(options, "--file"));
            apply_config(connection, json::parse(contents));
        }
        else if (command == "issue-key")
        {
            if (config.api_key_hmac_pepper.size() < 32)
            {
                throw std::runtime_error(
                    "AI_GATEWAY_API_KEY_HMAC_PEPPER must contain at least 32 bytes");
            }
            const auto expiry = options.find("--expires-at");
            const IssuedKey issued = issue_key(
                connection, config, require_option(options, "--tenant"),
                require_option(options, "--policy"), require_option(options, "--name"),
                normalize_expiry(expiry == options.end() ? std::string() : expiry->second));
            std::cerr << "Issued API Key ID: " << issued.public_id << '\n';
            std::cout << issued.key << '\n';
        }
        else if (command == "set-key-status")
        {
            set_key_status(connection, require_option(options, "--key-id"),
                           require_option(options, "--status"));
        }
        else if (command == "bump-version")
        {
            if (!options.empty())
            {
                throw std::runtime_error("bump-version accepts no options");
            }
            Transaction transaction(connection);
            bump_version(connection);
            transaction.commit();
        }
        else
        {
            throw std::runtime_error("unknown command: " + command);
        }
        return 0;
    }
    catch (const json::exception &)
    {
        std::cerr << "AiGatewayAdmin failed: invalid JSON configuration" << std::endl;
        return 1;
    }
    catch (const std::exception &error)
    {
        std::cerr << "AiGatewayAdmin failed: " << error.what() << std::endl;
        return 1;
    }
}
