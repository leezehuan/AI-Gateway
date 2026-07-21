#ifndef CHAT_CONFIG_HPP
#define CHAT_CONFIG_HPP

#include <cstdlib>
#include <string>

namespace chat_config
{
inline std::string getEnv(const char *name, const char *fallback)
{
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' ? value : fallback;
}

inline unsigned int getPort(const char *name, unsigned int fallback)
{
    const char *value = std::getenv(name);
    if (value == nullptr || value[0] == '\0')
    {
        return fallback;
    }

    char *end = nullptr;
    const long port = std::strtol(value, &end, 10);
    return end != value && *end == '\0' && port > 0 && port <= 65535
               ? static_cast<unsigned int>(port)
               : fallback;
}
} // namespace chat_config

#endif
