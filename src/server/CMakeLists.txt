find_package(Threads REQUIRED)

find_path(MUDUO_INCLUDE_DIR NAMES muduo/net/TcpServer.h)
find_library(MUDUO_NET_LIBRARY NAMES muduo_net)
find_library(MUDUO_BASE_LIBRARY NAMES muduo_base)
find_path(HIREDIS_INCLUDE_DIR NAMES hiredis/hiredis.h)
find_library(HIREDIS_LIBRARY NAMES hiredis)
find_path(MYSQL_INCLUDE_DIR NAMES mysql.h PATH_SUFFIXES mysql mariadb)
find_library(MYSQL_CLIENT_LIBRARY NAMES mysqlclient mariadb)

foreach(required_var
        MUDUO_INCLUDE_DIR MUDUO_NET_LIBRARY MUDUO_BASE_LIBRARY
        HIREDIS_INCLUDE_DIR HIREDIS_LIBRARY
        MYSQL_INCLUDE_DIR MYSQL_CLIENT_LIBRARY)
    if(NOT ${required_var})
        message(FATAL_ERROR
            "Missing dependency: ${required_var}. "
            "On WSL/Ubuntu run ./scripts/setup-wsl.sh, then configure again.")
    endif()
endforeach()

aux_source_directory(. SRC_LIST)
aux_source_directory(./db DB_LIST)
aux_source_directory(./model MODEL_LIST)
aux_source_directory(./redis REDIS_LIST)

add_executable(ChatServer ${SRC_LIST} ${DB_LIST} ${MODEL_LIST} ${REDIS_LIST})
target_include_directories(ChatServer PRIVATE
    ${MUDUO_INCLUDE_DIR}
    ${HIREDIS_INCLUDE_DIR}
    ${MYSQL_INCLUDE_DIR}
)
target_link_libraries(ChatServer PRIVATE
    ${MUDUO_NET_LIBRARY}
    ${MUDUO_BASE_LIBRARY}
    ${MYSQL_CLIENT_LIBRARY}
    ${HIREDIS_LIBRARY}
    Threads::Threads
)
