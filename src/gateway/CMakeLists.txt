find_package(Threads REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Python3 COMPONENTS Interpreter)

find_path(MARIADB_INCLUDE_DIR mysql.h
    PATH_SUFFIXES mariadb mysql
)
find_library(MARIADB_LIBRARY
    NAMES mariadb
)
if(MARIADB_LIBRARY MATCHES "\\.a$")
    find_file(MARIADB_VERSIONED_SHARED_LIBRARY
        NAMES libmariadb.so.3
        PATHS /usr/lib/${CMAKE_LIBRARY_ARCHITECTURE}
              /lib/${CMAKE_LIBRARY_ARCHITECTURE}
              /usr/lib /lib
        NO_DEFAULT_PATH
    )
    if(MARIADB_VERSIONED_SHARED_LIBRARY)
        set(MARIADB_LIBRARY "${MARIADB_VERSIONED_SHARED_LIBRARY}")
    endif()
endif()
if(NOT MARIADB_INCLUDE_DIR OR NOT MARIADB_LIBRARY)
    message(FATAL_ERROR "MariaDB Connector/C development files are required for AiGateway Phase 3")
endif()

add_library(ai_gateway_phase3 STATIC
    config.cpp
    mysql.cpp
    repository.cpp
    runtime.cpp
)
target_include_directories(ai_gateway_phase3 PUBLIC
    ${PROJECT_SOURCE_DIR}/include
    ${MARIADB_INCLUDE_DIR}
)
target_link_libraries(ai_gateway_phase3 PUBLIC
    ${MARIADB_LIBRARY}
    OpenSSL::Crypto
    Threads::Threads
)
target_compile_options(ai_gateway_phase3 PRIVATE -Wall -Wextra -Wpedantic)

add_executable(AiGateway
    main.cpp
    gateway.cpp
    curl_transport.cpp
    http_server.cpp
)

target_include_directories(AiGateway PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(AiGateway PRIVATE ai_gateway_phase3 CURL::libcurl Threads::Threads)
target_compile_definitions(AiGateway PRIVATE
    BOOST_ASIO_NO_DEPRECATED
    BOOST_ERROR_CODE_HEADER_ONLY
)
target_compile_options(AiGateway PRIVATE -Wall -Wextra -Wpedantic)

add_executable(AiGatewayAdmin admin.cpp)
target_include_directories(AiGatewayAdmin PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(AiGatewayAdmin PRIVATE ai_gateway_phase3)
target_compile_options(AiGatewayAdmin PRIVATE -Wall -Wextra -Wpedantic)

if(BUILD_TESTING AND Python3_Interpreter_FOUND)
    add_test(
        NAME ai_gateway.integration
        COMMAND ${Python3_EXECUTABLE}
                ${PROJECT_SOURCE_DIR}/tests/gateway/test_gateway.py
                $<TARGET_FILE:AiGateway>
                $<TARGET_FILE:AiGatewayAdmin>
                ${PROJECT_SOURCE_DIR}
    )
    set_tests_properties(ai_gateway.integration PROPERTIES TIMEOUT 120)

    add_test(
        NAME ai_gateway.admin.integration
        COMMAND ${Python3_EXECUTABLE}
                ${PROJECT_SOURCE_DIR}/tests/gateway/test_gateway_admin.py
                $<TARGET_FILE:AiGatewayAdmin>
                ${PROJECT_SOURCE_DIR}
    )
    set_tests_properties(ai_gateway.admin.integration PROPERTIES TIMEOUT 60)
endif()
