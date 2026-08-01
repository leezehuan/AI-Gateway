find_package(Threads REQUIRED)
find_package(CURL REQUIRED)
find_package(Python3 COMPONENTS Interpreter)

add_executable(AiGateway
    main.cpp
    config.cpp
    gateway.cpp
    curl_transport.cpp
    http_server.cpp
)

target_include_directories(AiGateway PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(AiGateway PRIVATE CURL::libcurl Threads::Threads)
target_compile_definitions(AiGateway PRIVATE
    BOOST_ASIO_NO_DEPRECATED
    BOOST_ERROR_CODE_HEADER_ONLY
)
target_compile_options(AiGateway PRIVATE -Wall -Wextra -Wpedantic)

if(BUILD_TESTING AND Python3_Interpreter_FOUND)
    add_test(
        NAME ai_gateway.integration
        COMMAND ${Python3_EXECUTABLE}
                ${PROJECT_SOURCE_DIR}/tests/gateway/test_gateway.py
                $<TARGET_FILE:AiGateway>
    )
    set_tests_properties(ai_gateway.integration PROPERTIES TIMEOUT 120)
endif()
