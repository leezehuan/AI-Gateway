cmake_minimum_required(VERSION 3.16)
project(ClusterChatServer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

include_directories(
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/include/server
    ${PROJECT_SOURCE_DIR}/include/server/db
    ${PROJECT_SOURCE_DIR}/include/server/model
    ${PROJECT_SOURCE_DIR}/include/server/redis
    ${PROJECT_SOURCE_DIR}/thirdparty
)

option(BUILD_CHAT_SERVER "Build the chat server" ON)
option(BUILD_CHAT_CLIENT "Build the chat client" ON)

add_subdirectory(src)
