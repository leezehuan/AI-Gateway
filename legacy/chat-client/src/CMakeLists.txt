find_package(Threads REQUIRED)

aux_source_directory(. SRC_LIST)

add_executable(ChatClient ${SRC_LIST})
target_link_libraries(ChatClient PRIVATE Threads::Threads)
