if(BUILD_CHAT_SERVER)
    add_subdirectory(server)
endif()

if(BUILD_CHAT_CLIENT)
    add_subdirectory(client)
endif()
