include_directories(${ROOT_DIR}/src/cmd)
add_library(
        request_lib
        OBJECT
        #fc.cpp
        fc_util.cpp
        fc_core.cpp
        fc_signal.cpp
        fc_time.cpp
        fc_connection.cpp
        fc_server.cpp
        fc_client.cpp
        fc_message.cpp
        fc_mbuf.cpp
        fc_event.cpp
        fc_array.cpp
        fc_string.cpp
        fc_memcache.cpp
        fc_request.cpp
        fc_stats.cpp
        fc_response.cpp
)

target_compile_definitions(request_lib PUBLIC PROJECT_ROOT="${ROOT_DIR}")
include_directories(${ROOT_DIR}/third_party/spdlog/include)

add_executable(STsCache fc.cpp)

target_link_libraries(
        STsCache
        request_lib
        semantic_cache_lib
        pthread
)
