add_library(
        slab_management
        OBJECT
        slab.cpp
        slab_management.cpp
        settings.cpp
        sha1.cpp
)

find_package(Threads REQUIRED)

target_link_libraries(
        slab_management
        spdlog::spdlog_header_only
        Threads::Threads
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:slab_management>
        PARENT_SCOPE)

