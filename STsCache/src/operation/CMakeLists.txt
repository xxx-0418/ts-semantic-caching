add_library(
        operation
        OBJECT
        operation.cpp
)

target_link_libraries(
        operation
        spdlog::spdlog_header_only
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:operation>
        PARENT_SCOPE)