add_library(
        operators
        OBJECT
        operators.cpp
)

target_link_libraries(
        operators
        spdlog::spdlog_header_only
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:operators>
        PARENT_SCOPE)