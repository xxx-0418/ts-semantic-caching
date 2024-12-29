add_subdirectory(slab_management)
add_library(
        storage
        OBJECT
        semantic_graph.cpp
        time_range_skiplist.cpp
)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:storage>
        PARENT_SCOPE)