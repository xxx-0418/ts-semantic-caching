add_library(
  catalog
  OBJECT
  column.cpp
  schema.cpp
  predicate.cpp
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:catalog>
    PARENT_SCOPE)
