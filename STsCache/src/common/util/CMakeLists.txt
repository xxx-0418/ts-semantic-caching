
add_library(
  string_util
  OBJECT
  string_util.cpp
)
set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:string_util>
  PARENT_SCOPE)
 