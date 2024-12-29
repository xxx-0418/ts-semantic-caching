add_subdirectory(util)

add_library(
  util
  OBJECT
  util.cpp
  log.cpp
)

target_link_libraries(util
  spdlog::spdlog_header_only
)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:util>
  PARENT_SCOPE)