# include_directories(${ROOT_DIR}/third_party/gtest/include)
# LINK_DIRECTORIES(${ROOT_DIR}/third_party/gtest/include)

include_directories(skip_list)

add_subdirectory(gtest)
include_directories(${GTEST_SOURCE_DIR}/include ${GTEST_SOURCE_DIR})

add_subdirectory(spdlog)