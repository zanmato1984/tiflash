message(STATUS "Finding GTest")

if(TARGET gtest AND TARGET gtest_main)
  if(NOT TARGET GTest::gtest)
    add_library(GTest::gtest ALIAS gtest)
  endif()
  if(NOT TARGET GTest::gtest_main)
    add_library(GTest::gtest_main ALIAS gtest_main)
  endif()
  return()
endif()

set(GTest_FIND_QUIETLY 0)
find_package(GTest REQUIRED)
