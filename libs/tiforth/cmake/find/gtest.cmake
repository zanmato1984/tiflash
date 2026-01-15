message(STATUS "Finding GTest")

# TiForth tests are built only when `tiforth` itself is the top-level project.
# When using bundled Arrow, Arrow will bring in googletest for ArrowTesting and
# expose `gtest`/`gtest_main` targets in the same CMake build.
if(TIFORTH_ARROW_PROVIDER STREQUAL "bundled")
  if(NOT TARGET gtest OR NOT TARGET gtest_main)
    message(
      FATAL_ERROR
      "Bundled Arrow test builds must provide googletest targets ('gtest' and 'gtest_main')")
  endif()
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
