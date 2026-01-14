if(TIFORTH_BUILD_TESTS)
  if(NOT TIFORTH_USE_INTERNAL_ARROW)
    message(FATAL_ERROR "Can't build TiForth tests without internal Arrow library")
  endif()

  message(STATUS "Building the TiForth googletest unit tests")
  enable_testing()
  include(cmake/find/gtest.cmake)
endif()

function(add_tiforth_test TEST_NAME TEST_SRC)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_NAME} tiforth ArrowTesting::arrow_testing_shared GTest::GTest GTest::Main pthread)
  set_target_properties(${TEST_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/gtests")
  if(TIFORTH_USE_INTERNAL_ARROW)
    set_target_properties(${TEST_NAME}
      PROPERTIES
        BUILD_RPATH "${ARROW_PREFIX}/lib;$<TARGET_FILE_DIR:tiforth>")
  endif()
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()
