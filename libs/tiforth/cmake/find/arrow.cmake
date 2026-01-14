if(TIFORTH_USE_INTERNAL_ARROW)
  message(STATUS "Using internal Arrow library")

  include(ExternalProject)

  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow")
  set(ARROW_CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DARROW_BUILD_SHARED=ON
    -DARROW_BUILD_STATIC=OFF
    -DARROW_COMPUTE=ON
    -DARROW_ACERO=OFF
    -DARROW_CSV=OFF
    -DARROW_DATASET=OFF
    -DARROW_FILESYSTEM=OFF
    -DARROW_JSON=OFF
    -DARROW_PARQUET=OFF
    -DARROW_SUBSTRAIT=OFF
    -DARROW_TESTING=ON
    "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>")

  set(ARROW_SHARED_LIBRARY "${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_COMPUTE_SHARED_LIBRARY "${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_compute${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_TESTING_SHARED_LIBRARY "${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}arrow_testing${CMAKE_SHARED_LIBRARY_SUFFIX}")

  ExternalProject_Add(arrow_ep
    PREFIX ${ARROW_PREFIX}
    SOURCE_SUBDIR cpp
    URL "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-22.0.0.tar.gz"
    URL_HASH "SHA256=8a95e6c7b9bec2bc0058feb73efe38ad6cfd49a0c7094db29b37ecaa8ab16051"
    BUILD_IN_SOURCE 1
    CMAKE_ARGS "${ARROW_CMAKE_ARGS}"
    INSTALL_BYPRODUCTS
      "${ARROW_SHARED_LIBRARY}"
      "${ARROW_COMPUTE_SHARED_LIBRARY}"
      "${ARROW_TESTING_SHARED_LIBRARY}"
    DOWNLOAD_EXTRACT_TIMESTAMP false)

  file(MAKE_DIRECTORY "${ARROW_PREFIX}/include")

  add_library(Arrow::arrow_shared SHARED IMPORTED)
  set_target_properties(Arrow::arrow_shared
    PROPERTIES
      IMPORTED_LOCATION "${ARROW_SHARED_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${ARROW_PREFIX}/include")
  add_dependencies(Arrow::arrow_shared arrow_ep)

  add_library(ArrowCompute::arrow_compute_shared SHARED IMPORTED)
  set_target_properties(ArrowCompute::arrow_compute_shared
    PROPERTIES
      IMPORTED_LOCATION "${ARROW_COMPUTE_SHARED_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${ARROW_PREFIX}/include")
  add_dependencies(ArrowCompute::arrow_compute_shared arrow_ep)

  add_library(ArrowTesting::arrow_testing_shared SHARED IMPORTED)
  set_target_properties(ArrowTesting::arrow_testing_shared
    PROPERTIES
      IMPORTED_LOCATION "${ARROW_TESTING_SHARED_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${ARROW_PREFIX}/include")
  add_dependencies(ArrowTesting::arrow_testing_shared arrow_ep)
else()
  message(STATUS "Finding Arrow")

  set(Arrow_FIND_QUIETLY 0)
  find_package(Arrow REQUIRED)
  find_package(ArrowCompute REQUIRED)
endif()
