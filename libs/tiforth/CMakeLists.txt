cmake_minimum_required(VERSION 3.23)

project(tiforth VERSION 0.0.1 LANGUAGES C CXX)

include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(TIFORTH_IS_TOP_LEVEL OFF)
if(PROJECT_IS_TOP_LEVEL)
  set(TIFORTH_IS_TOP_LEVEL ON)
endif()

set(TIFORTH_ARROW_PROVIDER "bundled" CACHE STRING "Arrow provider (bundled|system)")
set_property(CACHE TIFORTH_ARROW_PROVIDER PROPERTY STRINGS bundled system)
set(TIFORTH_ARROW_LINKAGE "shared" CACHE STRING "Arrow linkage (shared|static)")
set_property(CACHE TIFORTH_ARROW_LINKAGE PROPERTY STRINGS shared static)
set(TIFORTH_LIBRARY_LINKAGE "shared" CACHE STRING "TiForth library linkage (shared|static)")
set_property(CACHE TIFORTH_LIBRARY_LINKAGE PROPERTY STRINGS shared static)

if(NOT (TIFORTH_LIBRARY_LINKAGE STREQUAL "shared" OR TIFORTH_LIBRARY_LINKAGE STREQUAL "static"))
  message(
    FATAL_ERROR
    "Invalid TIFORTH_LIBRARY_LINKAGE='${TIFORTH_LIBRARY_LINKAGE}'. Allowed: shared|static")
endif()

option(TIFORTH_BUILD_TESTS "Build TiForth tests (top-level only)" OFF)
if(NOT TIFORTH_IS_TOP_LEVEL AND TIFORTH_BUILD_TESTS)
  message(FATAL_ERROR "TIFORTH_BUILD_TESTS must be OFF when tiforth is included as a subproject")
endif()

include(cmake/find/arrow.cmake)

add_subdirectory(src/tiforth)
add_subdirectory(src/tiforth_capi)

if(TIFORTH_BUILD_TESTS)
  include(cmake/testing/testing.cmake)
  add_subdirectory(tests)
endif()

install(TARGETS tiforth EXPORT tiforthTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT tiforthTargets
        NAMESPACE tiforth::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tiforth)

include(CMakePackageConfigHelpers)
configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/tiforthConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/tiforthConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tiforth)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/tiforthConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/tiforthConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/tiforthConfigVersion.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tiforth)
