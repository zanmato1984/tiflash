if(TIFORTH_LIBRARY_LINKAGE STREQUAL "shared")
  add_library(tiforth_capi SHARED)
else()
  add_library(tiforth_capi STATIC)
  set_target_properties(tiforth_capi PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()
add_library(tiforth::tiforth_capi ALIAS tiforth_capi)

target_sources(tiforth_capi PRIVATE tiforth_capi.cc)
target_compile_features(tiforth_capi PUBLIC cxx_std_20)
target_compile_definitions(tiforth_capi PRIVATE TIFORTH_CAPI_BUILDING)

target_include_directories(
  tiforth_capi
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include>)

target_link_libraries(tiforth_capi PUBLIC tiforth::tiforth)

if(TIFORTH_LIBRARY_LINKAGE STREQUAL "shared")
  set_target_properties(
    tiforth_capi
    PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})
endif()
