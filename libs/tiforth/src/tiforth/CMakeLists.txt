if(NOT TIFORTH_ARROW_CORE_TARGET)
  message(FATAL_ERROR "TiForth Arrow core target is not configured")
endif()

if(NOT TIFORTH_ARROW_COMPUTE_TARGET)
  message(FATAL_ERROR "TiForth Arrow compute target is not configured")
endif()

if(TIFORTH_LIBRARY_LINKAGE STREQUAL "shared")
  add_library(tiforth SHARED)
else()
  add_library(tiforth STATIC)
  set_target_properties(tiforth PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()
add_library(tiforth::tiforth ALIAS tiforth)

target_sources(tiforth PRIVATE collation.cc detail/arrow_compute.cc engine.cc expr.cc function_registry.cc pipeline.cc pipeline_exec.cc operators/filter.cc operators/hash_agg.cc operators/hash_join.cc operators/projection.cc operators/sort.cc task.cc type_metadata.cc)
target_compile_features(tiforth PUBLIC cxx_std_20)

target_include_directories(
  tiforth
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include> $<INSTALL_INTERFACE:include>)

if(TIFORTH_ARROW_INCLUDE_DIRS)
  foreach(_tiforth_arrow_inc_dir IN LISTS TIFORTH_ARROW_INCLUDE_DIRS)
    target_include_directories(tiforth SYSTEM PUBLIC "$<BUILD_INTERFACE:${_tiforth_arrow_inc_dir}>")
  endforeach()
endif()

target_link_libraries(tiforth PUBLIC ${TIFORTH_ARROW_CORE_TARGET})
target_link_libraries(tiforth PUBLIC ${TIFORTH_ARROW_COMPUTE_TARGET})

if(TIFORTH_LIBRARY_LINKAGE STREQUAL "shared")
  set_target_properties(
    tiforth
    PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})
endif()
