cmake_minimum_required(VERSION 3.21)

set(CMAKE_CXX_STANDARD 20)

set(TIFORTH_SRC
        "${CMAKE_CURRENT_SOURCE_DIR}/engine.cc"
        "${CMAKE_CURRENT_SOURCE_DIR}/pipeline.cc"
        "${CMAKE_CURRENT_SOURCE_DIR}/task.cc")

set(TIFORTH_DEPENDENCIES Arrow::arrow_shared ArrowCompute::arrow_compute_shared)

add_library(tiforth SHARED ${TIFORTH_SRC})
target_include_directories(tiforth PUBLIC "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(tiforth PUBLIC ${TIFORTH_DEPENDENCIES})
