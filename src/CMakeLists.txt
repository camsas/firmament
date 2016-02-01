
#add_subdirectory(base)
include(base/CMakeLists.txt)
include(engine/CMakeLists.txt)
#add_subdirectory(messages)
include(messages/CMakeLists.txt)
include(misc/CMakeLists.txt)
#add_subdirectory(platforms)
include(platforms/CMakeLists.txt)
#add_subdirectory(scheduling)
include(scheduling/CMakeLists.txt)
include(sim/CMakeLists.txt)
include(storage/CMakeLists.txt)

# XXX(malte): drop this
add_definitions(-D__PLATFORM_HAS_BOOST__)

###############################################################################
# Coordinator

set(COORDINATOR_SRCS
  engine/coordinator_main.cc
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${COORDINATOR_SRC}
  ${MESSAGES_PROTOBUF_SRCS}
  ${MISC_SRC}
  ${PLATFORMS_UNIX_SRC}
  ${SCHEDULING_SRC} ${SCHEDULING_PROTOBUF_SRCS}
  # XXX(malte): shouln't need to link simulation stuff with coordinator
  ${SIM_DFS_SRC}
  ${STORAGE_SRC}
  ${TRACE_GENERATOR_SRC}
  )

add_executable(coordinator ${COORDINATOR_SRCS})

add_dependencies(coordinator gtest pb2json pion spooky-hash
  thread-safe-stl-containers)

# XXX(malte): remove this
target_compile_definitions(coordinator PRIVATE -D__HTTP_UI__)

target_link_libraries(coordinator LINK_PUBLIC
  ${pb2json_LIBRARY} ${pion_LIBRARY} ${spooky-hash_BINARY}
  boost_chrono boost_date_time boost_filesystem boost_system boost_thread
  boost_regex boost_timer ctemplate crypto glog gflags hwloc jansson
  protobuf pthread ssl)

###############################################################################
# Google trace data extractor

set(GOOGLE_TRACE_PROCESSOR_SRCS
  sim/google_trace_processor_main.cc
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${MISC_SRC}
  ${SIM_GOOGLE_TRACE_PROCESSOR_SRCS}
  )

add_executable(google_trace_processor ${GOOGLE_TRACE_PROCESSOR_SRCS})

add_dependencies(google_trace_processor spooky-hash)

target_link_libraries(google_trace_processor LINK_PUBLIC ${spooky-hash_BINARY}
  boost_chrono boost_date_time boost_filesystem boost_system boost_thread
  boost_regex boost_timer crypto glog gflags protobuf pthread ssl)

###############################################################################
# Trace-driven simulator

set(SIMULATOR_SRCS
  sim/simulator_main.cc
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${EXECUTOR_SRC}
  # XXX(maltE): this is required for the --scheduler flag, but we should
  # disentangle it
  engine/coordinator.cc
  engine/health_monitor.cc
  engine/node.cc
  ${MESSAGES_PROTOBUF_SRCS}
  ${MISC_SRC}
  ${PLATFORMS_SIM_SRC}
  ${PLATFORMS_UNIX_SRC}
  ${SCHEDULING_SRC} ${SCHEDULING_PROTOBUF_SRCS}
  ${SIM_SRC} ${SIM_DFS_SRC} ${SIM_PROTOBUF_SRCS}
  ${STORAGE_SRC}
  ${TRACE_GENERATOR_SRC}
  )

add_executable(simulator ${SIMULATOR_SRCS})

add_dependencies(simulator gtest spooky-hash thread-safe-stl-containers)

target_compile_definitions(simulator PRIVATE
  -DSOLVER_DIR="${Firmament_BINARY_DIR}/third-party")

target_link_libraries(simulator LINK_PUBLIC ${spooky-hash_BINARY}
  boost_chrono boost_date_time boost_filesystem boost_system boost_thread
  boost_regex boost_timer ctemplate crypto glog gflags hwloc jansson
  protobuf pthread ssl)

###############################################################################
# TaskLib

set(TASK_LIB_SRCS
  engine/task_lib_main.cc
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${MESSAGES_PROTOBUF_SRCS}
  ${MISC_SRC}
  ${PLATFORMS_UNIX_SRC}
  ${STORAGE_SRC}
  ${TASK_LIB_SRC}
  )

add_library(task_lib_inject SHARED ${TASK_LIB_SRCS})

add_dependencies(task_lib_inject gtest spooky-hash thread-safe-stl-containers)

target_link_libraries(task_lib_inject LINK_PUBLIC ${spooky-hash_BINARY}
  boost_chrono boost_date_time boost_filesystem boost_system boost_thread
  boost_regex boost_timer crypto hwloc protobuf pthread ssl)

###############################################################################
# Worker

set(WORKER_SRCS
  engine/worker_main.cc
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${MESSAGES_PROTOBUF_SRCS}
  ${MISC_SRC}
  ${PLATFORMS_UNIX_SRC}
  ${STORAGE_SRC}
  ${WORKER_SRC}
  )

add_executable(worker ${WORKER_SRCS})

add_dependencies(worker gtest spooky-hash thread-safe-stl-containers)

target_link_libraries(worker LINK_PUBLIC ${spooky-hash_BINARY}
  boost_chrono boost_date_time boost_filesystem boost_system boost_thread
  boost_regex boost_timer crypto ctemplate glog gflags hwloc protobuf pthread
  ssl)
