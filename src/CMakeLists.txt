# shared libraries linked by all targets
set(Firmament_SHARED_LIBRARIES ${Boost_LIBRARIES} crypto pthread ssl)

include(base/CMakeLists.txt)
include(engine/CMakeLists.txt)
include(examples/CMakeLists.txt)
include(messages/CMakeLists.txt)
include(misc/CMakeLists.txt)
include(platforms/CMakeLists.txt)
include(scheduling/CMakeLists.txt)
include(sim/CMakeLists.txt)
include(storage/CMakeLists.txt)

# XXX(malte): drop this
add_definitions(-D__PLATFORM_HAS_BOOST__)

###############################################################################
# Object collections representing different modules (~= source dirs)

add_library(base OBJECT ${BASE_SRC} ${BASE_PROTOBUF_SRCS})
add_dependencies(base gtest spooky-hash thread-safe-stl-containers)
add_library(engine OBJECT ${ENGINE_SRC} ${BASE_PROTOBUF_HDRS}
  ${MESSAGES_PROTOBUF_HDRS} ${SCHEDULING_PROTOBUF_HDRS})
add_dependencies(engine gtest pb2json pion spooky-hash
  thread-safe-stl-containers)
add_library(executors OBJECT ${EXECUTOR_SRC} ${BASE_PROTOBUF_HDRS}
  ${MESSAGES_PROTOBUF_HDRS})
add_dependencies(executors gtest spooky-hash thread-safe-stl-containers)
add_library(messages OBJECT ${MESSAGES_PROTOBUF_SRCS} ${BASE_PROTOBUF_HDRS})
add_library(misc OBJECT ${MISC_SRC} ${BASE_PROTOBUF_HDRS})
add_dependencies(misc gtest spooky-hash thread-safe-stl-containers)
add_library(misc_trace_generator OBJECT ${MISC_TRACE_GENERATOR_SRC})
add_dependencies(misc_trace_generator gtest spooky-hash
  thread-safe-stl-containers)
add_library(platforms_unix OBJECT ${PLATFORMS_UNIX_SRC})
add_dependencies(platforms_unix gtest spooky-hash thread-safe-stl-containers)
add_library(platforms_sim OBJECT ${PLATFORMS_SIM_SRC})
add_dependencies(platforms_sim gtest spooky-hash thread-safe-stl-containers)
add_library(scheduling OBJECT ${SCHEDULING_SRC} ${SCHEDULING_PROTOBUF_SRCS}
  ${BASE_PROTOBUF_HDRS} ${MESSAGES_PROTOBUF_HDRS})
add_dependencies(scheduling gtest pb2json pion spooky-hash
  thread-safe-stl-containers)
# TODO(malte): N.B. this subsumes sim_dfs; should disentangle!
add_library(sim OBJECT ${SIM_SRC} ${SIM_DFS_SRC} ${SIM_PROTOBUF_SRCS}
  ${BASE_PROTOBUF_HDRS} ${MESSAGES_PROTOBUF_HDRS} ${SCHEDULING_PROTOBUF_HDRS})
# XXX(malte): get rid of this
target_compile_definitions(sim PRIVATE
  -DSOLVER_DIR="${Firmament_BINARY_DIR}/third_party")
add_dependencies(sim gtest spooky-hash thread-safe-stl-containers)
add_library(storage OBJECT ${STORAGE_SRC} ${BASE_PROTOBUF_HDRS}
  ${MESSAGES_PROTOBUF_HDRS})
add_dependencies(storage gtest spooky-hash thread-safe-stl-containers)

###############################################################################
# Coordinator

set(COORDINATOR_SRCS
  engine/coordinator_main.cc
  ${COORDINATOR_SRC}
  )

add_executable(coordinator ${COORDINATOR_SRCS}
  $<TARGET_OBJECTS:base>
  $<TARGET_OBJECTS:engine>
  $<TARGET_OBJECTS:executors>
  $<TARGET_OBJECTS:messages>
  $<TARGET_OBJECTS:misc>
  $<TARGET_OBJECTS:misc_trace_generator>
  $<TARGET_OBJECTS:platforms_unix>
  $<TARGET_OBJECTS:scheduling>
  $<TARGET_OBJECTS:storage>
  )

add_dependencies(coordinator gtest pb2json pion scripts_job spooky-hash
  thread-safe-stl-containers)

# XXX(malte): remove this
target_compile_definitions(coordinator PRIVATE -D__HTTP_UI__)

target_link_libraries(coordinator LINK_PUBLIC
  ${pb2json_LIBRARY} ${pion_LIBRARY} ${spooky-hash_BINARY}
  ${Firmament_SHARED_LIBRARIES} ctemplate glog gflags hwloc jansson
  protobuf)
if (${ENABLE_HDFS})
  target_link_libraries(coordinator ${libhdfs3_LIBRARY})
endif (${ENABLE_HDFS})

###############################################################################
# Google trace data extractor

set(GOOGLE_TRACE_PROCESSOR_SRCS
  sim/google_trace_processor_main.cc
  ${SIM_GOOGLE_TRACE_PROCESSOR_SRCS}
  )

add_executable(google_trace_processor ${GOOGLE_TRACE_PROCESSOR_SRCS}
  $<TARGET_OBJECTS:base>
  $<TARGET_OBJECTS:misc>
  )

add_dependencies(google_trace_processor gtest spooky-hash
  thread-safe-stl-containers)

target_link_libraries(google_trace_processor LINK_PUBLIC ${spooky-hash_BINARY}
  ${Firmament_SHARED_LIBRARIES} glog gflags protobuf)

###############################################################################
# Scheduling library (for integrations)

add_library(firmament_scheduling SHARED
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${EXECUTOR_SRC}
  ${MESSAGES_PROTOBUF_SRCS} ${MISC_SRC}
  ${MISC_TRACE_GENERATOR_SRC}
  ${SCHEDULING_SRC} ${SCHEDULING_PROTOBUF_SRCS})

add_dependencies(firmament_scheduling gtest pb2json pion spooky-hash
  thread-safe-stl-containers)

target_link_libraries(firmament_scheduling LINK_PUBLIC ${spooky-hash_BINARY}
  ${Firmament_SHARED_LIBRARIES} ctemplate glog gflags hwloc protobuf)

###############################################################################
# Trace-driven simulator

set(SIMULATOR_SRCS
  sim/simulator_main.cc
  # XXX(maltE): this is required for the --scheduler flag, but we should
  # disentangle it
  engine/coordinator.cc
  engine/health_monitor.cc
  engine/node.cc
  )

add_executable(simulator ${SIMULATOR_SRCS}
  $<TARGET_OBJECTS:base>
  $<TARGET_OBJECTS:executors>
  $<TARGET_OBJECTS:messages>
  $<TARGET_OBJECTS:misc>
  $<TARGET_OBJECTS:misc_trace_generator>
  $<TARGET_OBJECTS:platforms_unix>
  $<TARGET_OBJECTS:platforms_sim>
  $<TARGET_OBJECTS:scheduling>
  $<TARGET_OBJECTS:sim>
  $<TARGET_OBJECTS:storage>
  )

add_dependencies(simulator gtest spooky-hash thread-safe-stl-containers)

target_compile_definitions(simulator PRIVATE
  -DSOLVER_DIR="${Firmament_BINARY_DIR}/third_party")

target_link_libraries(simulator LINK_PUBLIC ${spooky-hash_BINARY}
  ${Firmament_SHARED_LIBRARIES} ctemplate glog gflags hwloc jansson protobuf)
if (${ENABLE_HDFS})
  target_link_libraries(simulator ${libhdfs3_LIBRARY})
endif (${ENABLE_HDFS})

###############################################################################
# TaskLib

# N.B.: can't use TARGET_OBJECTS here because the existing object files are not
# position-independent.
set(TASK_LIB_SRCS
  engine/task_lib_main.cc
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${MESSAGES_PROTOBUF_SRCS}
  ${MISC_SRC}
  ${PLATFORMS_UNIX_SRC}
  ${TASK_LIB_SRC}
  )

add_library(task_lib_inject SHARED ${TASK_LIB_SRCS})

add_dependencies(task_lib_inject gtest spooky-hash thread-safe-stl-containers)

target_link_libraries(task_lib_inject LINK_PUBLIC ${spooky-hash_BINARY}
  ${Firmament_SHARED_LIBRARIES} glog gflags hwloc protobuf)

###############################################################################
# Worker

set(WORKER_SRCS
  engine/worker_main.cc
  ${WORKER_SRC}
  )

add_executable(worker ${WORKER_SRCS}
  $<TARGET_OBJECTS:base>
  $<TARGET_OBJECTS:engine>
  $<TARGET_OBJECTS:executors>
  $<TARGET_OBJECTS:messages>
  $<TARGET_OBJECTS:misc>
  $<TARGET_OBJECTS:platforms_unix>
  )

add_dependencies(worker gtest spooky-hash thread-safe-stl-containers)

target_link_libraries(worker LINK_PUBLIC ${spooky-hash_BINARY}
  ${Firmament_SHARED_LIBRARIES} ctemplate glog gflags hwloc protobuf)
