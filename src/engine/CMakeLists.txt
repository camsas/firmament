file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/engine)

set(ENGINE_SRC
  engine/health_monitor.cc
  engine/node.cc
  )

set(EXECUTOR_SRC
  engine/executors/local_executor.cc
  engine/executors/remote_executor.cc
  # XXX(malte): we shouldn't always need to link the simulated executor
  engine/executors/simulated_executor.cc
  engine/executors/task_health_checker.cc
  engine/executors/topology_manager.cc
  )

set(COORDINATOR_SRC
  engine/coordinator.cc
  engine/coordinator_http_ui.cc
  )

set(WORKER_SRC
  engine/worker.cc
  )

#if (BUILD_HTTP_UI)
#  set(COORDINATOR_SRC ${COORDINATOR_SRC} engine/coordinator_http_ui.cc)
#endif (BUILD_HTTP_UI)

set(ENGINE_TESTS
  engine/coordinator_test.cc
  engine/simple_scheduler_test.cc
  engine/worker_test.cc
  engine/executors/topology_manager_test.cc
  )

set(TASK_LIB_SRC
  engine/task_lib.cc
)

###############################################################################
# Unit tests

if (BUILD_TESTS)
  foreach(T IN ITEMS ${ENGINE_TESTS})
    get_filename_component(TEST_NAME ${T} NAME_WE)
    add_executable(${TEST_NAME} ${T} ${COORDINATOR_SRC}
      $<TARGET_OBJECTS:base>
      $<TARGET_OBJECTS:engine>
      $<TARGET_OBJECTS:executors>
      $<TARGET_OBJECTS:messages>
      $<TARGET_OBJECTS:misc>
      $<TARGET_OBJECTS:misc_trace_generator>
      $<TARGET_OBJECTS:platforms_unix>
      $<TARGET_OBJECTS:scheduling>
      $<TARGET_OBJECTS:storage>)
    target_link_libraries(${TEST_NAME}
      ${Firmament_SHARED_LIBRARIES}
      ${spooky-hash_BINARY} ${gtest_LIBRARY} ${gtest_MAIN_LIBRARY}
      ${libhdfs3_LIBRARY} ${pion_LIBRARY} ${protobuf3_LIBRARY} ctemplate glog
      gflags hwloc)
    add_test(${TEST_NAME} ${TEST_NAME})
  endforeach(T)
endif (BUILD_TESTS)
