file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/engine)

set(ENGINE_SRC
  engine/health_monitor.cc
  engine/node.cc
  )

set(COORDINATOR_SRC
  ${ENGINE_SRC}
  engine/coordinator.cc
  engine/coordinator_http_ui.cc
  engine/executors/local_executor.cc
  engine/executors/remote_executor.cc
  # XXX(malte): we shouldn't always need to link the simulated executor
  engine/executors/simulated_executor.cc
  engine/executors/task_health_checker.cc
  engine/executors/topology_manager.cc
  )

#if (BUILD_HTTP_UI)
#  set(COORDINATOR_SRC ${COORDINATOR_SRC} engine/coordinator_http_ui.cc)
#endif (BUILD_HTTP_UI)

set(ENGINE_TESTS
  coordinator_test.cc
  simple_scheduler_test.cc
  worker_test.cc
  executors/local_executor_test.cc
  executors/topology_manager_test.cc
  )
