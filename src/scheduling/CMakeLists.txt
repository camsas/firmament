file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/src/scheduling)

set(SCHEDULING_SRC
  scheduling/common.cc
  scheduling/event_driven_scheduler.cc
  scheduling/knowledge_base.cc
  scheduling/label_utils.cc
  scheduling/flow/coco_cost_model.cc
  scheduling/flow/cost_model_utils.cc
  scheduling/flow/dimacs_add_node.cc
  scheduling/flow/dimacs_change_arc.cc
  scheduling/flow/dimacs_change_stats.cc
  scheduling/flow/dimacs_exporter.cc
  scheduling/flow/dimacs_new_arc.cc
  scheduling/flow/dimacs_remove_node.cc
  scheduling/flow/flow_graph.cc
  scheduling/flow/flow_graph_arc.cc
  scheduling/flow/flow_graph_change_manager.cc
  scheduling/flow/flow_graph_manager.cc
  scheduling/flow/flow_graph_node.cc
  scheduling/flow/flow_scheduler.cc
  scheduling/flow/json_exporter.cc
  scheduling/flow/net_cost_model.cc
  scheduling/flow/octopus_cost_model.cc
  scheduling/flow/quincy_cost_model.cc
  scheduling/flow/quincy_interference_cost_model.cc
  scheduling/flow/random_cost_model.cc
  scheduling/flow/sjf_cost_model.cc
  scheduling/flow/solver_dispatcher.cc
  scheduling/flow/trivial_cost_model.cc
  scheduling/flow/void_cost_model.cc
  scheduling/flow/wharemap_cost_model.cc
  # XXX(malte): shouldn't always be included
  scheduling/simple/simple_scheduler.cc
  )

set(SCHEDULING_PROTOBUFS
  scheduling/scheduling_delta.proto
  )

set(SCHEDULING_SERVICE_SRC
  scheduling/firmament_scheduler_service.cc
  scheduling/knowledge_base_populator.cc
  )

set(SCHEDULING_SERVICE_PROTOBUFS
  scheduling/firmament_scheduler.proto
  )

set(SCHEDULING_TESTS
  scheduling/flow/dimacs_exporter_test.cc
  scheduling/flow/flow_graph_change_manager_test.cc
  scheduling/flow/flow_graph_manager_test.cc
  scheduling/flow/flow_graph_test.cc
  scheduling/label_utils_test.cc
)

#add_library(firmament_scheduling ${SCHEDULING_SRC} ${SCHEDULING_PROTOBUFS_SRCS} ${SCHEDULING_PROTOBUF_HDRS})

###############################################################################
# Protocol buffers

PROTOBUF_LIST_COMPILE(SCHEDULING "${SCHEDULING_PROTOBUFS}" FALSE)
PROTOBUF_LIST_COMPILE(SCHEDULING_SERVICE "${SCHEDULING_SERVICE_PROTOBUFS}" TRUE)

###############################################################################
# Unit tests

if (BUILD_TESTS)
  foreach(T IN ITEMS ${SCHEDULING_TESTS})
    get_filename_component(TEST_NAME ${T} NAME_WE)
    add_executable(${TEST_NAME} ${T}
      $<TARGET_OBJECTS:base>
      $<TARGET_OBJECTS:engine>
      $<TARGET_OBJECTS:executors>
      $<TARGET_OBJECTS:messages>
      $<TARGET_OBJECTS:misc>
      $<TARGET_OBJECTS:misc_trace_generator>
      $<TARGET_OBJECTS:platforms_unix>
      $<TARGET_OBJECTS:scheduling>)
    target_link_libraries(${TEST_NAME}
      ${spooky-hash_BINARY} ${gmock_LIBRARY} ${gmock_MAIN_LIBRARY}
      ${gtest_LIBRARY} ${gtest_MAIN_LIBRARY} ${protobuf3_LIBRARY}
      ${Firmament_SHARED_LIBRARIES} ctemplate glog gflags hwloc)
    add_test(${TEST_NAME} ${TEST_NAME})
  endforeach(T)
endif (BUILD_TESTS)
