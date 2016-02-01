file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/scheduling)

set(SCHEDULING_SRC
  scheduling/common.cc
  scheduling/event_driven_scheduler.cc
  scheduling/knowledge_base.cc
  scheduling/flow/coco_cost_model.cc
  scheduling/flow/dimacs_add_node.cc
  scheduling/flow/dimacs_change_arc.cc
  scheduling/flow/dimacs_change_stats.cc
  scheduling/flow/dimacs_exporter.cc
  scheduling/flow/dimacs_new_arc.cc
  scheduling/flow/dimacs_remove_node.cc
  scheduling/flow/flow_graph.cc
  scheduling/flow/flow_graph_arc.cc
  scheduling/flow/flow_graph_manager.cc
  scheduling/flow/flow_graph_node.cc
  scheduling/flow/flow_scheduler.cc
  scheduling/flow/json_exporter.cc
  scheduling/flow/octopus_cost_model.cc
  scheduling/flow/quincy_cost_model.cc
  scheduling/flow/random_cost_model.cc
  scheduling/flow/sjf_cost_model.cc
  scheduling/flow/solver_dispatcher.cc
  scheduling/flow/trivial_cost_model.cc
  scheduling/flow/void_cost_model.cc
  scheduling/flow/wharemap_cost_model.cc
  # XXX(malte): shouldn't always be included
  scheduling/flow/sim/google_runtime_distribution.cc
  scheduling/flow/sim/simulated_quincy_cost_model.cc
  scheduling/simple/simple_scheduler.cc
  )

set(SCHEDULING_PROTOBUFS
  scheduling_delta
  )

set(SCHEDULING_TESTS
  scheduling/flow/dimacs_exporter_test.cc
  scheduling/flow/flow_graph_manager_test.cc
  scheduling/flow/flow_graph_test.cc
)

foreach(PB IN ITEMS ${SCHEDULING_PROTOBUFS})
  PROTOBUF_COMPILE(${PB} scheduling/${PB})
  set(SCHEDULING_PROTOBUF_SRCS ${SCHEDULING_PROTOBUF_SRCS} ${${PB}_PROTO_CC})
endforeach(PB)

#PROTOBUF_GENERATE_CPP(SCHEDULING_PROTOBUF_SRCS SCHEDULING_PROTOBUF_HDRS ${SCHEDULING_PROTOBUFS})

#add_library(firmament_scheduling ${SCHEDULING_SRC} ${SCHEDULING_PROTOBUFS_SRCS} ${SCHEDULING_PROTOBUF_HDRS})
