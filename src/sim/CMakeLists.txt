file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/sim)

set(SIM_DFS_SRC
  sim/dfs/google_block_distribution.cc
  sim/dfs/simulated_dfs.cc
  )

set(SIM_GOOGLE_TRACE_PROCESSOR_SRCS
  sim/google_trace_task_processor.cc
  )

set(SIM_SRC
  sim/event_manager.cc
  sim/google_trace_loader.cc
  sim/knowledge_base_simulator.cc
  sim/simulated_wall_time.cc
  sim/simulator_bridge.cc
  sim/simulator.cc
  sim/synthetic_trace_loader.cc
  sim/trace_utils.cc
  )

set(SIM_PROTOBUFS
  event_desc
  )

set(SIM_TESTS
  )

foreach(PB IN ITEMS ${SIM_PROTOBUFS})
  PROTOBUF_COMPILE(${PB} src/sim/${PB})
  set(SIM_PROTOBUF_SRCS ${SIM_PROTOBUF_SRCS} ${${PB}_PROTO_CC})
endforeach(PB)
