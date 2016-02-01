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
  sim/event_desc.proto
  )

set(SIM_TESTS
  )

###############################################################################
# Protocol buffers

PROTOBUF_LIST_COMPILE(SIM "${SIM_PROTOBUFS}")
