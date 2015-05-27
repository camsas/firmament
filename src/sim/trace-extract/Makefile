ROOT_DIR=../../..
SUFFIX=sim/trace-extract

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_thread -lboost_system -lboost_regex -lboost_timer -lboost_filesystem \
        -lssl -lcrypto ${EXT_DIR}/spooky_hash/SpookyV2.o

BINS = google_trace_simulator google_trace_processor google_trace_simulator_test
#OPTFLAGS += -fsanitize=address -fno-omit-frame-pointer

LIB =
SIMULATOR_OBJS = google_trace_simulator.o
PROCESSOR_OBJS = google_trace_task_processor.o
PBS = event_desc.pb.o

TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

SOLVER_DIR = $(shell readlink -f $(ROOT_DIR)/ext)
DEFINES += -DSOLVER_DIR=\"$(SOLVER_DIR)\"

$(OBJ_DIR)/google_trace_simulator: $(addprefix $(OBJ_DIR)/, $(SIMULATOR_OBJS)) \
  $(SRC_ROOT_DIR)/sim/trace-extract/google_trace_simulator_main.cc \
	$(BUILD_DIR)/base/libfirmament_base.a \
	$(BUILD_DIR)/misc/libfirmament_misc.a \
	$(BUILD_DIR)/scheduling/libfirmament_scheduling.a \
	$(BUILD_DIR)/scheduling/cost_models/libfirmament_cost_models.a \
	$(BUILD_DIR)/sim/libfirmament_sim.a \
	$(BUILD_DIR)/sim/trace-extract/event_desc.pb.o \
	$(BUILD_DIR)/sim/dfs/libfirmament_sim_dfs.a \
	$(BUILD_DIR)/storage/libfirmament_storage.a
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/sim/trace-extract/google_trace_simulator_main.cc \
		$(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/sim/trace-extract/google_trace_simulator.o \
		$(BUILD_DIR)/sim/trace-extract/event_desc.pb.o \
		$(BUILD_DIR)/scheduling/common.o \
		$(BUILD_DIR)/scheduling/dimacs_exporter.o \
		$(BUILD_DIR)/scheduling/dimacs_change_stats.o \
		$(BUILD_DIR)/scheduling/dimacs_add_node.o \
		$(BUILD_DIR)/scheduling/dimacs_change_arc.o \
		$(BUILD_DIR)/scheduling/dimacs_new_arc.o \
		$(BUILD_DIR)/scheduling/dimacs_remove_node.o \
		$(BUILD_DIR)/scheduling/flow_graph_arc.o \
		$(BUILD_DIR)/scheduling/flow_graph_node.o \
		$(BUILD_DIR)/scheduling/flow_graph.o \
		$(BUILD_DIR)/scheduling/flow_node_type.pb.o \
		$(BUILD_DIR)/scheduling/knowledge_base.o \
		$(BUILD_DIR)/scheduling/knowledge_base_simulator.o \
		$(BUILD_DIR)/scheduling/quincy_dispatcher.o \
		$(BUILD_DIR)/scheduling/scheduling_delta.pb.o \
		$(BUILD_DIR)/scheduling/cost_models/simulated_quincy_cost_model.o \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(BUILD_DIR)/base/libfirmament_base.a \
		$(BUILD_DIR)/scheduling/cost_models/libfirmament_cost_models.a \
		$(BUILD_DIR)/sim/libfirmament_sim.a \
		$(BUILD_DIR)/sim/dfs/libfirmament_sim_dfs.a \
		$(LIBS) \
		-o $(BUILD_DIR)/sim/trace-extract/google_trace_simulator, \
		"  DYNLNK  $(BUILD_DIR)/sim/trace-extract/google_trace_simulator")

$(OBJ_DIR)/google_trace_processor: $(addprefix $(OBJ_DIR)/, $(PROCESSOR_OBJS)) \
  $(SRC_ROOT_DIR)/sim/trace-extract/google_trace_processor_main.cc \
	$(BUILD_DIR)/misc/libfirmament_misc.a
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/sim/trace-extract/google_trace_processor_main.cc \
		$(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/sim/trace-extract/google_trace_task_processor.o \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(LIBS) \
		-o $(BUILD_DIR)/sim/trace-extract/google_trace_processor, \
		"  DYNLNK  $(BUILD_DIR)/sim/trace-extract/google_trace_processor")

# Does not use the standard testing infrastructure because the simulator is not compiled by default.
$(OBJ_DIR)/google_trace_simulator_test: $(addprefix $(OBJ_DIR)/, google_trace_simulator) \
  $(SRC_ROOT_DIR)/sim/trace-extract/google_trace_simulator_test.cc \
	$(BUILD_DIR)/base/libfirmament_base.a \
	$(BUILD_DIR)/misc/libfirmament_misc.a \
	$(BUILD_DIR)/scheduling/libfirmament_scheduling.a \
	$(BUILD_DIR)/scheduling/cost_models/libfirmament_cost_models.a \
	$(BUILD_DIR)/sim/libfirmament_sim.a \
	$(BUILD_DIR)/sim/trace-extract/event_desc.pb.o \
	$(BUILD_DIR)/sim/dfs/libfirmament_sim_dfs.a \
	$(BUILD_DIR)/storage/libfirmament_storage.a
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/sim/trace-extract/google_trace_simulator_test.cc \
		$(GTEST_LIB) $(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/sim/trace-extract/google_trace_simulator.o \
		$(BUILD_DIR)/sim/trace-extract/event_desc.pb.o \
		$(BUILD_DIR)/scheduling/common.o \
		$(BUILD_DIR)/scheduling/dimacs_exporter.o \
		$(BUILD_DIR)/scheduling/dimacs_change_stats.o \
		$(BUILD_DIR)/scheduling/dimacs_add_node.o \
		$(BUILD_DIR)/scheduling/dimacs_change_arc.o \
		$(BUILD_DIR)/scheduling/dimacs_new_arc.o \
		$(BUILD_DIR)/scheduling/dimacs_remove_node.o \
		$(BUILD_DIR)/scheduling/flow_graph_arc.o \
		$(BUILD_DIR)/scheduling/flow_graph_node.o \
		$(BUILD_DIR)/scheduling/flow_graph.o \
		$(BUILD_DIR)/scheduling/flow_node_type.pb.o \
		$(BUILD_DIR)/scheduling/knowledge_base.o \
		$(BUILD_DIR)/scheduling/knowledge_base_simulator.o \
		$(BUILD_DIR)/scheduling/quincy_dispatcher.o \
		$(BUILD_DIR)/scheduling/scheduling_delta.pb.o \
		$(BUILD_DIR)/scheduling/cost_models/simulated_quincy_cost_model.o \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(BUILD_DIR)/base/libfirmament_base.a \
		$(BUILD_DIR)/scheduling/cost_models/libfirmament_cost_models.a \
		$(BUILD_DIR)/sim/libfirmament_sim.a \
		$(BUILD_DIR)/sim/dfs/libfirmament_sim_dfs.a \
		$(LIBS) \
		-o $(BUILD_DIR)/sim/trace-extract/google_trace_simulator_test, \
		"  DYNLNK  $(BUILD_DIR)/sim/trace-extract/google_trace_simulator_test")
