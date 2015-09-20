ROOT_DIR=../..
SUFFIX=sim

include $(ROOT_DIR)/include/Makefile.config

BINS = simulator google_trace_processor
#OPTFLAGS += -fsanitize=address -fno-omit-frame-pointer

LIBS += -lboost_thread -lboost_system -lboost_regex -lboost_timer \
        -lboost_filesystem -lboost_chrono -lhwloc -lssl -lcrypto -lctemplate \
        -ljansson $(PB2JSON_LIB) $(PION_LIB) $(SPOOKY_OBJ)
LIB =
SIMULATOR_OBJS = event_manager.o google_trace_loader.o \
                 knowledge_base_simulator.o simulator.o \
                 simulator_bridge.o trace_utils.o
PROCESSOR_OBJS = google_trace_task_processor.o
PBS = event_desc.pb.o
TESTS = event_manager_test simulator_bridge_test
TESTS_DEPS = $(BUILD_DIR)/sim/google_trace_loader.o \
             $(BUILD_DIR)/sim/trace_utils.o \
             $(BUILD_DIR)/sim/event_desc.pb.o \
             $(BUILD_DIR)/sim/knowledge_base_simulator.o \
             $(BUILD_DIR)/scheduling/common.o \
             $(BUILD_DIR)/scheduling/flow/libfirmament_scheduling_flow.a \
             $(BUILD_DIR)/scheduling/libfirmament_scheduling.a \
             $(BUILD_DIR)/engine/executors/libfirmament_engine_executors.a \
             $(BUILD_DIR)/engine/libfirmament_engine.a \
             $(BUILD_DIR)/scheduling/simple/libfirmament_scheduling_simple.a \
             $(BUILD_DIR)/misc/libfirmament_misc.a \
             $(BUILD_DIR)/base/libfirmament_base.a \
             $(BUILD_DIR)/messages/libfirmament_messages.a \
             $(BUILD_DIR)/scheduling/flow/sim/libfirmament_simulated_quincy.a \
             $(BUILD_DIR)/sim/dfs/libfirmament_sim_dfs.a \
             $(BUILD_DIR)/platforms/sim/libfirmament_platforms_sim.a \
             $(BUILD_DIR)/platforms/unix/libfirmament_unix.a \
             $(BUILD_DIR)/storage/libfirmament_storage.a

TESTS_simulator_bridge_DEPS = $(BUILD_DIR)/sim/event_manager.o

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(BIN) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

SOLVER_DIR = $(shell readlink -f $(ROOT_DIR)/ext)
DEFINES += -DSOLVER_DIR=\"$(SOLVER_DIR)\"

$(OBJ_DIR)/simulator: $(addprefix $(OBJ_DIR)/, $(SIMULATOR_OBJS)) \
  $(SRC_ROOT_DIR)/sim/simulator_main.cc \
	$(BUILD_DIR)/scheduling/flow/libfirmament_scheduling_flow.a \
	$(BUILD_DIR)/scheduling/libfirmament_scheduling.a \
	$(BUILD_DIR)/engine/executors/libfirmament_engine_executors.a \
	$(BUILD_DIR)/engine/libfirmament_engine.a \
	$(BUILD_DIR)/scheduling/simple/libfirmament_scheduling_simple.a \
	$(BUILD_DIR)/misc/libfirmament_misc.a \
	$(BUILD_DIR)/base/libfirmament_base.a \
	$(BUILD_DIR)/messages/libfirmament_messages.a \
	$(BUILD_DIR)/scheduling/flow/sim/libfirmament_simulated_quincy.a \
	$(BUILD_DIR)/sim/dfs/libfirmament_sim_dfs.a \
	$(BUILD_DIR)/platforms/sim/libfirmament_platforms_sim.a \
	$(BUILD_DIR)/platforms/unix/libfirmament_unix.a \
	$(BUILD_DIR)/storage/libfirmament_storage.a \
	$(BUILD_DIR)/sim/event_desc.pb.o
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/sim/simulator_main.cc \
		$(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/sim/simulator_bridge.o \
		$(BUILD_DIR)/sim/event_manager.o \
		$(BUILD_DIR)/sim/google_trace_loader.o \
		$(BUILD_DIR)/sim/simulator.o \
		$(BUILD_DIR)/sim/trace_utils.o \
		$(BUILD_DIR)/sim/event_desc.pb.o \
		$(BUILD_DIR)/sim/knowledge_base_simulator.o \
		$(BUILD_DIR)/scheduling/common.o \
		$(BUILD_DIR)/scheduling/flow/libfirmament_scheduling_flow.a \
		$(BUILD_DIR)/scheduling/libfirmament_scheduling.a \
		$(BUILD_DIR)/engine/executors/libfirmament_engine_executors.a \
		$(BUILD_DIR)/engine/libfirmament_engine.a \
		$(BUILD_DIR)/scheduling/simple/libfirmament_scheduling_simple.a \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(BUILD_DIR)/base/libfirmament_base.a \
		$(BUILD_DIR)/messages/libfirmament_messages.a \
		$(BUILD_DIR)/scheduling/flow/sim/libfirmament_simulated_quincy.a \
		$(BUILD_DIR)/sim/dfs/libfirmament_sim_dfs.a \
		$(BUILD_DIR)/platforms/sim/libfirmament_platforms_sim.a \
		$(BUILD_DIR)/platforms/unix/libfirmament_unix.a \
		$(BUILD_DIR)/storage/libfirmament_storage.a \
		$(LIBS) \
		-o $(BUILD_DIR)/sim/simulator, \
		"  DYNLNK  $(BUILD_DIR)/sim/simulator")

$(OBJ_DIR)/google_trace_processor: $(addprefix $(OBJ_DIR)/, $(PROCESSOR_OBJS)) \
  $(SRC_ROOT_DIR)/sim/google_trace_processor_main.cc \
	$(BUILD_DIR)/misc/libfirmament_misc.a
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/sim/google_trace_processor_main.cc \
		$(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/sim/google_trace_task_processor.o \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(LIBS) \
		-o $(BUILD_DIR)/sim//google_trace_processor, \
		"  DYNLNK  $(BUILD_DIR)/sim/google_trace_processor")
