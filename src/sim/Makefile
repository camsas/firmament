ROOT_DIR=../..
SUFFIX=sim

include ../../include/Makefile.config
include ../../include/Makefile.common

BINS =
OBJS = simple_dtg_generator.o
PBS =
TESTS =
TESTS_DEPS =

OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

#all: $(OBJ_BIN) $(TESTS_BIN) .setup
all: $(addprefix $(OBJ_DIR)/, $(OBJS)) $(OBJ_BIN) $(TESTS_BIN) .setup

$(OBJ_DIR)/old-main: workload_generator.o ensemble_sim.o task_sim.o job_sim.o scheduler_sim.o \
	event_queue.o resource_sim.o
	$(CXX) $(CPPFLAGS) $(SRC_ROOT_DIR)/sim/simulation_main.cc \
	   $(OPTFLAGS) $(DEFINES) \
	   $(BUILD_DIR)/sim/workload_generator.o \
	   $(BUILD_DIR)/sim/ensemble_sim.o \
	   $(BUILD_DIR)/sim/task_sim.o $(BUILD_DIR)/sim/job_sim.o \
	   $(BUILD_DIR)/sim/scheduler_sim.o $(BUILD_DIR)/sim/resource_sim.o \
	   $(BUILD_DIR)/sim/event_queue.o \
	   $(BUILD_DIR)/base/libfirmament_base.a \
	   $(BUILD_DIR)/misc/libfirmament_misc.a \
	   -I$(SRC_ROOT_DIR) $(LIBS) \
	   -o $(BUILD_DIR)/sim/simulation
