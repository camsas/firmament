ROOT_DIR=../..
SUFFIX=sim

include ../../include/Makefile.config
include ../../include/Makefile.common

all: main

main: workload_generator.o ensemble_sim.o task_sim.o job_sim.o scheduler_sim.o \
	event_queue.o resource_sim.o
	$(CXX) $(CPPFLAGS) -static $(SRC_ROOT_DIR)/sim/simulation_main.cc \
	   $(CXXFLAGS) \
	   $(BUILD_DIR)/sim/workload_generator.o \
	   $(BUILD_DIR)/sim/ensemble_sim.o \
	   $(BUILD_DIR)/sim/task_sim.o $(BUILD_DIR)/sim/job_sim.o \
	   $(BUILD_DIR)/sim/scheduler_sim.o $(BUILD_DIR)/sim/resource_sim.o \
	   $(BUILD_DIR)/sim/event_queue.o \
	   $(BUILD_DIR)/base/libfirmament_base.a \
	   $(BUILD_DIR)/misc/libfirmament_misc.a \
	   -I$(SRC_ROOT_DIR) $(LIBS) \
	   -o $(BUILD_DIR)/sim/simulation
