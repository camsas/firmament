include ../../include/Makefile.config

all: main

setup:
	@mkdir -p $(BUILD_DIR)/sim/

main: workload_generator.o ensemble_sim.o
	@$(CXX) -static $(SRC_ROOT_DIR)/sim/simulation_main.cc \
	   $(BUILD_DIR)/sim/workload_generator.o $(BUILD_DIR)/sim/ensemble_sim.o \
	   $(BUILD_DIR)/base/ensemble.o \
	   -I$(SRC_ROOT_DIR) $(LIBS) \
	   -o $(BUILD_DIR)/sim/simulation

%.o: %.cc setup
	@$(CXX) -c $(SRC_ROOT_DIR)/sim/$< -I$(SRC_ROOT_DIR) -o $(BUILD_DIR)/sim/$@
