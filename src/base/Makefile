include ../../include/Makefile.config

all: task ensemble
	@$(AR) rcs $(BUILD_DIR)/base/libfirmament_base.a \
	    $(BUILD_DIR)/base/task.o

setup: 
	@mkdir -p $(BUILD_DIR)/base

ensemble: setup
	@$(CXX) -c $(SRC_ROOT_DIR)/base/ensemble.cc -I$(SRC_ROOT_DIR) \
	   -o $(BUILD_DIR)/base/ensemble.o

task: setup
	@$(CXX) -c $(SRC_ROOT_DIR)/base/task.cc -I$(SRC_ROOT_DIR) \
	   -o $(BUILD_DIR)/base/task.o
