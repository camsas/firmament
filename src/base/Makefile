include ../../include/Makefile.config

all: task
	@$(AR) rcs $(BUILD_DIR)/base/libfirmament_base.a \
	    $(BUILD_DIR)/base/task.o

task:
	@mkdir -p $(BUILD_DIR)/base
	@$(CXX) -c $(SRC_ROOT_DIR)/base/task.cc -I$(SRC_ROOT_DIR) \
	   -o $(BUILD_DIR)/base/task.o
