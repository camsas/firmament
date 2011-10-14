include ../../include/Makefile.config

#HDRS = "$(SRC_ROOT_DIR)/base/task.h" \
#       "$(SRC_ROOT_DIR)/base/job.h" \
#       "$(SRC_ROOT_DIR)/base/resource.h" \
#       "$(SRC_ROOT_DIR)/base/ensemble.h"

setup:
	@mkdir -p $(BUILD_DIR)/base

all: task.o resource.o ensemble.o job.o
	@$(AR) rcs $(BUILD_DIR)/base/libfirmament_base.a \
	    $(BUILD_DIR)/base/task.o $(BUILD_DIR)/base/resource.o \
	    $(BUILD_DIR)/base/ensemble.o $(BUILD_DIR)/base/job.o

%.o: %.cc %.h setup
	@$(CXX) -c $(SRC_ROOT_DIR)/base/$< -I$(SRC_ROOT_DIR) -o $(BUILD_DIR)/base/$@
