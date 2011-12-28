SUFFIX=base

include ../../include/Makefile.config
include ../../include/Makefile.common

all: task.o resource.o ensemble.o job.o task_desc.pb.h
	@$(AR) rcs $(BUILD_DIR)/base/libfirmament_base.a \
	    $(BUILD_DIR)/base/task.o $(BUILD_DIR)/base/resource.o \
	    $(BUILD_DIR)/base/ensemble.o $(BUILD_DIR)/base/job.o
