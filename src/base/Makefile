SUFFIX=base

include ../../include/Makefile.config
include ../../include/Makefile.common

all: task.o resource.o job.o ensemble.o
	@$(AR) rcs $(BUILD_DIR)/base/libfirmament_base.a \
	    $(BUILD_DIR)/base/task.o $(BUILD_DIR)/base/resource.o \
	    $(BUILD_DIR)/base/ensemble.o $(BUILD_DIR)/base/job.o

protobufs: task_desc.pb.o resource_desc.pb.o
