ROOT_DIR=../..
SUFFIX=base

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

all: $(BUILD_DIR)/base/libfirmament_base.a tests

$(BUILD_DIR)/base/libfirmament_base.a: protobufs $(BUILD_DIR)/base/task.o $(BUILD_DIR)/base/resource.o $(BUILD_DIR)/base/job.o $(BUILD_DIR)/base/ensemble.o $(BUILD_DIR)/base/data_object.o
	$(AR) rcs $(BUILD_DIR)/base/libfirmament_base.a \
	    $(BUILD_DIR)/base/task.o $(BUILD_DIR)/base/resource.o \
	    $(BUILD_DIR)/base/ensemble.o $(BUILD_DIR)/base/job.o \
	    $(BUILD_DIR)/base/task_desc.pb.o \
	    $(BUILD_DIR)/base/resource_desc.pb.o \
	    $(BUILD_DIR)/base/data_object_name.pb.o \
	    $(BUILD_DIR)/base/data_object.o

TEST_LD_DEPS=$(BUILD_DIR)/base/data_object_name.pb.o
tests: data_object-tests

protobufs: task_desc.pb.o resource_desc.pb.o ensemble_desc.pb.o \
	   data_object_name.pb.o
