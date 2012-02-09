ROOT_DIR=../..
SUFFIX=base

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIB = libfirmament_base.a
OBJS = task.o resource.o job.o ensemble.o data_object.o
PBS = task_desc.pb.o resource_desc.pb.o ensemble_desc.pb.o \
      data_object_name.pb.o
TESTS = data_object_test
TESTS_DEPS = $(OBJ_DIR)/data_object_name.pb.o

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
	$(AR) rcs $@ $^
