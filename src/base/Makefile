ROOT_DIR=../..
SUFFIX=base

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIB = libfirmament_base.a
OBJS = task.o resource.o job.o ensemble.o data_object.o task_graph.o
PBS = reference_desc.pb.o task_desc.pb.o resource_desc.pb.o \
      resource_topology_node_desc.pb.o task_perf_statistics_sample.pb.o \
      machine_perf_statistics_sample.pb.o \
      ensemble_desc.pb.o data_object_name.pb.o job_desc.pb.o
TESTS = data_object_test references_test task_graph_test
TESTS_DEPS = $(OBJ_DIR)/data_object_name.pb.o \
	     $(OBJ_DIR)/reference_desc.pb.o \
	     $(OBJ_DIR)/task_desc.pb.o

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
