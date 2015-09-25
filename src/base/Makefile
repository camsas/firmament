ROOT_DIR=../..
SUFFIX=base

include $(ROOT_DIR)/include/Makefile.config

LIB = libfirmament_base.a
OBJS = resource_status.o data_object.o
# These need to be in a particular order to satisfy dependencies.
# Do not reorder alphabetically.
PBS = coco_interference_scores.pb.o whare_map_stats.pb.o \
      reference_desc.pb.o resource_vector.pb.o \
      task_desc.pb.o resource_desc.pb.o resource_topology_node_desc.pb.o \
      task_perf_statistics_sample.pb.o \
      task_final_report.pb.o machine_perf_statistics_sample.pb.o \
      data_object_name.pb.o job_desc.pb.o \

TESTS = data_object_test references_test

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
