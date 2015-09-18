ROOT_DIR=../../..
SUFFIX=scheduling/flow

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_system -lboost_thread -lboost_timer -lssl -lcrypto -lctemplate

LIB = libfirmament_scheduling_flow.a

OBJS = coco_cost_model.o dimacs_add_node.o dimacs_change_arc.o \
       dimacs_change_stats.o dimacs_exporter.o dimacs_new_arc.o \
       dimacs_remove_node.o flow_graph.o flow_graph_arc.o \
       flow_graph_bridge.o flow_graph_node.o flow_scheduler.o json_exporter.o \
       octopus_cost_model.o quincy_cost_model.o sjf_cost_model.o \
       solver_dispatcher.o trivial_cost_model.o \
       random_cost_model.o void_cost_model.o wharemap_cost_model.o
PBS =
TESTS = dimacs_exporter_test flow_graph_test
TESTS_DEPS = $(BUILD_DIR)/base/libfirmament_base.a
TESTS_dimacs_exporter_DEPS = $(BUILD_DIR)/scheduling/common.o \
                             $(BUILD_DIR)/scheduling/knowledge_base.o \
                             $(BUILD_DIR)/scheduling/scheduling_delta.pb.o \
                             $(BUILD_DIR)/scheduling/flow/libfirmament_scheduling_flow.a \
                             $(BUILD_DIR)/misc/libfirmament_misc.a \
                             $(BUILD_DIR)/base/libfirmament_base.a

TESTS_flow_graph_DEPS = $(BUILD_DIR)/scheduling/common.o \
                        $(BUILD_DIR)/scheduling/knowledge_base.o \
                        $(BUILD_DIR)/scheduling/scheduling_delta.pb.o \
                        $(BUILD_DIR)/scheduling/flow/libfirmament_scheduling_flow.a \
                        $(BUILD_DIR)/misc/libfirmament_misc.a \
                        $(BUILD_DIR)/base/libfirmament_base.a

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup
	$(MAKE) -C sim all

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
