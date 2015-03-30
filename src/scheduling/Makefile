ROOT_DIR=../..
SUFFIX=scheduling

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_system -lboost_thread -lboost_regex -lhwloc -lssl -lcrypto

LIB = libfirmament_scheduling.a
OBJS = dimacs_exporter.o event_driven_scheduler.o flow_graph.o \
       quincy_scheduler.o simple_scheduler.o common.o \
       knowledge_base.o dimacs_change_arc.o dimacs_remove_node.o \
       dimacs_add_node.o flow_graph_node.o flow_graph_arc.o dimacs_new_arc.o \
       quincy_dispatcher.o knowledge_base_simulator.o

PBS = flow_node_type.pb.o scheduling_parameters.pb.o scheduling_delta.pb.o
TESTS = dimacs_exporter_test flow_graph_test
TESTS_DEPS = $(BUILD_DIR)/base/libfirmament_base.a
TESTS_dimacs_exporter_DEPS = $(OBJ_DIR)/flow_graph.o \
			     $(OBJ_DIR)/flow_node_type.pb.o \
			     $(OBJ_DIR)/common.o \
			     $(OBJ_DIR)/dimacs_change_arc.o \
			     $(OBJ_DIR)/dimacs_new_arc.o \
			     $(OBJ_DIR)/dimacs_remove_node.o \
			     $(OBJ_DIR)/dimacs_add_node.o \
			     $(OBJ_DIR)/flow_graph_arc.o \
			     $(OBJ_DIR)/flow_graph_node.o \
			     $(OBJ_DIR)/knowledge_base.o \
			     $(BUILD_DIR)/misc/libfirmament_misc.a \
			     $(BUILD_DIR)/base/libfirmament_base.a \
			     $(BUILD_DIR)/scheduling/cost_models/libfirmament_base.a

TESTS_flow_graph_DEPS = $(OBJ_DIR)/flow_node_type.pb.o \
			$(OBJ_DIR)/common.o \
			$(OBJ_DIR)/flow_graph_arc.o \
			$(OBJ_DIR)/dimacs_change_arc.o \
			$(OBJ_DIR)/dimacs_new_arc.o \
			$(OBJ_DIR)/dimacs_remove_node.o \
			$(OBJ_DIR)/dimacs_add_node.o \
			$(OBJ_DIR)/flow_graph_node.o \
			$(OBJ_DIR)/knowledge_base.o \
			$(BUILD_DIR)/misc/libfirmament_misc.a \
			$(BUILD_DIR)/base/libfirmament_base.a \
			$(BUILD_DIR)/scheduling/cost_models/libfirmament_base.a

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
