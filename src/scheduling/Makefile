ROOT_DIR=../..
SUFFIX=scheduling

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIBS += -lboost_system -lboost_thread -lboost_regex -lhwloc -lssl -lcrypto

LIB = libfirmament_scheduling.a
OBJS = dimacs_exporter.o event_driven_scheduler.o flow_graph.o \
       quincy_cost_model.o quincy_scheduler.o simple_scheduler.o \
       trivial_cost_model.o
PBS = flow_node_type.pb.o scheduling_parameters.pb.o scheduling_delta.pb.o
TESTS = dimacs_exporter_test flow_graph_test
TESTS_DEPS = $(OBJ_DIR)/../base/libfirmament_base.a
TESTS_dimacs_exporter_DEPS = $(OBJ_DIR)/flow_graph.o \
			     $(OBJ_DIR)/flow_node_type.pb.o \
 			     $(OBJ_DIR)/trivial_cost_model.o \
 			     $(OBJ_DIR)/quincy_cost_model.o \
			     $(OBJ_DIR)/../misc/libfirmament_misc.a \
			     $(OBJ_DIR)/../base/libfirmament_base.a
TESTS_flow_graph_DEPS = $(OBJ_DIR)/flow_node_type.pb.o \
 			$(OBJ_DIR)/trivial_cost_model.o \
 			$(OBJ_DIR)/quincy_cost_model.o \
			$(OBJ_DIR)/../misc/libfirmament_misc.a \
			$(OBJ_DIR)/../base/libfirmament_base.a


OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
