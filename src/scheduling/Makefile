ROOT_DIR=../..
SUFFIX=scheduling

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_system

LIB = libfirmament_scheduling.a
OBJS = event_driven_scheduler.o common.o \
       knowledge_base.o knowledge_base_simulator.o

PBS = scheduling_delta.pb.o
TESTS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
