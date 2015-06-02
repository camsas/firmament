ROOT_DIR=../../..
SUFFIX=scheduling/simple

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_system -lboost_timer

LIB = libfirmament_scheduling_simple.a

OBJS = simple_scheduler.o
PBS =
TESTS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
