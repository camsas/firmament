ROOT_DIR=../../..
SUFFIX=platforms/sim

include $(ROOT_DIR)/include/Makefile.config

LIB = libfirmament_platforms_sim.a

OBJS = simulated_messaging_adapter.o

TESTS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: $(OBJ_LIB) $(TESTS_BIN) .setup

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS))
