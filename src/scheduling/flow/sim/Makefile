ROOT_DIR=../../../..
SUFFIX=scheduling/flow/sim

include $(ROOT_DIR)/include/Makefile.config

LIB = libfirmament_simulated_quincy.a

OBJS = google_runtime_distribution.o simulated_quincy_cost_model.o
PBS =
TESTS =
TESTS_DEPS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
