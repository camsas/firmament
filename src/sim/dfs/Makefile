ROOT_DIR=../../..
SUFFIX=sim/dfs

include $(ROOT_DIR)/include/Makefile.config

BINS =
LIB = libfirmament_sim_dfs.a
OBJS = google_block_distribution.o simulated_dfs.o
PBS =
TESTS =
TESTS_DEPS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
