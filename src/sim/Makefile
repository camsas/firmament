ROOT_DIR=../..
SUFFIX=sim

include $(ROOT_DIR)/include/Makefile.config

BINS =
LIB = libfirmament_sim.a
OBJS = simple_dtg_generator.o simulation_driver.o event_queue.o
PBS = simulation_event.pb.o
TESTS =
TESTS_DEPS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
