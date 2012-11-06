ROOT_DIR=../..
SUFFIX=misc

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIBS += -lboost_system

LIB = libfirmament_misc.a
OBJS = event_logging.o utils.o
PBS =
TESTS = envelope_test utils_test
TESTS_envelope_DEPS = $(OBJ_DIR)/../messages/libfirmament_messages.a
TESTS_utils_DEPS = $(OBJ_DIR)/../base/libfirmament_base.a

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

all: protobufs .setup $(OBJ_LIB) $(TESTS_BIN)

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
