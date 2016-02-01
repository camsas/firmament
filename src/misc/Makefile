ROOT_DIR=../..
SUFFIX=misc

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_system -lssl -lcrypto $(SPOOKY_OBJ)

LIB = libfirmament_misc.a
OBJS = pb_utils.o string_utils.o trace_generator.o utils.o wall_time.o
PBS =
TESTS = envelope_test utils_test
TESTS_envelope_DEPS = $(BUILD_DIR)/messages/libfirmament_messages.a \
                      $(BUILD_DIR)/base/libfirmament_base.a
TESTS_utils_DEPS = $(BUILD_DIR)/base/libfirmament_base.a

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

all: protobufs .setup $(OBJ_LIB) $(TESTS_BIN)

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
