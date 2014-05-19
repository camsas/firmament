ROOT_DIR=../..
SUFFIX=misc

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_system -lssl -lcrypto

LIB = libfirmament_misc.a
OBJS = event_logging.o equivclasses.o pb_utils.o string_utils.o utils.o
PBS =
TESTS = envelope_test equivclasses_test utils_test
TESTS_envelope_DEPS = $(BUILD_DIR)/messages/libfirmament_messages.a \
		      $(BUILD_DIR)/base/libfirmament_base.a
TESTS_equivclasses_DEPS = $(BUILD_DIR)/base/libfirmament_base.a \
			  $(OBJ_DIR)/pb_utils.o
TESTS_utils_DEPS = $(BUILD_DIR)/base/libfirmament_base.a

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

all: protobufs .setup $(OBJ_LIB) $(TESTS_BIN)

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
