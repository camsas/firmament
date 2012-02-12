ROOT_DIR=../../..
SUFFIX=platforms/unix

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIB = libfirmament_unix.a
OBJS = messaging_streamsockets.o
TESTS = messaging_streamsockets_test
TESTS_DEPS = $(OBJ_DIR)/../common.pb.o

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

all: $(TESTS_BIN) $(OBJ_LIB) .setup

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS))
