ROOT_DIR=../../..
SUFFIX=platforms/unix

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIB = libfirmament_unix.a
OBJS = async_tcp_server.o messaging_streamsockets.o tcp_connection.o
TESTS = messaging_streamsockets_test stream_sockets_channel_test
TESTS_DEPS = $(OBJ_DIR)/../common.pb.o \
	     $(OBJ_DIR)/../../messages/libfirmament_messages.a \
	     $(OBJ_DIR)/$(LIB)

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

all: $(OBJ_LIB) $(TESTS_BIN) .setup

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS))
