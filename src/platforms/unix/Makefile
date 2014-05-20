ROOT_DIR=../../..
SUFFIX=platforms/unix
LIBS += -lboost_system -lboost_thread -lboost_regex

include $(ROOT_DIR)/include/Makefile.config

LIB = libfirmament_unix.a
OBJS = async_tcp_server.o common.o procfs_monitor.o procfs_machine.o \
       signal_handler.o stream_sockets_adapter.o tcp_connection.o
TESTS = procfs_monitor_test procfs_machine_test stream_sockets_adapter_test \
        stream_sockets_channel_test
TESTS_DEPS = $(BUILD_DIR)/platforms/common.pb.o \
	     $(BUILD_DIR)/messages/libfirmament_messages.a \
	     $(BUILD_DIR)/base/libfirmament_base.a
TESTS_stream_sockets_adapter_DEPS = $(OBJ_DIR)/async_tcp_server.o \
				    $(OBJ_DIR)/tcp_connection.o \
				    $(OBJ_DIR)/common.o

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: $(OBJ_LIB) $(TESTS_BIN) .setup

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS))
