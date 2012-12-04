ROOT_DIR=../..
SUFFIX=messages

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIB = libfirmament_messages.a
OBJS =
#PBS = test_message.pb.o base_message.pb.o
PBS = base_message.pb.o test_message.pb.o heartbeat_message.pb.o \
      registration_message.pb.o task_heartbeat_message.pb.o \
      task_state_message.pb.o
TESTS =
TESTS_DEPS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))

all: protobufs $(OBJ_LIB) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
