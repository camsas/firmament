ROOT_DIR=../..
SUFFIX=messages

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIB = libfirmament_messages.a
OBJS =
PBS = test_message.pb.o heartbeat_message.pb.o \
      registration_message.pb.o \
      task_delegation_message.pb.o task_heartbeat_message.pb.o \
      task_info_message.pb.o task_kill_message.pb.o \
      task_spawn_message.pb.o task_state_message.pb.o \
      storage_message.pb.o storage_registration_message.pb.o \
      create_message.pb.o delete_message.pb.o lookup_message.pb.o \
      copy_message.pb.o io_notification_message.pb.o
TESTS =
TESTS_DEPS =
OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))

all: protobufs $(OBJ_LIB) .setup

# N.B.: base_message is handled specially here, as it (unlike any of the others)
# has dependencies on other messages (and thus requires the correct headers to
# be generated before it is built). It depends on **all** other messages, so it
# must go last.
protobufs: $(addprefix $(OBJ_DIR)/, $(PBS)) $(OBJ_DIR)/base_message.pb.o

$(OBJ_LIB): $(OBJ_DIR)/base_message.pb.o $(addprefix $(OBJ_DIR)/, $(OBJS)) \
	$(addprefix $(OBJ_DIR)/, $(PBS))
