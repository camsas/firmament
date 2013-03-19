ROOT_DIR=../..
SUFFIX=scripts/upload

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIB =
OBJS = data_upload.o

LIBS = -lprotobuf-c -lrt
INCLUDES += -I/mnt/dev/camio/include/
CFLAGS += -std=c99 ${INCLUDES}
CPPFLAGS =

PB_BASE_DEPS = task_desc.pb-c.c resource_desc.pb-c.c \
	       resource_topology_node_desc.pb-c.c \
	       reference_desc.pb-c.c \
	       machine_perf_statistics_sample.pb-c.c \
	       task_perf_statistics_sample.pb-c.c
PB_MESSAGES_DEPS = test_message.pb-c.c heartbeat_message.pb-c.c \
	  registration_message.pb-c.c \
	  task_delegation_message.pb-c.c task_heartbeat_message.pb-c.c \
	  task_info_message.pb-c.c task_kill_message.pb-c.c \
	  task_spawn_message.pb-c.c task_state_message.pb-c.c \
	  storage_message.pb-c.c storage_registration_message.pb-c.c \
	  create_message.pb-c.c delete_message.pb-c.c lookup_message.pb-c.c \
	  copy_message.pb-c.c io_notification_message.pb-c.c

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))

all: data_upload $(OBJS) .setup

data_upload: data_upload.c $(CSRC_GEN_DIR)/messages/base_message.pb-c.c \
	$(addprefix $(CSRC_GEN_DIR)/base/, $(PB_BASE_DEPS)) \
	$(addprefix $(CSRC_GEN_DIR)/messages/, $(PB_MESSAGES_DEPS))
	$(CC) $(CFLAGS) -o $@ -I/mnt/dev/camio/include/ \
		-I$(CSRC_GEN_DIR) $(LDFLAGS) $^ $(LIBS) \
		/mnt/dev/camio/bin/libcamio.a

clean:
	rm data_upload *.o
