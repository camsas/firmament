ROOT_DIR=../..
SUFFIX=storage

include $(ROOT_DIR)/include/Makefile.config

LIBS += $(HDFS_LIB) -lboost_thread -lboost_system -lboost_regex \
        -lboost_date_time

LIB = libfirmament_storage.a

OBJS = hdfs_bridge.o simple_object_store.o

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))

#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
