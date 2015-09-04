ROOT_DIR=../../..
SUFFIX=engine/executors

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_date_time -lboost_regex -lboost_system -lboost_thread \
        -lcrypto -lhwloc -lssl
ifneq ($(BOOST_VERSION), "1.48")
 LIBS += -lboost_chrono
endif

LIB = libfirmament_engine_executors.a

OBJS = local_executor.o remote_executor.o simulated_executor.o \
       task_health_checker.o topology_manager.o

PBS =
TESTS = local_executor_test topology_manager_test
TESTS_DEPS = $(BUILD_DIR)/misc/libfirmament_misc.a \
             $(BUILD_DIR)/base/libfirmament_base.a \
             $(BUILD_DIR)/messages/libfirmament_messages.a

TESTS_local_executor_DEPS = $(BUILD_DIR)/engine/executors/topology_manager.o \
                            $(BUILD_DIR)/engine/executors/task_health_checker.o \
                            $(BUILD_DIR)/misc/libfirmament_misc.a \
                            $(BUILD_DIR)/base/libfirmament_base.a

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
