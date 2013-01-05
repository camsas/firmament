ROOT_DIR=../../..
SUFFIX=examples/timespin

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

LIBS += -lboost_thread -lboost_system -lboost_regex -lboost_date_time

LIB =
BINS = timespin
OBJS = timespin.o
PBS =
TESTS =
TESTS_DEPS =

OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

all: $(OBJ_BIN) .setup

$(OBJ_BIN): $(addprefix $(OBJ_DIR)/, $(OBJS)) \
	$(BUILD_DIR)/engine/libfirmament_task_lib.a
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/engine/task_lib_main.cc \
		$(CXXFLAGS) \
		$(BUILD_DIR)/engine/libfirmament_task_lib.a \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(BUILD_DIR)/messages/libfirmament_messages.a \
		$(BUILD_DIR)/base/libfirmament_base.a \
		$(BUILD_DIR)/platforms/unix/libfirmament_unix.a \
		$< \
		$(LIBS) -o $@ , \
		"  DYNLNK  $@")
