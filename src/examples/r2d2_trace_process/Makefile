ROOT_DIR=../../..
SUFFIX=examples/r2d2_trace_process

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

STDALONE_LIBS := $(LIBS)
LIBS += -lboost_thread -lboost_system -lboost_regex -lboost_date_time -lssl -lcrypto

LIB =
BINS = packet_join_task simple_trace_analysis_task
OBJS = packet_join_task.o simple_trace_analysis_task.o
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
		$(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/engine/libfirmament_task_lib.a \
		$(BUILD_DIR)/messages/libfirmament_messages.a \
		$(BUILD_DIR)/base/libfirmament_base.a \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(BUILD_DIR)/platforms/unix/libfirmament_unix.a \
		$< \
		$(LIBS) -o $@ , \
		"  DYNLNK  $@")

packet_join_task: packet_join_task.cc
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) $(OPTFLAGS) \
		$(SRC_ROOT_DIR)/$(SUFFIX)/$< \
		$(STDALONE_LIBS) -o $@, \
		"  STDALO  $@")

simple_trace_analysis_task: simple_trace_analysis_task.cc
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) $(OPTFLAGS) \
		$(SRC_ROOT_DIR)/$(SUFFIX)/$< \
		$(STDALONE_LIBS) -o $@, \
		"  STDALO  $@")

aggregate_bandwidth_analysis_task: aggregate_bandwidth_analysis_task.cc
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) $(OPTFLAGS) \
		$(SRC_ROOT_DIR)/$(SUFFIX)/$< \
		$(STDALONE_LIBS) -o $@, \
		"  STDALO  $@")
