ROOT_DIR=../../..
SUFFIX=sim/trace-extract

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_thread -lboost_system -lboost_regex -lssl -lcrypto

BINS =  google-trace-extractor google-trace-processor

LIB =
EXTRACTOR_OBJS = google_trace_extractor.o
PROCESSOR_OBJS = google_trace_task_processor.o
PBS =
TESTS =
TESTS_DEPS =

#OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
#TESTS_OBJS = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))
BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

include $(ROOT_DIR)/include/Makefile.common

all: .setup $(BIN)

#protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

#$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))

$(addprefix $(OBJ_DIR)/, google-trace-extractor): $(addprefix $(OBJ_DIR)/, $(EXTRACTOR_OBJS)) \
	$(BUILD_DIR)/base/libfirmament_base.a \
	$(BUILD_DIR)/misc/libfirmament_misc.a \
	$(BUILD_DIR)/scheduling/libfirmament_scheduling.a \
	$(BUILD_DIR)/storage/libfirmament_storage.a
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/sim/trace-extract/google_trace_extractor_main.cc \
		$(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/sim/trace-extract/google_trace_extractor.o \
		$(BUILD_DIR)/scheduling/common.o \
		$(BUILD_DIR)/scheduling/dimacs_exporter.o \
		$(BUILD_DIR)/scheduling/flow_graph.o \
		$(BUILD_DIR)/scheduling/flow_node_type.pb.o \
		$(BUILD_DIR)/scheduling/quincy_cost_model.o \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(BUILD_DIR)/base/libfirmament_base.a \
		$(LIBS) \
		-o $(BUILD_DIR)/sim/trace-extract/google_trace_extractor, \
		"  DYNLNK  $(BUILD_DIR)/sim/trace-extract/google_trace_extractor")

$(addprefix $(OBJ_DIR)/, google-trace-processor): $(addprefix $(OBJ_DIR)/, $(PROCESSOR_OBJS)) \
	$(BUILD_DIR)/misc/libfirmament_misc.a
	$(call quiet-command, \
		$(CXX) $(CPPFLAGS) \
		$(SRC_ROOT_DIR)/sim/trace-extract/google_trace_processor_main.cc \
		$(OPTFLAGS) $(DEFINES) \
		$(BUILD_DIR)/sim/trace-extract/google_trace_task_processor.o \
		$(BUILD_DIR)/misc/libfirmament_misc.a \
		$(LIBS) \
		-o $(BUILD_DIR)/sim/trace-extract/google_trace_processor, \
		"  DYNLNK  $(BUILD_DIR)/sim/trace-extract/google_trace_processor")
