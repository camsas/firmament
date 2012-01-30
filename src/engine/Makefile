ROOT_DIR=../..
SUFFIX=engine

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

all: coordinator-all worker-all

coordinator-all: coordinator coordinator-tests

worker-all: worker worker-tests

coordinator: coordinator-lib
	@mkdir -p $(BUILD_DIR)/engine
	@$(CXX) $(CPPFLAGS) -static $(SRC_ROOT_DIR)/engine/coordinator_main.cc \
	   $(CXXFLAGS) $(BUILD_DIR)/base/libfirmament_base.a \
	   $(BUILD_DIR)/engine/coordinator.o \
	   $(BUILD_DIR)/platforms/common.pb.o \
	   -o $(BUILD_DIR)/engine/coordinator

coordinator-lib: coordinator.o

# TODO: check if any of the below targets can make use of a generic
# build rule from Makefile.common, or can be converted to do so.

worker: worker-lib
	@mkdir -p $(BUILD_DIR)/engine
	@$(CXX) $(CPPFLAGS) -static $(SRC_ROOT_DIR)/engine/worker_main.cc \
	   $(CXXFLAGS) $(BUILD_DIR)/base/libfirmament_base.a \
	   $(BUILD_DIR)/engine/worker.o \
	   $(BUILD_DIR)/platforms/common.pb.o \
	   -o $(BUILD_DIR)/engine/worker

worker-lib: worker.o

TEST_LD_DEPS=$(BUILD_DIR)/platforms/common.pb.o
tests: worker-tests coordinator-tests
