SUFFIX=engine

include ../../include/Makefile.config
include ../../include/Makefile.common

all: worker worker-tests

# TODO: check if any of the below targets can make use of a generic
# build rule from Makefile.common, or can be converted to do so.

worker: worker-lib
	@mkdir -p $(BUILD_DIR)/engine
	@$(CXX) $(CPPFLAGS) -static $(SRC_ROOT_DIR)/engine/worker_main.cc \
	   $(CXXFLAGS) $(BUILD_DIR)/base/libfirmament_base.a $(BUILD_DIR)/engine/worker.o \
	   -o $(BUILD_DIR)/engine/worker

worker-lib:
	@mkdir -p $(BUILD_DIR)/engine
	@$(CXX) $(CPPFLAGS) -c $(SRC_ROOT_DIR)/engine/worker.cc \
	   -o $(BUILD_DIR)/engine/worker.o

worker-tests: worker-lib
	@mkdir -p $(BUILD_DIR)/tests/engine
	@$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(SRC_ROOT_DIR)/engine/worker_test.cc \
	   $(GTEST_LIB) $(BUILD_DIR)/engine/worker.o $(CXXFLAGS) \
	   $(LIBS) -o $(BUILD_DIR)/tests/engine/worker $(GTEST_FLAGS)
