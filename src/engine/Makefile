SUFFIX=engine

include ../../include/Makefile.config
include ../../include/Makefile.common

all: worker worker-tests

# TODO: check if any of the below targets can make use of a generic
# build rule from Makefile.common, or can be converted to do so.

worker: worker-lib
	@mkdir -p $(BUILD_DIR)/engine
	@$(CXX) -static $(SRC_ROOT_DIR)/engine/worker_main.cc \
	   $(BUILD_DIR)/engine/worker.o -I$(SRC_ROOT_DIR) $(LIBS) \
	   -o $(BUILD_DIR)/engine/worker

worker-lib:
	@mkdir -p $(BUILD_DIR)/engine
	@$(CXX) -c $(SRC_ROOT_DIR)/engine/worker.cc -I$(SRC_ROOT_DIR) \
	   -o $(BUILD_DIR)/engine/worker.o

worker-tests: worker-lib
	@mkdir -p $(BUILD_DIR)/tests/engine
	@$(CXX) $(CPPFLAGS) $(SRC_ROOT_DIR)/engine/worker_test.cc \
	   $(GTEST_LIB) $(BUILD_DIR)/engine/worker.o $(CXXFLAGS) \
	   $(LIBS) -o $(BUILD_DIR)/tests/engine/worker $(GTEST_FLAGS)
