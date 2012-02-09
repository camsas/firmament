ROOT_DIR=..
SUFFIX=tests

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

run:
	env HEAPCHECK=normal ./all_tests.py $(BUILD_DIR)

ALL_TESTS = $(shell find $(BUILD_DIR)/tests/ -name "*.o")
