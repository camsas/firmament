ROOT_DIR=../../..
SUFFIX=platforms/unix

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

all: setup libfirmament_unix.a tests

libfirmament_unix.a: messaging_streamsockets.o
	@$(AR) rcs $(BUILD_DIR)/$(SUFFIX)/libfirmament_unix.a \
	    $(BUILD_DIR)/$(SUFFIX)/messaging_streamsockets.o

TEST_LD_DEPS=$(BUILD_DIR)/platforms/common.pb.o
tests: messaging_streamsockets-tests
