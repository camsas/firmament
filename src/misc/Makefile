ROOT_DIR=../..
SUFFIX=misc

include ../../include/Makefile.config
include ../../include/Makefile.common

all: libfirmamemnt_misc.a

libfirmament_misc.a: event_logging.o utils.o
	$(AR) rcs $(BUILD_DIR)/misc/libfirmament_misc.a \
	    $(BUILD_DIR)/misc/event_logging.o \
	    $(BUILD_DIR)/misc/utils.o
