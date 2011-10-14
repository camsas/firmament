include ../../include/Makefile.config

HDRS = "$(SRC_ROOT_DIR)/misc/event_logging.h"

setup:
	@mkdir -p $(BUILD_DIR)/misc

all: event_logging.o
	@$(AR) rcs $(BUILD_DIR)/misc/libfirmament_misc.a \
	    $(BUILD_DIR)/misc/event_logging.o

%.o: %.cc %.h setup
	@$(CXX) -c $(SRC_ROOT_DIR)/misc/$< -I$(SRC_ROOT_DIR) -o $(BUILD_DIR)/misc/$@
