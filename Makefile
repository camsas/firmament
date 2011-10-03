.PHONY: clean engine base
.DEFAULT: all

# Get common build settings
include include/Makefile.config

all: ext engine doc

ext:
	@$(SCRIPTS_DIR)/fetch-externals.sh

doc:
#	@doxygen

engine: base
	@cd $(SRC_ROOT_DIR)/engine && make all
 
base:
	@cd $(SRC_ROOT_DIR)/base && make all

nonext: engine doc

test:
	@mkdir -p $(BUILD_DIR)
	@$(CC) test.cc -o $(BUILD_DIR)/test $(LIBS)
	env HEAPCHECK=normal $(BUILD_DIR)/test

clean:
	@rm -rf build
