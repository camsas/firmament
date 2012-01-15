.PHONY: clean
.DEFAULT: all

# Get common build settings
include include/Makefile.config

all: info ext platforms engine doc sim

info:
	@echo "Build using $(CXX)"

ext:
	@$(SCRIPTS_DIR)/fetch-externals.sh

doc:
#	@doxygen

engine: base
	@cd $(SRC_ROOT_DIR)/engine && make all

base:
	@cd $(SRC_ROOT_DIR)/base && make all

sim: base misc
	@cd $(SRC_ROOT_DIR)/sim && make all

misc:
	@cd $(SRC_ROOT_DIR)/misc && make all

# N.B.: This currently builds *all* platforms; we probably want a configure
#       script to decide which ones to build!
platforms:
	@cd $(SRC_ROOT_DIR)/platforms && make all

nonext: engine doc

tests:
	@cd $(SRC_ROOT_DIR)/tests && make all
	env HEAPCHECK=normal $(BUILD_DIR)/all_tests

clean:
	@rm -rf build
	@rm -rf src/generated/*
