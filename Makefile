.PHONY: clean test
.DEFAULT: all

# Get common build settings
include include/Makefile.config

all: tests-clean info ext platforms engine doc sim

info:
	@echo "Build using $(CXX)"

ext:
	$(SCRIPTS_DIR)/fetch-externals.sh

doc:
#	@doxygen

engine: tests-clean base platforms
	make -C $(SRC_ROOT_DIR)/engine all

base: tests-clean
	make -C $(SRC_ROOT_DIR)/base all

sim: base misc
	make -C $(SRC_ROOT_DIR)/sim all

misc:
	make -C $(SRC_ROOT_DIR)/misc all

# N.B.: This currently builds *all* platforms; we probably want a configure
#       script to decide which ones to build!
platforms:
	make -C $(SRC_ROOT_DIR)/platforms all

nonext: engine doc

test:
	make -C tests run

tests-clean:
	rm -f build/tests/all_tests.txt
	touch build/tests/all_tests.txt

clean: tests-clean
	rm -rf build
	rm -rf src/generated/*
