.PHONY: clean test ext
.DEFAULT: all

# Get common build settings
include include/Makefile.config

all: tests-clean info ext platforms misc engine doc

info:
	@echo "Build using $(CXX)"

ext: ext/.ext-ok

ext/.ext-ok:
	$(SCRIPTS_DIR)/fetch-externals.sh

doc:
#	@doxygen

engine: base platforms misc
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/engine all

base: ext
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/base all

sim: base misc
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/sim all

misc: messages ext
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/misc all

messages: base ext
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/messages all

# N.B.: This currently builds *all* platforms; we probably want a configure
#       script to decide which ones to build!
platforms: messages
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/platforms all

nonext: engine doc

test: ext
	$(MAKE) $(MAKEFLAGS) -C tests run

tests-clean:
	rm -f build/tests/all_tests.txt
	mkdir -p build/tests
	touch build/tests/all_tests.txt

lint:
	python tests/all_lint.py src/

clean:
	rm -rf build
	rm -rf src/generated/*
	find src/ -depth -name .setup -type f -delete
