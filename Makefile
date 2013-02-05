.PHONY: clean test ext help info
.DEFAULT: all

# Get common build settings
include include/Makefile.config

all: tests-clean ext platforms misc engine examples scripts

help: info

info:
	@echo "C++ compiler: $(CXX)"
	@echo "CPPFLAGS: $(CPPFLAGS)"
	@echo "CXXFLAGS: $(CXXFLAGS)"
	@echo "Platform: $(PLATFORM)"
	@echo "Build output in $(BUILD_DIR)"
	@echo
	@echo "Targets:"
	@echo "  - all: build all code"
	@echo "  - clean: remove all temporary build infrastructure"
	@echo "  - examples: build example jobs"
	@echo "  - ext: set up external dependencies (should only run once "
	@echo "         and automatically, but can be re-run manually)"
	@echo "  - test: run unit tests (must be preceded by clean & make all)"
	@echo "  - lint: run code style checker scripts"
	@echo "  - lint-verb: verbose code style checking"
	@echo

ext: ext/.ext-ok

ext/.ext-ok:
	$(SCRIPTS_DIR)/fetch-externals.sh

engine: base storage platforms misc  sim
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/engine all

examples: engine scripts
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/examples all

base: ext
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/base all

sim: base misc
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/sim all

misc: messages ext
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/misc all

messages:  base ext
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/messages all
	
storage: messages ext 	
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/storage all

# N.B.: This currently builds *all* platforms; we probably want a configure
#       script to decide which ones to build!
platforms: messages
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/platforms all

scripts:
	$(MAKE) $(MAKEFLAGS) -C $(ROOT_DIR)/scripts/job

test: ext
	$(MAKE) $(MAKEFLAGS) -C tests run

tests-clean:
	rm -f build/tests/all_tests.txt
	mkdir -p build/tests
	touch build/tests/all_tests.txt

lint:
	python tests/all_lint.py src/ False

lint-verb:
	python tests/all_lint.py src/ True

clean:
	rm -rf build
	rm -rf src/generated/*
	find src/ -depth -name .setup -type f -delete
