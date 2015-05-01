.PHONY: clean test ext help info
.DEFAULT: all

# Symlink correct makefile config if isn't already linked.
ifeq ("$(wildcard include/Makefile.config)","")
	# Determine the current architecture.
ARCH := $(shell uname -m)
OS_ID := $(shell lsb_release -s -i)
OS_RELEASE := $(shell lsb_release -s -r)

	ifeq ($(ARCH), ia64)
		MAKECFG := "Makefile.config.ia64"
	else
		MAKECFG := "Makefile.config.$(OS_ID)-$(OS_RELEASE)"
	endif

	# Symlink the correct file.
	DUMMY:="$(shell ln -s $(MAKECFG) include/Makefile.config)"
endif

# Get common build settings
include include/Makefile.config

all: tests-clean ext platforms misc engine scripts trace_simulator

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

cost_models: scheduling
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/scheduling/cost_models all

engine: base cost_models scheduling storage platforms misc sim
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/engine all

examples: engine scripts
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/examples all

base: ext
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/base all

scheduling: base misc platforms
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/scheduling all

sim: base misc
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/sim all
	
trace_simulator: base misc cost_models
	$(MAKE) $(MAKEFLAGS) -C $(SRC_ROOT_DIR)/sim/trace-extract all

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
	rm -rf src/generated-cxx/*
	rm -rf src/generated-c/*
	find src/ -depth -name .setup -type f -delete
