.PHONY: all clean ext
.DEFAULT: all
#-include Makefile.config

SUDO ?= sudo
export SUDO

BUILD_DIR ?= build
export BUILD_DIR

SRC_ROOT_DIR ?= src
export SRC_ROOT_DIR

SCRIPTS_DIR ?= scripts
export SCRIPTS_DIR

JOBS=-j 6
export JOBS

CC=clang++

LIBS=-lglog -lgflags -ltcmalloc -lprofiler

all: ext engine doc
#	@

ext:
	@$(SCRIPTS_DIR)/fetch-externals.sh

doc:

engine: base

base:
	@cd $(SRC_ROOT_DIR)/base && make all

nonext: engine doc
#	@

test:
	@mkdir -p $(BUILD_DIR)
	@$(CC) test.cc -o $(BUILD_DIR)/test $(LIBS)
	env HEAPCHECK=normal $(BUILD_DIR)/test

install:

clean:
	@rm -rf build
