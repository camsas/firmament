.PHONY: all clean ext
.DEFAULT: all
#-include Makefile.config

SUDO ?= sudo
export SUDO

DESTDIR ?=
export DESTDIR

#PREFIX ?= $(HOME)/
#export PREFIX

JOBS=-j 6
export JOBS

CC=clang

LIBS=-lglog -lgflags -ltcmalloc -lprofiler

all: ext engine doc
#	@

ext:
	@./fetch-externals.sh

doc:

engine:

nonext: engine doc
#	@

test:
	@mkdir -p build
	@$(CC) test.cc -o build/test $(LIBS)
	env HEAPCHECK=normal ./test

install:

clean:
	@rm -rf build

