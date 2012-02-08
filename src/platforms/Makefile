ROOT_DIR=../..
SUFFIX=platforms

include $(ROOT_DIR)/include/Makefile.config
include $(ROOT_DIR)/include/Makefile.common

all: setup protobufs libfirmament_unix.a

protobufs: common.pb.o

libfirmament_unix.a:
	make -C unix all
