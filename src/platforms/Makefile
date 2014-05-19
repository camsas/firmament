ROOT_DIR=../..
SUFFIX=platforms

include $(ROOT_DIR)/include/Makefile.config

PBS = common.pb.o
# TODO(malte): This is a temporary hack to force building of the unix platform
# in all cases. In the long run, we would like to be able to specify the
# platform, or auto-detect it.
LIB = libfirmament_unix.a
OBJ_LIB = $(addprefix $(OBJ_DIR)/unix/, $(LIB))

include $(ROOT_DIR)/include/Makefile.common

#all: protobufs $(OBJ_LIB) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

# TODO(malte): Hack; see note above.
#$(OBJ_LIB):
all: protobufs .setup
	$(MAKE) $(MAKEFLAGS) -C unix all
