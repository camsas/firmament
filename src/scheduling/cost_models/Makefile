ROOT_DIR=../../..
SUFFIX=scheduling/cost_models

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lboost_system

LIB = libfirmament_cost_models.a

OBJS = coco_cost_model.o quincy_cost_model.o sjf_cost_model.o \
       trivial_cost_model.o random_cost_model.o \
       wharemap_cost_model.o void_cost_model.o \
       octopus_cost_model.o simulated_dfs.o simulated_quincy_cost_model.o \
       google_block_distribution.o google_runtime_distribution.o

PBS =

TESTS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))

include $(ROOT_DIR)/include/Makefile.common

all: protobufs $(OBJ_LIB) $(TESTS_BIN) .setup

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
