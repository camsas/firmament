BINS = hello_world \
       fibonacci \
       r2d2_trace_process \
       timespin \
       store_example

OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

all:
	@$(foreach BIN,$(BINS),$(MAKE) $(MAKEFLAGS) -C $(BIN);)
