BINS = hello_world \
       r2d2_trace_process \
       timespin

OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

all:
	@$(foreach BIN,$(BINS),$(MAKE) -C $(BIN);)
