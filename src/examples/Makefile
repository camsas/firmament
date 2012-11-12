BINS = hello_world \
       fibonacci \
       timespin

OBJ_BIN = $(addprefix $(OBJ_DIR)/, $(BINS))

all:
	@$(foreach BIN,$(BINS),$(MAKE) $(MAKEFLAGS) -C $(BIN);)
