.PHONY: all
.DEFAULT: all

all:
	mkdir build
	cmake -Bbuild -H.
	@echo
	@echo "-----------------------------------------------"
	@echo "Firmament build system generated, run:"
	@echo " $$ cd build"
	@echo " $$ make"
	@echo "-----------------------------------------------"

doc:
	doxygen Doxyfile
