# makefile for cs2
# compileler definitions
# COMP_DUALS to compute prices
# PRINT_ANS to print flow (and prices if COMP_DUAL defined)
# COST_RESTART to be able to restart after a cost function change
# NO_ZERO_CYCLES finds an opeimal flow with no zero-cost cycles
# CHECK_SOLUTION check feasibility/optimality. HIGH OVERHEAD!

# change these to suit your system
CCOMP = g++
#CCOMP = gcc-4
CFLAGS = -O4 -DNDEBUG -DPRINT_ANS
#CFLAGS = -g -DCHECK_SOLUTION -Wall
#CFLAGS = -g -Wall
#CFLAGS = -O4 -DNDEBUG -DNO_ZERO_CYCLES

cs2.exe: cs2.c parser_cs2.c types_cs2.h timer.c
	$(CCOMP) $(CFLAGS) -o cs2.exe cs2.c -lm
#	$(CCOMP) $(CFLAGS) -DPRINT_ANS -DCOMP_DUALS -DCOST_RESTART -o cs2 cs2.c -lm

clean:
	rm -f cs2.exe *~
