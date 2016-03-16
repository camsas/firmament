# Build options
option(DEBUG "Set build config to debug." OFF)
option(VERBOSE "Enable verbose compilation output" OFF)

# Modular components
option(BUILD_SIMULATOR "Builds the trace-driven simulation front-end for Firmament." ON)
option(BUILD_EXAMPLES "Builds the example binaries linked with TaskLib." ON)
option(BUILD_SHUTTLE "Builds the Shuttle cluster manager front-end." ON)
option(BUILD_TASKLIB "Builds the TaskLib shared library for use with Shuttle." ON)

# Solver options
option(ENABLE_CS2 "Downloads, compiles and enables the cs2 solver (academic license only)." ON)
option(ENABLE_FLOWLESSLY "Downloads, compiles and enables the Flowlessly solver." OFF)

# Coordinator options
option(ENABLE_HDFS "Support HDFS-based locality information in Coordinator." ON)

# The VERBOSE flag controls build system verbosity
set(CMAKE_VERBOSE_MAKEFILE ${VERBOSE})
