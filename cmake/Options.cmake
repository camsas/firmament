# Build options
option(DEBUG "Set build config to debug." OFF)
option(VERBOSE "Enable verbose compilation output" OFF)

# Modular components
option(BUILD_SIMULATOR "Builds the trace-driven simulation front-end for Firmament." ON)
option(BUILD_EXAMPLES "Builds the example binaries linked with TaskLib." ON)
option(BUILD_SHUTTLE "Builds the Shuttle cluster manager front-end." ON)
option(BUILD_TASKLIB "Builds the TaskLib shared library for use with Shuttle." ON)

# Shuttle options
option(SHUTTLE_ENABLE_HDFS "Support HDFS-based locality information in Shuttle." ON)

set(CMAKE_VERBOSE_MAKEFILE ${VERBOSE})
