################################################################################
project(Firmament)
################################################################################

cmake_minimum_required(VERSION 2.8)

# Path variable configuration
include(cmake/Paths.cmake)

# CMake helpers
include(CheckLibraryExists)
include(CheckFunctionExists)
include(ExternalProject)
include(CheckCXXSourceCompiles)

# Compiler configuration
include(Config)
# Third-party dependencies and libraries
include(ExternalDependencies)
# Protocol buffer code generation
include(ProtobufCompile)
# Build options
include(Options)


# The "src" subdir has all source code
subdirs(src)
