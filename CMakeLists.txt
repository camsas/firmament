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

# Linting targets
include(LintTargets)

# Unit tests
enable_testing()
set(GTEST_ROOT ${gtest_BINARY_DIR})

# The "src" subdir has all source code
subdirs(src)
