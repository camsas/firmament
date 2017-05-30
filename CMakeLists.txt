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


# Build options
include(Options)
# Compiler configuration
include(Config)
# Third-party dependencies and libraries
include(ExternalDependencies)
# Protocol buffer code generation
include(ProtobufCompile)

# Linting targets
include(LintTargets)

# Docker target (image build)
include(DockerTargets)

# Doxy documentaion target
include(DoxyTargets)

# Unit tests
if (BUILD_TESTS)
  enable_testing()
  set(GTEST_ROOT ${gtest_BINARY_DIR})
endif (BUILD_TESTS)

# Helper scripts that depend on protobufs
subdirs(scripts/job)
# The "src" subdir has all source code
subdirs(src)
