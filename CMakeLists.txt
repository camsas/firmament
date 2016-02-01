project(Firmament)

cmake_minimum_required(VERSION 2.8)

set(CMAKE_MODULE_PATH ${Firmament_SOURCE_DIR}/cmake)

include(CheckLibraryExists)
include(CheckFunctionExists)
include(ExternalProject)
include(CheckCXXSourceCompiles)

include(Config)
include(ExternalDependencies)
include(ProtobufCompile)
include(Options)

include(FindBoost)
include(FindProtobuf)

find_package(Boost COMPONENTS chrono date_time filesystem system thread)

# Set subdirectories
subdirs(src)
