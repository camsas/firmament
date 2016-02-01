include_directories(${Firmament_SOURCE_DIR}/src)
include_directories(${Firmament_BINARY_DIR})
include_directories(${Firmament_SOURCE_DIR}/ext/thread-safe-stl-containers-svn)
include_directories(${Firmament_SOURCE_DIR}/ext/spooky_hash)
include_directories(${Firmament_SOURCE_DIR}/ext/pb2json-git)

set(SPOOKY_OBJ ${Firmament_SOURCE_DIR}/ext/spooky_hash/SpookyV2.o)
set(PB2JSON_OBJ ${Firmament_SOURCE_DIR}/ext/pb2json-git/pb2json.o)

# Shared compiler flags used by all builds
set(CMAKE_CXX_FLAGS "-O3 -std=c++11")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-long-long -Wno-variadic-macros -Wno-deprecated -Wno-vla -Wno-unused-parameter -Wno-error=unused-parameter -Wno-error=unused-function")

# Compiler-specific flags
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # using clang
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=language-extension-token")
else()
  # other compilers, usually g++
endif()


