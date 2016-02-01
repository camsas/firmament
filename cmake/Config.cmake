include_directories(${Firmament_SOURCE_DIR})
include_directories(${Firmament_BUILD_DIR}/src)

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


