# The initial SOURCE_DIR is the root of the source tree, where the main
# CMakeLists.txt file lives
set(Firmament_ROOT_DIR ${Firmament_SOURCE_DIR})
set(PROJECT_ROOT_DIR ${Firmament_ROOT_DIR})

# CMake modules
set(CMAKE_MODULE_PATH ${Firmament_ROOT_DIR}/cmake)

# Reset the source root
set(Firmament_SOURCE_DIR ${Firmament_ROOT_DIR}/src)
set(PROJECT_SOURCE_DIR ${Firmament_SOURCE_DIR})

# Convenience variable for the build root
set(Firmament_BUILD_DIR ${Firmament_BINARY_DIR})
set(PROJECT_BINARY_DIR ${Firmament_BINARY_DIR})
