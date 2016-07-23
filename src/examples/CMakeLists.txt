file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/src/examples)

set(EXAMPLES_HELLO_WORLD_SRC
  examples/hello_world/hello_world.cc
  )

set(EXAMPLES_TIMESPIN_SRC
  examples/timespin/timespin.cc
  )

file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/src/examples/hello_world)
add_executable(examples/hello_world/hello_world ${EXAMPLES_HELLO_WORLD_SRC}
  ${TASK_LIB_SRC}
  $<TARGET_OBJECTS:base>
  $<TARGET_OBJECTS:messages>
  $<TARGET_OBJECTS:misc>
  $<TARGET_OBJECTS:platforms_unix>
  )

add_dependencies(examples/hello_world/hello_world gtest spooky-hash
  thread-safe-stl-containers)

target_link_libraries(examples/hello_world/hello_world
  LINK_PUBLIC ${spooky-hash_BINARY} ${Firmament_SHARED_LIBRARIES} glog gflags
  protobuf)

file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/src/examples/timespin)
add_executable(examples/timespin/timespin ${EXAMPLES_TIMESPIN_SRC}
  ${TASK_LIB_SRC}
  $<TARGET_OBJECTS:base>
  $<TARGET_OBJECTS:messages>
  $<TARGET_OBJECTS:misc>
  $<TARGET_OBJECTS:platforms_unix>
  )

add_dependencies(examples/timespin/timespin gtest spooky-hash
  thread-safe-stl-containers)

target_link_libraries(examples/timespin/timespin
  LINK_PUBLIC ${spooky-hash_BINARY} ${Firmament_SHARED_LIBRARIES} glog gflags
  protobuf)
