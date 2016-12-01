file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/platforms)

set(PLATFORMS_SIM_SRC
  platforms/sim/simulated_messaging_adapter.cc
  )

set(PLATFORMS_UNIX_SRC
  platforms/unix/async_tcp_server.cc
  platforms/unix/common.cc
  platforms/unix/procfs_machine.cc
  platforms/unix/procfs_monitor.cc
  platforms/unix/signal_handler.cc
  platforms/unix/stream_sockets_adapter.cc
  platforms/unix/tcp_connection.cc
  )

set(PLATFORMS_UNIX_TESTS
  platforms/unix/procfs_machine_test.cc
  platforms/unix/procfs_monitor_test.cc
)

###############################################################################
# Unit tests

if (BUILD_TESTS)
  foreach(T IN ITEMS ${PLATFORMS_UNIX_TESTS})
    get_filename_component(TEST_NAME ${T} NAME_WE)
    add_executable(${TEST_NAME} ${T}
      $<TARGET_OBJECTS:base>
      $<TARGET_OBJECTS:messages>
      $<TARGET_OBJECTS:misc>
      $<TARGET_OBJECTS:platforms_unix>)
    target_link_libraries(${TEST_NAME}
      ${spooky-hash_BINARY} ${gtest_LIBRARY} ${gtest_MAIN_LIBRARY}
      ${protobuf3_LIBRARY} ${Firmament_SHARED_LIBRARIES} glog gflags)
    add_test(${TEST_NAME} ${TEST_NAME})
  endforeach(T)
endif (BUILD_TESTS)
