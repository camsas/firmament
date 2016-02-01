file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/platforms)

set(PLATFORMS_UNIX_SRC
  #platforms/sim/simulated_messaging_adapter.cc
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
  platforms/unix/stream_sockets_adapter_test.cc
  platforms/unix/stream_sockets_channel_test.cc
)

#add_library(firmament_scheduling ${PLATFORMS_SRC} ${PLATFORMS_PROTOBUFS_SRCS} ${PLATFORMS_PROTOBUF_HDRS})
