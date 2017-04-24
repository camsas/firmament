file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/src/messages)

set(MESSAGES_PROTOBUFS
  messages/base_message.proto
  messages/heartbeat_message.proto
  messages/registration_message.proto
  messages/task_delegation_message.proto
  messages/task_heartbeat_message.proto
  messages/task_info_message.proto
  messages/task_kill_message.proto
  messages/task_spawn_message.proto
  messages/task_state_message.proto
  messages/test_message.proto
  )

#add_library(firmament_base ${MESSAGES_SRC} ${MESSAGES_PROTOBUFS_SRCS} ${MESSAGES_PROTOBUF_HDRS})

###############################################################################
# Protocol buffers

PROTOBUF_LIST_COMPILE(MESSAGES "${MESSAGES_PROTOBUFS}" FALSE)
