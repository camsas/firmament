file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/messages)

set(MESSAGES_PROTOBUFS
  base_message
  copy_message
  create_message
  delete_message
  heartbeat_message
  io_notification_message
  lookup_message
  registration_message
  storage_message
  storage_registration_message
  task_delegation_message
  task_heartbeat_message
  task_info_message
  task_kill_message
  task_spawn_message
  task_state_message
  test_message
  )

foreach(PB IN ITEMS ${MESSAGES_PROTOBUFS})
  PROTOBUF_COMPILE(${PB} messages/${PB})
  set(MESSAGES_PROTOBUF_SRCS ${MESSAGES_PROTOBUF_SRCS} ${${PB}_PROTO_CC})
endforeach(PB)

#PROTOBUF_LIST_COMPILE(MESSAGES ${MESSAGES_PROTOBUFS})

#PROTOBUF_GENERATE_CPP(MESSAGES_PROTOBUF_SRCS MESSAGES_PROTOBUF_HDRS ${MESSAGES_PROTOBUFS})

#add_library(firmament_base ${MESSAGES_SRC} ${MESSAGES_PROTOBUFS_SRCS} ${MESSAGES_PROTOBUF_HDRS})
