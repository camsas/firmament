# XXX(malte): should we not hardcode this?set(${PROTOC} protoc)
set(PROTOC protoc)

function(PROTOBUF_COMPILE BASE_NAME PROTO_PATH)
  # Arguments for protoc: include and output directories
  set(PROTOC_ARGS
    -I${PROJECT_SOURCE_DIR}/src
    --cpp_out=${PROJECT_BINARY_DIR})

  # Names of variables to export
  set(PROTO_VAR ${BASE_NAME}_PROTO)
  set(SRC_VAR   ${BASE_NAME}_PROTO_CC)
  set(HDR_VAR   ${BASE_NAME}_PROTO_H)

  # Fully qualified paths for the input .proto files and the output C files.
  set(PROTO ${PROJECT_SOURCE_DIR}/src/${PROTO_PATH}.proto)
  set(CC    ${PROJECT_BINARY_DIR}/${PROTO_PATH}.pb.cc)
  set(H     ${PROJECT_BINARY_DIR}/${PROTO_PATH}.pb.h)

  # Export variables holding the target filenames.
  set(${PROTO_VAR} ${PROTO} PARENT_SCOPE)
  set(${SRC_VAR}   ${CC}    PARENT_SCOPE)
  set(${HDR_VAR}   ${H}     PARENT_SCOPE)

  # Compile the .proto file.
  ADD_CUSTOM_COMMAND(
    OUTPUT ${CC} ${H}
    COMMAND ${PROTOC} ${PROTOC_ARGS} ${PROTO}
    #DEPENDS ${PROJECT_BINARY_DIR}/${PROTO_PATH}
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
endfunction()

function(PROTOBUF_LIST_COMPILE ${PBLIST_NAME} ${PB_LIST})
  message("making ${PBLIST_NAME} protobufs")
  foreach(PB IN ITEMS ${PB_LIST})
    PROTOBUF_COMPILE(${PB} base/${PB})
    set(${PBLIST_NAME}_PROTOBUF_SRCS ${${PBLIST_NAME}_PROTOBUF_SRCS} ${${PB}_PROTO_CC})
  endforeach(PB)
endfunction()
