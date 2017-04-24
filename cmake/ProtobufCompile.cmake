set(PROTOC ${protobuf3_BINARY_DIR}/protoc)

function(PROTOBUF_COMPILE BASE_NAME PROTO_PATH GENERATE_GRPC)
  # Arguments for protoc: include and output directories
  if(${GENERATE_GRPC})
    set(PROTOC_ARGS
      -I${PROJECT_SOURCE_DIR}
      --cpp_out=${PROJECT_BINARY_DIR}/src
      --grpc_out=${PROJECT_BINARY_DIR}/src
      --plugin=protoc-gen-grpc=${grpc_SOURCE_DIR}/grpc_cpp_plugin)
  else(${GENERATE_GRPC})
    set(PROTOC_ARGS
      -I${PROJECT_SOURCE_DIR}
      --cpp_out=${PROJECT_BINARY_DIR}/src)
  endif(${GENERATE_GRPC})

  # Names of variables to export
  set(PROTO_VAR    ${BASE_NAME}_PROTO)
  set(SRC_VAR      ${BASE_NAME}_PROTO_CC)
  set(HDR_VAR      ${BASE_NAME}_PROTO_H)

  # Fully qualified paths for the input .proto files and the output C files.
  set(PROTO   ${PROJECT_ROOT_DIR}/${PROTO_PATH}.proto)

  if(${GENERATE_GRPC})
    set(CC ${PROJECT_BINARY_DIR}/${PROTO_PATH}.pb.cc
      ${PROJECT_BINARY_DIR}/${PROTO_PATH}.grpc.pb.cc)
    set(H ${PROJECT_BINARY_DIR}/${PROTO_PATH}.grpc.pb.h
      ${PROJECT_BINARY_DIR}/${PROTO_PATH}.pb.h)
  else(${GENERATE_GRPC})
    set(CC ${PROJECT_BINARY_DIR}/${PROTO_PATH}.pb.cc)
    set(H  ${PROJECT_BINARY_DIR}/${PROTO_PATH}.pb.h)
  endif(${GENERATE_GRPC})

  # Export variables holding the target filenames.
  set(${PROTO_VAR} ${PROTO} PARENT_SCOPE)
  set(${SRC_VAR}   ${CC}    PARENT_SCOPE)
  set(${HDR_VAR}   ${H}     PARENT_SCOPE)

  # Compile the .proto file.
  add_custom_command(
    OUTPUT ${CC} ${H}
    COMMAND ${PROTOC} ${PROTOC_ARGS} ${PROTO}
    #DEPENDS ${PROJECT_BINARY_DIR}/${PROTO_PATH}
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
endfunction()

function(PROTOBUF_LIST_COMPILE PBLIST_NAME PB_LIST GENERATE_GRPC)
  set(PB_SRC_LIST_NAME_VAR ${PBLIST_NAME}_PROTOBUF_SRCS)
  set(PB_HDR_LIST_NAME_VAR ${PBLIST_NAME}_PROTOBUF_HDRS)

  foreach(PB IN ITEMS ${PB_LIST})
    get_filename_component(PB_NAME ${PB} NAME_WE)
    get_filename_component(PB_DIR ${PB} DIRECTORY)
    PROTOBUF_COMPILE(${PB_NAME} src/${PB_DIR}/${PB_NAME} ${GENERATE_GRPC})
    set(SRC_LIST ${SRC_LIST} ${${PB_NAME}_PROTO_CC})
    set(HDR_LIST ${HDR_LIST} ${${PB_NAME}_PROTO_H})
  endforeach(PB)

  set(${PB_SRC_LIST_NAME_VAR} ${SRC_LIST})
  set(${PB_HDR_LIST_NAME_VAR} ${HDR_LIST})

  set(${PB_SRC_LIST_NAME_VAR} ${${PB_SRC_LIST_NAME_VAR}} PARENT_SCOPE)
  set(${PB_HDR_LIST_NAME_VAR} ${${PB_HDR_LIST_NAME_VAR}} PARENT_SCOPE)
endfunction()
