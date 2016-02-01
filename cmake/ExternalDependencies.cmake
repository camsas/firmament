# Add gRPC dependency
#ExternalProject_Add(
#    grpc
#    GIT_REPOSITORY https://github.com/grpc/grpc.git
#    GIT_TAG release-0_11
#    TIMEOUT 10
#    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/grpc
#    # no configure required
#    CONFIGURE_COMMAND ""
#    BUILD_COMMAND "make"
#    BUILD_IN_SOURCE 1
#    INSTALL_COMMAND make install prefix=${CMAKE_CURRENT_BINARY_DIR}/third_party/grpc
#    # Wrap download, configure and build steps in a script to log output
#    LOG_DOWNLOAD ON
#    LOG_BUILD ON
#    LOG_INSTALL ON)

if (${SHUTTLE_ENABLE_HDFS})
  # libHDFS requires libxml2
  find_package(LibXml2)
endif (${SHUTTLE_ENABLE_HDFS})
