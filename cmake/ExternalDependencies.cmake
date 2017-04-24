###############################################################################
# pkg-config (which we rely on for detecting some other libraries)
find_package(PkgConfig REQUIRED)

###############################################################################
# Boost
find_package(Boost REQUIRED COMPONENTS chrono date_time filesystem regex
  system thread timer)

###############################################################################
# cs2 solver
if (${ENABLE_CS2})
  ExternalProject_Add(
      cs2
      GIT_REPOSITORY https://github.com/ICGog/cs2.git
      TIMEOUT 10
      PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/cs2
      # XXX/malte): workaround for CMake issue forcing a spurious git stash
      UPDATE_COMMAND ""
      # no configure or install step required
      CONFIGURE_COMMAND ""
      INSTALL_COMMAND ""
      BUILD_IN_SOURCE ON
      BUILD_COMMAND make)
endif (${ENABLE_CS2})

###############################################################################
# Flowlessly solver
if (${ENABLE_PRIVATE_FLOWLESSLY})
  ExternalProject_Add(
      flowlessly
      GIT_REPOSITORY git@github.com:ICGog/FlowlesslyPrivate.git
      TIMEOUT 10
      PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/flowlessly
      # no install required, we link the library from the build tree
      INSTALL_COMMAND ""
      # Wrap download, configure and build steps in a script to log output
      LOG_DOWNLOAD ON
      LOG_BUILD ON)
else (${ENABLE_PRIVATE_FLOWLESSLY})
  ExternalProject_Add(
      flowlessly
      GIT_REPOSITORY https://github.com/ICGog/Flowlessly.git
      TIMEOUT 10
      PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/flowlessly
      # no install required, we link the library from the build tree
      INSTALL_COMMAND ""
      # Wrap download, configure and build steps in a script to log output
      LOG_DOWNLOAD ON
      LOG_BUILD ON)
endif (${ENABLE_PRIVATE_FLOWLESSLY})

###############################################################################
# cpplint
ExternalProject_Add(
    cpplint
    DOWNLOAD_COMMAND bash -c "wget -O cpplint/cpplint.py 'http://raw.githubusercontent.com/google/styleguide/gh-pages/cpplint/cpplint.py'"
    TIMEOUT 10
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/cpplint
    # no configure, build or install steps
    CONFIGURE_COMMAND /bin/chmod +x ${CMAKE_CURRENT_BINARY_DIR}/third_party/cpplint/src/cpplint/cpplint.py
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON)

ExternalProject_Get_Property(cpplint SOURCE_DIR)
LIST(APPEND CMAKE_PROGRAM_PATH "${SOURCE_DIR}")
set(cpplint_SOURCE_DIR ${SOURCE_DIR})

###############################################################################
# Google Flags
find_package(GFlags REQUIRED)

###############################################################################
# Google Log
find_package(GLog REQUIRED)

###############################################################################
# Google Test
ExternalProject_Add(
    gtest
    GIT_REPOSITORY https://github.com/google/googletest.git
    TIMEOUT 10
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/gtest
    # no install required, we link the library from the build tree
    INSTALL_COMMAND ""
    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_BUILD ON)

ExternalProject_Get_Property(gtest BINARY_DIR)
ExternalProject_Get_Property(gtest SOURCE_DIR)
set(gtest_BINARY_DIR ${BINARY_DIR})
set(gtest_SOURCE_DIR ${SOURCE_DIR})
set(gtest_INCLUDE_DIR ${gtest_SOURCE_DIR}/googletest/include)
include_directories(${gtest_INCLUDE_DIR})
set(gtest_LIBRARY ${gtest_BINARY_DIR}/googlemock/gtest/libgtest.a)
set(gtest_MAIN_LIBRARY ${gtest_BINARY_DIR}/googlemock/gtest/libgtest_main.a)

set(gmock_INCLUDE_DIR ${gtest_SOURCE_DIR}/googlemock/include)
include_directories(${gmock_INCLUDE_DIR})
set(gmock_LIBRARY ${gtest_BINARY_DIR}/googlemock/libgmock.a)
set(gmock_MAIN_LIBRARY ${gtest_BINARY_DIR}/googlemock/libgmock_main.a)

###############################################################################
# hwloc
find_package(Hwloc REQUIRED)

###############################################################################
# libctemplate
find_package(Ctemplate REQUIRED)

###############################################################################
# libhdfs3
if (${ENABLE_HDFS})
  # libHDFS requires libxml2
  find_package(LibXml2)
  ExternalProject_Add(
      libhdfs3
      GIT_REPOSITORY https://github.com/PivotalRD/libhdfs3.git
      GIT_TAG v2.2.31
      TIMEOUT 10
      PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/libhdfs3
      # no install required, we link the library from the build tree
      INSTALL_COMMAND ""
      # Wrap download, configure and build steps in a script to log output
      LOG_DOWNLOAD ON
      LOG_BUILD ON)
  ExternalProject_Get_Property(libhdfs3 BINARY_DIR)
  ExternalProject_Get_Property(libhdfs3 SOURCE_DIR)
  set(libhdfs3_BINARY_DIR ${BINARY_DIR})
  set(libhdfs3_SOURCE_DIR ${SOURCE_DIR})
  set(libhdfs3_INCLUDE_DIR ${libhdfs3_SOURCE_DIR}/src/client)
  include_directories(${libhdfs3_INCLUDE_DIR})
  set(libhdfs3_LIBRARY ${libhdfs3_BINARY_DIR}/src/libhdfs3.so)
endif (${ENABLE_HDFS})

###############################################################################
# OpenSSL
find_package(OpenSSL REQUIRED)

###############################################################################
# protobuf3
ExternalProject_Add(
    protobuf3
    GIT_REPOSITORY https://github.com/google/protobuf
    GIT_TAG v3.1.0
    TIMEOUT 10
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf3
    CONFIGURE_COMMAND "${CMAKE_COMMAND}"
                      "-H${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf3/src/protobuf3/cmake"
                      "-B${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf3/src/protobuf3-build"
                      "-Dprotobuf_BUILD_TESTS=off" "-DCMAKE_CXX_FLAGS=\"-fPIC\""
    # no install required, we link the library from the build tree
    INSTALL_COMMAND "")

ExternalProject_Get_Property(protobuf3 SOURCE_DIR)
ExternalProject_Get_Property(protobuf3 BINARY_DIR)
set(protobuf3_SOURCE_DIR ${SOURCE_DIR})
set(protobuf3_BINARY_DIR ${BINARY_DIR})
set(protobuf3_INCLUDE_DIR ${protobuf3_SOURCE_DIR}/src)
include_directories(${protobuf3_INCLUDE_DIR})
set(protobuf3_LIBRARY ${protobuf3_BINARY_DIR}/libprotobuf.a)


###############################################################################
# Pion integrated web server
ExternalProject_Add(
    pion
    GIT_REPOSITORY https://github.com/splunk/pion.git
    GIT_TAG develop
    TIMEOUT 10
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/pion
    BUILD_IN_SOURCE OFF
    CMAKE_ARGS -DBUILD_PIOND=off -DBUILD_HELLOSERVER=off -DBUILD_ALLOWNOTHINGSERVICE=off -DBUILD_COOKIESERVICE=off -DBUILD_ECHOSERVICE=off -DBUILD_FILESERVICE=off -DBUILD_HELLOSERVICE=off -DBUILD_LOGSERVICE=off -DBUILD_SHARED_LIBS=off
    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_BUILD ON
    LOG_INSTALL ON)

ExternalProject_Get_Property(pion SOURCE_DIR)
set(pion_SOURCE_DIR ${SOURCE_DIR})
set(pion_INSTALL_DIR ${pion_SOURCE_DIR}/bin)
set(pion_INCLUDE_DIR ${pion_INSTALL_DIR}/include)
include_directories(${pion_INCLUDE_DIR})
set(pion_LIBRARY ${pion_INSTALL_DIR}/lib/libpion.a)

###############################################################################
# Spooky hash
ExternalProject_Add(
    spooky-hash
    DOWNLOAD_COMMAND bash -c "wget -O spooky-hash/SpookyV2.cpp 'http://burtleburtle.net/bob/c/SpookyV2.cpp' && wget -O spooky-hash/SpookyV2.h 'http://burtleburtle.net/bob/c/SpookyV2.h'"
    TIMEOUT 10
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/spooky-hash
    BUILD_IN_SOURCE ON
    # no configure step
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ${CMAKE_CXX_COMPILER} -O3 -fPIC -c SpookyV2.cpp
    # no install step
    INSTALL_COMMAND ""
    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_BUILD ON)

ExternalProject_Get_Property(spooky-hash SOURCE_DIR)
set(spooky-hash_SOURCE_DIR ${SOURCE_DIR})
set(spooky-hash_INCLUDE_DIR ${spooky-hash_SOURCE_DIR})
include_directories(${spooky-hash_INCLUDE_DIR})
set(spooky-hash_BINARY ${spooky-hash_SOURCE_DIR}/SpookyV2.o)

###############################################################################
# Hacky thread-safe STL containers
ExternalProject_Add(
    thread-safe-stl-containers
    GIT_REPOSITORY https://github.com/ms705/thread-safe-stl-containers.git
    TIMEOUT 10
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/thread-safe-stl-containers
    # Headers only, so no need to do anything
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON)

ExternalProject_Get_Property(thread-safe-stl-containers SOURCE_DIR)
set(thread-safe-stl-containers_INCLUDE_DIR ${SOURCE_DIR})
include_directories(${thread-safe-stl-containers_INCLUDE_DIR})

###############################################################################
# Zlib
find_package(ZLIB REQUIRED)

###############################################################################
# grpc
ExternalProject_Add(
    grpc
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG v1.2.0
    TIMEOUT 10
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third_party/grpc
    DEPENDS protobuf3
    BUILD_IN_SOURCE ON
    INSTALL_COMMAND ""
    CMAKE_CACHE_ARGS
        -DCMAKE_BUILD_TYPE:STRING=Release
        -DCMAKE_VERBOSE_MAKEFILE:BOOL=OFF
        -DPROTOBUF_INCLUDE_DIRS:STRING=${PROTOBUF3_INCLUDE_DIR}
        -DPROTOBUF_LIBRARIES:STRING=${protobuf3_LIBRARY}
        -DZLIB_ROOT:STRING=${ZLIB_INSTALL}
    # Wrap download, configure and build steps in a script to log output
    LOG_DOWNLOAD ON
    LOG_BUILD ON
    LOG_INSTALL ON)

ExternalProject_Get_Property(grpc SOURCE_DIR)
ExternalProject_Get_Property(grpc BINARY_DIR)
set(grpc_SOURCE_DIR ${SOURCE_DIR})
set(grpc_BINARY_DIR ${BINARY_DIR})
set(grpc_INCLUDE_DIR ${grpc_SOURCE_DIR}/include)
include_directories(${grpc_INCLUDE_DIR})
set(grpc_LIBRARY
  ${grpc_SOURCE_DIR}/libgrpc++_unsecure.a
  ${grpc_SOURCE_DIR}/libgrpc_unsecure.a
  ${grpc_SOURCE_DIR}/libgpr.a)
message(${grpc_LIBRARY})