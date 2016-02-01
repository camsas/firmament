
# XXX(malte): remove this
add_definitions(-D__PLATFORM_HAS_BOOST__)
add_definitions(-D__HTTP_UI__)

#add_subdirectory(base)
include(base/CMakeLists.txt)
include(engine/CMakeLists.txt)
#add_subdirectory(messages)
include(messages/CMakeLists.txt)
include(misc/CMakeLists.txt)
#add_subdirectory(platforms)
include(platforms/CMakeLists.txt)
#add_subdirectory(scheduling)
include(scheduling/CMakeLists.txt)
include(sim/CMakeLists.txt)
include(storage/CMakeLists.txt)

set(SHUTTLE_SRCS
  ${BASE_SRC} ${BASE_PROTOBUF_SRCS}
  ${COORDINATOR_SRC}
  ${MESSAGES_PROTOBUF_SRCS}
  ${MISC_SRC}
  #${MESSAGES_PROTOBUF_HDRS}
  ${PLATFORMS_UNIX_SRC}
  ${SCHEDULING_SRC} ${SCHEDULING_PROTOBUF_SRCS}
  #${SCHEDULING_PROTOBUF_HDRS}
  # XXX(malte): shouln't need to link simulation stuff with coordinator
  ${SIM_DFS_SRC}
  ${STORAGE_SRC}
  )

add_executable(shuttle engine/coordinator_main.cc ${SHUTTLE_SRCS} ${SPOOKY_OBJ} ${PB2JSON_OBJ})
target_link_libraries(shuttle LINK_PUBLIC
  boost_chrono boost_date_time boost_filesystem boost_system boost_thread
  boost_regex boost_timer ctemplate crypto gflags glog hwloc jansson log4cpp
  pion protobuf pthread ssl)
