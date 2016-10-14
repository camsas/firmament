file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/storage)

set(STORAGE_SRC
  storage/simple_object_store.cc
  storage/stub_object_store.cc
  )

if (${ENABLE_HDFS})
  set(STORAGE_SRC
    ${STORAGE_SRC}
    storage/hdfs_data_locality_manager.cc
    )
endif (${ENABLE_HDFS})

set(STORAGE_TESTS
  storage/references_test.cc
  storage/stub_object_store_test.cc
)

###############################################################################
# Unit tests

if (BUILD_TESTS)
  foreach(T IN ITEMS ${STORAGE_TESTS})
    get_filename_component(TEST_NAME ${T} NAME_WE)
    add_executable(${TEST_NAME} ${T}
      $<TARGET_OBJECTS:base>
      $<TARGET_OBJECTS:engine>
      $<TARGET_OBJECTS:executors>
      $<TARGET_OBJECTS:messages>
      $<TARGET_OBJECTS:misc>
      $<TARGET_OBJECTS:misc_trace_generator>
      $<TARGET_OBJECTS:platforms_unix>
      $<TARGET_OBJECTS:scheduling>
      $<TARGET_OBJECTS:storage>)
    target_link_libraries(${TEST_NAME}
      ${spooky-hash_BINARY} ${gtest_LIBRARY} ${gtest_MAIN_LIBRARY}
      ${libhdfs3_LIBRARY} ${protobuf3_LIBRARY} ${Firmament_SHARED_LIBRARIES}
      ctemplate glog gflags hwloc)
    add_test(${TEST_NAME} ${TEST_NAME})
  endforeach(T)
endif (BUILD_TESTS)
