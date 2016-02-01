file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/storage)

set(STORAGE_SRC
  storage/simple_object_store.cc
  storage/stub_object_store.cc
  )

set(STORAGE_TESTS
  storage/stub_object_store_test.cc
)

###############################################################################
# Unit tests

foreach(T IN ITEMS ${STORAGE_TESTS})
  get_filename_component(TEST_NAME ${T} NAME_WE)
  add_executable(${TEST_NAME} ${T}
    $<TARGET_OBJECTS:base>
    $<TARGET_OBJECTS:misc>
    $<TARGET_OBJECTS:storage>)
  target_link_libraries(${TEST_NAME}
    ${spooky-hash_BINARY} ${gtest_LIBRARY} ${gtest_MAIN_LIBRARY}
    ${Firmament_SHARED_LIBRARIES} glog gflags protobuf)
  add_test(${TEST_NAME} ${TEST_NAME})
endforeach(T)
