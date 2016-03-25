file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/misc)

set(MISC_SRC
  misc/pb_utils.cc
  misc/wall_time.cc
  misc/string_utils.cc
  misc/utils.cc
  )

set(MISC_TRACE_GENERATOR_SRC
  misc/trace_generator.cc
  )

set(MISC_TESTS
  misc/envelope_test.cc
  misc/utils_test.cc
)

###############################################################################
# Unit tests

if (BUILD_TESTS)
  foreach(T IN ITEMS ${MISC_TESTS})
    get_filename_component(TEST_NAME ${T} NAME_WE)
    add_executable(${TEST_NAME} ${T}
      $<TARGET_OBJECTS:base>
      $<TARGET_OBJECTS:messages>
      $<TARGET_OBJECTS:misc>)
    target_link_libraries(${TEST_NAME}
      ${spooky-hash_BINARY} ${gtest_LIBRARY} ${gtest_MAIN_LIBRARY}
      ${Firmament_SHARED_LIBRARIES} glog gflags protobuf)
    add_test(${TEST_NAME} ${TEST_NAME})
  endforeach(T)
endif (BUILD_TESTS)
