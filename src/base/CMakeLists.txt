file(MAKE_DIRECTORY ${Firmament_BINARY_DIR}/src/base)

set(BASE_SRC
  base/data_object.cc
  base/resource_status.cc
  )

set(BASE_PROTOBUFS
  base/coco_interference_scores.proto
  base/data_object_name.proto
  base/job_desc.proto
  base/machine_perf_statistics_sample.proto
  base/reference_desc.proto
  base/resource_desc.proto
  base/resource_topology_node_desc.proto
  base/resource_vector.proto
  base/task_desc.proto
  base/task_final_report.proto
  base/task_instance_profile.proto
  base/task_perf_statistics_sample.proto
  base/whare_map_stats.proto
  )

set(BASE_TESTS
  base/data_object_test.cc
)

###############################################################################
# Protocol buffers

PROTOBUF_LIST_COMPILE(BASE "${BASE_PROTOBUFS}")

###############################################################################
# Unit tests

if (BUILD_TESTS)
  foreach(T IN ITEMS ${BASE_TESTS})
    get_filename_component(TEST_NAME ${T} NAME_WE)
    add_executable(${TEST_NAME} ${T}
      $<TARGET_OBJECTS:base>)
    target_link_libraries(${TEST_NAME}
      ${spooky-hash_BINARY} ${gtest_LIBRARY} ${gtest_MAIN_LIBRARY}
      ${Firmament_SHARED_LIBRARIES} glog gflags protobuf)
    add_test(${TEST_NAME} ${TEST_NAME})
  endforeach(T)
endif (BUILD_TESTS)
