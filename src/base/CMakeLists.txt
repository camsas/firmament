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
  base/references_test.cc
  base/task_graph_test.cc
)

add_library(BASE_OBJS OBJECT ${BASE_SRC} ${BASE_PROTOBUFS_SRCS})

###############################################################################
# Protocol buffers

PROTOBUF_LIST_COMPILE(BASE "${BASE_PROTOBUFS}")

#add_library(firmament_base ${BASE_SRC} ${BASE_PROTOBUFS_SRCS} ${BASE_PROTOBUF_HDRS})
