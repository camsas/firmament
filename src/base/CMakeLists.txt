file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/base)

set(BASE_SRC
  base/data_object.cc
  base/resource_status.cc
  base/task_graph.cc
  )

set(BASE_PROTOBUFS
  coco_interference_scores
  data_object_name
  job_desc
  machine_perf_statistics_sample
  reference_desc
  resource_desc
  resource_topology_node_desc
  resource_vector
  task_desc
  task_final_report
  task_instance_profile
  task_perf_statistics_sample
  whare_map_stats
  )

set(BASE_TESTS
  base/data_object_test.cc
  base/references_test.cc
  base/task_graph_test.cc
)

foreach(PB IN ITEMS ${BASE_PROTOBUFS})
  PROTOBUF_COMPILE(${PB} base/${PB})
  set(BASE_PROTOBUF_SRCS ${BASE_PROTOBUF_SRCS} ${${PB}_PROTO_CC})
endforeach(PB)

#PROTOBUF_LIST_COMPILE(BASE ${BASE_PROTOBUFS})

#PROTOBUF_GENERATE_CPP(BASE_PROTOBUF_SRCS BASE_PROTOBUF_HDRS ${BASE_PROTOBUFS})

#add_library(firmament_base ${BASE_SRC} ${BASE_PROTOBUFS_SRCS} ${BASE_PROTOBUF_HDRS})
