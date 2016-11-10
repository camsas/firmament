set(JOB_SUBMIT_PROTOBUFS
  ${Firmament_SOURCE_DIR}/base/job_desc.proto
  ${Firmament_SOURCE_DIR}/base/task_desc.proto
  ${Firmament_SOURCE_DIR}/base/reference_desc.proto
  ${Firmament_SOURCE_DIR}/base/resource_vector.proto
  ${Firmament_SOURCE_DIR}/base/task_final_report.proto
  )

add_custom_target(scripts_job
  COMMAND ${PROTOC} -I${Firmament_SOURCE_DIR}
    --python_out=${Firmament_ROOT_DIR}/scripts/job
    ${JOB_SUBMIT_PROTOBUFS} &&
    touch ${Firmament_ROOT_DIR}/scripts/job/base/__init__.py
  COMMENT "Generating job submission script protobufs...")

add_dependencies(scripts_job protobuf3)