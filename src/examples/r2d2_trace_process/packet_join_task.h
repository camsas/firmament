// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Extracts packet latency from R2D2 DAG packet traces.

#ifndef FIRMAMENT_EXAMPLE_R2D2_LATENCY_EXTRACTION_TASK_H
#define FIRMAMENT_EXAMPLE_R2D2_LATENCY_EXTRACTION_TASK_H

#include <string>

#ifdef __FIRMAMENT__
#include "base/task_interface.h"
#else
#include "base/common.h"
#endif
extern "C" {
#include "examples/r2d2_trace_process/common.h"
}

namespace firmament {
namespace examples {
namespace r2d2 {

void print_header_info(const string& dag_label, dag_capture_header_t* header);

#ifdef __FIRMAMENT__
class PacketJoinTask : public TaskInterface {
#else
class PacketJoinTask {
#endif
 public:
#ifdef __FIRMAMENT__
  explicit PacketJoinTask(TaskLib* task_lib, TaskID_t task_id)
    : TaskInterface(task_lib, task_id) {}
#else
  PacketJoinTask() {}
#endif
  void Invoke(void* dag0_shmem_ptr, char* dag1_filename, uint64_t offset,
              uint64_t count);
  sample_t* MatchPacketWithinWindow(void* dag0_ptr, uint64_t timestamp,
                                    sample_t* packet, uint64_t* start_idx,
                                    dag_match_result_t* result);
  uint64_t StartIndexGuess(void* dag0_ptr, dag_capture_header_t* dag1_header,
                           uint64_t timestamp);
};

}  // namespace hello_world
}  // namespace examples
}  // namespace firmament

#endif  // FIRMAMENT_EXAMPLE_R2D2_LATENCY_EXTRACTION_TASK_H
