// The Firmament project
// Copyright (c) The Firmament Authors.
//
// Extracts packet latency from R2D2 DAG packet traces.

#ifndef FIRMAMENT_EXAMPLE_R2D2_SIMPLE_TRACE_ANALYSIS_TASK_H
#define FIRMAMENT_EXAMPLE_R2D2_SIMPLE_TRACE_ANALYSIS_TASK_H

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
class SimpleTraceAnalysisTask : public TaskInterface {
#else
class SimpleTraceAnalysisTask {
#endif
 public:
  void DumpPacketInformation(sample_t* packet);
#ifdef __FIRMAMENT__
  explicit SimpleTraceAnalysisTask(TaskID_t task_id)
    : TaskInterface(task_id),
      prev_packet_timestamp_(0L) {}
#else
  SimpleTraceAnalysisTask()
    : prev_packet_timestamp_(0L) {}
#endif
  void Invoke(void* dag0_shmem_ptr, uint64_t offset, uint64_t count);
 private:
  uint64_t prev_packet_timestamp_;
};

}  // namespace r2d2
}  // namespace examples
}  // namespace firmament

#endif  // FIRMAMENT_EXAMPLE_R2D2_SIMPLE_TRACE_ANALYSIS_TASK_H
