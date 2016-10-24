// The Firmament project
// Copyright (c) The Firmament Authors.
//
// Joins two R2D2 DAG capture files, matching packets in the second one (DAG1)
// to packets in the first one (DAG0). Writes out the timestamp when the packet
// was received, the latency it experienced, and the time since the last packet
// arrived.


#ifndef FIRMAMENT_EXAMPLE_R2D2_PACKET_JOIN_TASK_H
#define FIRMAMENT_EXAMPLE_R2D2_PACKET_JOIN_TASK_H

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
  explicit PacketJoinTask(TaskID_t task_id)
    : TaskInterface(task_id),
      total_backward_steps_(0),
      total_forward_steps_(0) {}
#else
  PacketJoinTask()
    : total_backward_steps_(0),
      total_forward_steps_(0) {}
#endif
  void Invoke(void* dag0_shmem_ptr, char* dag1_filename, uint64_t offset,
              uint64_t count);
  sample_t* MatchPacketWithinWindow(void* dag0_ptr, uint64_t timestamp,
                                    sample_t* packet, uint64_t* start_idx,
                                    dag_match_result_t* result,
                                    uint64_t* bwd_steps, uint64_t* fwd_steps);
  uint64_t StartIndexGuess(void* dag0_ptr, dag_capture_header_t* dag1_header,
                           uint64_t timestamp);

 private:
  uint64_t total_backward_steps_;
  uint64_t total_forward_steps_;
};

}  // namespace r2d2
}  // namespace examples
}  // namespace firmament

#endif  // FIRMAMENT_EXAMPLE_R2D2_PACKET_JOIN_TASK_H
