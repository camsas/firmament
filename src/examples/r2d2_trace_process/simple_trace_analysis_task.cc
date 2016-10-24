// The Firmament project
// Copyright (c) The Firmament Authors.
//
// Extracts latency values from two R2D2 DAG capture files.

#include "examples/r2d2_trace_process/simple_trace_analysis_task.h"

#include <cstdlib>
#include <vector>
#include <iostream>  // NOLINT

#ifndef __FIRMAMENT__
extern "C" {
#include "examples/r2d2_trace_process/standalone_io.h"
}
#endif
#ifdef __FIRMAMENT__
#include "examples/task_lib_bridge.h"
#endif

#define WINDOW_SIZE 100*1000*1000  // 100ms

#ifndef __FIRMAMENT__
int main(int argc, char* argv[]) {
  // Initiate Google logging
  google::ParseCommandLineFlags(&argc, &argv, true);

  // Set up glog for logging output
  google::InitGoogleLogging(argv[0]);

  // Rudimentary command line parsing
  if (argc != 4)
    LOG(FATAL) << "usage: simple_trace_analysis_task <dag_capture_file> "
               << "<offset> <count>";
  char* dag0_filename = argv[1];
  uint64_t offset = atol(argv[2]);
  uint64_t count = atol(argv[3]);

  // Informative output
  VLOG(1) << "Simple trace analysis task starting.";
  VLOG(1) << "  DAG0 filename: " << dag0_filename;
  VLOG(1) << "  Offset to start at: " << offset;
  VLOG(1) << "  Num packets to consider: " << count;

  // Map dag0 data set into memory
  void* dag0_ptr = load_to_shmem(dag0_filename);

  // Analyze dag0 header
  dag_capture_header_t* head =
      reinterpret_cast<dag_capture_header_t*>(dag0_ptr);
  firmament::examples::r2d2::print_header_info("DAG0", head);

  // Special case: count == 0 (means ALL packets)
  if (count == 0) {
    count = head->samples;
    VLOG(1) << "Total samples: " << head->samples
            << ", considering full length.";
  }

  // Set up task and run
  firmament::examples::r2d2::SimpleTraceAnalysisTask t;
  t.Invoke(dag0_ptr, offset, count);
}
#endif

namespace firmament {

#ifdef __FIRMAMENT__
void task_main(TaskID_t task_id, vector<char*>* args) {
  examples::r2d2::SimpleTraceAnalysisTask t(task_id);
  LOG(INFO) << "Called task_main, starting " << t;
  //t.Invoke();
}
#endif

namespace examples {
namespace r2d2 {

void print_header_info(const string& dag_label, dag_capture_header_t* header) {
  VLOG(1) << "------------------------------";
  VLOG(1) << dag_label << " info:";
  VLOG(1) << "  Start time : " << header->start_time;
  VLOG(1) << "  End time   : " << header->end_time;
  VLOG(1) << "  Runtime    : " << ((header->end_time - header->start_time)
          / 1000 / 1000) << "ms";
  VLOG(1) << "  Samples    : " << header->samples;
  VLOG(1) << "------------------------------";
}

void SimpleTraceAnalysisTask::DumpPacketInformation(sample_t* packet) {
  // Computer inter-arrival time
  uint64_t inter_arrival_time;
  if (!prev_packet_timestamp_)
    inter_arrival_time = 0;
  else
    inter_arrival_time = packet->timestamp - prev_packet_timestamp_;
  uint64_t source = 0;
  if (packet->hash == 0xFEEDCAFEDEADBEEF)
    source = 1;
  // Dump output to stdout
  cout << packet->timestamp << "," << packet->value_type_dropped_len.type
       << "," << packet->value_type_dropped_len.length
       << "," << inter_arrival_time << "," << source << endl;
  // Record this packets arrival time
  prev_packet_timestamp_ = packet->timestamp;
}

void SimpleTraceAnalysisTask::Invoke(void* dag0_ptr, uint64_t offset,
                                     uint64_t count) {
  uint64_t i = 0;
  uint64_t lost_at_dag = 0;
  dag_capture_header_t* head_ptr =
      reinterpret_cast<dag_capture_header_t*>(dag0_ptr);
  sample_t* data_ptr = &(head_ptr->first_sample);
  // If we're not starting from the beginning, we need to snoop the timestamp of
  // the previous packet here.
  if (offset)
    prev_packet_timestamp_ = data_ptr[offset-1].timestamp;
  // Iterate over all packets in our section of the capture file and extract
  // information for each of them.
  for (i = offset; i < (offset + count); ++i) {
    sample_t* packet = &(data_ptr[i]);
    DumpPacketInformation(packet);
  }
  // Final informative output
  VLOG(1) << "-----------------------------";
  VLOG(1) << "COMPLETED:";
  VLOG(1) << "Packets processed : " << i;
  VLOG(1) << "Lost at DAG       : " << lost_at_dag;
}

}  // namespace r2d2
}  // namespace examples
}  // namespace firmament
