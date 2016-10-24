// The Firmament project
// Copyright (c) The Firmament Authors.
//
// Extracts latency values from two R2D2 DAG capture files.

#include "examples/r2d2_trace_process/aggregate_bandwidth_analysis_task.h"

#include <cstdlib>
#include <vector>
#include <iostream>  // NOLINT

#ifndef __FIRMAMENT__
extern "C" {
#include "examples/r2d2_trace_process/standalone_io.h"
}
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
    LOG(FATAL) << "usage: aggregate_bandwidth_analysis_task <dag_capture_file> "
               << "<offset> <count>";
  char* dag0_filename = argv[1];
  uint64_t offset = atol(argv[2]);
  uint64_t count = atol(argv[3]);

  // Informative output
  VLOG(1) << "Aggregate bandwidth analysis task starting.";
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
  firmament::examples::r2d2::AggregateBandwidthAnalysisTask t;
  t.Invoke(dag0_ptr, offset, count);
}
#endif

namespace firmament {

#ifdef __FIRMAMENT__
void task_main(TaskLib* task_lib, TaskID_t task_id, vector<char*>*) {
  examples::r2d2::AggregateBandwidthAnalysisTask t(task_lib, task_id);
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

void AggregateBandwidthAnalysisTask::Invoke(void* dag0_ptr, uint64_t offset,
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
  uint64_t first_timestamp = data_ptr[offset].timestamp;
  uint64_t bw_sample_start = first_timestamp;
  uint64_t bw_sample_width = WINDOW_SIZE;
  uint64_t bw_aggregate_counter_s0 = 0;
  uint64_t bw_aggregate_counter_s1 = 0;
  // Iterate over all packets in our section of the capture file and extract
  // information for each of them.
  for (i = offset; i < (offset + count); ++i) {
    sample_t* packet = &(data_ptr[i]);
    VLOG(2) << "TS: " << packet->timestamp - first_timestamp
            << ", BW-TS-start: " << bw_sample_start - first_timestamp
            << ", BW-TS-end: " << (bw_sample_start + bw_sample_width -
                                   first_timestamp);
    uint64_t* bw_aggregate_counter;
    if (packet->hash == 0xFEEDCAFEDEADBEEF ||
        packet->hash == 0xFFFFFFFFFFFFF106)
      bw_aggregate_counter = &bw_aggregate_counter_s1;
    else
      bw_aggregate_counter = &bw_aggregate_counter_s0;
    if (packet->timestamp <= bw_sample_start + bw_sample_width) {
      VLOG(2) << "WITHIN sample period, adding.";
      *bw_aggregate_counter += packet->value_type_dropped_len.length;
    } else {
      *bw_aggregate_counter += packet->value_type_dropped_len.length;
      uint64_t width = packet->timestamp - bw_sample_start;
      VLOG(2) << "BEYOND sample period, emitting point(s) for period of size "
              << width << ", aggregate size: " << bw_aggregate_counter_s0
              << "/" << bw_aggregate_counter_s1;
      if (packet->timestamp - prev_packet_timestamp_ > bw_sample_width) {
        // big gap, emit two data points on either side of the big gap
        VLOG(2) << "BIG gap: emitting two data points!";
        cout << prev_packet_timestamp_ << ","
             << bw_aggregate_counter_s0 << ","
             << fixed << (static_cast<double>(bw_aggregate_counter_s0 * 8) /
                          (static_cast<double>(width) /
                           static_cast<double>(SECS2NS))) << ","
             << fixed << (static_cast<double>(bw_aggregate_counter_s1 * 8) /
                          (static_cast<double>(width) /
                           static_cast<double>(SECS2NS)))
              << "\n";
        cout << packet->timestamp << ","
             << bw_aggregate_counter_s0 << ","
             << fixed << (static_cast<double>(bw_aggregate_counter_s0 * 8) /
                          (static_cast<double>(width) /
                           static_cast<double>(SECS2NS))) << ","
             << fixed << (static_cast<double>(bw_aggregate_counter_s1 * 8) /
                          (static_cast<double>(width) /
                           static_cast<double>(SECS2NS)))
             << "\n";
      } else {
        // simple, small gap, emit one data point and move on
        cout << packet->timestamp << ","
             << bw_aggregate_counter_s0 << ","
             << fixed << (static_cast<double>(bw_aggregate_counter_s0 * 8) /
                          (static_cast<double>(width) /
                           static_cast<double>(SECS2NS))) << ","
             << fixed << (static_cast<double>(bw_aggregate_counter_s1 * 8) /
                          (static_cast<double>(width) /
                           static_cast<double>(SECS2NS)))
             << "\n";
      }
      bw_sample_start = packet->timestamp;
      bw_aggregate_counter_s0 = 0;
      bw_aggregate_counter_s1 = 0;
    }
    prev_packet_timestamp_ = packet->timestamp;
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
