// The Firmament project
// Copyright (c) The Firmament Authors.
//
// Joins two R2D2 DAG capture files, matching packets in the second one (DAG1)
// to packets in the first one (DAG0). Writes out the timestamp when the packet
// was received, the latency it experienced, and the time since the last packet
// arrived.

#include "examples/r2d2_trace_process/common.h"
//#ifndef __FIRMAMENT__
#if 1
extern "C" {
#include "examples/r2d2_trace_process/standalone_io.h"
}
#endif
#include "examples/r2d2_trace_process/packet_join_task.h"
#ifdef __FIRMAMENT__
#include "examples/task_lib_bridge.h"
#endif

#include <cstdlib>
#include <vector>
#include <iostream>  // NOLINT

#define WINDOW_SIZE 1*1000*1000  // 100ms

#ifndef __FIRMAMENT__
int main(int argc, char* argv[]) {
  // Initiate Google logging
  google::ParseCommandLineFlags(&argc, &argv, true);

  // Set up glog for logging output
  google::InitGoogleLogging(argv[0]);

  // Rudimentary command line parsing
  if (argc != 5)
    LOG(FATAL) << "usage: packet_join_task <dag0_cap> <dag1_cap> "
               << "<offset> <count>";
  char* dag0_filename = argv[1];
  char* dag1_filename = argv[2];
  uint64_t offset = atol(argv[3]);
  uint64_t count = atol(argv[4]);

  // Informative output
  VLOG(1) << "Latency extraction task starting.";
  VLOG(1) << "  DAG0 filename: " << dag0_filename;
  VLOG(1) << "  DAG1 filename: " << dag1_filename;
  VLOG(1) << "  Offset into DAG1: " << offset;
  VLOG(1) << "  Num packets to consider: " << count;

  // Map dag0 data set into memory
  void* dag0_ptr = load_to_shmem(dag0_filename);

  // Analyze dag0 header
  dag_capture_header_t* head =
      reinterpret_cast<dag_capture_header_t*>(dag0_ptr);
  firmament::examples::r2d2::print_header_info("DAG0", head);

  // Special case: count == 0 (means ALL packets)
  if (count == 0)
    count = head->samples;

  // Set up task and run
  firmament::examples::r2d2::PacketJoinTask t;
  t.Invoke(dag0_ptr, dag1_filename, offset, count);
}
#endif

namespace firmament {

#ifdef __FIRMAMENT__
void task_main(TaskID_t task_id, vector<char*>* args) {
  // Rudimentary command line parsing
  if (args->size() != 5)
    LOG(FATAL) << "usage: packet_join_task <dag0_cap> <dag1_cap> "
               << "<offset> <count>";
  char* dag0_filename = args->at(1);
  char* dag1_filename = args->at(2);
  uint64_t offset = atol(args->at(3));
  uint64_t count = atol(args->at(4));

  // Informative output
  VLOG(1) << "Latency extraction task starting.";
  VLOG(1) << "  DAG0 filename: " << dag0_filename;
  VLOG(1) << "  DAG1 filename: " << dag1_filename;
  VLOG(1) << "  Offset into DAG1: " << offset;
  VLOG(1) << "  Num packets to consider: " << count;

  // XXX(malte): This should happen automatically in
  // Firmament!
  // Map dag0 data set into memory
  void* dag0_ptr = load_to_shmem(dag0_filename);

  // Analyze dag0 header
  dag_capture_header_t* head =
      reinterpret_cast<dag_capture_header_t*>(dag0_ptr);
  examples::r2d2::print_header_info("DAG0", head);

  // Special case: count == 0 (means ALL packets)
  if (count == 0)
    count = head->samples;

  // Set up task and run
  firmament::examples::r2d2::PacketJoinTask t(task_id);
  t.Invoke(dag0_ptr, dag1_filename, offset, count);
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

void PacketJoinTask::Invoke(void* dag0_ptr, char* dag1_filename,
                            uint64_t offset, uint64_t count) {
  // Open the dag1 data set
  FILE* fp = fopen(dag1_filename, "r");
  // Allocate buffer for and read header data
  dag_capture_header_t* head_buf = new dag_capture_header_t;
  CHECK_EQ(fread(head_buf, sizeof(dag_capture_header_t), 1, fp), 1);
  // Analyze header information of dag1 dataset
  print_header_info("DAG1", head_buf);
  // Now seek to the desired offset
  uint64_t offset_value = sizeof(sample_t) * offset;
  CHECK_EQ(fseek(fp, offset_value, SEEK_CUR), 0);
  VLOG(1) << "Seek to offset " << offset_value << " (sample #" << offset
          << ")";
  // Allocate buffer for 1 packet sample
  sample_t* sample_buf = new sample_t;
  bzero(sample_buf, sizeof(sample_t));
  uint64_t i = 0;
  uint64_t packets_after_end = 0;
  uint64_t packets_before_start = 0;
  uint64_t packets_matched = 0;
  uint64_t packets_unmatched = 0;
  uint64_t lost_at_dag = 0;
  uint64_t last_idx = ~0ULL;
  uint64_t* last_idx_ptr = &last_idx;
  uint64_t unmatched_since_last_match = 0;
  // Go and keep looking at samples until reaching the end, which is the last
  // considered packet's timestamp plus WINDOW_SIZE milliseconds.
  while (i < count && fread(sample_buf, sizeof(sample_t), 1, fp)) {
    uint64_t sample_ts = sample_buf->timestamp;
    if (last_idx == ~0ULL)
      last_idx = StartIndexGuess(dag0_ptr, head_buf, sample_ts);
    VLOG(2) << "*********************";
    VLOG(2) << "Processing packet " << (offset + i) << ":";
    VLOG(2) << " time: "
            << (static_cast<double>(sample_ts) / static_cast<double>(SECS2NS))
            << " (" << sample_ts << "ns)";
    VLOG(2) << " type: " << sample_buf->value_type_dropped_len.type;
    VLOG(2) << " dropped: " << sample_buf->value_type_dropped_len.dropped;
    VLOG(2) << " len: " << sample_buf->value_type_dropped_len.length;
    // Try matching the packet against DAG0's dataset within a window of +/-
    // WINDOW_SIZE from its timestamp.
    dag_match_result_t match_result;
    uint64_t bwd_steps;
    uint64_t fwd_steps;
    sample_t* matched_sample = MatchPacketWithinWindow(
        dag0_ptr, sample_ts, sample_buf, last_idx_ptr, &match_result,
        &bwd_steps, &fwd_steps);
    if (!matched_sample) {
      VLOG(2) << "Unmatched packet " << (offset + i) << " at time "
              << sample_ts << ", hash: " << sample_buf->hash
              << " (" << hex << sample_buf->hash << ")" << dec
              << "; presumed lost!";
      switch (match_result) {
        case RESULT_AFTER_END:
          packets_after_end++;
          break;
        case RESULT_BEFORE_START:
          packets_before_start++;
          break;
        case RESULT_UNMATCHED:
          packets_unmatched++;
          break;
        default:
          // impossible to reach
          LOG(FATAL) << "Unmatched packet result is invalid";
      }
      // Canonical output format shared with dag_join implementation
      //OutputUnmatchedPacketResult();
      unmatched_since_last_match++;
    } else {
      uint64_t matched_ts = matched_sample->timestamp;
      // Work out various metrics for the corresponding packet and log them to
      // the appropriate outputs.
      int64_t delay = (int64_t(sample_ts) - int64_t(matched_ts));
      int64_t inter_arrival_time = (last_idx > 0) ?
          (int64_t(matched_ts) - int64_t(matched_sample[-1].timestamp)) : 0;
      // Canonical output format shared with dag_join implementation
      //OutputMatchedPacketResult();
      cout << sample_ts << "," << delay << "," << inter_arrival_time
           << "," << unmatched_since_last_match << "," << bwd_steps
           << "," << fwd_steps << endl;
      packets_matched++;
      unmatched_since_last_match = 0;
    }
    if (unlikely(sample_buf->value_type_dropped_len.dropped > 0))
      lost_at_dag += sample_buf->value_type_dropped_len.dropped;
    // Progress update every 10000 packets
    if (i % 100000 == 0) {
      float perc = static_cast<double>(i) /
          static_cast<double>(head_buf->samples) * 100.0;
      VLOG(1) << perc << "% (" << i << " packets)";
    }
    // One-up, next packet
    i++;
  }
  // Final informative output
  VLOG(1) << "-----------------------------";
  VLOG(1) << "COMPLETED:";
  VLOG(1) << "Packets processed : " << i;
  VLOG(1) << "Before DAG0 start : " << packets_before_start;
  VLOG(1) << "Matched           : " << packets_matched;
  VLOG(1) << "Unmatched         : " << packets_unmatched;
  VLOG(1) << "After DAG0 end    : " << packets_after_end;
  VLOG(1) << "Lost at DAG       : " << lost_at_dag;
  VLOG(1) << "-----------------------------";
  VLOG(1) << "Avg BWD steps p.p.: " << fixed
          << (static_cast<double>(total_backward_steps_) /
             static_cast<double>(i));
  VLOG(1) << "Avg FWD steps p.p.: " << fixed
          << (static_cast<double>(total_forward_steps_) /
             static_cast<double>(i));
  // Clean up memory
  free(head_buf);
  free(sample_buf);
}

uint64_t PacketJoinTask::StartIndexGuess(
    void* dag0_ptr, dag_capture_header_t* dag1_head_ptr, uint64_t timestamp) {
  // Make a guess at a good offset to start
  dag_capture_header_t* dag0_head_ptr =
      reinterpret_cast<dag_capture_header_t*>(dag0_ptr);
  sample_t* dag0_sample_data = &dag0_head_ptr->first_sample;
  VLOG(2) << "First DAG0 TS is " << dag0_sample_data[0].timestamp;
  VLOG(2) << "First DAG1 TS is " << timestamp;
  uint64_t duration, second_dag_start;
  if (dag0_head_ptr->start_time <= dag1_head_ptr->start_time) {
    // DAG 0 started first
    VLOG(1) << "DAG0 started first";
    duration = dag0_head_ptr->end_time - dag1_head_ptr->start_time;
    second_dag_start = dag1_head_ptr->start_time;
  } else {
    // DAG 1 started first
    VLOG(1) << "DAG1 started first";
    duration = dag0_head_ptr->end_time - dag0_head_ptr->start_time;
    second_dag_start = dag0_head_ptr->start_time;
  }
  VLOG(2) << "Duration is " << duration;
  int64_t time_since_start = timestamp - second_dag_start;
  VLOG(2) << "time since DAG0 start: " << time_since_start;
  if (time_since_start <= 0)
    return 0;
  uint64_t start_guess = static_cast<uint64_t>(dag0_head_ptr->samples *
      (static_cast<double>(time_since_start) /
       static_cast<double>(duration)));
  VLOG(2) << "start_guess is " << start_guess;
  uint64_t ts_at_start_guess = dag0_sample_data[start_guess].timestamp;
  VLOG(2) << "TS at " << start_guess << " is: "
          << (static_cast<double>(ts_at_start_guess) /
              static_cast<double>(SECS2NS))
          << " (" << ts_at_start_guess << "ns)";
  return start_guess;
}

sample_t* PacketJoinTask::MatchPacketWithinWindow(
    void* dag0_ptr, uint64_t timestamp, sample_t* packet,
    uint64_t* init_idx, dag_match_result_t* result,
    uint64_t* bwd_steps, uint64_t* fwd_steps) {
  // Shortcut: if timestamp is before the start of DAG0 minus window size,
  // we can give up immediately.
  dag_capture_header_t* dag0_head_ptr =
      reinterpret_cast<dag_capture_header_t*>(dag0_ptr);
  if (timestamp < dag0_head_ptr->start_time) {
    VLOG(2) << "Returning early as match is impossible: DAG1 timestamp is "
            << (dag0_head_ptr->start_time - timestamp)
            << " before start of DAG0";
    *result = RESULT_BEFORE_START;
    return NULL;
  } else if (timestamp > dag0_head_ptr->end_time) {
    *result = RESULT_AFTER_END;
    return NULL;
  } else if (packet->hash == 0xFEEDCAFEDEADBEEF) {
    // Filter packets from second input source (which have no seq no)
    *result = RESULT_UNMATCHED;
    return NULL;
  }
  // Figure out the window to consider
  uint64_t min_time = timestamp - WINDOW_SIZE;
  uint64_t max_time = timestamp + WINDOW_SIZE;
  VLOG(2) << "Considering time window from " << min_time << " to " << max_time
          << ", initial index: " << *init_idx;
  sample_t* dag0_sample_data = &dag0_head_ptr->first_sample;
  uint64_t cur_idx = *init_idx;
  uint64_t cur_idx_fwd = *init_idx;
  uint64_t cur_idx_bwd = *init_idx;
  bool direction_fwd = true;
  VLOG(3) << "Initial index TS is "
          << dag0_sample_data[cur_idx].timestamp;
  // Try matching
  while (cur_idx <= dag0_head_ptr->samples &&
         (dag0_sample_data[cur_idx].timestamp > min_time ||
          dag0_sample_data[cur_idx].timestamp < max_time)) {
    VLOG(3) << "Considering index " << cur_idx << ", bounds ("
            << cur_idx_bwd << ", " << cur_idx_fwd << ")";
    VLOG(3) << "TS is " << dag0_sample_data[cur_idx].timestamp;
    if (packet->value_type_dropped_len.type == udp &&
        dag0_sample_data[cur_idx].value_type_dropped_len.type ==
        packet->value_type_dropped_len.type &&
/*        dag0_sample_data[cur_idx].value_type_dropped_len.length ==
        packet->value_type_dropped_len.length &&*/
        dag0_sample_data[cur_idx].hash == packet->hash) {
      // We have a match!
      //VLOG_EVERY_N(1, 1000) << "MATCHED";
      VLOG(2) << "Matched to packet at "
              << dag0_sample_data[cur_idx].timestamp
              << ", hash: " << packet->hash << " (" << hex << packet->hash
              << ")" << dec
              << ", last indices: BWD: " << cur_idx_bwd
              << ", FWD: " << cur_idx_fwd << ", CUR: " << cur_idx;
      *bwd_steps = (*init_idx - cur_idx_bwd);
      *fwd_steps = (cur_idx_fwd - *init_idx);
      VLOG(2) << "Made " << *bwd_steps  << " backward steps and "
              << *fwd_steps << " forward steps.";
      total_backward_steps_ += *bwd_steps;
      total_forward_steps_ += *fwd_steps;
      *init_idx = cur_idx;
      *result = RESULT_MATCHED;
      return &dag0_sample_data[cur_idx];
    }
    // Flip direction and continue
    if (direction_fwd) {
      if (cur_idx_bwd > 0 &&
          dag0_sample_data[cur_idx_bwd].timestamp > min_time) {
        // Flip to backward
        direction_fwd = false;
        cur_idx = --cur_idx_bwd;
      } else if (dag0_sample_data[cur_idx_fwd].timestamp < max_time) {
        // Keep going forward
        cur_idx = ++cur_idx_fwd;
      } else {
        // We can go neither forward nor backward any more, so stop.
        VLOG(2) << "Giving up on packet as we've exhausted the search range!";
        VLOG(2) << "Last indices: " << cur_idx_bwd << "/" << cur_idx
                << "/" << cur_idx_fwd;
        break;
      }
    } else if (!direction_fwd) {
      if ((cur_idx_fwd <= dag0_head_ptr->samples &&
          dag0_sample_data[cur_idx_fwd].timestamp < max_time)) {
        // Flip to forward
        direction_fwd = true;
        cur_idx = ++cur_idx_fwd;
      } else if (dag0_sample_data[cur_idx_bwd].timestamp > min_time) {
        // Keep going backward (unless we've hit zero)
        if (cur_idx_bwd > 0) {
          cur_idx = --cur_idx_bwd;
        } else {
          VLOG(2) << "Giving up on packet as we've exhausted the search range!";
          VLOG(2) << "Last indices: " << cur_idx_bwd << "/" << cur_idx
                  << "/" << cur_idx_fwd;
          break;
        }
      } else {
        // We can go neither forward nor backward any more, so stop.
        VLOG(2) << "Giving up on packet as we've exhausted the search range!";
        VLOG(2) << "Last indices: " << cur_idx_bwd << "/" << cur_idx
                << "/" << cur_idx_fwd;
        break;
      }
    }
  }
  // No match, presume packet lost
  VLOG_EVERY_N(1, 1000) << "1000 UNMATCHED packets! (ex.: "
                        << packet->hash << ")";
  VLOG(2) << "Gave up on packet at " << timestamp
          << ", last indices: BWD: " << cur_idx_bwd
          << ", FWD: " << cur_idx_fwd << ", CUR: " << cur_idx;
  *bwd_steps = (*init_idx - cur_idx_bwd);
  *fwd_steps = (cur_idx_fwd - *init_idx);
  VLOG(2) << "Made " << *bwd_steps  << " backward steps and "
          << *fwd_steps << " forward steps.";
  total_backward_steps_ += *bwd_steps;
  total_forward_steps_ += *fwd_steps;
  *result = RESULT_UNMATCHED;
  return NULL;
}

}  // namespace r2d2
}  // namespace examples
}  // namespace firmament
