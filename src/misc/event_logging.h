// Copyright 2011 Google Inc. All Rights Reserved.
// Author: maltes@google.com (Malte Schwarzkopf)

#ifndef FIRMAMENT_MISC_EVENT_LOGGING_H_
#define FIRMAMENT_MISC_EVENT_LOGGING_H_

#include <string>
#include <iostream>
#include <fstream>

#include "base/common.h"

namespace firmament {

class EventLogger {
 public:
  enum LogEventType {
    // Simulator events
    RESOURCE_UTILIZATION_SAMPLE = 0,
    JOB_ARRIVED_EVENT = 1,
    JOB_COMPLETED_EVENT = 2,
    JOB_HANDOFF_TO_PEERS_EVENT = 3,
  };

  explicit EventLogger(const string &out_filename);
  virtual ~EventLogger();

  ofstream *GetBuffer() {
    return buffer_;
  }
  // -----------------------------------------
  // Simulator event logging
  void LogUtilizationValues(double time, uint64_t ensemble_uid,
                            uint64_t occupied_resources,
                            double occupied_percent,
                            uint64_t pending_queue_length,
                            uint64_t unscheduled_tasks);
  void LogJobArrivalEvent(double time,
                          uint64_t job_uid,
                          uint64_t num_tasks);
  void LogJobCompletionEvent(double time, const uint64_t c_uid,
                             double duration);
  void LogHandoffToPeersEvent(double time,
                              uint64_t job_uid,
                              uint64_t ensemble_uid,
                              uint64_t num_tasks);
 private:
  ofstream *buffer_;
  void LogEvent(LogEventType type, double time,
                const vector<string> &parameters);
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_EVENT_LOGGING_H_
