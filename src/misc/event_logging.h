// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Event logging helper class. This provides logging hooks to emit events into
// an event trace (in ASCII format).
// The public methods of this class can have any signature apart from the first
// argument, which must always be a double representing the current timestamp.
// All public methods (after arbitrary processing) call into LogEvent() and
// supply and event type identifier, timestamp and a vector of strings
// representing any other elements to be logged (akin to variadic print
// functions).

#ifndef FIRMAMENT_MISC_EVENT_LOGGING_H_
#define FIRMAMENT_MISC_EVENT_LOGGING_H_

#include <string>
#include <vector>
#include <iostream>  // NOLINT
#include <fstream>  // NOLINT

#include "base/common.h"

namespace firmament {

class EventLogger {
 public:
  // Event types. Add new members here to register additional event types.
  enum LogEventType {
    // Simulator events
    RESOURCE_UTILIZATION_SAMPLE = 0,
    JOB_ARRIVED_EVENT = 1,
    JOB_COMPLETED_EVENT = 2,
    JOB_HANDOFF_TO_PEERS_EVENT = 3,
  };

  // Creates an EventLogger writing to <out_filename>.
  explicit EventLogger(const string &out_filename);
  virtual ~EventLogger();

  // TODO(malte): Why is this public?
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
  // Stream buffer used for outputting the log events.
  ofstream *buffer_;

  // Common logging method; see description in file-level documentation.
  void LogEvent(LogEventType type, double time,
                const vector<string> &parameters);
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_EVENT_LOGGING_H_
