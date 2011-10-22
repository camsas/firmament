// TODO: header

#include "misc/event_logging.h"

#include <stdio.h>

namespace firmament {

EventLogger::EventLogger(const string &out_filename) {
  if (out_filename != "") {
    buffer_ = new ofstream(out_filename.c_str());
    LOG(INFO) << "EventLogger set up.";
  }
}

EventLogger::~EventLogger() {
  buffer_->close();
  delete buffer_;
}

// ----------------------------------------------------------------------------
// Common functionality
// ----------------------------------------------------------------------------

void EventLogger::LogEvent(LogEventType type, double time,
                           const vector<string> &parameters) {
  // <event_type_id> <timestamp> <parameters...>
  string s = to_string(type) + " " + to_string(time);
  for (int i = 0; i < parameters.size(); ++i) {
    s += (" " + parameters[i]);
  }
  // Write out the line
  *buffer_ << s.c_str() << endl;
}

// ----------------------------------------------------------------------------
// Simulator log events
// ----------------------------------------------------------------------------

void EventLogger::LogJobArrivalEvent(double time,
                                     uint64_t job_uid,
                                     uint64_t num_tasks) {
  vector<string> outputs;
  outputs.push_back(to_string(job_uid));
  outputs.push_back(to_string(num_tasks));
  LogEvent(JOB_ARRIVED_EVENT, time, outputs);
}

void EventLogger::LogUtilizationValues(double time, uint64_t ensemble_uid,
                                       uint64_t occupied_resources,
                                       double occupied_percent,
                                       uint64_t pending_queue_length,
                                       uint64_t unscheduled_tasks) {
  vector<string> outputs;
  outputs.push_back(to_string(ensemble_uid));
  outputs.push_back(to_string(occupied_resources));
  outputs.push_back(to_string(occupied_percent));
  outputs.push_back(to_string(pending_queue_length));
  outputs.push_back(to_string(unscheduled_tasks));
  LogEvent(RESOURCE_UTILIZATION_SAMPLE, time, outputs);
}

void EventLogger::LogJobCompletionEvent(double time,
                                        uint64_t c_uid,
                                        double duration) {
  vector<string> outputs;
  outputs.push_back(to_string(c_uid));
  outputs.push_back(to_string(duration));
  LogEvent(JOB_COMPLETED_EVENT, time, outputs);
}

void EventLogger::LogHandoffToPeersEvent(double time,
                                         uint64_t job_uid,
                                         uint64_t ensemble_uid,
                                         uint64_t num_tasks) {
  vector<string> outputs;
  outputs.push_back(to_string(job_uid));
  outputs.push_back(to_string(ensemble_uid));
  outputs.push_back(to_string(num_tasks));
  LogEvent(JOB_HANDOFF_TO_PEERS_EVENT, time, outputs);
}

}  // namespace firmament
