// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.
#include "sim/trace-extract/google_trace_simulator.h"

#include <signal.h>
#include <SpookyV2.h>
#include <sys/stat.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/timer/timer.hpp>
#include <cstdio>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/flow_graph.h"
#include "sim/trace-extract/google_trace_loader.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

DEFINE_int32(num_files_to_process, 500, "Number of files to process.");
DEFINE_uint64(runtime, 9223372036854775807,
              "Maximum time in microsec to extract data for"
              "(from start of trace)");

DEFINE_string(stats_file, "", "File to write CSV of statistics.");
DEFINE_string(graph_output_file, "",
              "File to write incremental DIMACS export.");
DEFINE_bool(graph_output_events, true, "If -graph_output_file specified: "
                                       "export simulator events as comments?");

DEFINE_string(solver, "flowlessly",
              "Solver to use: flowlessly | cs2 | custom.");
DEFINE_uint64(solver_timeout, UINT64_MAX,
              "Timeout: terminate after waiting this number of seconds");
DEFINE_bool(run_incremental_scheduler, false,
            "Run the Flowlessly incremental scheduler.");

static bool ValidateSolver(const char* flagname, const string& solver) {
  if (solver.compare("cs2") && solver.compare("flowlessly") &&
      solver.compare("custom")) {
    LOG(ERROR) << "Solver can be one of: cs2, flowlessly or custom";
    return false;
  }
  return true;
}

static const bool solver_validator =
  google::RegisterFlagValidator(&FLAGS_solver, &ValidateSolver);

static bool ValidateRunIncremental(const char* flagname, bool run_incremental) {
  if (run_incremental && FLAGS_solver.compare("flowlessly")) {
    LOG(ERROR) << "run_incremental_scheduler can only be set with the "
               << "flowlessly solver";
    return false;
  }
  return true;
}

static const bool run_incremental_validator =
  google::RegisterFlagValidator(&FLAGS_run_incremental_scheduler,
                                &ValidateRunIncremental);

namespace firmament {
namespace sim {

GoogleTraceSimulator::GoogleTraceSimulator(const string& trace_path) :
  event_manager_(new GoogleTraceEventManager), trace_path_(trace_path) {
  bridge_ = new GoogleTraceBridge(trace_path, event_manager_);

  graph_output_ = NULL;
  if (!FLAGS_graph_output_file.empty()) {
    graph_output_ = fopen(FLAGS_graph_output_file.c_str(), "w");
    if (!graph_output_) {
      LOG(FATAL) << "Could not open for writing graph file "
                 << FLAGS_graph_output_file << ", error: " << strerror(errno);
    }
  }
  stats_file_ = NULL;
  if (!FLAGS_stats_file.empty()) {
    stats_file_ = fopen(FLAGS_stats_file.c_str(), "w");
    if (!stats_file_) {
      LOG(FATAL) << "Could not open for writing stats file "
                 << FLAGS_stats_file << ", error: " << strerror(errno);
    }
  }
  ResetSchedulingLatencyStats();
}

GoogleTraceSimulator::~GoogleTraceSimulator() {
  if (graph_output_) {
    fclose(graph_output_);
  }
  if (stats_file_) {
    fclose(stats_file_);
  }
  delete bridge_;
  delete event_manager_;
}

void GoogleTraceSimulator::Run() {
  FLAGS_add_root_task_to_graph = false;
  // Terminate if flow solving binary fails.
  FLAGS_flow_scheduling_strict = true;
  FLAGS_flow_scheduling_solver = FLAGS_solver;
  if (!FLAGS_solver.compare("flowlessly")) {
    FLAGS_incremental_flow = FLAGS_run_incremental_scheduler;
    FLAGS_only_read_assignment_changes = true;
    FLAGS_flow_scheduling_binary =
        SOLVER_DIR "/flowlessly-git/run_fast_cost_scaling";
  } else if (!FLAGS_solver.compare("cs2")) {
    FLAGS_incremental_flow = false;
    FLAGS_only_read_assignment_changes = false;
    FLAGS_flow_scheduling_binary = SOLVER_DIR "/cs2-git/cs2.exe";
  } else if (!FLAGS_solver.compare("custom")) {
    FLAGS_flow_scheduling_time_reported = true;
  }

  LOG(INFO) << "Starting Google trace simulator!";
  LOG(INFO) << "Time to simulate for: " << FLAGS_runtime << " microseconds.";

  bridge_->CreateRootResource();
  ReplayTrace();
}

void GoogleTraceSimulator::SolverTimeoutHandler(int sig) {
  signal(SIGALRM, SIG_IGN);
  LOG(FATAL) << "Timeout after waiting for solver for "
             << FLAGS_solver_timeout << " seconds.";
}

void GoogleTraceSimulator::ProcessSimulatorEvents(
    uint64_t cur_time,
    const ResourceTopologyNodeDescriptor& machine_tmpl) {
  while (true) {
    if (event_manager_->GetTimeOfNextEvent() > cur_time) {
      // Processed all events <= cur_time.
      break;
    }
    pair<uint64_t, EventDescriptor> event = event_manager_->GetNextEvent();
    string log_string;
    if (event.second.type() == EventDescriptor::ADD_MACHINE) {
      spf(&log_string, "ADD_MACHINE %ju @ %ju\n", event.second.machine_id(),
          event.first);
      LogEvent(graph_output_, log_string);
      bridge_->AddMachine(machine_tmpl, event.second.machine_id());
      UpdateSchedulingLatencyStats(event.first);
    } else if (event.second.type() == EventDescriptor::REMOVE_MACHINE) {
      spf(&log_string, "REMOVE_MACHINE %ju @ %ju\n", event.second.machine_id(),
          event.first);
      LogEvent(graph_output_, log_string);
      bridge_->RemoveMachine(event.second.machine_id());
      UpdateSchedulingLatencyStats(event.first);
    } else if (event.second.type() == EventDescriptor::UPDATE_MACHINE) {
      // TODO(ionel): Handle machine update event.
    } else if (event.second.type() == EventDescriptor::TASK_END_RUNTIME) {
      spf(&log_string, "TASK_END_RUNTIME %ju:%ju @ %ju\n",
          event.second.job_id(), event.second.task_index(), event.first);
      LogEvent(graph_output_, log_string);
      // Task has finished.
      TaskIdentifier task_identifier;
      task_identifier.task_index = event.second.task_index();
      task_identifier.job_id = event.second.job_id();
      bridge_->TaskCompleted(task_identifier);
      UpdateSchedulingLatencyStats(event.first);
    } else if (event.second.type() ==
               EventDescriptor::TASK_ASSIGNMENT_CHANGED) {
      // no-op: this event is just used to trigger solver re-run
    } else {
      LOG(FATAL) << "Unexpected event type " << event.second.type() << " @ "
                 << event.first;
    }
  }
}

void GoogleTraceSimulator::ProcessTaskEvent(
    uint64_t cur_time, const TaskIdentifier& task_identifier,
    uint64_t event_type) {
  string log_string;
  if (event_type == SUBMIT_EVENT) {
    spf(&log_string, "TASK_SUBMIT_EVENT: ID %ju:%ju @ %ju\n",
        task_identifier.job_id, task_identifier.task_index,
        cur_time);
    LogEvent(graph_output_, log_string);
    if (bridge_->AddNewTask(task_identifier)) {
      UpdateSchedulingLatencyStats(cur_time);
    } else {
      // duplicate task id -- ignore
    }
  }
}

void GoogleTraceSimulator::ReplayTrace() {
  // Output CSV header
  OutputStatsHeader(stats_file_);

  // Timing facilities
  boost::timer::cpu_timer timer;
  signal(SIGALRM, GoogleTraceSimulator::SolverTimeoutHandler);

  // Load the trace ingredients
  ResourceTopologyNodeDescriptor machine_tmpl;
  bridge_->LoadTraceData(&machine_tmpl);

  char line[200];
  vector<string> vals;
  FILE* f_task_events_ptr = NULL;
  uint64_t run_solver_at = 0;
  uint64_t num_events = 0;
  uint64_t num_scheduling_rounds = 0;
  ResetSchedulingLatencyStats();

  for (int32_t file_num = 0; file_num < FLAGS_num_files_to_process;
       file_num++) {
    string fname;
    spf(&fname, "%s/task_events/part-%05d-of-00500.csv", trace_path_.c_str(),
        file_num);
    if ((f_task_events_ptr = fopen(fname.c_str(), "r")) == NULL) {
      LOG(FATAL) << "Failed to open trace for reading of task events.";
    }
    while (!feof(f_task_events_ptr)) {
      if (fscanf(f_task_events_ptr, "%[^\n]%*[\n]", &line[0]) > 0) {
        boost::split(vals, line, is_any_of(","), token_compress_off);
        if (vals.size() != 13) {
          LOG(ERROR) << "Unexpected structure of task event row: found "
                     << vals.size() << " columns.";
        } else {
          TaskIdentifier task_id;
          uint64_t task_time = lexical_cast<uint64_t>(vals[0]);
          task_id.job_id = lexical_cast<uint64_t>(vals[2]);
          task_id.task_index = lexical_cast<uint64_t>(vals[3]);

          // Sub-sample the trace if we only retain < 100% of tasks.
          if (SpookyHash::Hash64(&task_id, sizeof(task_id), kSeed) >
              MaxEventIdToRetain()) {
            // skip event
            continue;
          }

          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);
          VLOG(2) << "TASK EVENT @ " << task_time;
          num_events++;
          if (event_manager_->HasSimulationCompleted(task_time, num_events,
                                                     num_scheduling_rounds)) {
            return;
          }

          while (task_time > run_solver_at) {
            ProcessSimulatorEvents(run_solver_at, machine_tmpl);

            if (!bridge_->flow_graph()->graph_changes().empty()) {
              // Only run solver if something has actually changed.
              // (Sometimes, all the events we received in a time interval
              // have been ignored, e.g. task submit events with duplicate IDs.)
              run_solver_at = RunSolver(run_solver_at);

              // We've done another round, check if we should keep going
              num_scheduling_rounds++;
              if (event_manager_->HasSimulationCompleted(
                      task_time, num_events, num_scheduling_rounds)) {
                return;
              }
            }

            // skip time until the next event happens
            uint64_t next_event =
              min(task_time, event_manager_->GetTimeOfNextEvent());
            VLOG(1) << "Next event at " << next_event;
            run_solver_at = max(next_event, run_solver_at);
            VLOG(1) << "Run solver by " << run_solver_at;
            ResetSchedulingLatencyStats();
          }

          ProcessSimulatorEvents(task_time, machine_tmpl);
          ProcessTaskEvent(task_time, task_id, event_type);
        }
      }
    }
    fclose(f_task_events_ptr);
  }
}

void GoogleTraceSimulator::ResetSchedulingLatencyStats() {
  first_event_in_scheduling_round_ = UINT64_MAX;
  last_event_in_scheduling_round_ = 0;
  num_events_in_scheduling_round_ = 0;
  sum_timestamps_in_scheduling_round_ = 0;
}

uint64_t GoogleTraceSimulator::RunSolver(uint64_t run_solver_at) {
  double algorithm_time;
  double flow_solver_time;
  LogStartOfSolverRun(graph_output_, bridge_->flow_graph(), run_solver_at);

  DIMACSChangeStats change_stats(bridge_->flow_graph()->graph_changes());
  // Set a timeout on the solver's run
  alarm(FLAGS_solver_timeout);
  boost::timer::cpu_timer timer;
  multimap<uint64_t, uint64_t>* task_mappings =
    bridge_->RunSolver(&algorithm_time, &flow_solver_time, graph_output_);
  alarm(0);

  // We're done, now update the flow graph with the results
  bridge_->UpdateFlowGraph(run_solver_at, task_mappings);
  delete task_mappings;
  // Also update any resource statistics, if required
  bridge_->UpdateResourceStats();

  double avg_event_timestamp_in_scheduling_round;
  // Set timestamp to max if we haven't seen any events. This has an
  // effect on the logic that computes the scheduling latency. It makes sure
  // that the latency is set to 0 in this case.
  if (num_events_in_scheduling_round_ == 0) {
    avg_event_timestamp_in_scheduling_round =
      numeric_limits<double>::max();
  } else {
    avg_event_timestamp_in_scheduling_round =
      static_cast<double>(sum_timestamps_in_scheduling_round_) /
      num_events_in_scheduling_round_;
  }
  // Log stats to CSV file
  LogSolverRunStats(avg_event_timestamp_in_scheduling_round, stats_file_, timer,
                    run_solver_at, algorithm_time, flow_solver_time,
                    change_stats);

  return event_manager_->GetTimeOfNextSolverRun(run_solver_at, algorithm_time);
}

void GoogleTraceSimulator::UpdateSchedulingLatencyStats(uint64_t time) {
  first_event_in_scheduling_round_ =
    min(time, first_event_in_scheduling_round_);
  last_event_in_scheduling_round_ =
    max(time, last_event_in_scheduling_round_);
  sum_timestamps_in_scheduling_round_ += time;
  num_events_in_scheduling_round_++;
}

} // namespace sim
} // namespace firmament
