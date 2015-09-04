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
#include "sim/trace-extract/google_trace_loader.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

DEFINE_int32(num_files_to_process, 500, "Number of files to process.");
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

DECLARE_uint64(heartbeat_interval);

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
  event_manager_(new GoogleTraceEventManager), num_duplicate_task_ids_(0),
  trace_path_(trace_path) {
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

  // Output CSV header.
  OutputStatsHeader(stats_file_);
  // Register timeout handler.
  signal(SIGALRM, GoogleTraceSimulator::SolverTimeoutHandler);

  ReplayTrace();
  LOG(INFO) << "Simulator has seen " << num_duplicate_task_ids_
            << " duplicate task ids";
}

void GoogleTraceSimulator::SolverTimeoutHandler(int sig) {
  signal(SIGALRM, SIG_IGN);
  LOG(FATAL) << "Timeout after waiting for solver for "
             << FLAGS_solver_timeout << " seconds.";
}

void GoogleTraceSimulator::ProcessSimulatorEvents(
    uint64_t events_up_to_time,
    const ResourceTopologyNodeDescriptor& machine_tmpl) {
  while (true) {
    if (event_manager_->GetTimeOfNextEvent() > events_up_to_time) {
      // Processed all events <= events_up_to_time.
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
      TraceTaskIdentifier task_identifier;
      task_identifier.task_index = event.second.task_index();
      task_identifier.job_id = event.second.job_id();
      spf(&log_string, "TASK_END_RUNTIME %ju:%ju @ %ju\n",
          task_identifier.job_id, task_identifier.task_index, event.first);
      LogEvent(graph_output_, log_string);
      bridge_->TaskCompleted(task_identifier);
      UpdateSchedulingLatencyStats(event.first);
    } else if (event.second.type() == EventDescriptor::MACHINE_HEARTBEAT) {
      spf(&log_string, "MACHINE_HEARTBEAT %ju\n", event.first);
      LogEvent(graph_output_, log_string);
      bridge_->AddMachineSamples(event.first);
    } else if (event.second.type() == EventDescriptor::TASK_SUBMIT) {
      TraceTaskIdentifier task_identifier;
      task_identifier.task_index = event.second.task_index();
      task_identifier.job_id = event.second.job_id();
      spf(&log_string, "TASK_SUBMIT_EVENT: ID %ju:%ju @ %ju\n",
          task_identifier.job_id, task_identifier.task_index, event.first);
      LogEvent(graph_output_, log_string);
      if (bridge_->AddTask(task_identifier)) {
        UpdateSchedulingLatencyStats(event.first);
      } else {
        num_duplicate_task_ids_++;
        // duplicate task id -- ignore
      }
    } else {
      LOG(FATAL) << "Unexpected event type " << event.second.type() << " @ "
                 << event.first;
    }
  }
}

void GoogleTraceSimulator::ReplayTrace() {
  // Load the trace ingredients
  ResourceTopologyNodeDescriptor machine_tmpl;
  bridge_->LoadTraceData(&machine_tmpl);

  char line[200];
  vector<string> vals;
  FILE* f_task_events_ptr = NULL;
  uint64_t run_solver_at = 0;
  uint64_t current_heartbeat_time = 0;
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
          TraceTaskIdentifier task_id;
          uint64_t task_event_time = lexical_cast<uint64_t>(vals[0]);
          task_id.job_id = lexical_cast<uint64_t>(vals[2]);
          task_id.task_index = lexical_cast<uint64_t>(vals[3]);
          uint64_t event_type = lexical_cast<uint64_t>(vals[5]);

          // Sub-sample the trace if we only retain < 100% of tasks.
          if (SpookyHash::Hash64(&task_id, sizeof(task_id), kSeed) >
              MaxEventIdToRetain()) {
            // skip event
            continue;
          }

          num_events++;
          if (event_type == SUBMIT_EVENT) {
            EventDescriptor event_desc;
            event_desc.set_type(EventDescriptor::TASK_SUBMIT);
            event_desc.set_job_id(task_id.job_id);
            event_desc.set_task_index(task_id.task_index);
            event_manager_->AddEvent(task_event_time, event_desc);
          } else {
            // Skip this event and read next event from the trace.
            continue;
          }

          for (; task_event_time > current_heartbeat_time;
               current_heartbeat_time += FLAGS_heartbeat_interval) {
            EventDescriptor event_desc;
            event_desc.set_type(EventDescriptor::MACHINE_HEARTBEAT);
            event_manager_->AddEvent(current_heartbeat_time, event_desc);
          }

          // In this loop we do all the outstanding scheduling rounds
          // before processing the task event (i.e. scheduling rounds
          // that must occur before task_event_time).
          while (task_event_time > run_solver_at) {
            ProcessSimulatorEvents(run_solver_at, machine_tmpl);
            // Run the solver and update the time to run the solver at.
            run_solver_at = RunSolverHelper(run_solver_at);
            num_scheduling_rounds++;
            if (event_manager_->HasSimulationCompleted(
                    num_events, num_scheduling_rounds)) {
              fclose(f_task_events_ptr);
              return;
            }
            // Make sure run_solver_at is updated if we don't have any events.
            run_solver_at = max(run_solver_at,
                                event_manager_->GetTimeOfNextEvent());
            VLOG(1) << "Run solver by " << run_solver_at;
            ResetSchedulingLatencyStats();
          }

          ProcessSimulatorEvents(task_event_time, machine_tmpl);
        }
      }
    }
    fclose(f_task_events_ptr);
  }
  // We have finished the task events, but there may still be machine
  // events that can trigger other task events. We need to keep on
  // processing until we finish all the events.
  while (event_manager_->HasSimulationCompleted(
      num_events, num_scheduling_rounds) == false) {
    ProcessSimulatorEvents(run_solver_at, machine_tmpl);
    // Run the solver and update the time to run the solver at.
    run_solver_at = RunSolverHelper(run_solver_at);
    num_scheduling_rounds++;
    // Make sure run_solver_at is updated if we don't have any events.
    run_solver_at = max(run_solver_at,
                        event_manager_->GetTimeOfNextEvent());
    ResetSchedulingLatencyStats();
  }
}

void GoogleTraceSimulator::ResetSchedulingLatencyStats() {
  first_event_in_scheduling_round_ = UINT64_MAX;
  last_event_in_scheduling_round_ = 0;
  num_events_in_scheduling_round_ = 0;
  sum_timestamps_in_scheduling_round_ = 0;
}

uint64_t GoogleTraceSimulator::RunSolverHelper(uint64_t run_solver_at) {
  LogStartOfSolverRun(graph_output_, run_solver_at);
  // Set a timeout on the solver's run
  alarm(FLAGS_solver_timeout);
  boost::timer::cpu_timer timer;
  scheduler::SchedulerStats scheduler_stats;
  bridge_->RunSolver(&scheduler_stats);
  alarm(0);

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
                    run_solver_at, scheduler_stats);

  return event_manager_->GetTimeOfNextSolverRun(
      run_solver_at, scheduler_stats.scheduler_runtime);
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
