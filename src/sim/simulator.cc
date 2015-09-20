// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Simulator tool.

#include "sim/simulator.h"

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
#include "sim/google_trace_loader.h"
#include "sim/synthetic_trace_loader.h"

using boost::lexical_cast;
using boost::algorithm::is_any_of;
using boost::token_compress_off;

DEFINE_string(stats_file, "", "File to write CSV of statistics.");
DEFINE_string(graph_output_file, "",
              "File to write incremental DIMACS export.");
DEFINE_bool(graph_output_events, true, "If -graph_output_file specified: "
                                       "export simulator events as comments?");
DEFINE_string(solver, "flowlessly",
              "Solver to use: flowlessly | cs2 | custom.");
DEFINE_uint64(scheduler_timeout, UINT64_MAX,
              "Timeout: terminate after waiting this number of seconds");
DEFINE_bool(run_incremental_scheduler, false,
            "Run the Flowlessly incremental scheduler.");
DEFINE_string(simulation, "google",
              "The type of simulation to run: google | synthetic");

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

static bool ValidateSimulation(const char* flagname, const string& simulation) {
  if (simulation.compare("google") && simulation.compare("synthetic")) {
    LOG(ERROR) << "Simulation can be one of: google or synthetic";
    return false;
  }
  return true;
}

static const bool simulation_validator =
  google::RegisterFlagValidator(&FLAGS_simulation, &ValidateSimulation);

namespace firmament {
namespace sim {

Simulator::Simulator() :
  event_manager_(new EventManager), num_duplicate_task_ids_(0) {
  bridge_ = new SimulatorBridge(event_manager_);
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

Simulator::~Simulator() {
  if (graph_output_) {
    fclose(graph_output_);
  }
  if (stats_file_) {
    fclose(stats_file_);
  }
  delete bridge_;
  delete event_manager_;
}

void Simulator::ProcessSimulatorEvents(
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

void Simulator::ReplaySimulation() {
  // Load the trace ingredients
  ResourceTopologyNodeDescriptor machine_tmpl;
  // Import a fictional machine resource topology
  LoadMachineTemplate(&machine_tmpl);
  TraceLoader* trace_loader = NULL;
  if (!FLAGS_simulation.compare("google")) {
    trace_loader = new GoogleTraceLoader(event_manager_);
  } else if (!FLAGS_simulation.compare("synthetic")) {
    trace_loader = new SyntheticTraceLoader(event_manager_);
  }
  CHECK_NOTNULL(trace_loader);
  bridge_->LoadTraceData(trace_loader);

  uint64_t run_scheduler_at = 0;
  uint64_t current_heartbeat_time = 0;
  uint64_t num_scheduling_rounds = 0;
  ResetSchedulingLatencyStats();

  while (!event_manager_->HasSimulationCompleted(num_scheduling_rounds)) {
    // Load the task events up to the next scheduler run.
    trace_loader->LoadTaskEvents(run_scheduler_at);
    // Add the machine heartbeat events up to the next scheduler run.
    for (; run_scheduler_at >= current_heartbeat_time;
         current_heartbeat_time += FLAGS_heartbeat_interval) {
      EventDescriptor event_desc;
      event_desc.set_type(EventDescriptor::MACHINE_HEARTBEAT);
      event_manager_->AddEvent(current_heartbeat_time, event_desc);
    }
    ProcessSimulatorEvents(run_scheduler_at, machine_tmpl);
    // Run the scheduler and update the time to run the scheduler at.
    run_scheduler_at = ScheduleJobsHelper(run_scheduler_at);
    num_scheduling_rounds++;
    ResetSchedulingLatencyStats();
  }
  delete trace_loader;
}

void Simulator::ResetSchedulingLatencyStats() {
  first_event_in_scheduling_round_ = UINT64_MAX;
  last_event_in_scheduling_round_ = 0;
  num_events_in_scheduling_round_ = 0;
  sum_timestamps_in_scheduling_round_ = 0;
}

void Simulator::Run() {
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
  signal(SIGALRM, Simulator::SchedulerTimeoutHandler);

  ReplaySimulation();
  LOG(INFO) << "Simulator has seen " << num_duplicate_task_ids_
            << " duplicate task ids";
}

uint64_t Simulator::ScheduleJobsHelper(uint64_t run_scheduler_at) {
  LogStartOfSchedulerRun(graph_output_, run_scheduler_at);
  // Set a timeout on the scheduler's run
  alarm(FLAGS_scheduler_timeout);
  boost::timer::cpu_timer timer;
  scheduler::SchedulerStats scheduler_stats;
  bridge_->ScheduleJobs(&scheduler_stats);
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
  LogSchedulerRunStats(avg_event_timestamp_in_scheduling_round, stats_file_,
                       timer, run_scheduler_at, scheduler_stats);

  return event_manager_->GetTimeOfNextSchedulerRun(
      run_scheduler_at, scheduler_stats.scheduler_runtime);
}

void Simulator::SchedulerTimeoutHandler(int sig) {
  signal(SIGALRM, SIG_IGN);
  LOG(FATAL) << "Timeout after waiting for scheduler for "
             << FLAGS_scheduler_timeout << " seconds.";
}

void Simulator::UpdateSchedulingLatencyStats(uint64_t time) {
  first_event_in_scheduling_round_ =
    min(time, first_event_in_scheduling_round_);
  last_event_in_scheduling_round_ =
    max(time, last_event_in_scheduling_round_);
  sum_timestamps_in_scheduling_round_ += time;
  num_events_in_scheduling_round_++;
}

} // namespace sim
} // namespace firmament
