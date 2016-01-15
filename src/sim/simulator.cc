// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Simulator tool.

#include "sim/simulator.h"

#include <signal.h>
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

DEFINE_string(solver, "flowlessly",
              "Solver to use: flowlessly | cs2 | custom.");
DEFINE_uint64(scheduler_timeout, UINT64_MAX,
              "Timeout: terminate after waiting this number of seconds");
DEFINE_bool(run_incremental_scheduler, false,
            "Run the Flowlessly incremental scheduler.");
DEFINE_string(simulation, "google",
              "The type of simulation to run: google | synthetic");

DECLARE_uint64(heartbeat_interval);
DECLARE_uint64(max_solver_runtime);
DECLARE_uint64(runtime);

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

Simulator::Simulator() : event_manager_(new EventManager) {
  bridge_ = new SimulatorBridge(event_manager_);
}

Simulator::~Simulator() {
  delete bridge_;
  delete event_manager_;
}

void Simulator::ReplaySimulation() {
  // Load the trace ingredients
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

  while (!event_manager_->HasSimulationCompleted(num_scheduling_rounds)) {
    // Load the task events up to the next scheduler run + max_solver_runtime.
    // This assures that we'll have the events ready to be processed when
    // the scheduler will callback to the simulator
    // (via OnSchedulingDecisionsCompletion) after it decides where to place
    // tasks.
    trace_loader->LoadTaskEvents(run_scheduler_at + FLAGS_max_solver_runtime,
                                 bridge_->job_num_tasks());
    // Add the machine heartbeat events up to the next scheduler run.
    for (; run_scheduler_at >= current_heartbeat_time;
         current_heartbeat_time += FLAGS_heartbeat_interval) {
      EventDescriptor event_desc;
      event_desc.set_type(EventDescriptor::MACHINE_HEARTBEAT);
      event_manager_->AddEvent(current_heartbeat_time, event_desc);
    }
    if (run_scheduler_at <= FLAGS_runtime) {
      bridge_->ProcessSimulatorEvents(run_scheduler_at);
      // Current timestamp is at the last event <= run_scheduler_at. We want
      // to make sure that it's at run_scheduler_at so that all the events
      // that happen during scheduling have a timestamp >= run_scheduler_at.
      event_manager_->UpdateCurrentTimestamp(run_scheduler_at);
      uint64_t old_run_scheduler_at = run_scheduler_at;
      // Run the scheduler and update the time to run the scheduler at.
      run_scheduler_at = ScheduleJobsHelper(run_scheduler_at);
      // We don't have to set the time to the previous value because
      // we already processed the events up to run_scheduler_at.
      if (old_run_scheduler_at == run_scheduler_at) {
        // The scheduler had nothing to do.
        // TODO(ionel): Figure out a way of only running the scheduler again
        // when a task has to be rescheduled or a new task has been submited.
        run_scheduler_at += 10000;
      }
      num_scheduling_rounds++;
    } else {
      bridge_->ProcessSimulatorEvents(FLAGS_runtime);
    }
  }
  delete trace_loader;
}

void Simulator::Run() {
  FLAGS_flow_scheduling_solver = FLAGS_solver;
  if (!FLAGS_solver.compare("flowlessly")) {
    FLAGS_incremental_flow = FLAGS_run_incremental_scheduler;
    FLAGS_only_read_assignment_changes = true;
    FLAGS_flow_scheduling_binary =
        SOLVER_DIR "/flowlessly-git/build/flow_scheduler";
  } else if (!FLAGS_solver.compare("cs2")) {
    FLAGS_incremental_flow = false;
    FLAGS_only_read_assignment_changes = false;
    FLAGS_flow_scheduling_binary = SOLVER_DIR "/cs2-git/cs2.exe";
  } else if (!FLAGS_solver.compare("custom")) {
  }

  LOG(INFO) << "Starting Google trace simulator!";
  // Register timeout handler.
  signal(SIGALRM, Simulator::SchedulerTimeoutHandler);
  ReplaySimulation();
  LOG(INFO) << "Simulator has seen " << bridge_->get_num_duplicate_task_ids()
            << " duplicate task ids";
}

uint64_t Simulator::ScheduleJobsHelper(uint64_t run_scheduler_at) {
  // Set a timeout on the scheduler's run
  alarm(FLAGS_scheduler_timeout);
  boost::timer::cpu_timer timer;
  scheduler::SchedulerStats scheduler_stats;
  bridge_->ScheduleJobs(&scheduler_stats);
  alarm(0);
  return event_manager_->GetTimeOfNextSchedulerRun(
      run_scheduler_at, scheduler_stats.scheduler_runtime);
}

void Simulator::SchedulerTimeoutHandler(int sig) {
  signal(SIGALRM, SIG_IGN);
  LOG(FATAL) << "Timeout after waiting for scheduler for "
             << FLAGS_scheduler_timeout << " seconds.";
}

} // namespace sim
} // namespace firmament
