/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

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
DEFINE_bool(run_incremental_scheduler, false,
            "Run the Flowlessly incremental scheduler.");
DEFINE_string(simulation, "google",
              "The type of simulation to run: google | synthetic");
DEFINE_bool(exit_simulation_after_last_task_event, false,
            "True if the simulation should not wait for the running tasks "
            "to complete");
DEFINE_double(trace_speed_up, 1, "Factor by which to speed up events");

DECLARE_uint64(heartbeat_interval);
DECLARE_uint64(max_solver_runtime);
DECLARE_uint64(runtime);
DECLARE_string(solver_runtime_accounting_mode);

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
  if (run_incremental && !FLAGS_solver.compare("cs2")) {
    LOG(ERROR) << "run_incremental_scheduler can not be set with the cs solver";
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

Simulator::Simulator() {
  event_manager_ = new EventManager(&simulated_time_);
  bridge_ = new SimulatorBridge(event_manager_, &simulated_time_);
  scheduler_run_cnt_ = 0;
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
  bool loaded_initial_machines = false;

  while (!event_manager_->HasSimulationCompleted(num_scheduling_rounds)) {
    // Make sure to process all the initial machine additions before we add
    // tasks.
    if (!loaded_initial_machines) {
      loaded_initial_machines = true;
      bridge_->ProcessSimulatorEvents(0);
    }
    // Load the task events up to the next scheduler run + max_solver_runtime.
    // This assures that we'll have the events ready to be processed when
    // the scheduler will callback to the simulator
    // (via OnSchedulingDecisionsCompletion) after it decides where to place
    // tasks.
    bool loaded_events =
      trace_loader->LoadTaskEvents(run_scheduler_at + FLAGS_max_solver_runtime,
                                   bridge_->job_num_tasks());
    // Add the machine heartbeat events up to the next scheduler run.
    for (; run_scheduler_at >= current_heartbeat_time;
         current_heartbeat_time += FLAGS_heartbeat_interval) {
      EventDescriptor event_desc;
      event_desc.set_type(EventDescriptor::MACHINE_HEARTBEAT);
      event_manager_->AddEvent(current_heartbeat_time, event_desc);
    }
    if (run_scheduler_at <= FLAGS_runtime / FLAGS_trace_speed_up) {
      bridge_->ProcessSimulatorEvents(run_scheduler_at);
      // Current timestamp is at the last event <= run_scheduler_at. We want
      // to make sure that it's at run_scheduler_at so that all the events
      // that happen during scheduling have a timestamp >= run_scheduler_at.
      simulated_time_.UpdateCurrentTimestamp(run_scheduler_at);
      // Run the scheduler and update the time to run the scheduler at.
      run_scheduler_at = ScheduleJobsHelper(run_scheduler_at);
      // We don't have to set the time to the previous value because
      // we already processed the events up to run_scheduler_at.
      num_scheduling_rounds++;
    } else {
      bridge_->ProcessSimulatorEvents(FLAGS_runtime / FLAGS_trace_speed_up);
    }
    if (!loaded_events && FLAGS_exit_simulation_after_last_task_event) {
      // The simulator has finished loading all the task events.
      break;
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
        SOLVER_DIR "/flowlessly/src/flowlessly-build/flow_scheduler";
  } else if (!FLAGS_solver.compare("cs2")) {
    FLAGS_incremental_flow = false;
    FLAGS_only_read_assignment_changes = false;
    FLAGS_flow_scheduling_binary = SOLVER_DIR "/cs2/src/cs2/cs2.exe";
  } else if (!FLAGS_solver.compare("custom")) {
  }

  LOG(INFO) << "Starting Google trace simulator!";
  ReplaySimulation();
  LOG(INFO) << "Simulator has seen " << bridge_->get_num_duplicate_task_ids()
            << " duplicate task ids";
}

uint64_t Simulator::ScheduleJobsHelper(uint64_t run_scheduler_at) {
  boost::timer::cpu_timer timer;
  scheduler::SchedulerStats scheduler_stats;
  bridge_->ScheduleJobs(&scheduler_stats);
  scheduler_run_cnt_++;
  alarm(0);
  if (scheduler_run_cnt_ <= 2 && FLAGS_batch_step == 0) {
    return run_scheduler_at;
  } else {
    if (FLAGS_solver_runtime_accounting_mode == "algorithm") {
      if (FLAGS_solver == "cs2") {
        // CS2 doesn't export algorithm runtime. We fallback to solver mode.
        return event_manager_->GetTimeOfNextSchedulerRun(
            run_scheduler_at, scheduler_stats.scheduler_runtime_);
      } else {
        return event_manager_->GetTimeOfNextSchedulerRun(
            run_scheduler_at, scheduler_stats.algorithm_runtime_);
      }
    } else if (FLAGS_solver_runtime_accounting_mode == "solver") {
      return event_manager_->GetTimeOfNextSchedulerRun(
          run_scheduler_at, scheduler_stats.scheduler_runtime_);
    } else if (FLAGS_solver_runtime_accounting_mode == "firmament") {
      return event_manager_->GetTimeOfNextSchedulerRun(
          run_scheduler_at, scheduler_stats.total_runtime_);
    } else {
      LOG(FATAL) << "Unexpected accounting mode: "
                 << FLAGS_solver_runtime_accounting_mode;
    }
  }
}

} // namespace sim
} // namespace firmament
