// TODO: header

#include <stdint.h>
#include <iostream>
#include <vector>

#include <glog/logging.h>
#include <gflags/gflags.h>

#include <boost/functional/hash.hpp>

#include "base/common.h"
#include "misc/event_logging.h"
#include "sim/workload_generator.h"
#include "sim/ensemble_sim.h"
#include "sim/task_sim.h"
#include "sim/job_sim.h"
#include "sim/scheduler_sim.h"
#include "sim/resource_sim.h"
#include "sim/event_queue.h"
#include "sim/sim_common.h"

using namespace firmament;

vector<Resource*> simulation_resources_;
EventLogger *event_logger_;
EventQueue event_queue_;
EnsembleSim *cluster_;

const uint64_t kNumMachines = 1;
const uint64_t kNumCoresPerMachine = 48;

double time_;

uint64_t MakeJobUID(Job *job) {
  boost::hash<string> hasher;
  return hasher(job->name());
}

uint64_t MakeEnsembleUID(Ensemble *ens) {
  boost::hash<string> hasher;
  return hasher(ens->name());
}


void LogUtilizationStats(double time, EnsembleSim *ensemble) {
  uint64_t e_uid = MakeEnsembleUID(ensemble);
  // Count number of resources in use
  double busy_resources = 0;
  vector<Resource*> *resources = ensemble->GetResources();
  for (vector<Resource*>::iterator res_iter = resources->begin();
       res_iter != resources->end();
       ++res_iter) {
    if ((*res_iter)->next_available() > time)
      ++busy_resources;
  }
  double busy_percent;
  if (ensemble->NumResourcesJoinedDirectly() > 0)
    busy_percent = static_cast<double>(busy_resources) /
      static_cast<double>(ensemble->NumResourcesJoinedDirectly());
  else
    busy_percent = 0;
  uint64_t pending = ensemble->NumPending();
  event_logger_->LogUtilizationValues(time, e_uid, busy_resources,
                                      busy_percent, pending);
}

void LogUtilizationStatsRecursively(double time, EnsembleSim *ensemble) {
  LogUtilizationStats(time, ensemble);
  vector<Ensemble*> *children = ensemble->GetNestedEnsembles();
  for (vector<Ensemble*>::const_iterator c_iter = children->begin();
       c_iter != children->end();
       ++c_iter)
    LogUtilizationStatsRecursively(time, static_cast<EnsembleSim*>(*c_iter));
}

void RunSimulationUntil(double time_from, double time_until,
                        vector<TaskSim*> *completed_tasks,
                        vector<JobSim*> *completed_jobs) {
  // Process all relevant events in the time frame between now and time_util
  SimEvent *evt = event_queue_.GetNextEvent();
  while (evt != NULL && evt->time() <= time_until) {
    // Process event
    switch (evt->type()) {
      case SimEvent::JOB_ARRIVED:
        // Add job to pending queue and set flag to run scheduler
        VLOG(1) << "Handling JOB_ARRIVED event "
                << MakeJobUID(evt->job()) << "!";
        event_logger_->LogJobArrivalEvent(evt->time(), MakeJobUID(evt->job()),
                                          evt->job()->NumTasks());
        cluster_->SubmitJob(evt->job(), evt->time());
        break;
      case SimEvent::JOB_COMPLETED:
        // Remove job from simulator and log completion
        VLOG(1) << "Handling JOB_COMPLETED event for job with UID "
                << MakeJobUID(evt->job()) << "!";
        event_logger_->LogJobCompletionEvent(evt->time(),
                                             MakeJobUID(evt->job()),
                                             evt->job()->runtime());
        break;
      case SimEvent::TASK_COMPLETED:
        // Task has finished, free up the resource it used
        VLOG(1) << "Handling TASK_COMPLETED event for job with UID "
                << MakeJobUID(evt->job()) << " and task " << evt->task() << "!";
        evt->task()->RelinquishBoundResource();
        break;
      default:
        LOG(FATAL) << "Encountered unrecognized event type " << evt->type();
    }
    // Remove this event as we have handled it.
    event_queue_.PopEvent();
    delete evt;
    // Handling events changes the cluster, so we try to schedule.
    cluster_->RunScheduler();
    // Get next event
    evt = event_queue_.GetNextEvent();
  }
//  scheduler_->ScheduleAllPending(&event_queue_);
}

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  WorkloadGenerator wl_gen;
  event_logger_ =  new EventLogger("test.log");

  // Our simulated "cluster"
  cluster_ = new EnsembleSim("testcluster", &event_queue_);

  // Make ensembles that together form a "cluster"
  for (uint32_t i = 0; i < kNumMachines; ++i) {
    EnsembleSim *e = new EnsembleSim("testmachine" + to_string(i),
                                     &event_queue_);
    // Add resources to the "machine" ensembles
    for (uint32_t i = 0; i < kNumCoresPerMachine; ++i) {
      ResourceSim *r = new ResourceSim("core" + to_string(i), 1);
      simulation_resources_.push_back(r);
      r->JoinEnsemble(e);
    }
    cluster_->AddNestedEnsemble(e);
  }
  LOG(INFO) << "Set up an ensemble with "
            << cluster_->NumResourcesJoinedDirectly() << " resources, "
            << "and " << cluster_->NumNestedEnsembles() << " nested ensembles.";

  LOG(INFO) << "Running for a total simulation time of "
            << FLAGS_simulation_runtime;
  double time = 0.0;

  // *********
  // Main simulation timer loop
  // *********
  while (time < FLAGS_simulation_runtime) {
    double time_to_next_arrival = wl_gen.GetNextInterarrivalTime();
    uint64_t job_size = wl_gen.GetNextJobSize();
    VLOG(1) << "New job arriving at " << (time + time_to_next_arrival) << ", "
            << "containing " << job_size << " tasks.";
    // Create the job
    JobSim *j = new JobSim("job_at_t_" + to_string(time + time_to_next_arrival),
                           job_size);
    for (uint64_t i = 0; i < job_size; ++i) {
      TaskSim *t = new TaskSim("t" + to_string(i),
                               wl_gen.GetNextTaskDuration());
      t->set_job(j);
      j->AddTask(t);
    }
    // Add job to cluster
    event_queue_.AddJobEvent(time + time_to_next_arrival,
                             SimEvent::JOB_ARRIVED, j);
    // Update timer
    double prev_time = time;
    time += time_to_next_arrival;
    vector<TaskSim*> completed_tasks;
    vector<JobSim*> completed_jobs;
    // Run cluster scheduling loop
    RunSimulationUntil(prev_time, time, &completed_tasks, &completed_jobs);

    // Log utilization stats
    LogUtilizationStatsRecursively(time, cluster_);

    // Remove things that have completed
    for (vector<TaskSim*>::const_iterator t_iter = completed_tasks.begin();
         t_iter != completed_tasks.end();
         ++t_iter) {
      CHECK((*t_iter)->job()->TaskState(
          (*t_iter)->index_in_job()) == Task::COMPLETED);
      delete *t_iter;
    }
    for (vector<TaskSim*>::const_iterator t_iter = completed_tasks.begin();
         t_iter != completed_tasks.end();
         ++t_iter)
      delete *t_iter;
  }

  delete event_logger_;
}
