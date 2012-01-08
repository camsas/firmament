// TODO: header

#include <stdint.h>
#include <iostream>
#include <vector>

#include <glog/logging.h>
#include <gflags/gflags.h>
#include "base/common.h"
#include "misc/event_logging.h"
#include "misc/utils.h"
#include "sim/workload_generator.h"
#include "sim/ensemble_sim.h"
#include "sim/task_sim.h"
#include "sim/job_sim.h"
#include "sim/scheduler_sim.h"
#include "sim/resource_sim.h"
#include "sim/event_queue.h"
#include "sim/sim_common.h"

using namespace firmament;

namespace firmament {

DEFINE_bool(use_prefs, false, "");
DEFINE_bool(peer_adjacent, true, "");
DEFINE_bool(schedule_use_nested, true,
            "Allow scheduling tasks on nested ensembles.");
DEFINE_bool(schedule_use_peered, true,
            "Allow hand-off of tasks to peered ensembles.");
DEFINE_double(non_preferred_penalty_factor, 2.0,
              "Penalty imposed on jobs running on non-prefered ensembles");
DEFINE_string(output, "output.csv", "Output file name and path");

vector<Resource*> simulation_resources_;
EventLogger *event_logger_;
EventQueue event_queue_;
EnsembleSim *cluster_;
map<uint32_t, vector<EnsembleSim*> > preferences_;

const uint64_t kNumMachines = 2;
const uint64_t kNumCoresPerMachine = 48;

double time_;

void LogUtilizationStats(double time, EnsembleSim *ensemble) {
  uint64_t e_uid = MakeEnsembleUID(ensemble);
  // Count number of resources in use
  uint64_t busy_resources = 0;
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
  uint64_t pending_jobs = ensemble->NumPendingJobs();
  uint64_t pending_tasks = ensemble->NumPendingTasks();
  event_logger_->LogUtilizationValues(time, e_uid, busy_resources,
                                      busy_percent, pending_jobs,
                                      pending_tasks);
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
  CHECK_NOTNULL(completed_tasks);
  CHECK_NOTNULL(completed_jobs);
  // Process all relevant events in the time frame between now and time_util
  SimEvent *evt = event_queue_.GetNextEvent();
  while (evt != NULL && evt->time() >= time_from
         && evt->time() <= time_until) {
    // Process event
//    CHECK_NOTNULL(evt->ensemble());
    switch (evt->type()) {
      case SimEvent::JOB_ARRIVED:
        {
        // Add job to pending queue and set flag to run scheduler
        SimJobEvent *jaevt = static_cast<SimJobEvent*>(evt);
        VLOG(1) << "Handling JOB_ARRIVED event "
                << MakeJobUID(jaevt->job()) << "!";
        event_logger_->LogJobArrivalEvent(jaevt->time(),
                                          MakeJobUID(jaevt->job()),
                                          jaevt->job()->NumTasks());
        jaevt->ensemble()->SubmitJob(jaevt->job(), jaevt->time());
        break;
        }
      case SimEvent::JOB_COMPLETED:
        {
        // Remove job from simulator and log completion
        SimJobEvent *jcevt = static_cast<SimJobEvent*>(evt);
        VLOG(1) << "Handling JOB_COMPLETED event for job with UID "
                << MakeJobUID(jcevt->job()) << "!";
        event_logger_->LogJobCompletionEvent(jcevt->time(),
                                             MakeJobUID(jcevt->job()),
                                             jcevt->job()->runtime());
        break;
        }
      case SimEvent::TASK_COMPLETED:
        {
        // Task has finished, free up the resource it used
        SimTaskEvent *tevt = static_cast<SimTaskEvent*>(evt);
        VLOG(1) << "Handling TASK_COMPLETED event for job with UID "
                << MakeJobUID(tevt->job()) << " and task " << tevt->task()
                << "!";
        tevt->task()->RelinquishBoundResource();
        break;
        }
      default:
        LOG(FATAL) << "Encountered unrecognized event type " << evt->type();
    }
    // Remove this event as we have handled it.
    event_queue_.PopEvent();
    // Handling events changes the cluster, so we try to schedule in the
    // relevant ensemble.
    CHECK_NOTNULL(evt->ensemble());
    evt->ensemble()->RunScheduler(evt->time());
    // After this, we can now delete the event, as we have finished dealing with
    // it.
    delete evt;
    // Get next event
    evt = event_queue_.GetNextEvent();
  }
}

}

int main(int argc, char *argv[]) {
//  google::ParseCommandLineFlags(&argc, &argv, true);
//  google::InitGoogleLogging(argv[0]);
  common::InitFirmament(argc, argv);

  WorkloadGenerator wl_gen;
  event_logger_ =  new EventLogger(FLAGS_output);

  // Our simulated "cluster"
  cluster_ = new EnsembleSim("testcluster");

  // Make ensembles that together form a "cluster"
  for (uint32_t i = 0; i < kNumMachines; ++i) {
    EnsembleSim *e = new EnsembleSim("testmachine" + to_string(i));
    // Add resources to the "machine" ensembles
    for (uint32_t j = 0; j < kNumCoresPerMachine; ++j) {
      ResourceSim *r = new ResourceSim("core" + to_string(j), 1);
      simulation_resources_.push_back(r);
      r->JoinEnsemble(e);
    }
    e->AddPreferredJobType(i % 2);
    preferences_[i % 2].push_back(e);
    string prefs;
    for (set<uint32_t>::const_iterator p_iter =
         e->preferred_jobtypes()->begin();
         p_iter != e->preferred_jobtypes()->end();
         ++p_iter)
      prefs += to_string(*p_iter) + ", ";
    VLOG(1) << "Ensemble prefers job types " << prefs;
    cluster_->AddNestedEnsemble(e);
  }
  LOG(INFO) << "Set up an ensemble with "
            << cluster_->NumResourcesJoinedDirectly() << " resources, "
            << "and " << cluster_->NumNestedEnsembles() << " nested ensembles.";

  // Cross-peer the existing ensembles.
  if (FLAGS_peer_adjacent) {
    vector<Ensemble*> *ens = cluster_->GetNestedEnsembles();
    for (uint64_t i = 0; i < ens->size(); ++i) {
      if (i > 0) {
        VLOG(1) << "Peering ensemble " << (*ens)[i-1]->name() << " with "
                << (*ens)[i]->name();
        (*ens)[i]->AddPeeredEnsemble((*ens)[i-1]);
        (*ens)[i-1]->AddPeeredEnsemble((*ens)[i]);
      }
    }
  }

  LOG(INFO) << "Running for a total simulation time of "
            << FLAGS_simulation_runtime;
  double time = 0.0;

  // *********
  // Main simulation timer loop
  // *********
  while (time < FLAGS_simulation_runtime) {
    double time_to_next_arrival = wl_gen.GetNextInterarrivalTime();
    uint64_t job_size = wl_gen.GetNextJobSize();
    uint32_t job_type = wl_gen.GetNextJobType();
    VLOG(1) << "New job arriving at " << (time + time_to_next_arrival) << ", "
            << "containing " << job_size << " tasks, job type: " << job_type;
    // Create the job
    JobSim *j = new JobSim("job_at_t_" + to_string(time + time_to_next_arrival),
                           job_size, job_type);
    for (uint64_t i = 0; i < job_size; ++i) {
      TaskSim *t = new TaskSim("t" + to_string(i),
                               wl_gen.GetNextTaskDuration());
      t->set_job(j);
      j->AddTask(t);
    }
    // Add job to cluster
    if (FLAGS_use_prefs) {
      EnsembleSim *e = preferences_[j->type()][0];
      VLOG(1) << "Preference-based assignment, choosing ensemble " << e->name()
              << " for job type " << j->type();
      event_queue_.AddJobEvent(time + time_to_next_arrival,
                               SimEvent::JOB_ARRIVED, j, e);
    } else {
      event_queue_.AddJobEvent(time + time_to_next_arrival,
                               SimEvent::JOB_ARRIVED, j, cluster_);
    }
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
