// TODO: header

#include "sim/scheduler_sim.h"
#include "sim/task_sim.h"

namespace firmament {

SchedulerSim::SchedulerSim(EnsembleSim *ensemble) :
  ensemble_(ensemble) {
}

void SchedulerSim::SubmitJob(JobSim *job, double time) {
  CHECK_NOTNULL(job);
  pending_queue_.push(pair<JobSim*, double>(job, time));
}

void SchedulerSim::ScheduleAllPending() {
  if (pending_queue_.empty())
    return;

  pair<JobSim*, double> head = pending_queue_.front();
  if (head.first == NULL)
    return;
  bool ignore = true;

  pair<JobSim*, double> next;
  VLOG(1) << "In ScheduleAllPending, pending queue size is "
          << pending_queue_.size();
  while (!pending_queue_.empty()) {
    next = pending_queue_.front();
    JobSim *job = next.first;
    if (job == head.first && !ignore)
      break;
    else if (ignore && job == head.first) {
      ignore = false;
    }
    VLOG(1) << "Trying to schedule next job: " << job->name() << " at time " << next.second;
    uint64_t tasks_scheduled = ScheduleJob(job, next.second);
    if (tasks_scheduled > 0) {
      pending_queue_.pop();
      job->set_state(Job::RUNNING);
      head = pending_queue_.front();
      ignore = true;
      if (tasks_scheduled < job->NumTasks()) {
        ++(next.second);
        pending_queue_.push(next);
      } else {
        event_queue_.AddJobEvent(job->finish_time(), SimEvent::JOB_COMPLETED, job,
                                         ensemble_);
      }
    } else {
      ++(pending_queue_.front().second);
      head = pending_queue_.front();
    }
  }
}

uint64_t SchedulerSim::ScheduleJob(JobSim *job, double time) {
  VLOG(1) << "In ScheduleJob for job " << job->name();
  VLOG(1) << "Ensemble " << ensemble_->name() << " has "
          << ensemble_->NumIdleResources(true) << " idle resources";
  uint64_t num_tasks_scheduled = 0;
  double max_task_runtime = 0;
  VLOG(1) << "total no tasks: " << job->NumTasks();
  if (ensemble_->NumIdleResources(true) < job->NumTasks())
    return 0;
  else {
    // Perform the actual scheduling
    vector<TaskSim*> *tasks = job->GetTasks();
    CHECK_NOTNULL(tasks);
    VLOG(2) << "Starting to schedule...";
    vector<Resource*> *own_resources = ensemble_->GetResources();
    // First, we try to schedule on our own, directly joined resources
    VLOG(2) << "Trying directly connected resources...";
    pair<uint64_t, double> schedule_result =
        AttemptScheduleOnResourceSet(own_resources, ensemble_,
                                     job, time);
    num_tasks_scheduled += schedule_result.first;
    max_task_runtime = max(max_task_runtime, schedule_result.second);
    // Second, we try nested ensembles, if we need to
    if (FLAGS_schedule_use_nested) {
      VLOG(2) << "Trying nested ensembles...";
      vector<Ensemble*> *children = ensemble_->GetNestedEnsembles();
      vector<Ensemble*>::iterator c_iter = children->begin();
      while (num_tasks_scheduled < job->NumTasks()
             && c_iter != children->end()) {
        vector<Resource*> *c_resources = (*c_iter)->GetResources();
        schedule_result = AttemptScheduleOnResourceSet(
            c_resources, static_cast<EnsembleSim*>(*c_iter),
            job, time);
        ++c_iter;
        num_tasks_scheduled += schedule_result.first;
        max_task_runtime = max(max_task_runtime, schedule_result.second);
      }
    }
    // Third, we try peered ensembles
    if (FLAGS_schedule_use_peered) {
      VLOG(2) << "Trying peered ensembles...";
      vector<Ensemble*> *peers = ensemble_->GetPeeredEnsembles();
      VLOG(2) << "We have " << peers->size() << " peers!";
      vector<Ensemble*>::iterator p_iter = peers->begin();
      while (num_tasks_scheduled < job->NumTasks()
             && p_iter != peers->end()) {
        VLOG(2) << "Trying out peer " << (*p_iter)->name();
        vector<Resource*> *p_resources = (*p_iter)->GetResources();
        schedule_result = AttemptScheduleOnResourceSet(
            p_resources, static_cast<EnsembleSim*>(*p_iter),
            job, time);
        ++p_iter;
        num_tasks_scheduled += schedule_result.first;
        max_task_runtime = max(max_task_runtime, schedule_result.second);
      }
    }
  }
  if (num_tasks_scheduled > 0) {
    job->set_start_time(time);
    job->set_finish_time(time + max_task_runtime);
    if (num_tasks_scheduled < job->NumTasks())
      VLOG(1) << "Successfully scheduled parts of job " << job->name()
              << "(" << num_tasks_scheduled << " tasks of " << job->NumTasks()
              << " scheduled, " << (job->NumTasks() - num_tasks_scheduled)
              << " remain pending.)";
    else
      VLOG(1) << "Successfully scheduled all tasks in job " << job->name();
  } else {
    VLOG(1) << "Failed to schedule any tasks in job " << job->name() << ", "
            << "it goes back into the pending queue.";
  }
  return num_tasks_scheduled;
}

pair<uint64_t, double> SchedulerSim::AttemptScheduleOnResourceSet(
    vector<Resource*> *resources, EnsembleSim *ensemble, JobSim* job, double time) {
  uint64_t num_tasks_scheduled = 0;
  double max_task_runtime = 0;
  vector<TaskSim*> *tasks = job->GetTasks();
  vector<TaskSim*>::iterator task_iter = tasks->begin();
  CHECK_NOTNULL(job);
  CHECK_NOTNULL(ensemble);
  CHECK_NOTNULL(*task_iter);
  for (vector<Resource*>::iterator res_iter = resources->begin();
       res_iter != resources->end();
       ++res_iter) {
    CHECK_NOTNULL(*res_iter);
    VLOG(2) << "Considering resource " << ensemble->name() << "/"
            << (*res_iter)->name();
    while (task_iter != tasks->end() && (*task_iter)->state() == Task::RUNNING)
      ++task_iter;
    if (task_iter == tasks->end())
      break;
    CHECK_NOTNULL(*task_iter);
    VLOG(2) << "Checking the resource's next availability, is "
            << (*res_iter)->next_available();
    if (!(*res_iter)->busy()) {
      CHECK_NOTNULL(*task_iter);
      // This resource is idle, so we can use it
      (*res_iter)->RunTask(*task_iter);
      set<uint32_t> *ensemble_prefs = ensemble->preferred_jobtypes();
      double task_runtime = 0;
      if (ensemble_prefs->find(job->type()) == ensemble_prefs->end()) {
        // This is a non-preferred ensemble for this job type, so we apply the
        // penalty factor to the task's runtime.
        VLOG(2) << "Applying non-preferred scheduling penalty of "
                << FLAGS_non_preferred_penalty_factor << " to task runtime "
                << (*task_iter)->runtime() << ", new runtime "
                << (FLAGS_non_preferred_penalty_factor * (*task_iter)->runtime());
        task_runtime = FLAGS_non_preferred_penalty_factor * (*task_iter)->runtime();
      } else {
        // This is a preferred ensemble for the task, so we do not apply a
        // penalty.
        task_runtime = (*task_iter)->runtime();
      }
      (*res_iter)->set_next_available(time + task_runtime);
      (*task_iter)->BindToResource(*res_iter);
      (*task_iter)->set_state(Task::RUNNING);
      event_queue_.AddTaskEvent((time + task_runtime),
                                        SimEvent::TASK_COMPLETED, job,
                                        (*task_iter), ensemble);
      max_task_runtime = max(max_task_runtime, task_runtime);
      ++num_tasks_scheduled;
      VLOG(1) << "Successfully scheduled task " << (*task_iter)->name()
              << " on resource " << (*res_iter)->name()
              << ", num_tasks_scheduled is now " << num_tasks_scheduled;
      ++task_iter;
    }
  }
  return pair<uint64_t, double>(num_tasks_scheduled, max_task_runtime);
}

}  // namespace firmament
