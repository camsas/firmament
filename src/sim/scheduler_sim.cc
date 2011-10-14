// TODO: header

#include "sim/scheduler_sim.h"
#include "sim/task_sim.h"

namespace firmament {

SchedulerSim::SchedulerSim(EnsembleSim *ensemble, EventQueue *event_queue) :
  ensemble_(ensemble), global_event_queue_(event_queue) {
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
        global_event_queue_->AddJobEvent(job->finish_time(), SimEvent::JOB_COMPLETED, job);
      }
    } else {
      ++(pending_queue_.front().second);
    }
  }
}

uint64_t SchedulerSim::ScheduleJob(JobSim *job, double time) {
  VLOG(1) << "In ScheduleJob for job " << job->name();
  VLOG(1) << "Ensemble has " << ensemble_->NumIdleResources()
          << " idle resources";
  uint64_t num_tasks_scheduled = 0;
  double max_task_runtime = 0;
  VLOG(1) << "total no tasks: " << job->NumTasks();
  if (ensemble_->NumIdleResources() < job->NumTasks())
    return false;
  else {
    // Perform the actual scheduling
    vector<TaskSim*> *tasks = job->GetTasks();
    CHECK_NOTNULL(tasks);
    vector<TaskSim*>::iterator task_iter = tasks->begin();
    VLOG(2) << "Starting to schedule...";
    vector<Resource*> *resources = ensemble_->GetResources();
    for (vector<Resource*>::iterator res_iter = resources->begin();
         res_iter != resources->end();
         ++res_iter) {
      VLOG(2) << "Considering resource " << (*res_iter)->name();
      while (task_iter != tasks->end() && (*task_iter)->state() == Task::RUNNING)
        ++task_iter;
      if (task_iter == tasks->end())
        break;
      VLOG(2) << *res_iter;
      VLOG(2) << "Checking the resource's next availability, is "
              << (*res_iter)->next_available();
      if (!ensemble_->ResourceBusy(*res_iter)) {
        // This resource is idle, so we can use it
        (*res_iter)->RunTask(*task_iter);
        (*res_iter)->set_next_available(time + (*task_iter)->runtime());
        (*task_iter)->BindToResource(static_cast<ResourceSim*>(*res_iter));
        (*task_iter)->set_state(Task::RUNNING);
        global_event_queue_->AddTaskEvent(time + (*task_iter)->runtime(),
                                          SimEvent::TASK_COMPLETED, job,
                                          (*task_iter));
        max_task_runtime = max(max_task_runtime, (*task_iter)->runtime());
        VLOG(1) << "Successfully scheduled task " << (*task_iter)->name()
                << " on resource " << (*res_iter)->name();
        ++task_iter;
        ++num_tasks_scheduled;
      }
    }
    //CHECK(task_iter == tasks->end());
  }
  if (num_tasks_scheduled > 0) {
    job->set_start_time(time);
    job->set_finish_time(time + max_task_runtime);
    if (num_tasks_scheduled < job->NumTasks())
      VLOG(1) << "Successfully scheduled parts of job " << job->name()
              << "(" << num_tasks_scheduled << " tasks scheduled, "
              << (num_tasks_scheduled - job->NumTasks()) << " remain pending.)";
    else
      VLOG(1) << "Successfully scheduled all tasks in job " << job->name();
  } else {
    VLOG(1) << "Failed to schedule any tasks in job " << job->name() << ", "
            << "it goes back into the pending queue.";
  }
  return num_tasks_scheduled;
}

}  // namespace firmament
