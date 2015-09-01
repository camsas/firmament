// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// General abstract superclass for event-driven schedulers. This contains shared
// implementation, e.g. task binding and remote delegation mechanisms.

#include "scheduling/event_driven_scheduler.h"

#include <deque>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "storage/reference_types.h"
#include "storage/reference_utils.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "engine/local_executor.h"
#include "engine/remote_executor.h"
#include "engine/simulated_executor.h"
#include "scheduling/knowledge_base.h"
#include "storage/object_store_interface.h"

#define TASK_FAIL_TIMEOUT 60000000ULL

namespace firmament {
namespace scheduler {

using executor::LocalExecutor;
using executor::RemoteExecutor;
using executor::SimulatedExecutor;
using common::pb_to_set;
using store::ObjectStoreInterface;

EventDrivenScheduler::EventDrivenScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    ResourceTopologyNodeDescriptor* resource_topology,
    shared_ptr<ObjectStoreInterface> object_store,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    shared_ptr<TopologyManager> topo_mgr,
    MessagingAdapterInterface<BaseMessage>* m_adapter,
    EventNotifierInterface* event_notifier,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri)
  : SchedulerInterface(job_map, knowledge_base, resource_map, resource_topology,
                       object_store, task_map),
      coordinator_uri_(coordinator_uri),
      coordinator_res_id_(coordinator_res_id),
      event_notifier_(event_notifier),
      m_adapter_ptr_(m_adapter),
      topology_manager_(topo_mgr) {
  VLOG(1) << "EventDrivenScheduler initiated.";
}

EventDrivenScheduler::~EventDrivenScheduler() {
  for (map<ResourceID_t, ExecutorInterface*>::const_iterator
       exec_iter = executors_.begin();
       exec_iter != executors_.end();
       ++exec_iter)
    delete exec_iter->second;
  executors_.clear();
  delete event_notifier_;
}

void EventDrivenScheduler::AddJob(JobDescriptor* jd_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  CHECK(InsertIfNotPresent(&jobs_to_schedule_,
                           JobIDFromString(jd_ptr->uuid()), jd_ptr));
}

void EventDrivenScheduler::BindTaskToResource(TaskDescriptor* td_ptr,
                                              ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  // Mark resource as busy and record task binding
  rd_ptr->set_state(ResourceDescriptor::RESOURCE_BUSY);
  rd_ptr->set_current_running_task(task_id);
  CHECK(InsertIfNotPresent(&task_bindings_, task_id, res_id));
  resource_bindings_.insert(pair<ResourceID_t, TaskID_t>(res_id, task_id));
}

ResourceID_t* EventDrivenScheduler::BoundResourceForTask(TaskID_t task_id) {
  ResourceID_t* rid = FindOrNull(task_bindings_, task_id);
  return rid;
}

vector<TaskID_t> EventDrivenScheduler::BoundTasksForResource(
  ResourceID_t res_id) {
  vector<TaskID_t> tasks;
  pair<multimap<ResourceID_t, TaskID_t>::iterator,
       multimap<ResourceID_t, TaskID_t>::iterator> range_it =
    resource_bindings_.equal_range(res_id);
  for (; range_it.first != range_it.second; range_it.first++) {
    tasks.push_back(range_it.first->second);
  }
  return tasks;
}

void EventDrivenScheduler::CheckRunningTasksHealth() {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  for (map<ResourceID_t, ExecutorInterface*>::const_iterator
       it = executors_.begin();
       it != executors_.end();
       ++it) {
    vector<TaskID_t> failed_tasks;
    if (!it->second->CheckRunningTasksHealth(&failed_tasks)) {
      // Handle task failures
      for (vector<TaskID_t>::const_iterator it = failed_tasks.begin();
           it != failed_tasks.end();
           ++it) {
        TaskDescriptor* td = FindPtrOrNull(*task_map_, *it);
        CHECK_NOTNULL(td);
        if (td->state() != TaskDescriptor::COMPLETED &&
            td->last_heartbeat_time() <=
            (GetCurrentTimestamp() - TASK_FAIL_TIMEOUT)) {
          LOG(INFO) << "Task " << td->uid() << " has not reported heartbeats "
                    << "for " << (TASK_FAIL_TIMEOUT / 1000000) << "s and its "
                    << "handler thread has exited. Declaring it FAILED!";
          HandleTaskFailure(td);
        }
      }
    }
  }
}

void EventDrivenScheduler::ClearScheduledJobs() {
  for (auto it = jobs_to_schedule_.begin(); it != jobs_to_schedule_.end(); ) {
    if (RunnableTasksForJob(it->second).size() == 0) {
      it = jobs_to_schedule_.erase(it);
    } else {
      it++;
    }
  }
}

void EventDrivenScheduler::DebugPrintRunnableTasks() {
  VLOG(1) << "Runnable task queue now contains " << runnable_tasks_.size()
          << " elements:";
  for (set<TaskID_t>::const_iterator t_iter = runnable_tasks_.begin();
       t_iter != runnable_tasks_.end();
       ++t_iter)
    VLOG(1) << "  " << *t_iter;
}

void EventDrivenScheduler::DeregisterResource(ResourceID_t res_id) {
  VLOG(1) << "Removing executor for resource " << res_id
          << " which is now deregistered from this scheduler.";
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
  CHECK_NOTNULL(exec);
  // Terminate any running tasks on the resource.
  // TODO(ionel): Terminate the tasks running on res_id or any of
  // its sub-resources. Make sure the tasks get re-scheduled.
  // exec->TerminateAllTasks();
  // Remove the executor for the resource.
  CHECK(executors_.erase(res_id));
  delete exec;
}

void EventDrivenScheduler::ExecuteTask(TaskDescriptor* td_ptr,
                                       ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  // Remove the task from the runnable set
  CHECK_EQ(runnable_tasks_.erase(task_id), 1)
    << "Failed to remove task " << task_id << " from runnable set!";
  if (VLOG_IS_ON(2))
    DebugPrintRunnableTasks();
  // Find an executor for this resource.
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
  CHECK_NOTNULL(exec);
  // Actually kick off the task
  // N.B. This is an asynchronous call, as the executor will spawn a thread.
  exec->RunTask(td_ptr, !td_ptr->inject_task_lib());
  // Mark task as running and report
  td_ptr->set_state(TaskDescriptor::RUNNING);
  td_ptr->set_scheduled_to_resource(rd_ptr->uuid());
  VLOG(1) << "Task " << task_id << " running.";
}

void EventDrivenScheduler::HandleJobCompletion(JobID_t job_id) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  JobDescriptor* jd = FindOrNull(*job_map_, job_id);
  CHECK_NOTNULL(jd);
  jd->set_state(JobDescriptor::COMPLETED);
  if (event_notifier_) {
    event_notifier_->OnJobCompletion(job_id);
  }
}

void EventDrivenScheduler::HandleReferenceStateChange(
    const ReferenceInterface& old_ref,
    const ReferenceInterface& new_ref,
    TaskDescriptor* td_ptr) {
  CHECK_EQ(old_ref.id(), new_ref.id());
  // Perform the appropriate actions for a reference changing status
  if (old_ref.Consumable() && new_ref.Consumable()) {
    // no change, return
    return;
  } else if (!old_ref.Consumable() && new_ref.Consumable()) {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    // something became available, unblock the waiting tasks
    set<TaskDescriptor*>* tasks = FindOrNull(reference_subscriptions_,
                                             old_ref.id());
    if (!tasks) {
      // Nobody cares about this ref, so we don't do anything
      return;
    }
    for (set<TaskDescriptor*>::iterator it = tasks->begin();
         it != tasks->end();
         ++it) {
      CHECK_NOTNULL(*it);
      bool any_outstanding = false;
      if ((*it)->state() == TaskDescriptor::COMPLETED ||
          (*it)->state() == TaskDescriptor::RUNNING)
        continue;
      for (RepeatedPtrField<ReferenceDescriptor>::const_iterator ref_it =
           (*it)->dependencies().begin();
           ref_it != (*it)->dependencies().end();
           ++ref_it) {
        set<ReferenceInterface*>* deps = object_store_->GetReferences(
            DataObjectIDFromProtobuf(ref_it->id()));
        for (set<ReferenceInterface*>::const_iterator dep_it = deps->begin();
             dep_it != deps->end();
             ++dep_it)
          if (!(*dep_it)->Consumable())
            any_outstanding = true;
      }
      if (!any_outstanding) {
        (*it)->set_state(TaskDescriptor::RUNNABLE);
        runnable_tasks_.insert((*it)->uid());
        blocked_tasks_.erase(*it);
      }
    }
  } else if (old_ref.Consumable() && !new_ref.Consumable()) {
    // failure or reference loss, re-run producing task(s)
    // TODO(malte): implement
  } else {
    // neither is consumable, so no scheduling implications
    return;
  }
}

void EventDrivenScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                                TaskFinalReport* report) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Find resource for task
  ResourceID_t* res_id_ptr = BoundResourceForTask(td_ptr->uid());
  CHECK_NOTNULL(res_id_ptr);
  // This copy is necessary because UnbindTaskFromResource ends up deleting the
  // ResourceID_t pointed to by res_id_ptr
  ResourceID_t res_id_tmp = *res_id_ptr;
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
  CHECK_NOTNULL(rs_ptr);
  VLOG(1) << "Handling completion of task " << td_ptr->uid()
          << ", freeing resource " << res_id_tmp;
  CHECK(UnbindTaskFromResource(td_ptr, res_id_tmp));
  // Record final report
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id_tmp);
  CHECK_NOTNULL(exec);
  exec->HandleTaskCompletion(td_ptr, report);
  // Store the final report in the TD for future reference
  td_ptr->mutable_final_report()->CopyFrom(*report);
  if (event_notifier_) {
    event_notifier_->OnTaskCompletion(td_ptr, rs_ptr->mutable_descriptor());
  }
}

void EventDrivenScheduler::HandleTaskDelegationFailure(
    TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Find the resource where the task was supposed to be delegated
  ResourceID_t* res_id_ptr = BoundResourceForTask(td_ptr->uid());
  CHECK_NOTNULL(res_id_ptr);
  CHECK(UnbindTaskFromResource(td_ptr, *res_id_ptr));
  // Go back to try scheduling this task again
  td_ptr->set_state(TaskDescriptor::RUNNABLE);
  runnable_tasks_.insert(td_ptr->uid());
  td_ptr->clear_start_time();
  JobDescriptor* jd = FindOrNull(*job_map_, JobIDFromString(td_ptr->job_id()));
  CHECK_NOTNULL(jd);
  // Try again to schedule...
  ScheduleJob(jd);
}

void EventDrivenScheduler::HandleTaskEviction(TaskDescriptor* td_ptr,
                                              ResourceDescriptor* rd_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  VLOG(1) << "Handling completion of task " << td_ptr->uid()
          << ", freeing resource " << res_id;
  CHECK(UnbindTaskFromResource(td_ptr, res_id));
  // Record final report
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
  CHECK_NOTNULL(exec);
  exec->HandleTaskEviction(td_ptr);
  if (event_notifier_) {
    event_notifier_->OnTaskEviction(td_ptr, rd_ptr);
  }
}

void EventDrivenScheduler::HandleTaskFailure(TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Find resource for task
  ResourceID_t* res_id_ptr = FindOrNull(task_bindings_, td_ptr->uid());
  CHECK_NOTNULL(res_id_ptr);
  // This copy is necessary because UnbindTaskFromResource ends up deleting the
  // ResourceID_t pointed to by res_id_ptr
  ResourceID_t res_id_tmp = *res_id_ptr;
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
  CHECK_NOTNULL(rs_ptr);
  VLOG(1) << "Handling failure of task " << td_ptr->uid()
          << ", freeing resource " << res_id_tmp;
  // TODO(malte): We should probably check if the resource has failed at this
  // point...
  // Executor cleanup: drop the task from the health checker's list, etc.
  ExecutorInterface* exec_ptr = FindPtrOrNull(executors_, res_id_tmp);
  CHECK_NOTNULL(exec_ptr);
  exec_ptr->HandleTaskFailure(td_ptr);
  // Remove the task's resource binding (as it is no longer currently bound)
  CHECK(UnbindTaskFromResource(td_ptr, res_id_tmp));
  // Set the task to "failed" state and deal with the consequences
  // (The state may already have been changed elsewhere, but since the failure
  // case can arise unexpectedly, we set it again here).
  td_ptr->set_state(TaskDescriptor::FAILED);
  // We only need to run the scheduler if the failed task was not delegated from
  // elsewhere, i.e. if it is managed by the local scheduler. If so, we kick the
  // scheduler if we haven't exceeded the retry limit.
  if (td_ptr->has_delegated_from()) {
    // XXX(malte): Need to forward message about task failure to delegator here!
  }
  if (event_notifier_) {
    event_notifier_->OnTaskFailure(td_ptr, rs_ptr->mutable_descriptor());
  }
}

void EventDrivenScheduler::HandleTaskFinalReport(const TaskFinalReport& report,
                                                 TaskDescriptor* td_ptr) {
  CHECK_NOTNULL(td_ptr);
  VLOG(1) << "Handling task final report for " << report.task_id();
  // Add the report to the TD if the task is not local (otherwise, the
  // scheduler has already done so)
  if (td_ptr->has_delegated_to()) {
    td_ptr->mutable_final_report()->CopyFrom(report);
  }
}

void EventDrivenScheduler::HandleTaskMigration(TaskDescriptor* td_ptr,
                                               ResourceDescriptor* rd_ptr) {
  CHECK_NOTNULL(td_ptr);
  CHECK_NOTNULL(rd_ptr);
  VLOG(1) << "Migrating task " << td_ptr->uid() << " to resource "
          << rd_ptr->uuid();
  rd_ptr->set_state(ResourceDescriptor::RESOURCE_BUSY);
  td_ptr->set_state(TaskDescriptor::RUNNING);
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t* old_res_id_ptr = FindOrNull(task_bindings_, task_id);
  CHECK_NOTNULL(old_res_id_ptr);
  // XXX(ionel): Assumes only one task per resource.
  ResourceStatus* old_rs = FindPtrOrNull(*resource_map_, *old_res_id_ptr);
  CHECK_NOTNULL(old_rs);
  ResourceDescriptor* old_rd = old_rs->mutable_descriptor();
  old_rd->set_state(ResourceDescriptor::RESOURCE_IDLE);
  rd_ptr->set_current_running_task(task_id);
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  InsertOrUpdate(&task_bindings_, task_id, res_id);
  if (event_notifier_) {
    event_notifier_->OnTaskMigration(td_ptr, rd_ptr);
  }
}

void EventDrivenScheduler::HandleTaskPlacement(
    TaskDescriptor* td_ptr,
    ResourceDescriptor* rd_ptr) {
  CHECK_NOTNULL(td_ptr);
  CHECK_NOTNULL(rd_ptr);
  TaskID_t task_id = td_ptr->uid();
  VLOG(1) << "Placing task " << task_id << " on resource " << rd_ptr->uuid();
  BindTaskToResource(td_ptr, rd_ptr);
  // Tag the job to which this task belongs as running
  JobDescriptor* jd =
    FindOrNull(*job_map_, JobIDFromString(td_ptr->job_id()));
  if (jd->state() != JobDescriptor::RUNNING)
    jd->set_state(JobDescriptor::RUNNING);
  ExecuteTask(td_ptr, rd_ptr);
  if (event_notifier_) {
    event_notifier_->OnTaskPlacement(td_ptr, rd_ptr);
  }
}

void EventDrivenScheduler::KillRunningTask(
    TaskID_t task_id,
    TaskKillMessage::TaskKillReason reason) {
  // Check if this task is managed by this coordinator
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  if (!td_ptr) {
    LOG(ERROR) << "Tried to kill unknown task " << task_id;
    return;
  }
  // Check if we have a bound resource for the task and if it is marked as
  // running
  ResourceID_t* rid = BoundResourceForTask(task_id);
  if (td_ptr->state() != TaskDescriptor::RUNNING || !rid) {
    LOG(ERROR) << "Task " << task_id << " is not running locally, "
               << "so cannot kill it!";
    return;
  }
  // Find the current remote endpoint for this task
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  // Manufacture the message
  BaseMessage bm;
  SUBMSG_WRITE(bm, task_kill, task_id, task_id);
  SUBMSG_WRITE(bm, task_kill, reason, reason);
  // Send the message
  LOG(INFO) << "Sending KILL message to task " << task_id << " on resource "
            << *rid << " (endpoint: " << td->last_heartbeat_location()  << ")";
  m_adapter_ptr_->SendMessageToEndpoint(td->last_heartbeat_location(), bm);
}

// Implementation of lazy graph reduction algorithm, as per p58, fig. 3.5 in
// Derek Murray's thesis on CIEL.
uint64_t EventDrivenScheduler::LazyGraphReduction(
    const set<DataObjectID_t*>& output_ids,
    TaskDescriptor* root_task,
    const JobID_t& job_id) {
  VLOG(2) << "Performing lazy graph reduction";
  // Local data structures
  deque<TaskDescriptor*> newly_active_tasks;
  bool do_schedule = false;
  // Add expected producer for object_id to queue, if the object reference is
  // not already concrete.
  VLOG(2) << "for a job with " << output_ids.size() << " outputs";
  for (set<DataObjectID_t*>::const_iterator output_id_iter = output_ids.begin();
       output_id_iter != output_ids.end();
       ++output_id_iter) {
    set<ReferenceInterface*> refs = ReferencesForID(**output_id_iter);
    if (!refs.empty()) {
      for (set<ReferenceInterface*>::const_iterator ref_iter = refs.begin();
           ref_iter != refs.end();
           ++ref_iter) {
        // TODO(malte): this logic is very simple-minded; sometimes, it may be
        // beneficial to produce locally instead of fetching remotely!
        if ((*ref_iter)->Consumable() &&
            !(*ref_iter)->desc().non_deterministic()) {
          // skip this output, as it is already present
          continue;
        }
      }
    }
    // otherwise, we add the producer for said output reference to the queue, if
    // it is not already scheduled.
    // N.B.: by this point, we know that no concrete reference exists in the set
    // of references available.
    set<TaskDescriptor*> tasks =
        ProducingTasksForDataObjectID(**output_id_iter, job_id);
    CHECK_GT(tasks.size(), 0) << "Could not find task producing output ID "
                             << **output_id_iter;
    for (set<TaskDescriptor*>::const_iterator t_iter = tasks.begin();
         t_iter != tasks.end();
         ++t_iter) {
      TaskDescriptor* task = *t_iter;
      if (task->state() == TaskDescriptor::CREATED ||
          task->state() == TaskDescriptor::FAILED) {
        VLOG(2) << "Setting task " << task->uid() << " active as it produces "
                << "output " << **output_id_iter << ", which we're interested "
                << "in.";
        task->set_state(TaskDescriptor::BLOCKING);
        newly_active_tasks.push_back(task);
      }
    }
  }
  // Add root task to queue
  TaskDescriptor* rtd_ptr = FindPtrOrNull(*task_map_, root_task->uid());
  CHECK_NOTNULL(rtd_ptr);
  // Only add the root task if it is not already scheduled, running, done
  // or failed.
  if (rtd_ptr->state() == TaskDescriptor::CREATED)
    newly_active_tasks.push_back(rtd_ptr);
  // Keep iterating over tasks as long as there are more to visit
  while (!newly_active_tasks.empty()) {
    TaskDescriptor* current_task = newly_active_tasks.front();
    VLOG(2) << "Next active task considered is " << current_task->uid();
    newly_active_tasks.pop_front();
    // Find any unfulfilled dependencies
    bool will_block = false;
    for (RepeatedPtrField<ReferenceDescriptor>::const_iterator iter =
         current_task->dependencies().begin();
         iter != current_task->dependencies().end();
         ++iter) {
      ReferenceInterface* ref = ReferenceFromDescriptor(*iter);
      // Subscribe the current task to the reference, to enable it to be
      // unblocked if it becomes available.
      // Note that we subscribe even tasks whose dependencies are concrete, as
      // they may later disappear and failures will be handled via the
      // subscription relationship.
      set<TaskDescriptor*>* subscribers = FindOrNull(
          reference_subscriptions_, ref->id());
      if (!subscribers) {
        InsertIfNotPresent(&reference_subscriptions_,
                           ref->id(), set<TaskDescriptor*>());
        subscribers = FindOrNull(reference_subscriptions_, ref->id());
      }
      subscribers->insert(current_task);
      // Now proceed to check if it is available
      if (ref->Consumable()) {
        // This input reference is consumable. So far, so good.
        VLOG(2) << "Task " << current_task->uid() << "'s dependency " << *ref
                << " is consumable.";
      } else {
        // This input reference is not consumable; set the task to block and
        // look at its predecessors (which may produce the necessary input, and
        // may be runnable).
        VLOG(2) << "Task " << current_task->uid()
                << " is blocking on reference " << *ref;
        will_block = true;
        // Look at predecessor task (producing this reference)
        set<TaskDescriptor*> producing_tasks =
            ProducingTasksForDataObjectID(ref->id(), job_id);
        if (producing_tasks.size() == 0) {
          LOG(ERROR) << "Failed to find producing task for ref " << ref
                     << "; will block until it is produced.";
          continue;
        }
        for (set<TaskDescriptor*>::const_iterator t_iter =
             producing_tasks.begin();
             t_iter != producing_tasks.end();
             ++t_iter) {
          TaskDescriptor* task = *t_iter;
          if (task->state() == TaskDescriptor::CREATED ||
              task->state() == TaskDescriptor::COMPLETED) {
            task->set_state(TaskDescriptor::BLOCKING);
            newly_active_tasks.push_back(task);
          }
        }
      }
    }
    // Process any eager children not related via dependencies
    for (RepeatedPtrField<TaskDescriptor>::iterator it =
         current_task->mutable_spawned()->begin();
         it != current_task->mutable_spawned()->end();
         ++it) {
      if (it->outputs_size() == 0)
        newly_active_tasks.push_back(&(*it));
    }
    if (!will_block || (current_task->dependencies_size() == 0
                        && current_task->outputs_size() == 0)) {
      // This task is runnable
      VLOG(2) << "Adding task " << current_task->uid() << " to RUNNABLE set.";
      current_task->set_state(TaskDescriptor::RUNNABLE);
      runnable_tasks_.insert(current_task->uid());
    }
  }
  VLOG(1) << "do_schedule is " << do_schedule << ", runnable_task set "
          << "contains " << runnable_tasks_.size() << " tasks.";
  return runnable_tasks_.size();
}

bool EventDrivenScheduler::PlaceDelegatedTask(TaskDescriptor* td,
                                              ResourceID_t target_resource) {
  // Check if the resource is available
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, target_resource);
  // Do we know about this resource?
  if (!rs_ptr) {
    // Requested resource unknown or does not exist any more
    LOG(WARNING) << "Attempted to place delegated task " << td->uid()
                 << " on resource " << target_resource << ", which is "
                 << "unknown!";
    return false;
  }
  ResourceDescriptor* rd = rs_ptr->mutable_descriptor();
  CHECK_NOTNULL(rd);
  // Is the resource still idle?
  if (rd->state() != ResourceDescriptor::RESOURCE_IDLE) {
    // Resource is no longer idle
    LOG(WARNING) << "Attempted to place delegated task " << td->uid()
                 << " on resource " << target_resource << ", which is "
                 << "not idle!";
    return false;
  }
  // Otherwise, bind the task
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  runnable_tasks_.insert(td->uid());
  InsertIfNotPresent(task_map_.get(), td->uid(), td);
  HandleTaskPlacement(td, rd);
  td->set_state(TaskDescriptor::RUNNING);
  return true;
}

set<TaskDescriptor*> EventDrivenScheduler::ProducingTasksForDataObjectID(
    const DataObjectID_t& id, const JobID_t& cur_job) {
  // Find all producing tasks for an object ID, as indicated by the references
  // stored locally.
  set<TaskDescriptor*> producing_tasks;
  VLOG(2) << "Looking up producing task for object " << id;
  set<ReferenceInterface*>* refs = object_store_->GetReferences(id);
  if (!refs)
    return producing_tasks;
  for (set<ReferenceInterface*>::const_iterator ref_iter = refs->begin();
       ref_iter != refs->end();
       ++ref_iter) {
    if ((*ref_iter)->desc().has_producing_task()) {
      TaskDescriptor** td_ptr =
          FindOrNull(*task_map_, (*ref_iter)->desc().producing_task());
      if (td_ptr) {
        if (JobIDFromString((*td_ptr)->job_id()) == cur_job) {
          VLOG(2) << "... is " << (*td_ptr)->uid() << " (in THIS job).";
          producing_tasks.insert(*td_ptr);
        } else {
          VLOG(2) << "... is " << (*td_ptr)->uid() << " (in job "
                  << (*td_ptr)->job_id() << ").";
          // Someone in another job is producing this object. We have a choice
          // of making him runnable, or ignoring him.
          // We do the former if the reference is public, and the latter if it
          // is private.
          if ((*ref_iter)->desc().scope() == ReferenceDescriptor::PUBLIC)
            producing_tasks.insert(*td_ptr);
        }
      } else {
        VLOG(2) << "... NOT FOUND";
      }
    }
  }
  return producing_tasks;
}

const set<ReferenceInterface*> EventDrivenScheduler::ReferencesForID(
    const DataObjectID_t& id) {
  // Find all locally known references for a specific object
  VLOG(2) << "Looking up object " << id;
//  ReferenceDescriptor* rd = FindOrNull(*object_map_, id);
  set<ReferenceInterface*>* ref_set = object_store_->GetReferences(id);

  if (!ref_set) {
    VLOG(2) << "... NOT FOUND";
    set<ReferenceInterface*> es;  // empty set
    return es;
  } else {
    VLOG(2) << " ... FOUND, " << ref_set->size() << " references.";
    // Return the unfiltered set of all known references to this name
    return *ref_set;
  }
}

void EventDrivenScheduler::RegisterLocalResource(ResourceID_t res_id) {
  // Create an executor for each resource.
  VLOG(1) << "Adding executor for local resource " << res_id;
  LocalExecutor* exec = new LocalExecutor(res_id, coordinator_uri_,
                                          topology_manager_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

// Simple 2-argument wrapper
void EventDrivenScheduler::RegisterResource(ResourceID_t res_id,
                                            bool local,
                                            bool simulated) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  if (local) {
    RegisterLocalResource(res_id);
  } else if (simulated) {
    RegisterSimulatedResource(res_id);
  } else {
    RegisterRemoteResource(res_id);
  }
}

void EventDrivenScheduler::RegisterRemoteResource(ResourceID_t res_id) {
  // Create an executor for each resource.
  VLOG(1) << "Adding executor for remote resource " << res_id;
  RemoteExecutor* exec = new RemoteExecutor(res_id, coordinator_res_id_,
                                            coordinator_uri_,
                                            resource_map_.get(),
                                            m_adapter_ptr_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

void EventDrivenScheduler::RegisterSimulatedResource(ResourceID_t res_id) {
  VLOG(1) << "Adding executor for simulated resource " << res_id;
  SimulatedExecutor* exec = new SimulatedExecutor(res_id, coordinator_uri_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

const set<TaskID_t>& EventDrivenScheduler::RunnableTasksForJob(
    JobDescriptor* job_desc) {
  // TODO(malte): check if this is broken
  set<DataObjectID_t*> outputs =
      DataObjectIDsFromProtobuf(job_desc->output_ids());
  TaskDescriptor* rtp = job_desc->mutable_root_task();
  LazyGraphReduction(outputs, rtp, JobIDFromString(job_desc->uuid()));
  return runnable_tasks_;
}

bool EventDrivenScheduler::UnbindTaskFromResource(TaskDescriptor* td_ptr,
                                                  ResourceID_t res_id) {
  TaskID_t task_id = td_ptr->uid();
  // Set the bound resource idle again.
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs_ptr);
  // XXX(ionel): Assumes only one task is running per resource.
  rs_ptr->mutable_descriptor()->set_state(ResourceDescriptor::RESOURCE_IDLE);
  rs_ptr->mutable_descriptor()->clear_current_running_task();
  ResourceID_t* res_id_ptr = FindOrNull(task_bindings_, task_id);
  if (res_id_ptr) {
    pair<multimap<ResourceID_t, TaskID_t>::iterator,
         multimap<ResourceID_t, TaskID_t>::iterator> range_it =
      resource_bindings_.equal_range(*res_id_ptr);
    for (; range_it.first != range_it.second; range_it.first++) {
      if (range_it.first->second == task_id) {
        // We've found the element.
        resource_bindings_.erase(range_it.first);
        break;
      }
    }
    return task_bindings_.erase(task_id) == 1;
  } else {
    return false;
  }
}

}  // namespace scheduler
}  // namespace firmament
