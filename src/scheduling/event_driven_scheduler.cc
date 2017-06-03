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

// General abstract superclass for event-driven schedulers. This contains shared
// implementation, e.g. task binding and remote delegation mechanisms.

#include "scheduling/event_driven_scheduler.h"

#include <deque>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/units.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/utils.h"
#include "engine/executors/local_executor.h"
#include "engine/executors/remote_executor.h"
#include "engine/executors/simulated_executor.h"
#include "scheduling/knowledge_base.h"
#include "storage/object_store_interface.h"
#include "storage/reference_types.h"
#include "storage/reference_utils.h"

DEFINE_uint64(task_fail_timeout, 60, "Time (in seconds) after which to declare "
              "a task as failed if it has not sent heartbeats");

namespace firmament {
namespace scheduler {

using executor::LocalExecutor;
using executor::RemoteExecutor;
using executor::SimulatedExecutor;
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
    SchedulingEventNotifierInterface* event_notifier,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri,
    TimeInterface* time_manager,
    TraceGenerator* trace_generator)
  : SchedulerInterface(job_map, knowledge_base, resource_map, resource_topology,
                       object_store, task_map),
      coordinator_uri_(coordinator_uri),
      coordinator_res_id_(coordinator_res_id),
      event_notifier_(event_notifier),
      m_adapter_ptr_(m_adapter),
      topology_manager_(topo_mgr),
      time_manager_(time_manager),
      trace_generator_(trace_generator) {
  VLOG(1) << "EventDrivenScheduler initiated.";
}

EventDrivenScheduler::~EventDrivenScheduler() {
  // time_manager_ and trace_generator are not owned by the
  // EventDrivenScheduler. We don't have to delete them.
  for (unordered_map<ResourceID_t, ExecutorInterface*>::const_iterator
       exec_iter = executors_.begin();
       exec_iter != executors_.end();
       ++exec_iter) {
    delete exec_iter->second;
  }
  executors_.clear();
  // We don't delete event_notifier_ and m_adapter_ptr because they're owned by
  // the coordinator or the simulator_bridge.
}

void EventDrivenScheduler::AddJob(JobDescriptor* jd_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  InsertOrUpdate(&jobs_to_schedule_, JobIDFromString(jd_ptr->uuid()), jd_ptr);
}

void EventDrivenScheduler::BindTaskToResource(TaskDescriptor* td_ptr,
                                              ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  // Mark resource as busy and record task binding
  rd_ptr->set_state(ResourceDescriptor::RESOURCE_BUSY);
  rd_ptr->add_current_running_tasks(task_id);
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
  pair<unordered_multimap<ResourceID_t, TaskID_t,
                          boost::hash<ResourceID_t>>::iterator,
       unordered_multimap<ResourceID_t, TaskID_t,
                          boost::hash<ResourceID_t>>::iterator> range_it =
    resource_bindings_.equal_range(res_id);
  for (; range_it.first != range_it.second; range_it.first++) {
    tasks.push_back(range_it.first->second);
  }
  return tasks;
}

void EventDrivenScheduler::CheckRunningTasksHealth() {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  for (auto& executor : executors_) {
    vector<TaskID_t> failed_tasks;
    if (!executor.second->CheckRunningTasksHealth(&failed_tasks)) {
      // Handle task failures
      for (auto& failed_task : failed_tasks) {
        TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, failed_task);
        CHECK_NOTNULL(td_ptr);
        if (td_ptr->state() != TaskDescriptor::COMPLETED &&
            td_ptr->last_heartbeat_time() <=
            (time_manager_->GetCurrentTimestamp() - FLAGS_task_fail_timeout *
             SECONDS_TO_MICROSECONDS)) {
          LOG(INFO) << "Task " << td_ptr->uid() << " has not reported "
                    << "heartbeats for " << FLAGS_task_fail_timeout
                    << "s and its handler thread has exited. "
                    << "Declaring it FAILED!";
          HandleTaskFailure(td_ptr);
        }
      }
    }
  }
}

void EventDrivenScheduler::CleanStateForDeregisteredResource(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  const ResourceDescriptor& rd = rtnd_ptr->resource_desc();
  ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  VLOG(2) << "Removing executor for resource " << res_id
          << " which is now deregistered from this scheduler.";
  if (rd.type() == ResourceDescriptor::RESOURCE_PU) {
    ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
    CHECK_NOTNULL(exec);
    // TODO(ionel): Terminate the tasks running on res_id or any of
    // its sub-resources. Make sure the tasks get re-scheduled.
    // exec->TerminateAllTasks();
    CHECK(executors_.erase(res_id));
    delete exec;
  } else if (rd.type() == ResourceDescriptor::RESOURCE_MACHINE) {
    trace_generator_->RemoveMachine(rd);
  }
  resource_bindings_.erase(res_id);
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs_ptr);
  resource_map_->erase(res_id);
  delete rs_ptr;
}

void EventDrivenScheduler::DebugPrintRunnableTasks() {
  for (auto& runnable_tasks_per_job : runnable_tasks_) {
    for (auto& task : runnable_tasks_per_job.second) {
      VLOG(1) << "  " << task;
    }
  }
}

void EventDrivenScheduler::DeregisterResource(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  DFSTraversePostOrderResourceProtobufTreeReturnRTND(
      rtnd_ptr,
      boost::bind(&EventDrivenScheduler::CleanStateForDeregisteredResource,
                  this, _1));
  // We've finished using ResourceTopologyNodeDescriptor. We can now deconnect
  // it from its parent.
  if (!rtnd_ptr->parent_id().empty()) {
    RemoveResourceNodeFromParentChildrenList(rtnd_ptr);
  } else {
    LOG(WARNING) << "Deregistered node without a parent";
  }
}

void EventDrivenScheduler::ExecuteTask(TaskDescriptor* td_ptr,
                                       ResourceDescriptor* rd_ptr) {
  TaskID_t task_id = td_ptr->uid();
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
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
  VLOG(2) << "Task " << task_id << " running.";
}

void EventDrivenScheduler::HandleJobCompletion(JobID_t job_id) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  JobDescriptor* jd = FindOrNull(*job_map_, job_id);
  CHECK_NOTNULL(jd);
  jobs_to_schedule_.erase(job_id);
  runnable_tasks_.erase(job_id);
  jd->set_state(JobDescriptor::COMPLETED);
  if (event_notifier_) {
    event_notifier_->OnJobCompletion(job_id);
  }
}

void EventDrivenScheduler::HandleJobRemoval(JobID_t job_id) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  JobDescriptor* jd = FindOrNull(*job_map_, job_id);
  CHECK_NOTNULL(jd);
  jobs_to_schedule_.erase(job_id);
  runnable_tasks_.erase(job_id);
  if (event_notifier_) {
    event_notifier_->OnJobRemoval(job_id);
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
    unordered_set<TaskDescriptor*>* tasks = FindOrNull(reference_subscriptions_,
                                                       old_ref.id());
    if (!tasks) {
      // Nobody cares about this ref, so we don't do anything
      return;
    }
    for (auto& task : *tasks) {
      CHECK_NOTNULL(task);
      bool any_outstanding = false;
      if (task->state() == TaskDescriptor::COMPLETED ||
          task->state() == TaskDescriptor::RUNNING)
        continue;
      for (auto& dependency : task->dependencies()) {
        unordered_set<ReferenceInterface*>* deps = object_store_->GetReferences(
            DataObjectIDFromProtobuf(dependency.id()));
        for (auto& dep : *deps) {
          if (!dep->Consumable())
            any_outstanding = true;
        }
      }
      if (!any_outstanding) {
        task->set_state(TaskDescriptor::RUNNABLE);
        InsertTaskIntoRunnables(JobIDFromString(task->job_id()), task->uid());
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
  ResourceStatus* rs_ptr = NULL;
  if (res_id_ptr) {
    // This copy is necessary because UnbindTaskFromResource ends up deleting
    // the ResourceID_t pointed to by res_id_ptr
    ResourceID_t res_id_tmp = *res_id_ptr;
    rs_ptr = FindPtrOrNull(*resource_map_, res_id_tmp);
    CHECK_NOTNULL(rs_ptr);
    VLOG(1) << "Handling completion of task " << td_ptr->uid()
            << ", freeing resource " << res_id_tmp;
    CHECK(UnbindTaskFromResource(td_ptr, res_id_tmp));
    // Record final report
    ExecutorInterface* exec = FindPtrOrNull(executors_, res_id_tmp);
    CHECK_NOTNULL(exec);
    exec->HandleTaskCompletion(td_ptr, report);
  } else {
    // The task does not have a bound resource. It can happen when a machine
    // temporarly fails. As a result of the failure, we mark the task as failed
    // and unbind it from the machine's resource. However, upon machine recovery
    // we can receive a task completion notification.
    VLOG(1) << "Handling completion of task " << td_ptr->uid();
    ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
    rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    CHECK_NOTNULL(rs_ptr);
  }
  td_ptr->set_state(TaskDescriptor::COMPLETED);
  // Store the final report in the TD for future reference
  td_ptr->mutable_final_report()->CopyFrom(*report);
  trace_generator_->TaskCompleted(td_ptr->uid(), rs_ptr->descriptor());
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
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  InsertTaskIntoRunnables(job_id, td_ptr->uid());
  td_ptr->clear_start_time();
  JobDescriptor* jd = FindOrNull(*job_map_, job_id);
  CHECK_NOTNULL(jd);
  // Try again to schedule...
  scheduler::SchedulerStats scheduler_stats;
  ScheduleJob(jd, &scheduler_stats);
}

void EventDrivenScheduler::HandleTaskDelegationSuccess(
    TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Remove the task from the runnable set
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  TaskID_t task_id = td_ptr->uid();
  // We don't have to check if we're actually removing the task because
  // some schedulers may already remove it before calling
  // HandleTaskDelegationSuccess.
  runnable_tasks_[job_id].erase(task_id);
}

void EventDrivenScheduler::HandleTaskEviction(TaskDescriptor* td_ptr,
                                              ResourceDescriptor* rd_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  VLOG(1) << "Handling eviction of task " << td_ptr->uid()
          << ", freeing resource " << res_id;
  CHECK(UnbindTaskFromResource(td_ptr, res_id));
  // Record final report
  ExecutorInterface* exec = FindPtrOrNull(executors_, res_id);
  td_ptr->set_state(TaskDescriptor::RUNNABLE);
  InsertTaskIntoRunnables(JobIDFromString(td_ptr->job_id()), td_ptr->uid());
  CHECK_NOTNULL(exec);
  exec->HandleTaskEviction(td_ptr);
  trace_generator_->TaskEvicted(td_ptr->uid(), *rd_ptr, false);
  if (event_notifier_) {
    event_notifier_->OnTaskEviction(td_ptr, rd_ptr);
  }
  // The Google-style trace requires a submit event to be printed after
  // a task is evicted.
  trace_generator_->TaskSubmitted(td_ptr);
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
  if (!td_ptr->delegated_from().empty()) {
    // XXX(malte): Need to forward message about task failure to delegator here!
  }
  trace_generator_->TaskFailed(td_ptr->uid(), rs_ptr->descriptor());
  if (event_notifier_) {
    event_notifier_->OnTaskFailure(td_ptr, rs_ptr->mutable_descriptor());
  }
}

void EventDrivenScheduler::HandleTaskFinalReport(const TaskFinalReport& report,
                                                 TaskDescriptor* td_ptr) {
  CHECK_NOTNULL(td_ptr);
  VLOG(2) << "Handling task final report for " << report.task_id();
  // Add the report to the TD if the task is not local (otherwise, the
  // scheduler has already done so)
  if (!td_ptr->delegated_to().empty()) {
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
  ResourceStatus* old_rs = FindPtrOrNull(*resource_map_, *old_res_id_ptr);
  CHECK_NOTNULL(old_rs);
  ResourceDescriptor* old_rd = old_rs->mutable_descriptor();
  CHECK(UnbindTaskFromResource(td_ptr, *old_res_id_ptr));
  BindTaskToResource(td_ptr, rd_ptr);
  trace_generator_->TaskMigrated(td_ptr, *old_rd, *rd_ptr);
  if (event_notifier_) {
    event_notifier_->OnTaskMigration(td_ptr, old_rd, rd_ptr);
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
  // Remove the task from the runnable_tasks.
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  unordered_set<TaskID_t>* runnables_for_job =
    FindOrNull(runnable_tasks_, job_id);
  if (runnables_for_job) {
    runnables_for_job->erase(task_id);
  }
  ExecuteTask(td_ptr, rd_ptr);
  trace_generator_->TaskScheduled(task_id, *rd_ptr);
  if (event_notifier_) {
    event_notifier_->OnTaskPlacement(td_ptr, rd_ptr);
  }
}

void EventDrivenScheduler::HandleTaskRemoval(TaskDescriptor* td_ptr) {
  bool was_running = false;
  if (td_ptr->state() == TaskDescriptor::RUNNING) {
    was_running = true;
    KillRunningTask(td_ptr->uid(), TaskKillMessage::USER_ABORT);
  } else {
    td_ptr->set_state(TaskDescriptor::ABORTED);
  }
  trace_generator_->TaskRemoved(td_ptr->uid(), was_running);
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
  td_ptr->set_state(TaskDescriptor::ABORTED);
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, *rid);
  // Manufacture the message
  BaseMessage bm;
  SUBMSG_WRITE(bm, task_kill, task_id, task_id);
  SUBMSG_WRITE(bm, task_kill, reason, reason);
  // Send the message
  LOG(INFO) << "Sending KILL message to task " << task_id << " on resource "
            << *rid << " (endpoint: " << td_ptr->last_heartbeat_location()
            << ")";
  m_adapter_ptr_->SendMessageToEndpoint(td_ptr->last_heartbeat_location(), bm);
  if (!rid) {
    CHECK(UnbindTaskFromResource(td_ptr, *rid));
  }
  trace_generator_->TaskKilled(task_id, rs_ptr->descriptor());
}

void EventDrivenScheduler::InsertTaskIntoRunnables(JobID_t job_id,
                                                   TaskID_t task_id) {
  unordered_set<TaskID_t>* runnable_tasks_for_job =
    FindOrNull(runnable_tasks_, job_id);
  if (runnable_tasks_for_job == NULL) {
    unordered_set<TaskID_t> new_runnable_tasks_for_job;
    new_runnable_tasks_for_job.insert(task_id);
    InsertOrUpdate(&runnable_tasks_, job_id, new_runnable_tasks_for_job);
  } else {
    runnable_tasks_for_job->insert(task_id);
  }
}

// Implementation of lazy graph reduction algorithm, as per p58, fig. 3.5 in
// Derek Murray's thesis on CIEL.
void EventDrivenScheduler::LazyGraphReduction(
    const unordered_set<DataObjectID_t*>& output_ids,
    TaskDescriptor* root_task,
    const JobID_t& job_id) {
  VLOG(2) << "Performing lazy graph reduction";
  // Local data structures
  deque<TaskDescriptor*> newly_active_tasks;
  // Add expected producer for object_id to queue, if the object reference is
  // not already concrete.
  VLOG(2) << "for a job with " << output_ids.size() << " outputs";
  for (auto& output_id : output_ids) {
    unordered_set<ReferenceInterface*> refs = ReferencesForID(*output_id);
    if (!refs.empty()) {
      for (auto& ref : refs) {
        // TODO(malte): this logic is very simple-minded; sometimes, it may be
        // beneficial to produce locally instead of fetching remotely!
        if (ref->Consumable() && !ref->desc().non_deterministic()) {
          // skip this output, as it is already present
          continue;
        }
      }
    }
    // otherwise, we add the producer for said output reference to the queue, if
    // it is not already scheduled.
    // N.B.: by this point, we know that no concrete reference exists in the set
    // of references available.
    unordered_set<TaskDescriptor*> tasks =
        ProducingTasksForDataObjectID(*output_id, job_id);
    CHECK_GT(tasks.size(), 0) << "Could not find task producing output ID "
                              << *output_id;
    for (auto& task : tasks) {
      if (task->state() == TaskDescriptor::CREATED ||
          task->state() == TaskDescriptor::FAILED) {
        VLOG(2) << "Setting task " << task->uid() << " active as it produces "
                << "output " << *output_id << ", which we're interested in.";
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
  if (rtd_ptr->state() == TaskDescriptor::CREATED ||
      rtd_ptr->state() == TaskDescriptor::RUNNING ||
      rtd_ptr->state() == TaskDescriptor::RUNNABLE ||
      rtd_ptr->state() == TaskDescriptor::COMPLETED) {
    newly_active_tasks.push_back(rtd_ptr);
  }
  // Keep iterating over tasks as long as there are more to visit
  while (!newly_active_tasks.empty()) {
    TaskDescriptor* current_task = newly_active_tasks.front();
    VLOG(2) << "Next active task considered is " << current_task->uid();
    newly_active_tasks.pop_front();
    // Find any unfulfilled dependencies
    bool will_block = false;
    if (current_task->state() == TaskDescriptor::CREATED ||
        current_task->state() == TaskDescriptor::BLOCKING) {
      for (auto& dependency : current_task->dependencies()) {
        ReferenceInterface* ref = ReferenceFromDescriptor(dependency);
        // Subscribe the current task to the reference, to enable it to be
        // unblocked if it becomes available.
        // Note that we subscribe even tasks whose dependencies are concrete, as
        // they may later disappear and failures will be handled via the
        // subscription relationship.
        unordered_set<TaskDescriptor*>* subscribers = FindOrNull(
                                                                 reference_subscriptions_, ref->id());
        if (!subscribers) {
          InsertIfNotPresent(&reference_subscriptions_,
                             ref->id(), unordered_set<TaskDescriptor*>());
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
          unordered_set<TaskDescriptor*> producing_tasks =
            ProducingTasksForDataObjectID(ref->id(), job_id);
          if (producing_tasks.size() == 0) {
            LOG(ERROR) << "Failed to find producing task for ref " << ref
                       << "; will block until it is produced.";
            continue;
          }
          for (auto& task : producing_tasks) {
            if (task->state() == TaskDescriptor::CREATED ||
                task->state() == TaskDescriptor::COMPLETED) {
              task->set_state(TaskDescriptor::BLOCKING);
              newly_active_tasks.push_back(task);
            }
          }
        }
      }
    }
    // Process any eager children not related via dependencies
    for (auto& child_task : *current_task->mutable_spawned()) {
      if (child_task.outputs_size() == 0)
        newly_active_tasks.push_back(&child_task);
    }
    if (current_task->state() == TaskDescriptor::CREATED ||
        current_task->state() == TaskDescriptor::BLOCKING) {
      if (!will_block || (current_task->dependencies_size() == 0
                          && current_task->outputs_size() == 0)) {
        current_task->set_state(TaskDescriptor::RUNNABLE);
        InsertTaskIntoRunnables(JobIDFromString(current_task->job_id()),
                                current_task->uid());
      }
    }
  }
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
  InsertIfNotPresent(task_map_.get(), td->uid(), td);
  HandleTaskPlacement(td, rd);
  td->set_state(TaskDescriptor::RUNNING);
  return true;
}

unordered_set<TaskDescriptor*>
EventDrivenScheduler::ProducingTasksForDataObjectID(
    const DataObjectID_t& id, const JobID_t& cur_job) {
  // Find all producing tasks for an object ID, as indicated by the references
  // stored locally.
  unordered_set<TaskDescriptor*> producing_tasks;
  VLOG(2) << "Looking up producing task for object " << id;
  unordered_set<ReferenceInterface*>* refs = object_store_->GetReferences(id);
  if (!refs)
    return producing_tasks;
  for (auto& ref : *refs) {
    if (ref->desc().producing_task() != 0) {
      TaskDescriptor* td_ptr =
        FindPtrOrNull(*task_map_, ref->desc().producing_task());
      if (td_ptr) {
        if (JobIDFromString(td_ptr->job_id()) == cur_job) {
          VLOG(2) << "... is " << td_ptr->uid() << " (in THIS job).";
          producing_tasks.insert(td_ptr);
        } else {
          VLOG(2) << "... is " << td_ptr->uid() << " (in job "
                  << td_ptr->job_id() << ").";
          // Someone in another job is producing this object. We have a choice
          // of making him runnable, or ignoring him.
          // We do the former if the reference is public, and the latter if it
          // is private.
          if (ref->desc().scope() == ReferenceDescriptor::PUBLIC)
            producing_tasks.insert(td_ptr);
        }
      } else {
        VLOG(2) << "... NOT FOUND";
      }
    }
  }
  return producing_tasks;
}

const unordered_set<ReferenceInterface*> EventDrivenScheduler::ReferencesForID(
    const DataObjectID_t& id) {
  // Find all locally known references for a specific object
  VLOG(2) << "Looking up object " << id;
  unordered_set<ReferenceInterface*>* ref_set =
    object_store_->GetReferences(id);
  if (!ref_set) {
    VLOG(2) << "... NOT FOUND";
    unordered_set<ReferenceInterface*> es;  // empty set
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
                                          time_manager_, topology_manager_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

// Simple 2-argument wrapper
void EventDrivenScheduler::RegisterResource(
    ResourceTopologyNodeDescriptor* rtnd_ptr,
    bool local,
    bool simulated) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  BFSTraverseResourceProtobufTreeReturnRTND(
      rtnd_ptr, boost::bind(&EventDrivenScheduler::SetupPUs, this, _1,
                            local, simulated));
}

void EventDrivenScheduler::RegisterRemoteResource(ResourceID_t res_id) {
  // Create an executor for each resource.
  VLOG(1) << "Adding executor for remote resource " << res_id;
  RemoteExecutor* exec = new RemoteExecutor(res_id, coordinator_res_id_,
                                            coordinator_uri_,
                                            resource_map_.get(),
                                            m_adapter_ptr_,
                                            time_manager_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

void EventDrivenScheduler::RegisterSimulatedResource(ResourceID_t res_id) {
  VLOG(1) << "Adding executor for simulated resource " << res_id;
  SimulatedExecutor* exec = new SimulatedExecutor(res_id, coordinator_uri_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

void EventDrivenScheduler::RemoveResourceNodeFromParentChildrenList(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  ResourceStatus* parent_rs_ptr =
    FindPtrOrNull(*resource_map_, ResourceIDFromString(rtnd_ptr->parent_id()));
  CHECK_NOTNULL(parent_rs_ptr);
  // The parent of the node is the topology root.
  RepeatedPtrField<ResourceTopologyNodeDescriptor>* parent_children =
    parent_rs_ptr->mutable_topology_node()->mutable_children();
  int32_t index = 0;
  // Find the node in the parent's children list.
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator it =
         parent_children->begin(); it != parent_children->end();
       ++it, ++index) {
    if (it->resource_desc().uuid()
        .compare(rtnd_ptr->resource_desc().uuid()) == 0) {
      break;
    }
  }
  if (index < parent_children->size()) {
    // Found the node.
    if (index < parent_children->size() - 1) {
      // The node is not the last one.
      parent_children->SwapElements(index, parent_children->size() - 1);
    }
    parent_children->RemoveLast();
  } else {
    LOG(FATAL) << "Could not find the resource in the parent's list";
  }
}

const unordered_set<TaskID_t>& EventDrivenScheduler::ComputeRunnableTasksForJob(
    JobDescriptor* job_desc) {
  // TODO(malte): check if this is broken
  unordered_set<DataObjectID_t*> outputs =
      DataObjectIDsFromProtobuf(job_desc->output_ids());
  TaskDescriptor* rtp = job_desc->mutable_root_task();
  LazyGraphReduction(outputs, rtp, JobIDFromString(job_desc->uuid()));
  JobID_t job_id = JobIDFromString(job_desc->uuid());
  unordered_set<TaskID_t>* runnable_tasks_for_job =
    FindOrNull(runnable_tasks_, job_id);
  if (runnable_tasks_for_job != NULL) {
    return *runnable_tasks_for_job;
  } else {
    unordered_set<TaskID_t> new_runnable_tasks_for_job;
    InsertOrUpdate(&runnable_tasks_, job_id, new_runnable_tasks_for_job);
    return runnable_tasks_[job_id];
  }
}

void EventDrivenScheduler::SetupPUs(ResourceTopologyNodeDescriptor* rtnd_ptr,
                                    bool local,
                                    bool simulated) {
  CHECK_NOTNULL(rtnd_ptr);
  ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
  if (rd_ptr->type() == ResourceDescriptor::RESOURCE_PU) {
    // TODO(malte): We make the assumption here that any local PU resource is
    // exclusively owned by the calling coordinator, and set its state to IDLE
    // if it is currently unknown. If coordinators were to ever shared PUs,
    // we'd need something more clever here.
    rd_ptr->set_schedulable(true);
    if (rd_ptr->state() == ResourceDescriptor::RESOURCE_UNKNOWN) {
      rd_ptr->set_state(ResourceDescriptor::RESOURCE_IDLE);
    }
    ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
    if (local) {
      RegisterLocalResource(res_id);
    } else if (simulated) {
      RegisterSimulatedResource(res_id);
    } else {
      RegisterRemoteResource(res_id);
    }
  }
}

bool EventDrivenScheduler::UnbindTaskFromResource(TaskDescriptor* td_ptr,
                                                  ResourceID_t res_id) {
  TaskID_t task_id = td_ptr->uid();
  // Set the bound resource idle again.
  ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs_ptr);
  ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
  // We don't have to remove the task from rd_ptr's running tasks because
  // we've already cleared the list.
  if (rd_ptr->current_running_tasks_size() == 0) {
    rd_ptr->set_state(ResourceDescriptor::RESOURCE_IDLE);
  }
  ResourceID_t* res_id_ptr = FindOrNull(task_bindings_, task_id);
  if (res_id_ptr) {
    pair<unordered_multimap<ResourceID_t, TaskID_t,
                            boost::hash<ResourceID_t>>::iterator,
         unordered_multimap<ResourceID_t, TaskID_t,
                            boost::hash<ResourceID_t>>::iterator>
      range_it = resource_bindings_.equal_range(*res_id_ptr);
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
