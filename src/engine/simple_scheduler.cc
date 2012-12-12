// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive implementation of a simple-minded queue-based scheduler.

#include "engine/simple_scheduler.h"

#include <string>
#include <deque>
#include <map>
#include <utility>

#include "storage/reference_types.h"
#include "base/reference_types.h"
#include "base/reference_utils.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "engine/local_executor.h"
#include "storage/object_store_interface.h"

namespace firmament {
namespace scheduler {

using executor::LocalExecutor;
using common::pb_to_set;

SimpleScheduler::SimpleScheduler(shared_ptr<JobMap_t> job_map,
                                 shared_ptr<ResourceMap_t> resource_map,
                                 shared_ptr<store::ObjectStoreInterface> object_store,
                                 shared_ptr<TaskMap_t> task_map,
                                 shared_ptr<TopologyManager> topo_mgr,
                                 const string& coordinator_uri)
    : SchedulerInterface(job_map, resource_map, object_store, task_map),
      coordinator_uri_(coordinator_uri),
      topology_manager_(topo_mgr),
      scheduling_(false) {
  VLOG(1) << "SimpleScheduler initiated.";
}

SimpleScheduler::~SimpleScheduler() {
  for (map<ResourceID_t, ExecutorInterface*>::const_iterator exec_iter =
       executors_.begin();
       exec_iter != executors_.end();
       ++exec_iter)
    delete exec_iter->second;
  executors_.clear();
}

void SimpleScheduler::BindTaskToResource(
    TaskDescriptor* task_desc,
//    shared_ptr<ResourceDescriptor> res_desc) {
    ResourceDescriptor* res_desc) {
  // TODO(malte): stub
  VLOG(1) << "Binding task " << task_desc->uid() << " to resource "
          << res_desc->uuid();
  // TODO(malte): safety checks
  res_desc->set_state(ResourceDescriptor::RESOURCE_BUSY);
  task_desc->set_state(TaskDescriptor::RUNNING);
  CHECK(InsertIfNotPresent(&task_bindings_, task_desc->uid(),
                           ResourceIDFromString(res_desc->uuid())));
  if (VLOG_IS_ON(1))
    DebugPrintRunnableTasks();
  // Remove the task from the runnable set
  CHECK(runnable_tasks_.erase(task_desc->uid()));
  if (VLOG_IS_ON(1))
    DebugPrintRunnableTasks();
  // Find an executor for this resource.
  ExecutorInterface** exec =
      FindOrNull(executors_, ResourceIDFromString(res_desc->uuid()));
  CHECK(exec);
  // Actually kick off the task
  // N.B. This is an asynchronous call, as the executor will spawn a thread.
  (*exec)->RunTask(task_desc, true);
  VLOG(1) << "Task running";
}

const ResourceID_t* SimpleScheduler::FindResourceForTask(
    TaskDescriptor* task_desc) {
  // TODO(malte): This is an extremely simple-minded approach to resource
  // selection (i.e. the essence of scheduling). We will simply traverse the
  // resource map in some order, and grab the first resource available.
  VLOG(2) << "Trying to place task " << task_desc->uid() << "...";
  // Find the first idle resource in the resource map
  for (ResourceMap_t::iterator res_iter = resource_map_->begin();
       res_iter != resource_map_->end();
       ++res_iter) {
    ResourceID_t* rid = new ResourceID_t(res_iter->first);
    VLOG(3) << "Considering resource " << *rid << ", which is in state "
            << res_iter->second.first->state();
    if (res_iter->second.first->state() == ResourceDescriptor::RESOURCE_IDLE)
      return rid;
  }
  // We have not found any idle resources in our local resource map. At this
  // point, we should start looking beyond the machine boundary and towards
  // remote resources.
  return NULL;
}

void SimpleScheduler::DebugPrintRunnableTasks() {
  VLOG(1) << "Runnable task queue now contains " << runnable_tasks_.size()
          << " elements:";
  for (set<TaskID_t>::const_iterator t_iter = runnable_tasks_.begin();
       t_iter != runnable_tasks_.end();
       ++t_iter)
    VLOG(1) << "  " << *t_iter;
}

void SimpleScheduler::DeregisterResource(ResourceID_t res_id) {
  VLOG(1) << "Removing executor for resource " << res_id
          << " which is now deregistered from this scheduler.";
  ExecutorInterface** exec = FindOrNull(executors_, res_id);
  // Terminate any running tasks on the resource.
  //(*exec)->TerminateAllTasks();
  // Remove the executor for the resource.
  CHECK(executors_.erase(res_id));
  delete exec;
}

void SimpleScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr) {
  // Find resource for task
  ResourceID_t* res_id_ptr = FindOrNull(task_bindings_, td_ptr->uid());
  VLOG(1) << "Handling completion of task " << td_ptr->uid()
          << ", freeing resource " << *res_id_ptr;
  // Set the bound resource idle again.
  pair<ResourceDescriptor*, uint64_t>* res =
      FindOrNull(*resource_map_, *res_id_ptr);
  res->first->set_state(ResourceDescriptor::RESOURCE_IDLE);
  // Remove the task's resource binding (as it is no longer currently bound)
  task_bindings_.erase(td_ptr->uid());
  // TODO(malte): Check if this job still has any outstanding tasks, otherwise
  // mark it as completed.
}

void SimpleScheduler::RegisterResource(ResourceID_t res_id) {
  // Create an executor for each resource.
  VLOG(1) << "Adding executor for resource " << res_id;
  LocalExecutor* exec = new LocalExecutor(res_id, coordinator_uri_,
                                          topology_manager_);
  CHECK(InsertIfNotPresent(&executors_, res_id, exec));
}

const set<TaskID_t>& SimpleScheduler::RunnableTasksForJob(
    JobDescriptor* job_desc) {
  // XXX(malte): Obviously, this is pretty broken.
  set<DataObjectID_t> dummy = pb_to_set(job_desc->output_ids());
  TaskDescriptor* rtp = job_desc->mutable_root_task();
  return LazyGraphReduction(dummy, rtp);
}

// Implementation of lazy graph reduction algorithm, as per p58, fig. 3.5 in
// Derek Murray's thesis on CIEL.
const set<TaskID_t>& SimpleScheduler::LazyGraphReduction(
    const set<DataObjectID_t>& output_ids,
    TaskDescriptor* root_task) {
  VLOG(2) << "Performing lazy graph reduction";
  // Local data structures
  deque<TaskDescriptor* > newly_active_tasks;
  bool do_schedule = false;
  // Add expected producer for object_id to queue, if the object reference is
  // not already concrete.
  VLOG(2) << "for a job with " << output_ids.size() << " outputs";
  for (set<DataObjectID_t>::const_iterator output_id_iter = output_ids.begin();
       output_id_iter != output_ids.end();
       ++output_id_iter) {
    shared_ptr<ReferenceInterface> ref = ReferenceForID(*output_id_iter);
    if (ref && ref->Consumable()) {
      // skip this output, as it is already present
      continue;
    }
    // otherwise, we add the producer for said output reference to the queue, if
    // it is not already scheduled.
    TaskDescriptor* task = ProducingTaskForDataObjectID(*output_id_iter);
    CHECK(task != NULL) << "Could not find task producing output ID "
                        << *output_id_iter;
    if (task->state() == TaskDescriptor::CREATED) {
      task->set_state(TaskDescriptor::BLOCKING);
      newly_active_tasks.push_back(task);
    }
  }
  // Add root task to queue
  TaskDescriptor** rtd_ptr = FindOrNull(*task_map_, root_task->uid());
  CHECK(rtd_ptr);
  newly_active_tasks.push_back(*rtd_ptr);
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
      shared_ptr<ReferenceInterface> ref = ReferenceFromDescriptor(*iter);
      if (ref->Consumable()) {
        // This input reference is consumable. So far, so good.
        VLOG(2) << "Task " << current_task->uid() << "'s dependency " << ref
                << " is consumable.";
      } else {
        // This input reference is not consumable; set the task to block and
        // look at its predecessors (which may produce the necessary input, and
        // may be runnable).
        VLOG(2) << "Task " << current_task->uid()
                << " is blocking on reference " << *ref;
        will_block = true;
        // Look at predecessor task (producing this reference)
        TaskDescriptor* producing_task =
            ProducingTaskForDataObjectID(ref->id());
        if (producing_task) {
          if (producing_task->state() == TaskDescriptor::CREATED ||
              producing_task->state() == TaskDescriptor::COMPLETED) {
            producing_task->set_state(TaskDescriptor::BLOCKING);
            newly_active_tasks.push_back(producing_task);
          }
        } else {
          LOG(ERROR) << "Failed to find producing task for ref " << ref
                     << "; will block until it is produced.";
          continue;
        }
      }
    }
    if (!will_block) {
      // This task is runnable
      VLOG(2) << "Adding task " << current_task->uid() << " to RUNNABLE set.";
      current_task->set_state(TaskDescriptor::RUNNABLE);
      runnable_tasks_.insert(current_task->uid());
    }
  }
  VLOG(1) << "do_schedule is " << do_schedule;
  return runnable_tasks_;
}

uint64_t SimpleScheduler::ScheduleJob(JobDescriptor* job_desc) {
  uint64_t total_scheduled = 0;
  VLOG(2) << "Preparing to schedule job " << job_desc->uuid();
  scheduling_ = true;
  // Get the set of runnable tasks for this job
  set<TaskID_t> runnable_tasks = RunnableTasksForJob(job_desc);
  VLOG(2) << "Scheduling job " << job_desc->uuid() << ", which has "
          << runnable_tasks.size() << " runnable tasks.";
  for (set<TaskID_t>::const_iterator task_iter =
       runnable_tasks.begin();
       task_iter != runnable_tasks.end();
       ++task_iter) {
    TaskDescriptor** td = FindOrNull(*task_map_, *task_iter);
    CHECK(td);
    VLOG(2) << "Considering task " << (*td)->uid() << ":\n"
            << (*td)->DebugString();
    // TODO(malte): check passing semantics here.
    const ResourceID_t* best_resource = FindResourceForTask(*td);
    if (!best_resource) {
      VLOG(2) << "No suitable resource found, will need to try again.";
    } else {
      pair<ResourceDescriptor*, uint64_t>* rp = FindOrNull(*resource_map_,
                                                          *best_resource);
      CHECK(rp);
      LOG(INFO) << "Scheduling task " << (*td)->uid() << " on resource "
                << rp->first->uuid() << " [" << rp << "]";
      BindTaskToResource(*td, rp->first);
      total_scheduled++;
    }
  }
  if (total_scheduled > 0)
    job_desc->set_state(JobDescriptor::RUNNING);
  scheduling_ = false;
  return total_scheduled;
}

shared_ptr<ReferenceInterface> SimpleScheduler::ReferenceForID(
    DataObjectID_t id) {
  // XXX(malte): stub
  VLOG(2) << "Looking up object " << id;
//  ReferenceDescriptor* rd = FindOrNull(*object_map_, id);
  ReferenceDescriptor* rd = 0;

  if (!rd) {
    VLOG(2) << "... NOT FOUND";
    return shared_ptr<ReferenceInterface>();  // NULL
  } else {
    VLOG(2) << " ... ref has type " << rd->type();
    return ReferenceFromDescriptor(*rd);
  }
}

TaskDescriptor* SimpleScheduler::ProducingTaskForDataObjectID(
    DataObjectID_t id) {
  // XXX(malte): stub
  VLOG(2) << "Looking up producing task for object " << id;
//  ReferenceDescriptor* rd = FindOrNull(*object_map_, id);
  ReferenceDescriptor* rd = 0;
  if (!rd || !rd->has_producing_task()) {
    return NULL;
  } else {
    TaskDescriptor** td_ptr = FindOrNull(*task_map_, rd->producing_task());
    if (td_ptr)
      VLOG(2) << "... is " << (*td_ptr)->uid();
    else
      VLOG(2) << "... NOT FOUND";
    return (td_ptr ? *td_ptr : NULL);
  }
}

}  // namespace scheduler
}  // namespace firmament
