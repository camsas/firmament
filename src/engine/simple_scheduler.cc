// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive implementation of a simple-minded queue-based scheduler.

#include "engine/simple_scheduler.h"

#include <string>
#include <deque>
#include <utility>

#include "base/reference_types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "engine/local_executor.h"

namespace firmament {
namespace scheduler {

using executor::LocalExecutor;
using common::pb_to_set;

SimpleScheduler::SimpleScheduler(shared_ptr<JobMap_t> job_map,
                                 shared_ptr<ResourceMap_t> resource_map,
                                 shared_ptr<DataObjectMap_t> object_map,
                                 shared_ptr<TaskMap_t> task_map,
                                 const string& coordinator_uri)
    : SchedulerInterface(job_map, resource_map, object_map, task_map),
      coordinator_uri_(coordinator_uri) {
  VLOG(1) << "SimpleScheduler initiated.";
}

void SimpleScheduler::BindTaskToResource(
    shared_ptr<TaskDescriptor> task_desc,
//    shared_ptr<ResourceDescriptor> res_desc) {
    ResourceDescriptor* res_desc) {
  // TODO(malte): stub
  VLOG(1) << "Binding task " << task_desc->uid() << " to resource "
          << res_desc->uuid();
  // TODO(malte): safety checks
  res_desc->set_state(ResourceDescriptor::RESOURCE_BUSY);
  task_desc->set_state(TaskDescriptor::RUNNING);
  // XXX(malte): The below call, while innocent-looking, is actually a rather
  // bad idea -- it ends up calling the shared_ptr destructor and blows away the
  // TD. Need to find another way.
  //CHECK(runnable_tasks_.erase(task_desc));
  // TODO(malte): hacked-up task execution
  LocalExecutor exec(ResourceIDFromString(res_desc->uuid()), coordinator_uri_);
  // XXX(malte): This is currently a SYNCHRONOUS call, and obviously shouldn't
  // be.
  exec.RunTask(task_desc);
  VLOG(1) << "RunTask returned";
}

const ResourceID_t* SimpleScheduler::FindResourceForTask(
    shared_ptr<TaskDescriptor> task_desc) {
  // TODO(malte): stub
  VLOG(2) << "Trying to place task " << task_desc->uid() << "...";
  // Find the first idle resource in the resource map
  for (ResourceMap_t::iterator res_iter = resource_map_->begin();
       res_iter != resource_map_->end();
       ++res_iter) {
    ResourceID_t* rid = new ResourceID_t(res_iter->first);
    VLOG(3) << "Considering resource " << *rid << ", which is in state "
            << res_iter->second.first.state();
    if (res_iter->second.first.state() == ResourceDescriptor::RESOURCE_IDLE)
      return rid;
  }
  return NULL;
}

const set<shared_ptr<TaskDescriptor> >& SimpleScheduler::RunnableTasksForJob(
    shared_ptr<JobDescriptor> job_desc) {
  // XXX(malte): Obviously, this is pretty broken.
  set<DataObjectID_t> dummy = pb_to_set(job_desc->output_ids());
  shared_ptr<TaskDescriptor> rtp(job_desc, job_desc->mutable_root_task());
  return LazyGraphReduction(dummy, rtp);
}

// Implementation of lazy graph reduction algorithm, as per p58, fig. 3.5 in
// Derek Murray's thesis on CIEL.
const set<shared_ptr<TaskDescriptor> >& SimpleScheduler::LazyGraphReduction(
    const set<DataObjectID_t>& output_ids,
    shared_ptr<TaskDescriptor> root_task) {
  VLOG(2) << "Performing lazy graph reduction";
  // Local data structures
  deque<shared_ptr<TaskDescriptor> > newly_active_tasks;
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
    shared_ptr<TaskDescriptor> task =
        ProducingTaskForDataObjectID(*output_id_iter);
    CHECK(task != NULL) << "Could not find task producing output ID "
                        << *output_id_iter;
    if (task->state() == TaskDescriptor::CREATED) {
      task->set_state(TaskDescriptor::BLOCKING);
      newly_active_tasks.push_back(task);
    }
  }
  // Add root task to queue
  newly_active_tasks.push_back(root_task);
  while (!newly_active_tasks.empty()) {
    shared_ptr<TaskDescriptor> current_task = newly_active_tasks.front();
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
                << " is blocking on reference " << ref;
        will_block = true;
        // Look at predecessor task (producing this reference)
        shared_ptr<TaskDescriptor> producing_task =
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
      runnable_tasks_.insert(current_task);
    }
  }
  VLOG(1) << "do_schedule is " << do_schedule;
  return runnable_tasks_;
}

uint64_t SimpleScheduler::ScheduleJob(shared_ptr<JobDescriptor> job_desc) {
  VLOG(2) << "Preparing to schedule job " << job_desc->uuid();
  // Get the set of runnable tasks for this job
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      RunnableTasksForJob(job_desc);
  VLOG(2) << "Scheduling job " << job_desc->uuid() << ", which has "
          << runnable_tasks.size() << " runnable tasks.";
  for (set<shared_ptr<TaskDescriptor> >::const_iterator task_iter =
       runnable_tasks.begin();
       task_iter != runnable_tasks.end();
       ++task_iter) {
    VLOG(2) << "Considering task " << (*task_iter)->uid() << ":\n"
            << (*task_iter)->DebugString();
    // TODO(malte): check passing semantics here.
    const ResourceID_t* best_resource = FindResourceForTask(*task_iter);
    if (!best_resource) {
      VLOG(2) << "No suitable resource found, will need to try again.";
    } else {
      pair<ResourceDescriptor, uint64_t>* rp = FindOrNull(*resource_map_,
                                                          *best_resource);
      CHECK(rp);
      LOG(INFO) << "Scheduling task " << (*task_iter)->uid() << " on resource "
                << rp->first.uuid() << "[" << rp << "]";
      BindTaskToResource(*task_iter, &(rp->first));
    }
  }
  return 0;
}

shared_ptr<ReferenceInterface> SimpleScheduler::ReferenceForID(
    DataObjectID_t id) {
  // XXX(malte): stub
  VLOG(1) << "looking up object " << id;
  ReferenceDescriptor* rd = FindOrNull(*object_map_, id);
  if (!rd)
    return shared_ptr<ReferenceInterface>();  // NULL
  else
    return ReferenceFromDescriptor(*rd);
}

shared_ptr<TaskDescriptor> SimpleScheduler::ProducingTaskForDataObjectID(
    DataObjectID_t id) {
  // XXX(malte): stub
  VLOG(1) << "looking up producing task for object " << id;
  ReferenceDescriptor* rd = FindOrNull(*object_map_, id);
  if (!rd || !rd->has_producing_task()) {
    return shared_ptr<TaskDescriptor>();  // NULL
  } else {
    shared_ptr<TaskDescriptor> temp_td(new TaskDescriptor);
    temp_td->set_uid(rd->producing_task());
    return temp_td;
  }
}

}  // namespace scheduler
}  // namespace firmament
