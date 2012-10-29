// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive implementation of a simple-minded queue-based scheduler.

#include "engine/simple_scheduler.h"

#include <deque>

#include "base/reference_types.h"

namespace firmament {
namespace scheduler {

SimpleScheduler::SimpleScheduler() {
}

const set<shared_ptr<TaskDescriptor> > SimpleScheduler::RunnableTasksForJob(
    JobDescriptor& job_desc) {
  set<string> dummy;
  shared_ptr<TaskDescriptor> rtp(job_desc.mutable_root_task());
  return LazyGraphReduction(dummy, rtp);
}

// Implementation of lazy graph reduction algorithm, as per p58, fig. 3.5 in
// Derek Murray's thesis on CIEL.
const set<shared_ptr<TaskDescriptor> > SimpleScheduler::LazyGraphReduction(
    const set<string>& output_ids,
    shared_ptr<TaskDescriptor> root_task) {
  VLOG(2) << "Performing lazy graph reduction";
  // Local data structures
  set<shared_ptr<TaskDescriptor> > runnable_tasks;
  set<shared_ptr<TaskDescriptor> > blocked_tasks;
  deque<shared_ptr<TaskDescriptor> > newly_active_tasks;
  bool do_schedule = false;
  // Add expected producer for object_id to queue, if the object reference is
  // not already concrete.
  for (set<string>::const_iterator output_id_iter = output_ids.begin();
       output_id_iter != output_ids.end();
       ++output_id_iter) {
    shared_ptr<ReferenceInterface> ref = ReferenceForID(*output_id_iter);
    if (ref && ref->Consumable()) {
      // skip this output, as it is already present
      continue;
    }
    // otherwise, we add the producer for said output reference to the queue, if
    // it is not already scheduled.
    shared_ptr<TaskDescriptor> task = TaskForOutputID(*output_id_iter);
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
    newly_active_tasks.pop_front();
    // Find any unfulfilled dependencies
    bool will_block = false;
    for (RepeatedPtrField<ReferenceDescriptor>::const_iterator iter =
         current_task->dependencies().begin();
         iter != current_task->dependencies().end();
         ++iter) {
      shared_ptr<ReferenceInterface> ref = ReferenceFromDescriptor(*iter);
      if (ref->Consumable()) {
        // TODO
      } else {
        VLOG(2) << "Task " << current_task << " is blocking on reference " << ref;
        will_block = true;
      }
    }
    if (!will_block) {
      // This task is runnable
      VLOG(2) << "Adding task " << current_task << " to RUNNABLE set.";
      current_task->set_state(TaskDescriptor::RUNNABLE);
      runnable_tasks.insert(current_task);
    }
  }
  VLOG(1) << "do_schedule is " << do_schedule;
  return runnable_tasks;
}

shared_ptr<ReferenceInterface> SimpleScheduler::ReferenceFromDescriptor(
    const ReferenceDescriptor& desc) {
  switch (desc.type()) {
    case ReferenceDescriptor::CONCRETE:
      return shared_ptr<ReferenceInterface>(new ConcreteReference(desc));
      break;
    default:
      LOG(FATAL) << "Unknown or unrecognized reference type.";
  }
}

shared_ptr<ReferenceInterface> SimpleScheduler::ReferenceForID(
    const string& id) {
  // TODO(malte): stub
  return shared_ptr<ReferenceInterface>();  // NULL
}

shared_ptr<TaskDescriptor> SimpleScheduler::TaskForOutputID(
    const string& id) {
  // TODO(malte): stub
  return shared_ptr<TaskDescriptor>();  // NULL
}

}  // namespace scheduler
}  // namespace firmament
