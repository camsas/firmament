// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The scheduler interface assumed by the engine.

#ifndef FIRMAMENT_ENGINE_SCHEDULER_INTERFACE_H
#define FIRMAMENT_ENGINE_SCHEDULER_INTERFACE_H

#include <set>

#include "base/types.h"
#include "base/job_desc.pb.h"
#include "misc/printable_interface.h"
#include "engine/topology_manager.h"
#include "storage/object_store_interface.h"

namespace firmament {
namespace scheduler {

using machine::topology::TopologyManager;
using store::DataObjectMap_t;

class SchedulerInterface : public PrintableInterface {
 public:
  // Constructor.
  SchedulerInterface(shared_ptr<JobMap_t> job_map,
                     shared_ptr<ResourceMap_t> resource_map,
                     shared_ptr<store::ObjectStoreInterface> store,
                     shared_ptr<TaskMap_t> task_map)
      : job_map_(job_map),  resource_map_(resource_map), task_map_(task_map), object_store_(store) {}
  // Adds a job to the set of active jobs that are considered for scheduling.
  // TODO(malte): Determine if we actually need this, given the reactive design
  // of the scheduler.
  //void AddJob(shared_ptr<JobDescriptor> job_desc);
  // Unregister a resource ID from the scheduler. No-op if the resource ID is
  // not actually registered with it.
  virtual void DeregisterResource(ResourceID_t res_id) = 0;
  // Handle the completion of a task. This usually involves freeing up its
  // resource by setting it idle, and recording any bookkeeping data required.
  virtual void HandleTaskCompletion(TaskDescriptor* td_ptr) = 0;
  // Registers a resource ID with the scheduler, who may subsequently assign
  // work to this resource. A resource may be registered with multiple
  // schedulers.
  // TODO(malte): Work out what the semantics of doing so are!
  virtual void RegisterResource(ResourceID_t res_id) = 0;
  // Finds runnable tasks for the job in the argument and adds them to the
  // global runnable set.
  virtual const set<TaskID_t>& RunnableTasksForJob(
      JobDescriptor* job_desc) = 0;
  // Schedules all runnable tasks in a job.
  // Returns the number of tasks scheduled.
  virtual uint64_t ScheduleJob(JobDescriptor* job_desc) = 0;
  // Runs a scheduling iteration for all active jobs.
  // TODO(malte): Determine if the need this, given the reactive design of the
  // scheduler.
  //void ScheduleAllJobs();
 protected:
  // Binds a task to a resource, i.e. effects a scheduling assignment. This will
  // modify various bits of meta-data tracking assignments. It will then
  // delegate the actual execution of the task binary to the appropriate local
  // execution handler.
  virtual void BindTaskToResource(TaskDescriptor* task_desc,
                                  ResourceDescriptor* res_desc) = 0;
  // Finds a resource for a runnable task. This is the core placement logic.
  // Returns the resource ID of the resource chosen in the second argument, or
  // NULL if no resource could be found.
  virtual const ResourceID_t* FindResourceForTask(
      TaskDescriptor* task_desc) = 0;
  // Pointers to the associated coordinator's job, resource and object maps
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<ResourceMap_t> resource_map_;
  shared_ptr<TaskMap_t> task_map_;
  shared_ptr<store::ObjectStoreInterface> object_store_; 
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_SCHEDULER_INTERFACE_H
