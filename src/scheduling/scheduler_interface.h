// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The scheduler interface assumed by the engine.

#ifndef FIRMAMENT_SCHEDULING_SCHEDULER_INTERFACE_H
#define FIRMAMENT_SCHEDULING_SCHEDULER_INTERFACE_H

#include <limits>
#include <set>
#include <vector>

#include <ctemplate/template.h>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "base/job_desc.pb.h"
#include "base/types.h"
#include "base/task_final_report.pb.h"
#include "misc/printable_interface.h"
#include "engine/executors/executor_interface.h"
#include "engine/executors/topology_manager.h"
#include "scheduling/knowledge_base.h"
#include "storage/object_store_interface.h"

namespace firmament {
namespace scheduler {

using ctemplate::TemplateDictionary;
using machine::topology::TopologyManager;
using store::DataObjectMap_t;
using store::ObjectStoreInterface;

struct SchedulerStats {
  SchedulerStats() : algorithm_runtime(numeric_limits<uint64_t>::max()),
    scheduler_runtime(0) {
  }
  // Accounts only the algorithmic part of the scheduler (in u-sec).
  uint64_t algorithm_runtime;
  // Accounts the entire scheduling time in u-sec (i.e. DIMACS write, solver
  // runtime, DIMACS read).
  uint64_t scheduler_runtime;
};

class SchedulerInterface : public PrintableInterface {
 public:
  SchedulerInterface(shared_ptr<JobMap_t> job_map,
                     shared_ptr<KnowledgeBase> knowledge_base,
                     shared_ptr<ResourceMap_t> resource_map,
                     ResourceTopologyNodeDescriptor* resource_topology,
                     shared_ptr<ObjectStoreInterface> object_store,
                     shared_ptr<TaskMap_t> task_map)
    : job_map_(job_map), knowledge_base_(knowledge_base),
    resource_map_(resource_map), task_map_(task_map),
    object_store_(object_store), resource_topology_(resource_topology) {}

  /**
   * Adds a new job. The job will be scheduled on the next run of the scheduler
   * if it has any runnable tasks.
   * @param jd_ptr the job descriptor
   */
  virtual void AddJob(JobDescriptor* jd_ptr) = 0;

  /**
   * Finds the resource to which a particular task ID is currently bound.
   * @param task_id the id of the task for which to do the lookup
   * @return NULL if the task does not exist or is not currently bound.
   * Otherwise, it returns its resource id
   */
  virtual ResourceID_t* BoundResourceForTask(TaskID_t task_id) = 0;

  /**
   * Finds the tasks which are bound to a particular resource ID.
   * @param res_id the id of the resource for which to do the lookup
   * @return a vector of task ids
   */
  virtual vector<TaskID_t> BoundTasksForResource(ResourceID_t res_id) = 0;

  /**
   * Checks if all running tasks managed by this scheduler are healthy. It
   * invokes failure handlers if any failures are detected.
   */
  virtual void CheckRunningTasksHealth() = 0;

  /**
   * Unregisters a resource ID from the scheduler. No-op if the resource ID is
   * not actually registered with it.
   * @param res_id the id of the resource to de-register
   */
  virtual void DeregisterResource(ResourceID_t res_id) = 0;

  // TODO(malte): comment
  virtual void HandleReferenceStateChange(const ReferenceInterface& old_ref,
                                          const ReferenceInterface& new_ref,
                                          TaskDescriptor* td_ptr) = 0;

  /**
   * Handles the completion of a job (all tasks are completed, failed or
   * aborted). May clean up scheduler-specific state.
   * @param job_id the id of the completed job
   */
  virtual void HandleJobCompletion(JobID_t job_id) = 0;

  /**
   * Handles the completion of a task. This usually involves freeing up its
   * resource by setting it idle, and recording any bookkeeping data required.
   * @param td_ptr the task descriptor of the completed task
   * @param report the task report to be populated with statistics
   * (e.g., finish time).
   */
  virtual void HandleTaskCompletion(TaskDescriptor* td_ptr,
                                    TaskFinalReport* report) = 0;
  /**
   * Handles the failure of an attempt to delegate a task to a subordinate
   * coordinator. This can happen because the resource is no longer there (it
   * failed) or it is no longer idle (someone else put a task there).
   * @param td_ptr the descriptor of the task that could not be delegated
   */
  virtual void HandleTaskDelegationFailure(TaskDescriptor* td_ptr) = 0;

  /**
   * Handles the eviction of a task.
   * @param td_ptr the task descriptor of the evicted task
   * @param res_id the id of the resource from which the task was evicted
   */
  virtual void HandleTaskEviction(TaskDescriptor* td_ptr,
                                  ResourceDescriptor* rd_ptr) = 0;

  /**
   * Handles the failure of a task. This usually involves freeing up its
   * resource by setting it idle, and kicking off the necessary fault tolerance
   * handling procedures.
   * @param td_ptr the task descriptor of the failed task
   */
  virtual void HandleTaskFailure(TaskDescriptor* td_ptr) = 0;

  shared_ptr<KnowledgeBase> knowledge_base() const {
    CHECK_NOTNULL(knowledge_base_.get());
    return knowledge_base_;
  }

  /**
   * Updates the state using the task's report.
   * @param report the report to use
   * @param td_ptr the descriptor of the task for which to update the state.
   */
  virtual void HandleTaskFinalReport(const TaskFinalReport& report,
                                     TaskDescriptor* td_ptr) = 0;

  /**
   * Kills a running task.
   * @param task_id the id of the task to kill
   * @param reason the reason to kill the task
   */
  virtual void KillRunningTask(TaskID_t task_id,
                               TaskKillMessage::TaskKillReason reason) = 0;

  /**
   * Places a task delegated from a superior coordinator to a resource managed
   * by this scheduler.
   * @param td_ptr the task descriptor of the delegated task
   * @param targer_resource the resource on which to place the task
   */
  virtual bool PlaceDelegatedTask(TaskDescriptor* td_ptr,
                                  ResourceID_t target_res_id) = 0;

  /**
   * Populates the scheduler resource specific UI template fields.
   * @param res_id the id of the resource for which to populate fields
   * @param dict the template dictionary to populate
   */
  virtual void PopulateSchedulerResourceUI(ResourceID_t res_id,
                                           TemplateDictionary* dict) const = 0;

  /**
   * Populates the scheduler task specific UI template fields.
   * @param task_id the id of the task for which to populate fields
   * @param dict the template dictionary to populate
   */
  virtual void PopulateSchedulerTaskUI(TaskID_t task_id,
                                       TemplateDictionary* dict) const = 0;

  /**
   * Registers a resource ID with the scheduler, who may subsequently assign
   * work to this resource.
   * @param res_id the id of the resource
   * @param local boolean to indicate if the resource is local or not
   */
  // TODO(malte): Add support for registering a resource with multiple
  // schedulers.
  // TODO(ionel): Remove default argument value once we refactor the method
  // to receive the executor.
  virtual void RegisterResource(ResourceID_t res_id,
                                bool local,
                                bool simulated = false) = 0;

  /**
   * Runs a scheduling iteration for all active jobs.
   * @return the number of tasks scheduled
   */
  virtual uint64_t ScheduleAllJobs(SchedulerStats* scheduler_stats) = 0;

  /**
   * Schedules all runnable tasks in a job.
   * WARNING: Using this method is inefficient because for every
   * invocation it traverses the entire resource graph.
   * @param jd_ptr the job descriptor for which to schedule tasks
   * @return the number of tasks scheduled
   */
  virtual uint64_t ScheduleJob(JobDescriptor* jd_ptr,
                               SchedulerStats* scheduler_stats) = 0;

  /**
   * Schedules the given jobs.
   * @param jds_ptr a vector of job descriptors
   * @return the number of tasks scheduled
   */
  virtual uint64_t ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                                SchedulerStats* scheduler_stats) = 0;

 protected:
  /**
   * Handles the migration of a task.
   * @param td_ptr the descriptor of the migrated task
   * @param rd_ptr the descriptor of the resource to which the task was migrated
   */
  virtual void HandleTaskMigration(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr) = 0;

  /**
   * Places a task to a resource, i.e. effects a scheduling assignment.
   * This will modify various bits of meta-data tracking assignments. It will
   * then delegate the actual execution of the task binary to the appropriate
   * local execution handler.
   * @param td_ptr the descriptor of the task to bind
   * @param rd_ptr the descriptor of the resource to bind to
   */
  virtual void HandleTaskPlacement(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr) = 0;

  /**
   * Finds runnable tasks for the job in the argument and adds them to the
   * global runnable set.
   * @param jd_ptr the descriptor of the job for which to find tasks
   */
  virtual const set<TaskID_t>& ComputeRunnableTasksForJob(
      JobDescriptor* jd_ptr) = 0;

  // Pointers to the associated coordinator's job, resource and object maps
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<KnowledgeBase> knowledge_base_;
  shared_ptr<ResourceMap_t> resource_map_;
  shared_ptr<TaskMap_t> task_map_;
  shared_ptr<store::ObjectStoreInterface> object_store_;
  // Resource topology (including any registered remote resources)
  ResourceTopologyNodeDescriptor* resource_topology_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_SCHEDULER_INTERFACE_H
