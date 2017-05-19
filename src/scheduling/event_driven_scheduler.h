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

// General abstract superclass for event-driven schedulers.

#ifndef FIRMAMENT_SCHEDULING_EVENT_DRIVEN_SCHEDULER_H
#define FIRMAMENT_SCHEDULING_EVENT_DRIVEN_SCHEDULER_H

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "base/task_final_report.pb.h"
#include "engine/executors/executor_interface.h"
#include "misc/messaging_interface.h"
#include "misc/time_interface.h"
#include "misc/trace_generator.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/scheduling_event_notifier_interface.h"
#include "storage/reference_interface.h"

namespace firmament {
namespace scheduler {

using executor::ExecutorInterface;

class EventDrivenScheduler : public SchedulerInterface {
 public:
  EventDrivenScheduler(shared_ptr<JobMap_t> job_map,
                       shared_ptr<ResourceMap_t> resource_map,
                       ResourceTopologyNodeDescriptor* resource_topology,
                       shared_ptr<store::ObjectStoreInterface> object_store,
                       shared_ptr<TaskMap_t> task_map,
                       shared_ptr<KnowledgeBase> knowledge_base,
                       shared_ptr<TopologyManager> topo_mgr,
                       MessagingAdapterInterface<BaseMessage>* m_adapter,
                       SchedulingEventNotifierInterface* event_notifier,
                       ResourceID_t coordinator_res_id,
                       const string& coordinator_uri,
                       TimeInterface* time_manager,
                       TraceGenerator* trace_generator);
  ~EventDrivenScheduler();
  virtual void AddJob(JobDescriptor* jd_ptr);
  ResourceID_t* BoundResourceForTask(TaskID_t task_id);
  vector<TaskID_t> BoundTasksForResource(ResourceID_t res_id);
  void CheckRunningTasksHealth();
  virtual void DeregisterResource(ResourceTopologyNodeDescriptor* rtnd_ptr);
  virtual void HandleJobCompletion(JobID_t job_id);
  virtual void HandleJobRemoval(JobID_t job_id);
  virtual void HandleReferenceStateChange(const ReferenceInterface& old_ref,
                                          const ReferenceInterface& new_ref,
                                          TaskDescriptor* td_ptr);
  virtual void HandleTaskCompletion(TaskDescriptor* td_ptr,
                                    TaskFinalReport* report);
  virtual void HandleTaskDelegationFailure(TaskDescriptor* td_ptr);
  virtual void HandleTaskDelegationSuccess(TaskDescriptor* td_ptr);
  virtual void HandleTaskEviction(TaskDescriptor* td_ptr,
                                  ResourceDescriptor* rd_ptr);
  virtual void HandleTaskFailure(TaskDescriptor* td_ptr);
  virtual void HandleTaskFinalReport(const TaskFinalReport& report,
                                     TaskDescriptor* td_ptr);
  virtual void HandleTaskRemoval(TaskDescriptor* td_ptr);
  virtual void KillRunningTask(TaskID_t task_id,
                               TaskKillMessage::TaskKillReason reason);
  bool PlaceDelegatedTask(TaskDescriptor* td, ResourceID_t target_resource);
  virtual void RegisterResource(ResourceTopologyNodeDescriptor* rtnd_ptr,
                                bool local,
                                bool simulated);
  // N.B. ScheduleJob must be implemented in scheduler-specific logic
  virtual uint64_t ScheduleAllJobs(SchedulerStats* scheduler_stats) = 0;
  virtual uint64_t ScheduleAllJobs(SchedulerStats* scheduler_stats,
                                   vector<SchedulingDelta>* deltas) = 0;
  virtual uint64_t ScheduleJob(JobDescriptor* jd_ptr,
                               SchedulerStats* scheduler_stats) = 0;
  virtual uint64_t ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                                SchedulerStats* scheduler_stats,
                                vector<SchedulingDelta>* deltas = NULL) = 0;
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<EventDrivenScheduler>";
  }

 protected:
  FRIEND_TEST(SimpleSchedulerTest, FindRunnableTasksForJob);
  FRIEND_TEST(SimpleSchedulerTest, FindRunnableTasksForComplexJob);
  FRIEND_TEST(SimpleSchedulerTest, FindRunnableTasksForComplexJob2);
  void BindTaskToResource(TaskDescriptor* td_ptr, ResourceDescriptor* rd_ptr);
  void CleanStateForDeregisteredResource(
      ResourceTopologyNodeDescriptor* rtnd_ptr);
  void DebugPrintRunnableTasks();
  void ExecuteTask(TaskDescriptor* td_ptr, ResourceDescriptor* rd_ptr);
  virtual void HandleTaskMigration(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr);
  virtual void HandleTaskPlacement(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr);
  void InsertTaskIntoRunnables(JobID_t job_id, TaskID_t task_id);
  void LazyGraphReduction(const unordered_set<DataObjectID_t*>& output_ids,
                          TaskDescriptor* root_task,
                          const JobID_t& job_id);
  unordered_set<TaskDescriptor*> ProducingTasksForDataObjectID(
      const DataObjectID_t& id,
      const JobID_t& cur_job);
  const unordered_set<ReferenceInterface*> ReferencesForID(
      const DataObjectID_t& id);
  void RegisterLocalResource(ResourceID_t res_id);
  void RegisterRemoteResource(ResourceID_t res_id);
  void RegisterSimulatedResource(ResourceID_t res_id);

  /**
   * Removes a resource from its parent's children list.
   * @param rtnd_ptr the resource node to remove
   */
  void RemoveResourceNodeFromParentChildrenList(
      ResourceTopologyNodeDescriptor* rtnd_ptr);

  const unordered_set<TaskID_t>& ComputeRunnableTasksForJob(
      JobDescriptor* job_desc);
  void SetupPUs(ResourceTopologyNodeDescriptor* rtnd_ptr,
                bool local,
                bool simulated);
  bool UnbindTaskFromResource(TaskDescriptor* td_ptr, ResourceID_t res_id);

  // Cached sets of runnable and blocked tasks; these are updated on each
  // execution of LazyGraphReduction. Note that this set includes tasks from all
  // jobs.
  unordered_map<JobID_t, unordered_set<TaskID_t>,
    boost::hash<JobID_t>> runnable_tasks_;
  // Initialized to hold the URI of the (currently unique) coordinator this
  // scheduler is associated with. This is passed down to the executor and to
  // tasks so that they can find the coordinator at runtime.
  const string coordinator_uri_;
  // We also record the resource ID of the owning coordinator.
  ResourceID_t coordinator_res_id_;
  // Object that is injected into the scheduler by other modules in order
  // to be notified when certain events happen.
  SchedulingEventNotifierInterface* event_notifier_;
  // A map holding pointers to all executors known to this scheduler. This
  // includes both executors for local and for remote resources.
  unordered_map<ResourceID_t, ExecutorInterface*,
    boost::hash<ResourceID_t>> executors_;
  // A vector holding descriptors of the jobs to be scheduled in the next
  // scheduling round.
  unordered_map<JobID_t, JobDescriptor*,
    boost::hash<JobID_t>> jobs_to_schedule_;
  // Pointer to messaging adapter to use for communication with remote
  // resources.
  MessagingAdapterInterface<BaseMessage>* m_adapter_ptr_;
  // A lock indicating if the scheduler is currently
  // in the process of making scheduling decisions.
  boost::recursive_mutex scheduling_lock_;
  // Map of reference subscriptions
  map<DataObjectID_t, unordered_set<TaskDescriptor*>> reference_subscriptions_;
  // The current resource to task bindings managed by this scheduler.
  unordered_multimap<ResourceID_t, TaskID_t, boost::hash<ResourceID_t>>
    resource_bindings_;
  // The current task bindings managed by this scheduler.
  unordered_map<TaskID_t, ResourceID_t> task_bindings_;
  // Pointer to the coordinator's topology manager
  shared_ptr<TopologyManager> topology_manager_;
  TimeInterface* time_manager_;
  TraceGenerator* trace_generator_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_EVENT_DRIVEN_SCHEDULER_H
