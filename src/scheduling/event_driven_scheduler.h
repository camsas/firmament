// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// General abstract superclass for event-driven schedulers.

#ifndef FIRMAMENT_ENGINE_EVENT_DRIVEN_SCHEDULER_H
#define FIRMAMENT_ENGINE_EVENT_DRIVEN_SCHEDULER_H

#include <map>
#include <set>
#include <string>

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "base/task_final_report.pb.h"
#include "engine/executor_interface.h"
#include "scheduling/scheduler_interface.h"
#include "storage/reference_interface.h"

namespace firmament {
namespace scheduler {

using executor::ExecutorInterface;

class EventDrivenScheduler : public SchedulerInterface {
 public:
  EventDrivenScheduler(shared_ptr<JobMap_t> job_map,
                       shared_ptr<ResourceMap_t> resource_map,
                       const ResourceTopologyNodeDescriptor& resource_topology,
                       shared_ptr<store::ObjectStoreInterface> object_store,
                       shared_ptr<TaskMap_t> task_map,
                       shared_ptr<TopologyManager> topo_mgr,
                       MessagingAdapterInterface<BaseMessage>* m_adapter,
                       ResourceID_t coordinator_res_id,
                       const string& coordinator_uri);
  ~EventDrivenScheduler();
  ResourceID_t* BoundResourceForTask(TaskID_t task_id);
  void CheckRunningTasksHealth() ;
  virtual void DeregisterResource(ResourceID_t res_id);
  ExecutorInterface* GetExecutorForTask(TaskID_t task_id);
  void HandleJobCompletion(JobID_t job_id);
  void HandleReferenceStateChange(const ReferenceInterface& old_ref,
                                  const ReferenceInterface& new_ref,
                                  TaskDescriptor* td_ptr);
  void HandleTaskCompletion(TaskDescriptor* td_ptr,
                            TaskFinalReport* report);
  void HandleTaskFailure(TaskDescriptor* td_ptr);
  void KillRunningTask(TaskID_t task_id,
                       TaskKillMessage::TaskKillReason reason);
  bool PlaceDelegatedTask(TaskDescriptor* td, ResourceID_t target_resource);
  virtual void RegisterResource(ResourceID_t res_id, bool local);
  const set<TaskID_t>& RunnableTasksForJob(JobDescriptor* job_desc);
  // N.B. ScheduleJob must be implemented in scheduler-specific logic
  virtual uint64_t ScheduleJob(JobDescriptor* job_desc) = 0;
  bool UnbindResourceForTask(TaskID_t task_id);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<EventDrivenScheduler>";
  }


 protected:
  void BindTaskToResource(TaskDescriptor* task_desc,
                          ResourceDescriptor* res_desc);
  void DebugPrintRunnableTasks();
  uint64_t LazyGraphReduction(const set<DataObjectID_t*>& output_ids,
                              TaskDescriptor* root_task,
                              const JobID_t& job_id);
  set<TaskDescriptor*> ProducingTasksForDataObjectID(const DataObjectID_t& id,
                                                     const JobID_t& cur_job);
  const set<ReferenceInterface*> ReferencesForID(const DataObjectID_t& id);
  void RegisterLocalResource(ResourceID_t res_id);
  void RegisterRemoteResource(ResourceID_t res_id);

  // Cached sets of runnable and blocked tasks; these are updated on each
  // execution of LazyGraphReduction. Note that this set includes tasks from all
  // jobs.
  set<TaskID_t> runnable_tasks_;
  set<TaskDescriptor*> blocked_tasks_;
  // Initialized to hold the URI of the (currently unique) coordinator this
  // scheduler is associated with. This is passed down to the executor and to
  // tasks so that they can find the coordinator at runtime.
  const string coordinator_uri_;
  // We also record the resource ID of the owning coordinator.
  ResourceID_t coordinator_res_id_;
  // A map holding pointers to all executors known to this scheduler. This
  // includes both executors for local and for remote resources.
  map<ResourceID_t, ExecutorInterface*> executors_;
  // The current task bindings managed by this scheduler.
  map<TaskID_t, ResourceID_t> task_bindings_;
  // Map of reference subscriptions
  map<DataObjectID_t, set<TaskDescriptor*> > reference_subscriptions_;
  // Pointer to the coordinator's topology manager
  shared_ptr<TopologyManager> topology_manager_;
  // Pointer to messaging adapter to use for communication with remote
  // resources.
  MessagingAdapterInterface<BaseMessage>* m_adapter_ptr_;
  // A lock indicating if the scheduler is currently
  // in the process of making scheduling decisions.
  boost::recursive_mutex scheduling_lock_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EVENT_DRIVEN_SCHEDULER_H
