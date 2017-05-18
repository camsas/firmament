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

// Quincy scheduler.

#ifndef FIRMAMENT_SCHEDULING_FLOW_FLOW_SCHEDULER_H
#define FIRMAMENT_SCHEDULING_FLOW_FLOW_SCHEDULER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "engine/executors/executor_interface.h"
#include "misc/time_interface.h"
#include "scheduling/event_driven_scheduler.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/scheduling_delta.pb.h"
#include "scheduling/scheduling_event_notifier_interface.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/dimacs_exporter.h"
#include "scheduling/flow/flow_graph_manager.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "storage/reference_interface.h"

DECLARE_int32(flow_scheduling_cost_model);

namespace firmament {
namespace scheduler {

using executor::ExecutorInterface;

class FlowScheduler : public EventDrivenScheduler {
 public:
  FlowScheduler(shared_ptr<JobMap_t> job_map,
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
                TraceGenerator* trace_generator);
  ~FlowScheduler();
  virtual void DeregisterResource(ResourceTopologyNodeDescriptor* rtnd_ptr);
  virtual void HandleJobCompletion(JobID_t job_id);
  virtual void HandleJobRemoval(JobID_t job_id);
  virtual void HandleTaskCompletion(TaskDescriptor* td_ptr,
                                    TaskFinalReport* report);
  virtual void HandleTaskEviction(TaskDescriptor* td_ptr,
                                  ResourceDescriptor* rd_ptr);
  virtual void HandleTaskFailure(TaskDescriptor* td_ptr);
  virtual void HandleTaskFinalReport(const TaskFinalReport& report,
                                     TaskDescriptor* td_ptr);
  virtual void HandleTaskRemoval(TaskDescriptor* td_ptr);
  virtual void KillRunningTask(TaskID_t task_id,
                               TaskKillMessage::TaskKillReason reason);
  virtual void PopulateSchedulerResourceUI(ResourceID_t res_id,
                                           TemplateDictionary* dict) const;
  virtual void PopulateSchedulerTaskUI(TaskID_t task_id,
                                       TemplateDictionary* dict) const;
  virtual void RegisterResource(ResourceTopologyNodeDescriptor* rtnd_ptr,
                                bool local,
                                bool simulated);
  virtual uint64_t ScheduleAllJobs(SchedulerStats* scheduler_stats);
  virtual uint64_t ScheduleAllJobs(SchedulerStats* scheduler_stats,
                                   vector<SchedulingDelta>* deltas);
  virtual uint64_t ScheduleJob(JobDescriptor* jd_ptr,
                               SchedulerStats* scheduler_stats);
  virtual uint64_t ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                                SchedulerStats* scheduler_stats,
                                vector<SchedulingDelta>* deltas = NULL);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<FlowScheduler for coordinator " << coordinator_uri_
                   << ">";
  }

  const CostModelInterface& cost_model() const {
    return *cost_model_;
  }
  const SolverDispatcher& dispatcher() const {
    return *solver_dispatcher_;
  }

 protected:
  virtual void HandleTaskMigration(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr);
  virtual void HandleTaskPlacement(TaskDescriptor* td_ptr,
                                   ResourceDescriptor* rd_ptr);

 private:
  uint64_t ApplySchedulingDeltas(const vector<SchedulingDelta*>& deltas);
  void HandleTasksFromDeregisteredResource(
      ResourceTopologyNodeDescriptor* rtnd_ptr);
  void LogDebugCostModel();
  TaskDescriptor* ProducingTaskForDataObjectID(DataObjectID_t id);
  void RegisterLocalResource(ResourceID_t res_id);
  void RegisterRemoteResource(ResourceID_t res_id);
  uint64_t RunSchedulingIteration(SchedulerStats* scheduler_stats,
                                  vector<SchedulingDelta>* deltas_output);
  void UpdateCostModelResourceStats();

  // Pointer to the coordinator's topology manager
  shared_ptr<TopologyManager> topology_manager_;
  // Local storage of the current flow graph
  shared_ptr<FlowGraphManager> flow_graph_manager_;
  // The dispatcher runs different flow solvers.
  SolverDispatcher* solver_dispatcher_;
  // The scheduler's active cost model, used to construct the flow network and
  // assign costs to edges
  CostModelInterface* cost_model_;
  // Timestamp when the time-dependent costs in the graph were last updated
  uint64_t last_updated_time_dependent_costs_;
  // Set containing the resource ids of the PUs.
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  // Set storing the graph node id of the PUs that have been removed
  // while the solver was running. This set is used to make sure we don't
  // place tasks on PUs that have been removed.
  set<uint64_t> pus_removed_during_solver_run_;
  // Set of task node ids that have completed while the solver was running.
  // We use this set to make sure we don't try to place again the completed
  // tasks.
  set<uint64_t> tasks_completed_during_solver_run_;
  DIMACSChangeStats* dimacs_stats_;
  uint64_t solver_run_cnt_;
  unordered_set<ResourceTopologyNodeDescriptor*> resource_roots_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_SCHEDULER_H
