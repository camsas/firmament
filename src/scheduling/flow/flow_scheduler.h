// The Firmament project
// Copyright (c) 2012-2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2012-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
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
#include "engine/executor_interface.h"
#include "scheduling/event_driven_scheduler.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/scheduling_delta.pb.h"
#include "scheduling/flow/dimacs_exporter.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "scheduling/flow/scheduling_parameters.pb.h"
#include "storage/reference_interface.h"

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
                KnowledgeBase* kb,
                shared_ptr<TopologyManager> topo_mgr,
                MessagingAdapterInterface<BaseMessage>* m_adapter,
                ResourceID_t coordinator_res_id,
                const string& coordinator_uri,
                const SchedulingParameters& params);
  ~FlowScheduler();
  virtual void DeregisterResource(ResourceID_t res_id);
  virtual void HandleJobCompletion(JobID_t job_id);
  virtual void HandleTaskCompletion(TaskDescriptor* td_ptr,
                                    TaskFinalReport* report);
  virtual void HandleTaskEviction(TaskDescriptor* td_ptr, ResourceID_t res_id);
  virtual void HandleTaskFailure(TaskDescriptor* td_ptr);
  virtual void KillRunningTask(TaskID_t task_id,
                               TaskKillMessage::TaskKillReason reason);
  virtual void RegisterResource(ResourceID_t res_id, bool local);
  virtual uint64_t ScheduleJob(JobDescriptor* job_desc);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<FlowScheduler, parameters: "
                   << parameters_.DebugString() << ">";
  }

  const CostModelInterface& cost_model() const {
    return *cost_model_;
  }
  const SolverDispatcher& dispatcher() const {
    return *solver_dispatcher_;
  }

 protected:
  const ResourceID_t* FindResourceForTask(TaskDescriptor* task_desc);

 private:
  uint64_t ApplySchedulingDeltas(const vector<SchedulingDelta*>& deltas);
  void PrintGraph(vector< map<uint64_t, uint64_t> > adj_map);
  TaskDescriptor* ProducingTaskForDataObjectID(DataObjectID_t id);
  void RegisterLocalResource(ResourceID_t res_id);
  void RegisterRemoteResource(ResourceID_t res_id);
  uint64_t RunSchedulingIteration();
  void UpdateCostModelResourceStats();
  void UpdateResourceTopology(
      ResourceTopologyNodeDescriptor* resource_tree);

  // Pointer to the coordinator's topology manager
  shared_ptr<TopologyManager> topology_manager_;
  // Store a pointer to an external knowledge base.
  KnowledgeBase* knowledge_base_;
  // Local storage of the current flow graph
  shared_ptr<FlowGraph> flow_graph_;
  // Flow scheduler parameters (passed in as protobuf to constructor)
  SchedulingParameters parameters_;
  // The dispatcher runs different flow solvers.
  SolverDispatcher* solver_dispatcher_;
  // The scheduler's active cost model, used to construct the flow network and
  // assign costs to edges
  CostModelInterface* cost_model_;
  // Timestamp when the time-dependent costs in the graph were last updated
  uint64_t last_updated_time_dependent_costs_;
  // Set containing the resource ids of the PUs.
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_SCHEDULER_H
