// The Firmament project
// Copyright (c) 2012-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Quincy scheduler.

#ifndef FIRMAMENT_ENGINE_QUINCY_SCHEDULER_H
#define FIRMAMENT_ENGINE_QUINCY_SCHEDULER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "base/flow_node_type.pb.h"
#include "engine/executor_interface.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/flow_graph.h"
#include "scheduling/scheduling_parameters.pb.h"
#include "scheduling/event_driven_scheduler.h"
#include "storage/reference_interface.h"

namespace firmament {
namespace scheduler {

using executor::ExecutorInterface;

class QuincyScheduler : public EventDrivenScheduler {
 public:
  QuincyScheduler(shared_ptr<JobMap_t> job_map,
                  shared_ptr<ResourceMap_t> resource_map,
                  shared_ptr<store::ObjectStoreInterface> object_store,
                  shared_ptr<TaskMap_t> task_map,
                  shared_ptr<TopologyManager> topo_mgr,
                  MessagingAdapterInterface<BaseMessage>* m_adapter,
                  ResourceID_t coordinator_res_id,
                  const string& coordinator_uri,
                  const SchedulingParameters& params);
  ~QuincyScheduler();
  void DeregisterResource(ResourceID_t res_id);
  void RegisterResource(ResourceID_t res_id, bool local);
  void HandleTaskCompletion(TaskDescriptor* td_ptr);
  bool PlaceDelegatedTask(TaskDescriptor* td, ResourceID_t target_resource);
  const set<TaskID_t>& RunnableTasksForJob(JobDescriptor* job_desc);
  uint64_t ScheduleJob(JobDescriptor* job_desc);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<QuincyScheduler, parameters: "
                   << parameters_.DebugString() << ">";
  }

 protected:
  void BindTaskToResource(TaskDescriptor* task_desc,
                          ResourceDescriptor* res_desc);
  const ResourceID_t* FindResourceForTask(TaskDescriptor* task_desc);

 private:
  uint64_t AssignNode(
      vector< map< uint64_t, uint64_t > > &flow_graph,
      const map<uint64_t, uint64_t>& nodes_type,
      uint64_t node);
  void ApplyDeltas();
  bool CheckNodeType(
      const map<uint64_t, uint64_t>& nodes_type, uint64_t node, uint64_t type);
  map<uint64_t, uint64_t> GetMappings(
      vector< map< uint64_t, uint64_t > >& flow_graph,
      const map<uint64_t, uint64_t>& nodes_type,
      set<uint64_t> leaves,
      uint64_t sink);
  void PrintGraph(vector< map<uint64_t, uint64_t> > adj_map);
  vector< map< uint64_t, uint64_t> > ReadFlowGraph(
      char* file_name, uint64_t num_vertices);
  void RegisterLocalResource(ResourceID_t res_id);
  void RegisterRemoteResource(ResourceID_t res_id);
  void RunSchedulingIteration();

  TaskDescriptor* ProducingTaskForDataObjectID(DataObjectID_t id);
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
  map<ResourceID_t, ExecutorInterface*> executors_;
  map<TaskID_t, ResourceID_t> task_bindings_;
  // Pointer to the coordinator's topology manager
  shared_ptr<TopologyManager> topology_manager_;
  // Pointer to messaging adapter to use for communication with remote
  // resources.
  MessagingAdapterInterface<BaseMessage>* m_adapter_ptr_;
  // Flag (effectively a lock) indicating if the scheduler is currently
  // in the process of making scheduling decisions.
  bool scheduling_;
  // Local storage of the current flow graph
  FlowGraph flow_graph_;
  // Flow scheduler parameters (passed in as protobuf to constructor)
  SchedulingParameters parameters_;
  // DIMACS exporter for interfacing to the solver
  DIMACSExporter exporter_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_QUINCY_SCHEDULER_H
