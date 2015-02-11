// The Firmament project
// Copyright (c) 2012-2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2012-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
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
#include "engine/executor_interface.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/event_driven_scheduler.h"
#include "scheduling/flow_graph.h"
#include "scheduling/flow_node_type.pb.h"
#include "scheduling/quincy_dispatcher.h"
#include "scheduling/scheduling_delta.pb.h"
#include "scheduling/scheduling_parameters.pb.h"
#include "storage/reference_interface.h"

namespace firmament {
namespace scheduler {

using executor::ExecutorInterface;

class QuincyScheduler : public EventDrivenScheduler {
 public:
  QuincyScheduler(shared_ptr<JobMap_t> job_map,
                  shared_ptr<ResourceMap_t> resource_map,
                  const ResourceTopologyNodeDescriptor& resource_topology,
                  shared_ptr<ObjectStoreInterface> object_store,
                  shared_ptr<TaskMap_t> task_map,
                  shared_ptr<TopologyManager> topo_mgr,
                  MessagingAdapterInterface<BaseMessage>* m_adapter,
                  ResourceID_t coordinator_res_id,
                  const string& coordinator_uri,
                  const SchedulingParameters& params);
  ~QuincyScheduler();
  void HandleTaskCompletion(TaskDescriptor* td_ptr,
                            TaskFinalReport* report);
  virtual void RegisterResource(ResourceID_t res_id, bool local);
  uint64_t ScheduleJob(JobDescriptor* job_desc);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<QuincyScheduler, parameters: "
                   << parameters_.DebugString() << ">";
  }

 protected:
  const ResourceID_t* FindResourceForTask(TaskDescriptor* task_desc);

 private:
  uint64_t ApplySchedulingDeltas(const vector<SchedulingDelta*>& deltas);
  void ApplyDeltas();
  void NodeBindingToSchedulingDelta(const FlowGraphNode& src,
                                    const FlowGraphNode& dst,
                                    SchedulingDelta* delta);
  void PrintGraph(vector< map<uint64_t, uint64_t> > adj_map);
  TaskDescriptor* ProducingTaskForDataObjectID(DataObjectID_t id);
  void RegisterLocalResource(ResourceID_t res_id);
  void RegisterRemoteResource(ResourceID_t res_id);
  uint64_t RunSchedulingIteration();
  void UpdateResourceTopology(
      const ResourceTopologyNodeDescriptor& resource_tree);

  map<TaskID_t, ResourceID_t> task_bindings_;
  // Pointer to the coordinator's topology manager
  shared_ptr<TopologyManager> topology_manager_;
  // Local storage of the current flow graph
  shared_ptr<FlowGraph> flow_graph_;
  // Flow scheduler parameters (passed in as protobuf to constructor)
  SchedulingParameters parameters_;
  QuincyDispatcher* quincy_dispatcher_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_QUINCY_SCHEDULER_H
