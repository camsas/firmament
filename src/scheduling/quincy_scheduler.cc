// The Firmament project
// Copyright (c) 2013-2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of a Quincy-style min-cost flow scheduler.

#include "scheduling/quincy_scheduler.h"

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "storage/reference_types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "misc/string_utils.h"
#include "engine/local_executor.h"
#include "engine/remote_executor.h"
#include "storage/object_store_interface.h"
#include "scheduling/cost_models.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"
#include "scheduling/knowledge_base.h"

DEFINE_int32(flow_scheduling_cost_model, 0,
             "Flow scheduler cost model to use. "
             "Values: 0 = TRIVIAL, 1 = RANDOM, 2 = SJF, 3 = QUINCY, "
             "4 = WHARE, 5 = COCO, 6 = OCTOPUS");

namespace firmament {
namespace scheduler {

using executor::LocalExecutor;
using executor::RemoteExecutor;
using common::pb_to_set;
using store::ObjectStoreInterface;

QuincyScheduler::QuincyScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    const ResourceTopologyNodeDescriptor& resource_topology,
    shared_ptr<ObjectStoreInterface> object_store,
    shared_ptr<TaskMap_t> task_map,
    KnowledgeBase* kb,
    shared_ptr<TopologyManager> topo_mgr,
    MessagingAdapterInterface<BaseMessage>* m_adapter,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri,
    const SchedulingParameters& params)
    : EventDrivenScheduler(job_map, resource_map, resource_topology,
                           object_store, task_map, topo_mgr, m_adapter,
                           coordinator_res_id, coordinator_uri),
      topology_manager_(topo_mgr),
      knowledge_base_(kb),
      parameters_(params),
      leaf_res_ids_(new unordered_set<ResourceID_t,
                      boost::hash<boost::uuids::uuid>>) {
  // Select the cost model to use
  VLOG(1) << "Set cost model to use in flow graph to \""
          << FLAGS_flow_scheduling_cost_model << "\"";

  switch (FLAGS_flow_scheduling_cost_model) {
    case FlowSchedulingCostModelType::COST_MODEL_TRIVIAL:
      cost_model_ = new TrivialCostModel(task_map, leaf_res_ids_);
      VLOG(1) << "Using the trivial cost model";
      break;
    case FlowSchedulingCostModelType::COST_MODEL_RANDOM:
      cost_model_ = new RandomCostModel(task_map, leaf_res_ids_);
      VLOG(1) << "Using the random cost model";
      break;
    case FlowSchedulingCostModelType::COST_MODEL_COCO:
      cost_model_ = new CocoCostModel(resource_map, task_map, leaf_res_ids_,
                                     knowledge_base_);
      VLOG(1) << "Using the coco cost model";
      break;
    case FlowSchedulingCostModelType::COST_MODEL_SJF:
      cost_model_ = new SJFCostModel(task_map, leaf_res_ids_, knowledge_base_);
      VLOG(1) << "Using the SJF cost model";
      break;
    case FlowSchedulingCostModelType::COST_MODEL_QUINCY:
      cost_model_ = new QuincyCostModel(resource_map, job_map, task_map,
                                       &task_bindings_, leaf_res_ids_,
                                       knowledge_base_);
      VLOG(1) << "Using the Quincy cost model";
      break;
    case FlowSchedulingCostModelType::COST_MODEL_WHARE:
      cost_model_ = new WhareMapCostModel(resource_map, task_map,
                                         knowledge_base_);
      VLOG(1) << "Using the Whare-Map cost model";
      break;
    case FlowSchedulingCostModelType::COST_MODEL_OCTOPUS:
      cost_model_ = new OctopusCostModel(resource_map);
      VLOG(1) << "Using the octopus cost model";
      break;
    default:
      LOG(FATAL) << "Unknown flow scheduling cost model specificed "
                 << "(" << FLAGS_flow_scheduling_cost_model << ")";
  }

  flow_graph_.reset(new FlowGraph(cost_model_, leaf_res_ids_));
  knowledge_base_->SetCostModel(cost_model_);

  LOG(INFO) << "QuincyScheduler initiated; parameters: "
            << parameters_.ShortDebugString();
  // Set up the initial flow graph
  UpdateResourceTopology(resource_topology);
  // Set up the dispatcher, which starts the flow solver
  quincy_dispatcher_ = new QuincyDispatcher(flow_graph_, false);
}

QuincyScheduler::~QuincyScheduler() {
  delete quincy_dispatcher_;
  delete leaf_res_ids_;
  // XXX(ionel): stub
}

const ResourceID_t* QuincyScheduler::FindResourceForTask(
    TaskDescriptor*) {
  // XXX(ionel): stub
  return NULL;
}

uint64_t QuincyScheduler::ApplySchedulingDeltas(
    const vector<SchedulingDelta*>& deltas) {
  uint64_t num_scheduled = 0;
  // Perform the necessary actions to apply the scheduling changes passed to the
  // method
  VLOG(1) << "Applying " << deltas.size() << " scheduling deltas...";
  for (vector<SchedulingDelta*>::const_iterator it = deltas.begin();
       it != deltas.end();
       ++it) {
    VLOG(1) << "Processing delta of type " << (*it)->type();
    TaskID_t task_id = (*it)->task_id();
    ResourceID_t res_id = ResourceIDFromString((*it)->resource_id());
    if ((*it)->type() == SchedulingDelta::PLACE) {
      VLOG(1) << "Trying to place task " << task_id
              << " on resource " << (*it)->resource_id();
      TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
      CHECK_NOTNULL(td);
      CHECK_NOTNULL(rs);
      VLOG(1) << "About to bind task " << td->uid() << " to resource "
              << rs->mutable_descriptor()->uuid();
      BindTaskToResource(td, rs->mutable_descriptor());
      // After the task is bound, we now remove all of its edges into the flow
      // graph apart from the bound resource.
      // N.B.: This disables preemption and migration!
      flow_graph_->TaskScheduled(task_id, res_id);
      // Tag the job to which this task belongs as running
      JobDescriptor* jd = FindOrNull(*job_map_, JobIDFromString(td->job_id()));
      if (jd->state() != JobDescriptor::RUNNING)
        jd->set_state(JobDescriptor::RUNNING);
      num_scheduled++;
      (*it)->set_actioned(true);
    }
  }
  return num_scheduled;
}

void QuincyScheduler::DeregisterResource(ResourceID_t res_id) {
  EventDrivenScheduler::DeregisterResource(res_id);
  {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    flow_graph_->RemoveMachine(res_id);
  }
}

void QuincyScheduler::HandleJobCompletion(JobID_t job_id) {
  // Call into superclass handler
  EventDrivenScheduler::HandleJobCompletion(job_id);
  {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    // Job completed, so remove its nodes
    flow_graph_->JobCompleted(job_id);
  }
}

void QuincyScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                           TaskFinalReport* report) {
  // Call into superclass handler
  EventDrivenScheduler::HandleTaskCompletion(td_ptr, report);
  {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    flow_graph_->TaskCompleted(td_ptr->uid());
  }
}

void QuincyScheduler::HandleTaskFailure(TaskDescriptor* td_ptr) {
  EventDrivenScheduler::HandleTaskFailure(td_ptr);
  {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    flow_graph_->TaskFailed(td_ptr->uid());
  }
}

void QuincyScheduler::KillRunningTask(TaskID_t task_id,
                                      TaskKillMessage::TaskKillReason reason) {
  EventDrivenScheduler::KillRunningTask(task_id, reason);
  {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    flow_graph_->TaskKilled(task_id);
  }
}

uint64_t QuincyScheduler::ScheduleJob(JobDescriptor* job_desc) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  LOG(INFO) << "START SCHEDULING " << job_desc->uuid();
  // Check if we have any runnable tasks in this job
  const set<TaskID_t> runnable_tasks = RunnableTasksForJob(job_desc);
  if (runnable_tasks.size() > 0) {
    // Check if the job is already in the flow graph
    // If not, simply add the whole job
    flow_graph_->AddOrUpdateJobNodes(job_desc);
    // If it is, only add the new bits
    // Run a scheduler iteration
    uint64_t newly_scheduled = RunSchedulingIteration();
    LOG(INFO) << "STOP SCHEDULING " << job_desc->uuid();
    return newly_scheduled;
  } else {
    LOG(INFO) << "STOP SCHEDULING " << job_desc->uuid();
    return 0;
  }
}

void QuincyScheduler::RegisterResource(ResourceID_t res_id, bool local) {
  {
    boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
    // Update the flow graph
    UpdateResourceTopology(resource_topology_);
  }
  // Call into superclass method to do scheduler resource initialisation.
  // This will create the executor for the new resource.
  EventDrivenScheduler::RegisterResource(res_id, local);
}

uint64_t QuincyScheduler::RunSchedulingIteration() {
  multimap<uint64_t, uint64_t>* task_mappings = quincy_dispatcher_->Run();
  // Solver's done, let's post-process the results.
  multimap<uint64_t, uint64_t>::iterator it;
  vector<SchedulingDelta*> deltas;
  for (it = task_mappings->begin(); it != task_mappings->end(); it++) {
    VLOG(1) << "Bind " << it->first << " to " << it->second << endl;
    SchedulingDelta* delta = new SchedulingDelta;
    quincy_dispatcher_->NodeBindingToSchedulingDelta(
        *flow_graph_->Node(it->first), *flow_graph_->Node(it->second),
        &task_bindings_, delta);
    if (delta->type() == SchedulingDelta::NOOP)
      continue;
    // Mark the task as scheduled
    flow_graph_->Node(it->first)->type_.set_type(FlowNodeType::SCHEDULED_TASK);
    // Remember the delta
    deltas.push_back(delta);
  }
  uint64_t num_scheduled = ApplySchedulingDeltas(deltas);
  // Drop all deltas that were actioned
  for (vector<SchedulingDelta*>::iterator it = deltas.begin();
       it != deltas.end(); ) {
    if ((*it)->actioned())
      it = deltas.erase(it);
    else
      it++;
  }
  if (deltas.size() > 0)
    LOG(WARNING) << "Not all deltas were processed, " << deltas.size()
                 << " remain!";

  switch (FLAGS_flow_scheduling_cost_model) {
    case FlowSchedulingCostModelType::COST_MODEL_WHARE:
      flow_graph_->ComputeTopologyStatistics(
          flow_graph_->sink_node(),
          boost::bind(&QuincyScheduler::GatherWhareMCStats, this, _1, _2));
      break;
    case FlowSchedulingCostModelType::COST_MODEL_OCTOPUS:
      flow_graph_->ComputeTopologyStatistics(
          flow_graph_->sink_node(),
          boost::bind(&QuincyScheduler::GatherOctopusStats, this, _1, _2));
      flow_graph_->ComputeTopologyStatistics(
          flow_graph_->sink_node(),
          boost::bind(&QuincyScheduler::UpdateOctopusCosts, this, _1, _2));
      break;
    default:
      LOG(INFO) << "No resource stats update required";
  }
  return num_scheduled;
}

void QuincyScheduler::PrintGraph(vector< map<uint64_t, uint64_t> > adj_map) {
  for (vector< map<uint64_t, uint64_t> >::size_type i = 1;
       i < adj_map.size(); ++i) {
    map<uint64_t, uint64_t>::iterator it;
    for (it = adj_map[i].begin();
         it != adj_map[i].end(); it++) {
      cout << i << " " << it->first << " " << it->second << endl;
    }
  }
}

void QuincyScheduler::UpdateResourceTopology(
    const ResourceTopologyNodeDescriptor& root) {
  // Run a topology refresh (somewhat expensive!); if only two nodes exist, the
  // flow graph is empty apart from cluster aggregator and sink.
  VLOG(1) << "Num nodes in flow graph is: " << flow_graph_->NumNodes();
  if (flow_graph_->NumNodes() == 1) {
    flow_graph_->AddResourceTopology(root);
  } else {
    flow_graph_->AddMachine(root);
  }
}

FlowGraphNode* QuincyScheduler::GatherWhareMCStats(FlowGraphNode* accumulator,
                                                   FlowGraphNode* other) {
  if (accumulator->type_.type() == FlowNodeType::ROOT_TASK ||
      accumulator->type_.type() == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_.type() == FlowNodeType::SINK) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    if (accumulator->type_.type() == FlowNodeType::PU) {
      // Base case. We are at a PU and we gather the statistics.
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, accumulator->resource_id_);
      CHECK_NOTNULL(rs_ptr);
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      if (rd_ptr->has_current_running_task()) {
        TaskDescriptor* td_ptr =
          FindPtrOrNull(*task_map_, rd_ptr->current_running_task());
        if (td_ptr->has_task_type()) {
          // TODO(ionel): Gather the statistics.
          WhareMapStats* wms_ptr = rd_ptr->mutable_whare_map_stats();
          if (td_ptr->task_type() == TaskDescriptor::DEVIL) {
            wms_ptr->set_num_devils(1);
          } else if (td_ptr->task_type() == TaskDescriptor::RABBIT) {
            wms_ptr->set_num_rabbits(1);
          } else if (td_ptr->task_type() == TaskDescriptor::SHEEP) {
            wms_ptr->set_num_sheep(1);
          } else if (td_ptr->task_type() == TaskDescriptor::TURTLE) {
            wms_ptr->set_num_turtles(1);
          } else {
            LOG(FATAL) << "Unexpected task type";
          }
        } else {
          LOG(WARNING) << "Task " << td_ptr->uid() << " does not have a type";
        }
      }
    }
    return accumulator;
  }
  if (accumulator->type_.type() == FlowNodeType::EQUIVALENCE_CLASS) {

    if (!other->resource_id_.is_nil() &&
        other->type_.type() == FlowNodeType::MACHINE) {
      // If the other node is a machine.
      //    AccumulateWhareMapStats(accumulator, other);
    }
    // TODO(ionel): Update knowledge base.
    return accumulator;
  }
  ResourceStatus* acc_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  CHECK_NOTNULL(acc_rs_ptr);
  WhareMapStats* wms_acc_ptr =
    acc_rs_ptr->mutable_descriptor()->mutable_whare_map_stats();
  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, other->resource_id_);
  CHECK_NOTNULL(other_rs_ptr);
  WhareMapStats* wms_other_ptr =
    other_rs_ptr->mutable_descriptor()->mutable_whare_map_stats();
  if (accumulator->type_.type() == FlowNodeType::MACHINE) {
    AccumulateWhareMapStats(wms_acc_ptr, wms_other_ptr);
    // TODO(ionel): Update knowledge base.
    return accumulator;
  }
  AccumulateWhareMapStats(wms_acc_ptr, wms_other_ptr);
  return accumulator;
}

void QuincyScheduler::AccumulateWhareMapStats(WhareMapStats* accumulator,
                                              WhareMapStats* other) {
  accumulator->set_num_devils(accumulator->num_devils() +
                              other->num_devils());
  accumulator->set_num_rabbits(accumulator->num_rabbits() +
                               other->num_rabbits());
  accumulator->set_num_sheep(accumulator->num_sheep() +
                             other->num_sheep());
  accumulator->set_num_turtles(accumulator->num_turtles() +
                               other->num_turtles());
}

FlowGraphNode* QuincyScheduler::GatherOctopusStats(FlowGraphNode* accumulator,
                                                   FlowGraphNode* other) {
  if (accumulator->type_.type() == FlowNodeType::ROOT_TASK ||
      accumulator->type_.type() == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_.type() == FlowNodeType::SINK ||
      accumulator->type_.type() == FlowNodeType::EQUIVALENCE_CLASS) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    return accumulator;
  }
  if (other->resource_id_.is_nil()) {
    if (accumulator->type_.type() == FlowNodeType::PU) {
      // Base case. We are at a PU and we gather the statistics.
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, accumulator->resource_id_);
      CHECK_NOTNULL(rs_ptr);
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      if (rd_ptr->has_current_running_task()) {
        rd_ptr->set_num_running_tasks(1);
      } else {
        rd_ptr->set_num_running_tasks(0);
      }
    }
    return accumulator;
  }

  ResourceStatus* acc_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  CHECK_NOTNULL(acc_rs_ptr);
  ResourceDescriptor* acc_rd_ptr = acc_rs_ptr->mutable_descriptor();

  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  CHECK_NOTNULL(other_rs_ptr);
  ResourceDescriptor* other_rd_ptr = other_rs_ptr->mutable_descriptor();
  acc_rd_ptr->set_num_running_tasks(acc_rd_ptr->num_running_tasks() +
                                    other_rd_ptr->num_running_tasks());
  return accumulator;
}

FlowGraphNode* QuincyScheduler::UpdateOctopusCosts(FlowGraphNode* accumulator,
                                                   FlowGraphNode* other) {
  if (accumulator->type_.type() == FlowNodeType::ROOT_TASK ||
      accumulator->type_.type() == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_.type() == FlowNodeType::SINK ||
      accumulator->type_.type() == FlowNodeType::EQUIVALENCE_CLASS) {
    // Reset the state.
    ResourceStatus* other_rs_ptr =
      FindPtrOrNull(*resource_map_, accumulator->resource_id_);
    if (other_rs_ptr != NULL) {
      ResourceDescriptor* other_rd_ptr = other_rs_ptr->mutable_descriptor();
      other_rd_ptr->set_num_running_tasks(0);
    }
    return accumulator;
  }
  if (other->resource_id_.is_nil()) {
    return accumulator;
  }
  unordered_map<uint64_t, FlowGraphArc*>::iterator arc_it =
    accumulator->outgoing_arc_map_.find(other->id_);
  if (arc_it == accumulator->outgoing_arc_map_.end()) {
    LOG(FATAL) << "Could not find arc";
  }
  arc_it->second->cost_ =
    cost_model_->ResourceNodeToResourceNodeCost(accumulator->resource_id_,
                                                other->resource_id_);
  // Reset the state.
  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  CHECK_NOTNULL(other_rs_ptr);
  ResourceDescriptor* other_rd_ptr = other_rs_ptr->mutable_descriptor();
  other_rd_ptr->set_num_running_tasks(0);

  return accumulator;
}

}  // namespace scheduler
}  // namespace firmament
