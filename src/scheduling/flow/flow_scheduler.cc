// The Firmament project
// Copyright (c) 2013-2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Implementation of a Quincy-style min-cost flow scheduler.

#include "scheduling/flow/flow_scheduler.h"

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "misc/string_utils.h"
#include "storage/object_store_interface.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_models.h"
#include "scheduling/flow/cost_model_interface.h"

DEFINE_int32(flow_scheduling_cost_model, 0,
             "Flow scheduler cost model to use. "
             "Values: 0 = TRIVIAL, 1 = RANDOM, 2 = SJF, 3 = QUINCY, "
             "4 = WHARE, 5 = COCO, 6 = OCTOPUS");
DEFINE_int64(time_dependent_cost_update_frequency, 10000000ULL,
             "Update frequency for time-dependent costs, in microseconds.");
DEFINE_bool(debug_cost_model, false,
            "Store cost model debug info in CSV files.");

namespace firmament {
namespace scheduler {

using common::pb_to_set;
using store::ObjectStoreInterface;

FlowScheduler::FlowScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    ResourceTopologyNodeDescriptor* resource_topology,
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
      last_updated_time_dependent_costs_(0ULL),
      leaf_res_ids_(new unordered_set<ResourceID_t,
                      boost::hash<boost::uuids::uuid>>) {
  // Select the cost model to use
  VLOG(1) << "Set cost model to use in flow graph to \""
          << FLAGS_flow_scheduling_cost_model << "\"";

  switch (FLAGS_flow_scheduling_cost_model) {
    case CostModelType::COST_MODEL_TRIVIAL:
      cost_model_ = new TrivialCostModel(task_map, leaf_res_ids_);
      VLOG(1) << "Using the trivial cost model";
      break;
    case CostModelType::COST_MODEL_RANDOM:
      cost_model_ = new RandomCostModel(task_map, leaf_res_ids_);
      VLOG(1) << "Using the random cost model";
      break;
    case CostModelType::COST_MODEL_COCO:
      cost_model_ = new CocoCostModel(resource_map, *resource_topology,
                                      task_map, leaf_res_ids_, knowledge_base_);
      VLOG(1) << "Using the coco cost model";
      break;
    case CostModelType::COST_MODEL_SJF:
      cost_model_ = new SJFCostModel(task_map, leaf_res_ids_, knowledge_base_);
      VLOG(1) << "Using the SJF cost model";
      break;
    case CostModelType::COST_MODEL_QUINCY:
      cost_model_ = new QuincyCostModel(resource_map, job_map, task_map,
                                        &task_bindings_, leaf_res_ids_,
                                        knowledge_base_);
      VLOG(1) << "Using the Quincy cost model";
      break;
    case CostModelType::COST_MODEL_WHARE:
      cost_model_ = new WhareMapCostModel(resource_map, task_map,
                                          knowledge_base_);
      VLOG(1) << "Using the Whare-Map cost model";
      break;
    case CostModelType::COST_MODEL_OCTOPUS:
      cost_model_ = new OctopusCostModel(resource_map, task_map);
      VLOG(1) << "Using the octopus cost model";
      break;
    default:
      LOG(FATAL) << "Unknown flow scheduling cost model specificed "
                 << "(" << FLAGS_flow_scheduling_cost_model << ")";
  }

  flow_graph_.reset(new FlowGraph(cost_model_, leaf_res_ids_));
  cost_model_->SetFlowGraph(flow_graph_);
  knowledge_base_->SetCostModel(cost_model_);

  LOG(INFO) << "FlowScheduler initiated; parameters: "
            << parameters_.ShortDebugString();
  // Set up the initial flow graph
  UpdateResourceTopology(resource_topology);
  // Set up the dispatcher, which starts the flow solver
  solver_dispatcher_ = new SolverDispatcher(flow_graph_, false);
}

FlowScheduler::~FlowScheduler() {
  delete solver_dispatcher_;
  delete leaf_res_ids_;
  // XXX(ionel): stub
}

const ResourceID_t* FlowScheduler::FindResourceForTask(
    TaskDescriptor*) {
  // XXX(ionel): stub
  return NULL;
}

uint64_t FlowScheduler::ApplySchedulingDeltas(
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
    if ((*it)->type() == SchedulingDelta::NOOP) {
      // We should not get any NOOP deltas as they get filtered before, but
      // let's handle it anyway.
      continue;
    } else if ((*it)->type() == SchedulingDelta::PLACE) {
      VLOG(1) << "Trying to place task " << task_id
              << " on resource " << (*it)->resource_id();
      TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
      CHECK_NOTNULL(td);
      CHECK_NOTNULL(rs);
      LOG(INFO) << "Scheduler binding task " << td->uid() << " to resource "
                << rs->mutable_descriptor()->uuid();
      BindTaskToResource(td, rs->mutable_descriptor());
      // Mark the task as scheduled
      FlowGraphNode* node = flow_graph_->NodeForTaskID(task_id);
      CHECK_NOTNULL(node);
      node->type_ = FlowNodeType::SCHEDULED_TASK;
      // After the task is bound, we now remove all of its edges into the flow
      // graph apart from the bound resource.
      // N.B.: This disables preemption and migration, unless FLAGS_preemption
      // is set!
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

void FlowScheduler::DeregisterResource(ResourceID_t res_id) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  EventDrivenScheduler::DeregisterResource(res_id);
  flow_graph_->RemoveMachine(res_id);
}

void FlowScheduler::HandleJobCompletion(JobID_t job_id) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Call into superclass handler
  EventDrivenScheduler::HandleJobCompletion(job_id);
  // Job completed, so remove its nodes
  flow_graph_->JobCompleted(job_id);
}

void FlowScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                           TaskFinalReport* report) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Call into superclass handler
  EventDrivenScheduler::HandleTaskCompletion(td_ptr, report);
  // We don't need to do any flow graph stuff for delegated tasks as
  // they are not currently represented in the flow graph.
  // Otherwise, we need to remove nodes, etc.
  if (!td_ptr->has_delegated_from()) {
    flow_graph_->TaskCompleted(td_ptr->uid());
  }
}

void FlowScheduler::HandleTaskEviction(TaskDescriptor* td_ptr,
                                         ResourceID_t res_id) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  EventDrivenScheduler::HandleTaskEviction(td_ptr, res_id);
  flow_graph_->TaskEvicted(td_ptr->uid(), res_id);
}

void FlowScheduler::HandleTaskFailure(TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  EventDrivenScheduler::HandleTaskFailure(td_ptr);
  flow_graph_->TaskFailed(td_ptr->uid());
}

void FlowScheduler::KillRunningTask(TaskID_t task_id,
                                      TaskKillMessage::TaskKillReason reason) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  EventDrivenScheduler::KillRunningTask(task_id, reason);
  flow_graph_->TaskKilled(task_id);
}

uint64_t FlowScheduler::ScheduleJob(JobDescriptor* job_desc) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  LOG(INFO) << "START SCHEDULING (via " << job_desc->uuid() << ")";
  // First, we update the cost model's resource topology statistics (e.g. based
  // on machine load and prior decisions); these need to be known before
  // AddOrUpdateJobNodes is invoked below, as it may add arcs depending on these
  // metrics.
  UpdateCostModelResourceStats();
  // Check if we have any runnable tasks in this job
  const set<TaskID_t> runnable_tasks = RunnableTasksForJob(job_desc);
  if (runnable_tasks.size() > 0) {
    // Check if the job is already in the flow graph
    // If not, simply add the whole job
    flow_graph_->AddOrUpdateJobNodes(job_desc);
    // If it is, only add the new bits
    // Run a scheduler iteration
    uint64_t total_scheduled = 0;
    total_scheduled = RunSchedulingIteration();
    LOG(INFO) << "STOP SCHEDULING, placed " << total_scheduled << " tasks";
    // If we have cost model debug logging turned on, write some debugging
    // information now.
    if (FLAGS_debug_cost_model) {
      string csv_log;
      spf(&csv_log, "%s/cost_model_%d.csv", FLAGS_debug_output_dir.c_str(),
          solver_dispatcher_->seq_num());
      FILE* csv_log_file = fopen(csv_log.c_str(), "w");
      CHECK_NOTNULL(csv_log_file);
      string debug_info = cost_model_->DebugInfoCSV();
      fputs(debug_info.c_str(), csv_log_file);
      CHECK_EQ(fclose(csv_log_file), 0);
    }
    // Resource reservations may have changed, so reconsider equivalence classes
    flow_graph_->AddOrUpdateJobNodes(job_desc);
    return total_scheduled;
  } else {
    LOG(INFO) << "STOP SCHEDULING, nothing to do";
    return 0;
  }
}

void FlowScheduler::RegisterResource(ResourceID_t res_id,
                                     bool local,
                                     bool simulated) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  // Update the flow graph
  UpdateResourceTopology(resource_topology_);
  // Call into superclass method to do scheduler resource initialisation.
  // This will create the executor for the new resource.
  EventDrivenScheduler::RegisterResource(res_id, local, simulated);
}

uint64_t FlowScheduler::RunSchedulingIteration() {
  // If this is the first iteration ever, we should ensure that the cost
  // model's notion of statistics is correct.
  if (solver_dispatcher_->seq_num() == 0)
    UpdateCostModelResourceStats();

  // If it's time to revisit time-dependent costs, do so now, just before
  // we run the solver.
  uint64_t cur_time = GetCurrentTimestamp();
  if (last_updated_time_dependent_costs_ <= (cur_time -
      static_cast<uint64_t>(FLAGS_time_dependent_cost_update_frequency))) {
    // First collect all non-finished jobs
    // TODO(malte): this can be removed when we've factored archived tasks
    // and jobs out of the job_map_ into separate data structures.
    // (cf. issue #24).
    vector<JobDescriptor*> job_vec;
    for (auto it = job_map_->begin();
         it != job_map_->end();
         ++it) {
      // We only need to reconsider this job if it is still active
      if (it->second.state() != JobDescriptor::COMPLETED &&
          it->second.state() != JobDescriptor::FAILED &&
          it->second.state() != JobDescriptor::ABORTED) {
        job_vec.push_back(&it->second);
      }
    }
    // This will re-visit all jobs and update their time-dependent costs
    VLOG(1) << "Flow scheduler updating time-dependent costs.";
    flow_graph_->UpdateTimeDependentCosts(&job_vec);
    last_updated_time_dependent_costs_ = cur_time;
  }
  // Run the flow solver! This is where all the juicy goodness happens :)
  multimap<uint64_t, uint64_t>* task_mappings = solver_dispatcher_->Run();
  // Solver's done, let's post-process the results.
  multimap<uint64_t, uint64_t>::iterator it;
  vector<SchedulingDelta*> deltas;
  for (it = task_mappings->begin(); it != task_mappings->end(); it++) {
    VLOG(1) << "Bind " << it->first << " to " << it->second << endl;
    // Some sanity checks
    FlowGraphNode* src = flow_graph_->Node(it->first);
    FlowGraphNode* dst = flow_graph_->Node(it->second);
    // Source must be a task node as this point
    CHECK(src->type_ == FlowNodeType::SCHEDULED_TASK ||
          src->type_ == FlowNodeType::UNSCHEDULED_TASK ||
          src->type_ == FlowNodeType::ROOT_TASK);
    // Destination must be a PU node
    CHECK(dst->type_ == FlowNodeType::PU);
    // Get the TD and RD for the source and destination
    TaskDescriptor* task = FindPtrOrNull(*task_map_, src->task_id_);
    CHECK_NOTNULL(task);
    ResourceStatus* target_res_status =
      FindPtrOrNull(*resource_map_, dst->resource_id_);
    CHECK_NOTNULL(target_res_status);
    const ResourceDescriptor& resource = target_res_status->descriptor();
    solver_dispatcher_->NodeBindingToSchedulingDelta(
        *task, resource, &task_bindings_, &deltas);
  }
  uint64_t num_scheduled = ApplySchedulingDeltas(deltas);
  // Drop all deltas that were actioned
  for (vector<SchedulingDelta*>::iterator it = deltas.begin();
       it != deltas.end(); ) {
    if ((*it)->actioned()) {
      it = deltas.erase(it);
    } else {
      it++;
    }
  }
  if (deltas.size() > 0) {
    LOG(WARNING) << "Not all deltas were processed, " << deltas.size()
                 << " remain: ";
    for (auto it = deltas.begin(); it != deltas.end(); ++it)
      LOG(WARNING) << " * " << (*it)->DebugString();
  }

  // The application of deltas may have changed relevant statistics, so
  // we update them.
  UpdateCostModelResourceStats();

  return num_scheduled;
}

void FlowScheduler::PrintGraph(vector< map<uint64_t, uint64_t> > adj_map) {
  for (vector< map<uint64_t, uint64_t> >::size_type i = 1;
       i < adj_map.size(); ++i) {
    map<uint64_t, uint64_t>::iterator it;
    for (it = adj_map[i].begin();
         it != adj_map[i].end(); it++) {
      cout << i << " " << it->first << " " << it->second << endl;
    }
  }
}

void FlowScheduler::UpdateCostModelResourceStats() {
  if (FLAGS_flow_scheduling_cost_model ==
      CostModelType::COST_MODEL_COCO ||
      FLAGS_flow_scheduling_cost_model ==
      CostModelType::COST_MODEL_OCTOPUS ||
      FLAGS_flow_scheduling_cost_model ==
      CostModelType::COST_MODEL_WHARE) {
    LOG(INFO) << "Updating resource statistics in flow graph";
    flow_graph_->ComputeTopologyStatistics(
        flow_graph_->sink_node(),
        boost::bind(&CostModelInterface::PrepareStats,
                    cost_model_, _1),
        boost::bind(&CostModelInterface::GatherStats,
                    cost_model_, _1, _2));
    flow_graph_->ComputeTopologyStatistics(
        flow_graph_->sink_node(),
        boost::bind(&CostModelInterface::UpdateStats,
                    cost_model_, _1, _2));
  } else {
    LOG(INFO) << "No resource stats update required";
  }
}

void FlowScheduler::UpdateResourceTopology(
    ResourceTopologyNodeDescriptor* root) {
  // Run a topology refresh (somewhat expensive!); if only two nodes exist, the
  // flow graph is empty apart from cluster aggregator and sink.
  VLOG(1) << "Num nodes in flow graph is: " << flow_graph_->NumNodes();
  if (flow_graph_->NumNodes() == 1) {
    flow_graph_->AddResourceTopology(root);
  } else {
    flow_graph_->AddMachine(root);
  }
  // We also need to update any stats or state in the cost model, as the
  // resource topology has changed.
  UpdateCostModelResourceStats();
}

}  // namespace scheduler
}  // namespace firmament
