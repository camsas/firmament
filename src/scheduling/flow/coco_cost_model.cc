// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Co-ordinated co-location cost model.

#include "scheduling/flow/coco_cost_model.h"

#include <cmath>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/flow_graph.h"

DECLARE_bool(preemption);

namespace firmament {

CocoCostModel::CocoCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    const ResourceTopologyNodeDescriptor& resource_topology,
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids,
    KnowledgeBase* kb)
  : resource_map_(resource_map),
    resource_topology_(resource_topology),
    task_map_(task_map),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(kb) {
  // XXX(malte): dodgy hack for local cluster
  max_capacity_.cpu_cores_ = 12;
  max_capacity_.ram_cap_ = 65536;
  max_capacity_.network_bw_ = 1170;
  max_capacity_.disk_bw_ = 250;
  // Set an initial value for infinity -- this overshoots a bit; would be nice
  // to have a tighter bound based on actual costs observed
  infinity_ = omega_ * (CostVector_t::dimensions_ - 1) +
              MAX_PRIORITY_VALUE * omega_;
  // Shut up unused warnings for now
  CHECK_NOTNULL(leaf_res_ids_);
}

void CocoCostModel::AccumulateResourceStats(ResourceDescriptor* accumulator,
                                            ResourceDescriptor* other) {
  // Track the aggregate available resources below the accumulator node
  ResourceVector* acc_avail = accumulator->mutable_available_resources();
  ResourceVector* other_avail = other->mutable_available_resources();
  acc_avail->set_cpu_cores(acc_avail->cpu_cores() + other_avail->cpu_cores());
  acc_avail->set_ram_cap(acc_avail->ram_cap() + other_avail->ram_cap());
  acc_avail->set_net_bw(acc_avail->net_bw() + other_avail->net_bw());
  acc_avail->set_disk_bw(acc_avail->disk_bw() + other_avail->disk_bw());
  // Track the maximum resources available in any dimensions at resources below
  // the accumulator node
  ResourceVector* acc_max =
    accumulator->mutable_max_available_resources_below();
  ResourceVector* other_max = other->mutable_max_available_resources_below();
  acc_max->set_cpu_cores(max(acc_max->cpu_cores(), other_max->cpu_cores()));
  acc_max->set_ram_cap(max(acc_max->ram_cap(), other_max->ram_cap()));
  acc_max->set_net_bw(max(acc_max->net_bw(), other_max->net_bw()));
  acc_max->set_disk_bw(max(acc_max->disk_bw(), other_max->disk_bw()));
  // Track the minimum resources available in any dimensions at resources below
  // the accumulator node
  ResourceVector* acc_min =
    accumulator->mutable_min_available_resources_below();
  ResourceVector* other_min = other->mutable_min_available_resources_below();
  // Note that we have a special case for zero here, as non-reporting resources
  // would otherwise dominate the min().
  if (fabsl(acc_min->cpu_cores()) == 0.0)
    acc_min->set_cpu_cores(other_min->cpu_cores());
  else if (other_min->cpu_cores() > 0.0)
    acc_min->set_cpu_cores(min(acc_min->cpu_cores(), other_min->cpu_cores()));
  if (acc_min->ram_cap() == 0)
    acc_min->set_ram_cap(other_min->ram_cap());
  else if (other_min->ram_cap() > 0)
    acc_min->set_ram_cap(min(acc_min->ram_cap(), other_min->ram_cap()));
  if (acc_min->net_bw() == 0)
    acc_min->set_net_bw(other_min->net_bw());
  else if (other_min->net_bw() > 0)
    acc_min->set_net_bw(min(acc_min->net_bw(), other_min->net_bw()));
  if (acc_min->disk_bw() == 0)
    acc_min->set_disk_bw(other_min->disk_bw());
  else if (other_min->disk_bw() > 0)
    acc_min->set_disk_bw(min(acc_min->disk_bw(), other_min->disk_bw()));
}

uint64_t CocoCostModel::ComputeInterferenceScore(ResourceID_t res_id) {
  // Find resource within topology
  VLOG(1) << "Computing interference scores for resources below " << res_id;
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  // TODO(malte): rs can be NULL if the resource is not yet present (this
  // happens e.g. on startup). It's safe to ignore for now as the cost will
  // be set correctly on the first update iteration, although we should fix it
  // later.
  if (!rs) {
    LOG(WARNING) << "Returning zero interference score for not-yet-existent "
                 << "resource ID " << res_id << ". This is common during "
                 << "initialisation, but should not persist in normal"
                 << "operation.";
    return 0ULL;
  }
  const ResourceDescriptor& rd = rs->descriptor();
  const ResourceTopologyNodeDescriptor& rtnd = rs->topology_node();
  // TODO(malte): note that the below implicitly assumes that each leaf runs
  // exactly one task; we may need to revisit this assumption in the future.
  uint64_t num_total_leaves_below = rd.num_leaves_below();
  uint64_t num_idle_leaves_below = num_total_leaves_below;
  double scale_factor = 1;
  if (rd.has_num_running_tasks_below() && num_total_leaves_below > 0) {
    num_idle_leaves_below = num_total_leaves_below -
      rd.num_running_tasks_below();
    VLOG(1) << num_idle_leaves_below << " of " << num_total_leaves_below
            << " leaves are idle.";
    scale_factor =
      exp(static_cast<double>(num_total_leaves_below - num_idle_leaves_below) /
          static_cast<double>(num_total_leaves_below));
    VLOG(1) << "Scale factor: " << scale_factor;
  }
  uint64_t summed_interference_costs = 0;
  if (num_total_leaves_below == 0) {
    // Leaves haven't been initialised yet
    return 0;
  } else if (num_total_leaves_below == 1 &&
             rd.type() == ResourceDescriptor::RESOURCE_PU) {
    // Base case, we're at a PU
    if (rd.has_current_running_task()) {
      return GetInterferenceScoreForTask(rd.current_running_task());
    } else {
      return 0;
    }
  } else {
    // Recursively compute the score
    double num_siblings = 1.0;
    if (rtnd.children().size() > 1)
      num_siblings = rtnd.children().size() - 1;
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::const_iterator it =
         rtnd.children().begin();
         it != rtnd.children().end();
         ++it) {
      uint64_t child_interference_cost = (ComputeInterferenceScore(
            ResourceIDFromString(it->resource_desc().uuid())) / num_siblings);
      VLOG(1) << "Interference cost for " << it->resource_desc().uuid()
              << " is " << child_interference_cost;
      summed_interference_costs += child_interference_cost;
    }
  }
  VLOG(1) << "Total aggregate cost: " << summed_interference_costs;
  uint64_t interference_cost =
    (scale_factor * summed_interference_costs) - summed_interference_costs;
  VLOG(1) << "After scaling: " << interference_cost;
  return interference_cost;
}

const string CocoCostModel::DebugInfo() const {
  string out;
  out += "Resource load info:\n";
  ResourceStatus* root_rs =
    FindPtrOrNull(*resource_map_,
                  ResourceIDFromString(
                    resource_topology_.resource_desc().uuid()));
  CHECK_NOTNULL(root_rs);
  ResourceTopologyNodeDescriptor* root_rtnd =
    root_rs->mutable_topology_node();
  queue<ResourceTopologyNodeDescriptor*> to_visit;
  to_visit.push(root_rtnd);
  while (!to_visit.empty()) {
    ResourceTopologyNodeDescriptor* res_node_desc = to_visit.front();
    to_visit.pop();
    out += res_node_desc->resource_desc().uuid() + ": \n";
    out += " - AVAILABLE: " +
      res_node_desc->resource_desc().available_resources().DebugString();
    out += " - MIN_BELOW: " +
      res_node_desc->resource_desc().
      min_available_resources_below().DebugString();
    out += " - MAX_BELOW: " +
      res_node_desc->resource_desc().
      max_available_resources_below().DebugString();
    for (auto rtnd_iter =
         res_node_desc->mutable_children()->pointer_begin();
         rtnd_iter != res_node_desc->mutable_children()->pointer_end();
         ++rtnd_iter) {
      to_visit.push(*rtnd_iter);
    }
  }
  return out;
}

int64_t CocoCostModel::FlattenCostVector(CostVector_t cv) {
  // Compute priority dimension and ensure that it always dominates
  uint64_t priority_value = cv.priority_ * omega_;
  // Compute the rest of the cost vector
  uint64_t accumulator = 0;
  accumulator += cv.cpu_cores_;
  accumulator += cv.ram_cap_;
  accumulator += cv.network_bw_;
  accumulator += cv.disk_bw_;
  accumulator += cv.machine_type_score_;
  accumulator += cv.interference_score_;
  accumulator += cv.locality_score_;
  if (accumulator > infinity_)
    infinity_ = accumulator + 1;
  return accumulator + priority_value;
}

const TaskDescriptor& CocoCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

uint64_t CocoCostModel::GetInterferenceScoreForTask(TaskID_t task_id) {
  return omega_;
}

vector<EquivClass_t>* CocoCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // We have one task agg per job. The id of the aggregator is the hash
  // of the job id.
  EquivClass_t task_agg = static_cast<EquivClass_t>(HashJobID(*td_ptr));
  equiv_classes->push_back(task_agg);
  task_aggs_.insert(task_agg);
  unordered_map<EquivClass_t, set<TaskID_t> >::iterator task_ec_it =
    task_ec_to_set_task_id_.find(task_agg);
  if (task_ec_it != task_ec_to_set_task_id_.end()) {
    task_ec_it->second.insert(task_id);
  } else {
    set<TaskID_t> task_set;
    task_set.insert(task_id);
    CHECK(InsertIfNotPresent(&task_ec_to_set_task_id_, task_agg, task_set));
  }
  return equiv_classes;
}

vector<EquivClass_t>* CocoCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  // Not implemented.
  return NULL;
}

vector<ResourceID_t>* CocoCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  if (task_aggs_.find(ec) != task_aggs_.end()) {
    ResourceStatus* root_rs =
      FindPtrOrNull(*resource_map_,
                    ResourceIDFromString(
                      resource_topology_.resource_desc().uuid()));
    CHECK_NOTNULL(root_rs);
    ResourceTopologyNodeDescriptor* root_rtnd =
      root_rs->mutable_topology_node();
    if (root_rtnd->children_size() == 0) {
      return prefered_res;
    } else {
      // BFS over resource topology.
      // We hand-roll the BFS here instead of using one of the
      // implementations in utils, since we do not always need to go
      // all the way to the leaves.
      queue<ResourceTopologyNodeDescriptor*> to_visit;
      to_visit.push(root_rtnd);
      while (!to_visit.empty()) {
        ResourceTopologyNodeDescriptor* res_node_desc = to_visit.front();
        to_visit.pop();
        TaskFitIndication_t task_fit = TaskFitsUnderResourceAggregate(
            ec, res_node_desc->resource_desc());
        if (task_fit == TASK_ALWAYS_FITS_IN_UNRESERVED ||
            task_fit == TASK_ALWAYS_FITS_IN_AVAILABLE) {
          // We fit under all subordinate resources, so put an arc here and
          // stop exploring the subtree.
          VLOG(1) << "Tasks in EC " << ec << " do fit into resources below "
                  << res_node_desc->resource_desc().uuid();
          prefered_res->push_back(
              ResourceIDFromString(res_node_desc->resource_desc().uuid()));
          continue;
        } else if (task_fit == TASK_NEVER_FITS) {
          // We don't fit into *any* subordinate resources, so give up on this
          // subtree.
          VLOG(1) << "Tasks in EC " << ec << " definitely do not fit into "
                  << "resources below "
                  << res_node_desc->resource_desc().uuid();
          continue;
        }
        // Neither of the two applies, which implies that we must have one of
        // the TASK_SOMETIMES_FITS_* cases.
        CHECK(task_fit == TASK_SOMETIMES_FITS_IN_AVAILABLE ||
              task_fit == TASK_SOMETIMES_FITS_IN_UNRESERVED);
        VLOG(1) << "Tasks in EC " << ec << " sometimes fit into "
                << "resources below "
                << res_node_desc->resource_desc().uuid();
        // We may have some suitable resources here, so let's continue exploring
        // the subtree.
        for (auto rtnd_iter =
             res_node_desc->mutable_children()->pointer_begin();
             rtnd_iter != res_node_desc->mutable_children()->pointer_end();
             ++rtnd_iter) {
          to_visit.push(*rtnd_iter);
        }
      }
    }
  }
  return prefered_res;
}

vector<TaskID_t>* CocoCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<TaskID_t>* prefered_task = new vector<TaskID_t>();
  if (task_aggs_.find(tec) != task_aggs_.end()) {
    // tec is a task aggregator.
    // This is where we add preference arcs from tasks to new equiv class
    // aggregators.
    // XXX(ionel): This is very slow because it iterates over all tasks.
    for (TaskMap_t::iterator it = task_map_->begin(); it != task_map_->end();
         ++it) {
      vector<EquivClass_t>* tec_vec = GetTaskEquivClasses(it->first);
      for (auto tvi = tec_vec->begin(); tvi != tec_vec->end(); ++tvi) {
        if (*tvi == tec) {
          // XXX(malte): task_map_ contains ALL tasks ever seen by the system,
          // including those that have completed, failed or are otherwise no
          // longer present in the flow graph. We do some crude filtering here,
          // but clearly we should instead maintain a collection of tasks
          // actually eligible for scheduling.
          if (it->second->state() == TaskDescriptor::RUNNABLE ||
              (FLAGS_preemption &&
               it->second->state() == TaskDescriptor::RUNNING)) {
            prefered_task->push_back(it->first);
          }
        }
      }
    }
  } else {
    LOG(FATAL) << "Unexpected type of task equivalence aggregator";
  }
  return prefered_task;
}

vector<ResourceID_t>* CocoCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  // TODO(malte): implement!
  return NULL;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    CocoCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  // TODO(malte): implement!
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(NULL, NULL);
}


// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t CocoCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  // Baseline value (based on resource request)
  CostVector_t cost_vector;
  cost_vector.priority_ = td.priority();
  cost_vector.cpu_cores_ = omega_ +
    NormalizeCost(td.resource_request().cpu_cores(), max_capacity_.cpu_cores_);
  cost_vector.ram_cap_ = omega_ +
    NormalizeCost(td.resource_request().ram_cap(), max_capacity_.ram_cap_);
  cost_vector.network_bw_ = omega_ +
    NormalizeCost(td.resource_request().net_bw(), max_capacity_.network_bw_);
  cost_vector.disk_bw_ = omega_ +
    NormalizeCost(td.resource_request().disk_bw(), max_capacity_.disk_bw_);
  cost_vector.machine_type_score_ = 1;
  cost_vector.interference_score_ = 1;
  cost_vector.locality_score_ = 0;
  uint64_t base_cost = FlattenCostVector(cost_vector);
  uint64_t time_since_submit = GetCurrentTimestamp() - td.submit_time();
  // timestamps are in microseconds, but we scale to tenths of a second here in
  // order to keep the costs small
  uint64_t wait_time_cost = WAIT_TIME_MULTIPLIER * (time_since_submit / 100000);
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Task " << task_id << "'s cost to unscheduled aggregator:";
    VLOG(1) << "  Baseline vector: ";
    VLOG(1) << "  Flattened: " << base_cost;
    PrintCostVector(cost_vector);
    VLOG(1) << "  Wait time component: " << wait_time_cost;
    VLOG(1) << "  TOTAL: " << (wait_time_cost + base_cost);
  }
  return (wait_time_cost + base_cost);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t CocoCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t CocoCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  // Tasks may not use the cluster aggregator in the CoCo model
  return infinity_;
}

Cost_t CocoCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                             ResourceID_t resource_id) {
  // Main CoCo cost calculation, depending on who the resource node is
  return 0LL;
}

Cost_t CocoCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  // Get RD for this resource
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, destination);
  if (!rs)
    return 0LL;
  const ResourceDescriptor& rd = rs->descriptor();
  // Compute resource request dimensions (normalized by largest machine)
  CostVector_t cost_vector;
  bzero(&cost_vector, sizeof(CostVector_t));
  cost_vector.priority_ = 0;
  if (rd.type() == ResourceDescriptor::RESOURCE_PU) {
    cost_vector.cpu_cores_ =
        NormalizeCost(max_capacity_.cpu_cores_ -
                      rd.available_resources().cpu_cores(),
                      max_capacity_.cpu_cores_);
  } else if (rd.type() == ResourceDescriptor::RESOURCE_MACHINE) {
    cost_vector.ram_cap_ =
        NormalizeCost(max_capacity_.ram_cap_ -
                      rd.available_resources().ram_cap(),
                      max_capacity_.ram_cap_);
    cost_vector.network_bw_ =
        NormalizeCost(max_capacity_.network_bw_ -
                      rd.available_resources().net_bw(),
                      max_capacity_.network_bw_);
    cost_vector.disk_bw_ =
        NormalizeCost(max_capacity_.disk_bw_ -
                      rd.available_resources().disk_bw(),
                      max_capacity_.disk_bw_);
  }
  // XXX(malte): unimplemented
  cost_vector.machine_type_score_ = 0;
  cost_vector.interference_score_ = ComputeInterferenceScore(destination);
  // XXX(malte): unimplemented
  cost_vector.locality_score_ = 0;
  Cost_t flat_cost = FlattenCostVector(cost_vector);
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Resource " << source << "'s cost to resource "
            << destination << ":";
    PrintCostVector(cost_vector);
    VLOG(1) << "  Flattened: " << flat_cost;
  }
  // Return the flattened vector
  return flat_cost;
}

// The cost from the resource leaf to the sink is 0.
Cost_t CocoCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t CocoCostModel::TaskContinuationCost(TaskID_t task_id) {
  // XXX(malte): not currently used.
  LOG(FATAL) << "Unimplememented!";
  return 0LL;
}

Cost_t CocoCostModel::TaskPreemptionCost(TaskID_t task_id) {
  // XXX(malte): not currently used.
  LOG(FATAL) << "Unimplememented!";
  return 0LL;
}

Cost_t CocoCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                 EquivClass_t tec) {
  // Set cost from task to TA proportional to its resource requirements
  const TaskDescriptor& td = GetTask(task_id);
  if (!td.has_resource_request()) {
    LOG(ERROR) << "Task " << task_id << " does not have a resource "
               << "specification!";
  }
  // Compute resource request dimensions (normalized by largest machine)
  CostVector_t cost_vector;
  cost_vector.priority_ = td.priority();
  cost_vector.cpu_cores_ = NormalizeCost(td.resource_request().cpu_cores(),
                                         max_capacity_.cpu_cores_);
  cost_vector.ram_cap_ = NormalizeCost(td.resource_request().ram_cap(),
                                       max_capacity_.ram_cap_);
  cost_vector.network_bw_ = NormalizeCost(td.resource_request().net_bw(),
                                          max_capacity_.network_bw_);
  cost_vector.disk_bw_ = NormalizeCost(td.resource_request().disk_bw(),
                                       max_capacity_.disk_bw_);
  cost_vector.machine_type_score_ = 0;
  cost_vector.interference_score_ = 0;
  cost_vector.locality_score_ = 0;
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Task " << task_id << "'s cost to EC "
            << tec << ":";
    PrintCostVector(cost_vector);
    VLOG(1) << "  Flattened: " << FlattenCostVector(cost_vector);
  }
  // Return the flattened vector
  return FlattenCostVector(cost_vector);
}

Cost_t CocoCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                               ResourceID_t res_id) {
  return 0LL;
}

Cost_t CocoCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                             EquivClass_t tec2) {
  return 0LL;
}

ResourceID_t CocoCostModel::MachineResIDForResource(ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  while (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_MACHINE) {
    CHECK(rtnd->has_parent_id()) << "Non-machine resource "
      << rtnd->resource_desc().uuid() << " has no parent!";
    rs = FindPtrOrNull(*resource_map_, ResourceIDFromString(rtnd->parent_id()));
    rtnd = rs->mutable_topology_node();
  }
  return ResourceIDFromString(rtnd->resource_desc().uuid());
}

uint32_t CocoCostModel::NormalizeCost(uint64_t raw_cost, uint64_t max_cost) {
  if (omega_ == 0 || max_cost == 0)
    return 0;
  return (static_cast<double>(raw_cost) /
          static_cast<double>(max_cost)) * omega_;
}

void CocoCostModel::PrintCostVector(CostVector_t cv) {
  LOG(INFO) << "[ PRIORITY: " << cv.priority_ << ", ";
  LOG(INFO) << "  CPU: " << cv.cpu_cores_ << ", ";
  LOG(INFO) << "  RAM: " << cv.ram_cap_ << ", ";
  LOG(INFO) << "  NET: " << cv.network_bw_ << ", ";
  LOG(INFO) << "  DISK: " << cv.disk_bw_ << ", ";
  LOG(INFO) << "  MACHINE TYPE: " << cv.machine_type_score_ << ", ";
  LOG(INFO) << "  INTERFERENCE: " << cv.interference_score_ << ", ";
  LOG(INFO) << "  LOCALITY: " << cv.locality_score_ << " ]";
}

void CocoCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void CocoCostModel::AddTask(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  const TaskDescriptor& td = GetTask(task_id);
  for (auto it = equiv_classes->begin();
       it != equiv_classes->end();
       ++it) {
    InsertIfNotPresent(&task_ec_to_resource_request_, *it,
                       td.resource_request());
  }
  delete equiv_classes;
}

void CocoCostModel::RemoveMachine(ResourceID_t res_id) {
}

void CocoCostModel::RemoveTask(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  for (vector<EquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    unordered_map<EquivClass_t, set<TaskID_t> >::iterator set_it =
      task_ec_to_set_task_id_.find(*it);
    if (set_it != task_ec_to_set_task_id_.end()) {
      set_it->second.erase(task_id);
      if (set_it->second.size() == 0) {
        task_ec_to_set_task_id_.erase(*it);
        task_aggs_.erase(*it);
      }
    }
  }
}

FlowGraphNode* CocoCostModel::GatherStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  if (accumulator->type_ == FlowNodeType::ROOT_TASK ||
      accumulator->type_ == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_ == FlowNodeType::SINK ||
      accumulator->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    // Cases: 1) TASK -> EQUIV
    //        2) TASK -> RESOURCE
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    if (accumulator->type_ == FlowNodeType::PU) {
      // Base case: (PU -> SINK). We are at a PU and we gather the statistics.
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, accumulator->resource_id_);
      if (!rs_ptr)
        return accumulator;
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      // Early exit if the resource is not yet there
      if (!rd_ptr)
        return accumulator;
      // Use the KB to find load information and compute available resources
      ResourceID_t machine_res_id =
        MachineResIDForResource(accumulator->resource_id_);
      const deque<MachinePerfStatisticsSample>* machine_stats =
        knowledge_base_->GetStatsForMachine(machine_res_id);
      if (machine_stats && machine_stats->size() > 0) {
        VLOG(1) << "Updating PU " << accumulator->resource_id_ << "'s "
                << "resource stats!";
        // Take the most recent sample for now
        MachinePerfStatisticsSample latest_stats;
        latest_stats.CopyFrom(machine_stats->back());
        // Get the CPU stats for this PU
        string label = rd_ptr->friendly_name();
        uint64_t idx = label.find("PU #");
        if (idx != string::npos) {
          string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
          int64_t core_id = strtoll(core_id_substr.c_str(), 0, 10);
          rd_ptr->mutable_available_resources()->set_cpu_cores(
              latest_stats.cpus_usage(core_id).idle() / 100.0);
          rd_ptr->mutable_max_available_resources_below()->set_cpu_cores(
              latest_stats.cpus_usage(core_id).idle() / 100.0);
          rd_ptr->mutable_min_available_resources_below()->set_cpu_cores(
              latest_stats.cpus_usage(core_id).idle() / 100.0);
        }
        if (rd_ptr->has_current_running_task()) {
          rd_ptr->set_num_running_tasks_below(1);
        } else {
          rd_ptr->set_num_running_tasks_below(0);
        }
        rd_ptr->set_num_leaves_below(1);
      }
    }
    return accumulator;
  }
  if (accumulator->type_ == FlowNodeType::COORDINATOR) {
    if (!other->resource_id_.is_nil() &&
        other->type_ == FlowNodeType::MACHINE) {
      // Case: (EQUIV -> MACHINE).
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, other->resource_id_);
      if (!rs_ptr)
        return accumulator;
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      // Use the KB to find load information and compute available resources
      const deque<MachinePerfStatisticsSample>* machine_stats =
        knowledge_base_->GetStatsForMachine(other->resource_id_);
      if (machine_stats && machine_stats->size() > 0) {
        VLOG(1) << "Updating machine " << other->resource_id_ << "'s "
                << "resource stats!";
        // Take the most recent sample for now
        const MachinePerfStatisticsSample& latest_stats =
          machine_stats->back();
        // The CPU utilization gets added up automaticaly, so we only set the
        // per-machine properties here
        rd_ptr->mutable_available_resources()->set_ram_cap(
            (latest_stats.total_ram() - latest_stats.free_ram()) / BYTES_TO_MB);
        rd_ptr->mutable_max_available_resources_below()->set_ram_cap(
            (latest_stats.total_ram() - latest_stats.free_ram()) / BYTES_TO_MB);
        rd_ptr->mutable_min_available_resources_below()->set_ram_cap(
            (latest_stats.total_ram() - latest_stats.free_ram()) / BYTES_TO_MB);
        rd_ptr->mutable_available_resources()->set_disk_bw(
            max_capacity_.disk_bw_ - (latest_stats.disk_bw() / BYTES_TO_MB));
        rd_ptr->mutable_max_available_resources_below()->set_disk_bw(
            max_capacity_.disk_bw_ - (latest_stats.disk_bw() / BYTES_TO_MB));
        rd_ptr->mutable_min_available_resources_below()->set_disk_bw(
            max_capacity_.disk_bw_ - (latest_stats.disk_bw() / BYTES_TO_MB));
        rd_ptr->mutable_available_resources()->set_net_bw(
            max_capacity_.network_bw_ - (latest_stats.net_bw() / BYTES_TO_MB));
        rd_ptr->mutable_max_available_resources_below()->set_net_bw(
            max_capacity_.network_bw_ - (latest_stats.net_bw() / BYTES_TO_MB));
        rd_ptr->mutable_min_available_resources_below()->set_net_bw(
            max_capacity_.network_bw_ - (latest_stats.net_bw() / BYTES_TO_MB));
      }
    } else {
      LOG(FATAL) << "Unexpected arc from node " << accumulator->id_
                 << " of type " << accumulator->type_ << " to " << other->id_
                 << " of type " << other->type_;
    }
  }
  // Case: (RESOURCE -> RESOURCE)
  ResourceStatus* acc_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, other->resource_id_);
  if (acc_rs_ptr && other_rs_ptr) {
    ResourceDescriptor* acc_rd_ptr =  acc_rs_ptr->mutable_descriptor();
    ResourceDescriptor* other_rd_ptr = other_rs_ptr->mutable_descriptor();
    AccumulateResourceStats(acc_rd_ptr, other_rd_ptr);
    acc_rd_ptr->set_num_running_tasks_below(
        acc_rd_ptr->num_running_tasks_below() +
        other_rd_ptr->num_running_tasks_below());
    acc_rd_ptr->set_num_leaves_below(acc_rd_ptr->num_leaves_below() +
                                    other_rd_ptr->num_leaves_below());
  }
  return accumulator;
}

CocoCostModel::ResourceVectorFitIndication_t
CocoCostModel::CompareResourceVectors(
    const ResourceVector& rv1,
    const ResourceVector& rv2) {
  bool at_least_one_nonfit = false;
  bool at_least_one_fit = false;
  // CPU cores
  if (rv1.cpu_cores() <= rv2.cpu_cores())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
  VLOG(2) << "CPU cores: " << rv1.cpu_cores()
          << ", " << rv2.cpu_cores()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // RAM capacity
  if (rv1.ram_cap() <= rv2.ram_cap())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
  VLOG(2) << "RAM cap: " << rv1.ram_cap()
          << ", " << rv2.ram_cap()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // Disk bandwidth
  if (rv1.disk_bw() <= rv2.disk_bw())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
  VLOG(2) << "Disk BW: " << rv1.disk_bw()
          << ", " << rv2.disk_bw()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // Network bandwidth
  if (rv1.net_bw() <= rv2.net_bw())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
  VLOG(2) << "Net BW: " << rv1.net_bw()
          << ", " << rv2.net_bw()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // We must either fit or not fit in at least one dimension!
  CHECK(at_least_one_nonfit || at_least_one_fit);
  if (at_least_one_fit && !at_least_one_nonfit) {
    // rv1 fits into rv2 in ALL dimensions
    return RESOURCE_VECTOR_WHOLLY_FITS;
  } else if (at_least_one_fit && at_least_one_nonfit) {
    // rv1 fits into rv2 in SOME dimensions
    return RESOURCE_VECTOR_PARTIALLY_FITS;
  } else {
    // rv2 fits into rv2 in NO dimension
    return RESOURCE_VECTOR_DOES_NOT_FIT;
  }
}

void CocoCostModel::PrepareStats(FlowGraphNode* accumulator) {
  ResourceStatus* rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  if (!rs_ptr)
    return;
  ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
  // Early exit if the resource is not yet there
  if (!rd_ptr) {
    LOG(WARNING) << "Queried RD that does not exist yet, for "
                 << accumulator->resource_id_;
  }
  rd_ptr->mutable_available_resources()->clear_cpu_cores();
  rd_ptr->mutable_available_resources()->clear_ram_cap();
  rd_ptr->mutable_available_resources()->clear_net_bw();
  rd_ptr->mutable_available_resources()->clear_disk_bw();
  rd_ptr->clear_num_running_tasks_below();
  rd_ptr->clear_num_leaves_below();
}

CocoCostModel::TaskFitIndication_t
CocoCostModel::TaskFitsUnderResourceAggregate(
    EquivClass_t tec,
    const ResourceDescriptor& res) {
  ResourceVector* request = FindOrNull(task_ec_to_resource_request_, tec);
  CHECK_NOTNULL(request);
  // TODO(malte): this remains disabled for now, as we don't currently track
  // resource reservations.
  //if (CompareResourceVectors(request, res.min_unreserved_resources_below()) ==
  //    RESOURCE_VECTOR_WHOLLY_FITS) {
  //  // We fit into unreserved space on *all* subordinate resources.
  //  return TASK_ALWAYS_FITS_IN_UNRESERVED;
  //}
  if (CompareResourceVectors(*request, res.min_available_resources_below()) ==
      RESOURCE_VECTOR_WHOLLY_FITS) {
    // We fit in available space (but not unreserved space) on *all* subordinate
    // resources.
    return TASK_ALWAYS_FITS_IN_AVAILABLE;
  }
  // TODO(malte): this remains disabled for now, as we don't currently track
  // resource reservations.
  //if (CompareResourceVectors(request, res.max_unreserved_resources_below()) ==
  //    RESOURCE_VECTOR_PARTIALLY_FITS) {
  //  // We fit into unreserved space on *some* (at least one) subordinate
  //  // resources.
  //  return TASK_SOMETIMES_FITS_IN_UNRESERVED;
  //}
  if (CompareResourceVectors(*request, res.max_available_resources_below()) ==
      RESOURCE_VECTOR_PARTIALLY_FITS) {
    // We fit in available space (but not unreserved space) on *some*
    // (at least one) subordinate resources.
    return TASK_SOMETIMES_FITS_IN_AVAILABLE;
  }
  // Otherwise, we don't fit in *any* subordinate resources.
  return TASK_NEVER_FITS;
}

FlowGraphNode* CocoCostModel::UpdateStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  if (accumulator->type_ == FlowNodeType::ROOT_TASK ||
      accumulator->type_ == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_ == FlowNodeType::SINK) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    // Cases: 1) TASK -> EQUIV
    //        2) TASK -> RESOURCE
    return accumulator;
  }
  if (other->resource_id_.is_nil()) {
    if (accumulator->type_ == FlowNodeType::PU) {
      // Base case: (PU -> SINK)
      // We don't have to do anything.
    }
    return accumulator;
  }
  // Case: RESOURCE -> RESOURCE
  FlowGraphArc* arc = FlowGraph::GetArc(accumulator, other);
  uint64_t new_cost = ResourceNodeToResourceNodeCost(accumulator->resource_id_,
                                                     other->resource_id_);
  if (arc->cost_ != new_cost) {
    arc->cost_ = new_cost;
    DIMACSChange *chg = new DIMACSChangeArc(*arc);
    chg->set_comment("CoCo/UpdateStats");
    flow_graph_->AddGraphChange(chg);
  }

  return accumulator;
}

}  // namespace firmament
