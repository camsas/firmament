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
  max_capacity_.ram_cap_ = 64;
  max_capacity_.network_bw_ = 1170;
  max_capacity_.disk_bw_ = 250;
  // Set an initial value for infinity -- this overshoots a bit; would be nice
  // to have a tighter bound based on actual costs observed
  infinity_ = omega_ * (CostVector_t::dimensions_ - 1) +
              MAX_PRIORITY_VALUE * omega_;
  // Shut up unused warnings for now
  CHECK_NOTNULL(knowledge_base_);
  CHECK_NOTNULL(leaf_res_ids_);
}

uint64_t CocoCostModel::ComputeInterferenceScore(ResourceID_t res_id) {
  // Find resource within topology
  VLOG(1) << "Computing interference scores for resources below " << res_id;
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  // TODO(malte): rs can be NULL if the resource is not yet present (this
  // happens e.g. on startup). It's safe to ignore for now as the cost will
  // be set correctly on the first update iteration, although we should fix it
  // later.
  if (!rs)
    return 0ULL;
  const ResourceDescriptor& rd = rs->descriptor();
  const ResourceTopologyNodeDescriptor& rtnd = rs->topology_node();
  // XXX(malte): this is a hack that assumes that each leaf runs exactly one
  // task
  uint64_t num_total_children = rtnd.children_size();
  uint64_t num_idle_children = num_total_children - rd.num_running_tasks();
  double scale_factor = 1;
  if (num_total_children > 0) {
     scale_factor = exp(static_cast<double>(num_idle_children) /
                        static_cast<double>(num_total_children));
  }
  // Need number of siblings here, so we look up the parent and count its
  // children
  double num_siblings = 1;
  if (rtnd.has_parent_id()) {
    ResourceStatus* parent_rs = FindPtrOrNull(*resource_map_,
        ResourceIDFromString(rtnd.parent_id()));
    CHECK_NOTNULL(parent_rs);
    const ResourceTopologyNodeDescriptor& parent_rtnd =
      parent_rs->topology_node();
    num_siblings = static_cast<double>(parent_rtnd.children_size() - 1);
  }
  uint64_t summed_interference_costs = 0;
  if (num_total_children == 0) {
    // Base case, we're at a PU
    if (rd.has_current_running_task()) {
      return GetInterferenceScoreForTask(rd.current_running_task());
    } else {
      return 0;
    }
  } else {
    // Recursively compute the score
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::const_iterator it =
         rtnd.children().begin();
         it != rtnd.children().end();
         ++it) {
      summed_interference_costs += ComputeInterferenceScore(
          ResourceIDFromString(it->resource_desc().uuid()));
    }
  }
  uint64_t interference_cost = scale_factor *
                               (summed_interference_costs / num_siblings);
  return interference_cost;
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
  return 1ULL;
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
  LOG(ERROR) << "Not implemented";
  vector<EquivClass_t>* null_vec = new vector<EquivClass_t>();
  return null_vec;
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
        // We skip coordinator nodes but keep exploring, since coordinator
        // notes are not useful for scheduling.
        // TODO(malte): This is currently required because of restrictions on
        // the types of node that can have preferences arcs pointing to them;
        // we might want to revisit it later.
        if (res_node_desc->resource_desc().type() !=
            ResourceDescriptor::RESOURCE_COORDINATOR) {
          TaskFitIndication_t task_fit = TaskFitsUnderResourceAggregate(
                res_node_desc->resource_desc());
          if (task_fit == TASK_ALWAYS_FITS_IN_UNRESERVED ||
              task_fit == TASK_ALWAYS_FITS_IN_AVAILABLE) {
            // We fit under all subordinate resources, so put an arc here and
            // stop exploring the subtree.
            prefered_res->push_back(
              ResourceIDFromString(res_node_desc->resource_desc().uuid()));
            continue;
          } else if (task_fit == TASK_NEVER_FITS) {
            // We don't fit into *any* subordinate resources, so give up on this
            // subtree.
            continue;
          }
          // Neither of the two applies, which implies that we must have one of
          // the TASK_SOMETIMES_FITS_* cases.
          CHECK(task_fit == TASK_SOMETIMES_FITS_IN_AVAILABLE ||
                task_fit == TASK_SOMETIMES_FITS_IN_UNRESERVED);
        }
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
  cost_vector.cpu_cores_ = NormalizeCost(td.resource_request().cpu_cores(),
                                         max_capacity_.cpu_cores_);
  cost_vector.ram_cap_ = NormalizeCost(td.resource_request().ram_cap(),
                                       max_capacity_.ram_cap_);
  cost_vector.network_bw_ = NormalizeCost(td.resource_request().net_bw(),
                                          max_capacity_.network_bw_);
  cost_vector.disk_bw_ = NormalizeCost(td.resource_request().disk_bw(),
                                       max_capacity_.disk_bw_);
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
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->descriptor();
  // Compute resource request dimensions (normalized by largest machine)
  CostVector_t cost_vector;
  cost_vector.priority_ = 0;
  cost_vector.cpu_cores_ = NormalizeCost(rd.available_resources().cpu_cores(),
                                         max_capacity_.cpu_cores_);
  cost_vector.ram_cap_ = NormalizeCost(rd.available_resources().ram_cap(),
                                       max_capacity_.ram_cap_);
  cost_vector.network_bw_ = NormalizeCost(rd.available_resources().net_bw(),
                                          max_capacity_.network_bw_);
  cost_vector.disk_bw_ = NormalizeCost(rd.available_resources().disk_bw(),
                                       max_capacity_.disk_bw_);
  // XXX(malte): unimplemented
  cost_vector.machine_type_score_ = 0;
  cost_vector.interference_score_ = ComputeInterferenceScore(destination);
  // XXX(malte): unimplemented
  cost_vector.locality_score_ = 0;
  // Return the combination
  return FlattenCostVector(cost_vector);
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
  // Return the combination
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
  // TODO(ionel): Implement.
  if (accumulator->type_ == FlowNodeType::ROOT_TASK ||
      accumulator->type_ == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_ == FlowNodeType::SINK) {
    // TODO(ionel): Implement.
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
  if (rv1.cpu_cores() >= rv2.cpu_cores())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
  // RAM capacity
  if (rv1.ram_cap() >= rv2.ram_cap())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
  // Disk bandwidth
  if (rv1.disk_bw() >= rv2.disk_bw())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
  // Network bandwidth
  if (rv1.net_bw() >= rv2.net_bw())
    at_least_one_fit = true;
  else
    at_least_one_nonfit = true;
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

CocoCostModel::TaskFitIndication_t
CocoCostModel::TaskFitsUnderResourceAggregate(
    const ResourceDescriptor& res) {
  ResourceVector request;
  request.set_cpu_cores(0);
  request.set_ram_cap(0);
  request.set_disk_bw(0);
  request.set_net_bw(0);
  if (CompareResourceVectors(request, res.min_unreserved_resources_below()) ==
      RESOURCE_VECTOR_WHOLLY_FITS) {
    // We fit into unreserved space on *all* subordinate resources.
    return TASK_ALWAYS_FITS_IN_UNRESERVED;
  }
  if (CompareResourceVectors(request, res.min_available_resources_below()) ==
      RESOURCE_VECTOR_WHOLLY_FITS) {
    // We fit in available space (but not unreserved space) on *all* subordinate
    // resources.
    return TASK_ALWAYS_FITS_IN_AVAILABLE;
  }
  if (CompareResourceVectors(request, res.max_unreserved_resources_below()) ==
      RESOURCE_VECTOR_WHOLLY_FITS) {
    // We fit into unreserved space on *some* (at least one) subordinate
    // resources.
    return TASK_SOMETIMES_FITS_IN_UNRESERVED;
  }
  if (CompareResourceVectors(request, res.max_available_resources_below()) ==
      RESOURCE_VECTOR_WHOLLY_FITS) {
    // We fit in available space (but not unreserved space) on *some*
    // (at least one) subordinate resources.
    return TASK_SOMETIMES_FITS_IN_AVAILABLE;
  }
  // Otherwise, we don't fit in *any* subordinate resources.
  return TASK_NEVER_FITS;
}

FlowGraphNode* CocoCostModel::UpdateStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  return NULL;
}

}  // namespace firmament
