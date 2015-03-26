// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Co-ordinated co-location cost model.

#include "scheduling/coco_cost_model.h"

#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

CocoCostModel::CocoCostModel(shared_ptr<ResourceMap_t> resource_map,
                             shared_ptr<TaskMap_t> task_map,
                             unordered_set<ResourceID_t,
                               boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                             KnowledgeBase* kb)
  : resource_map_(resource_map),
    task_map_(task_map),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(kb) {
  // XXX(malte): dodgy hack for local cluster
  max_capacity_.cpu_cores_ = 12;
  max_capacity_.ram_gb_ = 64;
  max_capacity_.network_bw_ = 1170;
  max_capacity_.disk_bw_ = 350;
  // Set an initial value for infinity -- one is fine, since all costs so far
  // are zero
  infinity_ = 1;
  // Shut up unused warnings for now
  CHECK_NOTNULL(knowledge_base_);
  CHECK_NOTNULL(leaf_res_ids_);
}

uint64_t CocoCostModel::ComputeInterferenceScore(ResourceID_t res_id) {
  // XXX(malte): Implement!
  return 0ULL;
}

const TaskDescriptor& CocoCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
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
  cost_vector.ram_gb_ = NormalizeCost(td.resource_request().ram_gb(),
                                      max_capacity_.ram_gb_);
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
  cost_vector.cpu_cores_ = NormalizeCost(rd.load().cpu_cores(),
                                         max_capacity_.cpu_cores_);
  cost_vector.ram_gb_ = NormalizeCost(rd.load().ram_gb(),
                                      max_capacity_.ram_gb_);
  cost_vector.network_bw_ = NormalizeCost(rd.load().net_bw(),
                                          max_capacity_.network_bw_);
  cost_vector.disk_bw_ = NormalizeCost(rd.load().disk_bw(),
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
  cost_vector.ram_gb_ = NormalizeCost(td.resource_request().ram_gb(),
                                      max_capacity_.ram_gb_);
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

vector<EquivClass_t>* CocoCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  return equiv_classes;
}

int64_t CocoCostModel::FlattenCostVector(CostVector_t cv) {
  // Compute priority dimension and ensure that it always dominates
  uint64_t priority_value = cv.priority_ * omega_;
  // Compute the rest of the cost vector
  uint64_t accumulator = 0;
  accumulator += cv.cpu_cores_;
  accumulator += cv.ram_gb_;
  accumulator += cv.network_bw_;
  accumulator += cv.disk_bw_;
  accumulator += cv.machine_type_score_;
  accumulator += cv.interference_score_;
  accumulator += cv.locality_score_;
  if (accumulator > infinity_)
    infinity_ = accumulator + 1;
  return accumulator + priority_value;
}

vector<EquivClass_t>* CocoCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  LOG(ERROR) << "Not implemented";
  vector<EquivClass_t>* null_vec = new vector<EquivClass_t>();
  return null_vec;
}

vector<ResourceID_t>* CocoCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  // TODO(malte): implement!
  LOG(ERROR) << "Not implemented!";
  return prefered_res;
}

vector<TaskID_t>* CocoCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<EquivClass_t>* null_vec = new vector<EquivClass_t>();
  LOG(ERROR) << "Not implemented!";
  return null_vec;
}

vector<ResourceID_t>* CocoCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    CocoCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  vector<EquivClass_t>* equiv_classes1 = new vector<EquivClass_t>();
  vector<EquivClass_t>* equiv_classes2 = new vector<EquivClass_t>();
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(equiv_classes1, equiv_classes2);
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
  LOG(INFO) << "  RAM: " << cv.ram_gb_ << ", ";
  LOG(INFO) << "  NET: " << cv.network_bw_ << ", ";
  LOG(INFO) << "  DISK: " << cv.disk_bw_ << ", ";
  LOG(INFO) << "  MACHINE TYPE: " << cv.machine_type_score_ << ", ";
  LOG(INFO) << "  INTERFERENCE: " << cv.interference_score_ << ", ";
  LOG(INFO) << "  LOCALITY: " << cv.locality_score_ << " ]";
}

void CocoCostModel::AddMachine(const ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void CocoCostModel::RemoveMachine(ResourceID_t res_id) {
}

void CocoCostModel::RemoveTask(TaskID_t task_id) {
}

}  // namespace firmament
