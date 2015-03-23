// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// WhareMap cost model.

#include "scheduling/wharemap_cost_model.h"

#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/utils.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

WhareMapCostModel::WhareMapCostModel(shared_ptr<ResourceMap_t> resource_map,
                                     shared_ptr<TaskMap_t> task_map,
                                     KnowledgeBase* kb)
  : resource_map_(resource_map),
    task_map_(task_map),
    knowledge_base_(kb) {
}

const TaskDescriptor& WhareMapCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t WhareMapCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  // TODO(ionel): Implement!
  const TaskDescriptor& td = GetTask(task_id);
  uint64_t now = GetCurrentTimestamp();
  uint64_t time_since_submit = now - td.submit_time();
  // timestamps are in microseconds, but we scale to tenths of a second here in
  // order to keep the costs small
  return (time_since_submit / 100000);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t WhareMapCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t WhareMapCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  // TODO(ionel): Implement!
  vector<TaskEquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  uint64_t avg_runtime =
    knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
  delete equiv_classes;
  return (avg_runtime * 100);
}

Cost_t WhareMapCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                                 ResourceID_t resource_id) {
  // Tasks do not have preference arcs to resources.
  LOG(FATAL) << "Should not be called";
  return 0LL;
}

Cost_t WhareMapCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  // TODO(ionel): Implement!
  return 0LL;
}

// The cost from the resource leaf to the sink is 0.
Cost_t WhareMapCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t WhareMapCostModel::TaskContinuationCost(TaskID_t task_id) {
  LOG(FATAL) << "Should not be called";
  return 0LL;
}

Cost_t WhareMapCostModel::TaskPreemptionCost(TaskID_t task_id) {
  LOG(FATAL) << "Should not be called";
  return 0LL;
}

Cost_t WhareMapCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                     TaskEquivClass_t tec) {
  // TODO(ionel): Implement!
  return 0LL;
}

Cost_t WhareMapCostModel::EquivClassToResourceNode(TaskEquivClass_t tec,
                                                   ResourceID_t res_id) {
  // TODO(ionel): Implement!
  return 0LL;
}

Cost_t WhareMapCostModel::EquivClassToEquivClass(TaskEquivClass_t tec1,
                                                 TaskEquivClass_t tec2) {
  // TODO(ionel): Implement!
  return 0LL;
}

vector<TaskEquivClass_t>* WhareMapCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<TaskEquivClass_t>* equiv_classes = new vector<TaskEquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // We have one task agg per job. The id of the aggregator is the hash
  // of the job id.
  size_t hash = 42;
  boost::hash_combine(hash, td_ptr->job_id());
  TaskEquivClass_t task_agg = static_cast<TaskEquivClass_t>(hash);
  equiv_classes->push_back(task_agg);
  task_aggs_.insert(task_agg);
  return equiv_classes;
}

vector<TaskEquivClass_t>* WhareMapCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  vector<TaskEquivClass_t>* equiv_classes = new vector<TaskEquivClass_t>();
  // Get the machine aggregator corresponding to this machine.
  TaskEquivClass_t* ec_class = FindOrNull(machine_to_ec_, res_id);
  CHECK_NOTNULL(ec_class);
  equiv_classes->push_back(*ec_class);
  return equiv_classes;
}

vector<ResourceID_t>* WhareMapCostModel::GetOutgoingEquivClassPrefArcs(
    TaskEquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  if (task_aggs_.find(tec) != task_aggs_.end()) {
    // tec is a task aggregator.
    // TODO(ionel): Add arcs from task aggregators to machines.
  } else if (machine_aggs_.find(tec) != machine_aggs_.end()) {
    // tec is a machine aggregator.
    // TODO(ionel): Add arcs from machine aggregators to machines.
  } else {
    LOG(FATAL) << "Unexpected type of task equivalence aggregator";
  }
  return prefered_res;
}

vector<TaskID_t>* WhareMapCostModel::GetIncomingEquivClassPrefArcs(
    TaskEquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* WhareMapCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  // Tasks do not have preference arcs to resources.
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<TaskEquivClass_t>*, vector<TaskEquivClass_t>*>
    WhareMapCostModel::GetEquivClassToEquivClassesArcs(TaskEquivClass_t tec) {
  vector<TaskEquivClass_t>* incoming_ec = new vector<TaskEquivClass_t>();
  vector<TaskEquivClass_t>* outgoing_ec = new vector<TaskEquivClass_t>();
  if (task_aggs_.find(tec) != task_aggs_.end()) {
    // Add the machine equivalence classes to the vector.
    for (unordered_set<TaskEquivClass_t>::iterator
           it = machine_aggs_.begin();
         it != machine_aggs_.end();
         ++it) {
      outgoing_ec->push_back(*it);
    }
  } else if (machine_aggs_.find(tec) != machine_aggs_.end()) {
    // Add the task equivalence classes to the vector.
    for (unordered_set<TaskEquivClass_t>::iterator
           it = task_aggs_.begin();
         it != task_aggs_.end();
         ++it) {
      incoming_ec->push_back(*it);
    }
  } else {
    LOG(FATAL) << "Unexpected type of task equiv class";
  }
  return pair<vector<TaskEquivClass_t>*,
              vector<TaskEquivClass_t>*>(incoming_ec, outgoing_ec);
}

void WhareMapCostModel::AddMachine(
    const ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_EQ(rtnd_ptr->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  size_t hash = 42;
  BFSTraverseResourceProtobufTreeToHash(
      rtnd_ptr, &hash,
      boost::bind(&WhareMapCostModel::ComputeMachineTypeHash, this, _1, _2));
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  // Set the number of cores for the machine.
  TaskEquivClass_t machine_ec = static_cast<TaskEquivClass_t>(hash);
  // Add mapping between task equiv class and resource id.
  machine_ec_to_res_id_.insert(
      pair<TaskEquivClass_t, ResourceID_t>(machine_ec, res_id));
  // Add mapping between resource id and resource topology node.
  InsertIfNotPresent(&machine_to_rtnd_, res_id, rtnd_ptr);
  // Add mapping between resource id and machine equiv class.
  InsertIfNotPresent(&machine_to_ec_, res_id, machine_ec);
  // Add machine to the machine aggregators set.
  machine_aggs_.insert(machine_ec);
}

void WhareMapCostModel::RemoveMachine(ResourceID_t res_id) {
  TaskEquivClass_t* machine_ec = FindOrNull(machine_to_ec_, res_id);
  CHECK_NOTNULL(machine_ec);
  // Rempve the machine from the machine ec map.
  multimap<TaskEquivClass_t, ResourceID_t>::iterator it =
    machine_ec_to_res_id_.find(*machine_ec);
  multimap<TaskEquivClass_t, ResourceID_t>::iterator it_to =
    machine_ec_to_res_id_.upper_bound(*machine_ec);
  uint32_t num_machines_per_ec = 0;
  for (; it != it_to; it++, num_machines_per_ec++) {
    if (it->second == res_id) {
      break;
    }
  }
  // Check we actually found the machine.
  if (it == it_to) {
    LOG(FATAL) << "Could not find the machine";
  }
  machine_ec_to_res_id_.erase(it);
  machine_to_rtnd_.erase(res_id);
  machine_to_ec_.erase(res_id);
  // Remove the machine ec from the agg set if we removed the
  // last machine of this type.
  if (num_machines_per_ec == 1) {
    machine_aggs_.erase(*machine_ec);
  }
}

void WhareMapCostModel::ComputeMachineTypeHash(
    const ResourceTopologyNodeDescriptor* rtnd_ptr, size_t* hash) {
  boost::hash_combine(*hash, rtnd_ptr->resource_desc().type());
}

}  // namespace firmament
