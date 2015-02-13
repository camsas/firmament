// The Firmament project
// Copyright (c) 2014 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <string>

#include "scheduling/random_cost_model.h"

namespace firmament {

RandomCostModel::RandomCostModel() { }

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t RandomCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  uint32_t seed = 0;
  int64_t half_max_arc_cost = FLAGS_flow_max_arc_cost / 2;
  return half_max_arc_cost + rand_r(&seed) % half_max_arc_cost + 1;
}

// The costfrom the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t RandomCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t RandomCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t RandomCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t RandomCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

Cost_t RandomCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

// The cost from the resource leaf to the sink is 0.
Cost_t RandomCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t RandomCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t RandomCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t RandomCostModel::TaskToEquivClassAggregator(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

}  // namespace firmament
