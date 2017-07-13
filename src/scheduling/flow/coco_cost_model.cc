 /*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

// Co-ordinated co-location cost model.

#include "scheduling/flow/coco_cost_model.h"

#include <algorithm>
#include <cmath>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"

DEFINE_int64(coco_wait_time_multiplier, 1,
             "CoCo wait time multiplier factor");
DEFINE_int64(penalty_turtle_any, 50,
             "Turtle penalty when co-located with others");
DEFINE_int64(penalty_sheep_turtle, 10,
             "Sheep penalty when co-located with turtle");
DEFINE_int64(penalty_sheep_sheep, 50,
             "Sheep penalty when co-located with sheep");
DEFINE_int64(penalty_sheep_rabbit, 100,
             "Sheep penalty when co-located with rabbit");
DEFINE_int64(penalty_sheep_devil, 200,
             "Sheep penalty when co-located with devil");
DEFINE_int64(penalty_rabbit_turtle, 10,
             "Rabbit penalty when co-located with turtle");
DEFINE_int64(penalty_rabbit_sheep, 50,
             "Rabbit penalty when co-located with sheep");
DEFINE_int64(penalty_rabbit_rabbit, 200,
             "Rabbit penalty when co-located with rabbit");
DEFINE_int64(penalty_rabbit_devil, 1000,
             "Rabbit penalty when co-located with devil");
DEFINE_int64(penalty_devil_turtle, 10,
             "Devil penalty when co-located with turtle");
DEFINE_int64(penalty_devil_sheep, 200,
             "Devil penalty when co-located with sheep");
DEFINE_int64(penalty_devil_rabbit, 1000,
             "Devil penalty when co-located with rabbit");
DEFINE_int64(penalty_devil_devil, 200,
             "Devil penalty when co-located with devil");

DECLARE_bool(preemption);
DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

CocoCostModel::CocoCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    const ResourceTopologyNodeDescriptor& resource_topology,
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids,
    shared_ptr<KnowledgeBase> knowledge_base,
    TimeInterface* time_manager)
  : resource_map_(resource_map),
    resource_topology_(resource_topology),
    task_map_(task_map),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(knowledge_base),
    time_manager_(time_manager) {
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
  // CPU core capacity is additive, while all other properties are machine-level
  // properties that only get added once we get beyond the machine level.
  acc_avail->set_cpu_cores(acc_avail->cpu_cores() + other_avail->cpu_cores());
  acc_avail->set_ram_cap(acc_avail->ram_cap() + other_avail->ram_cap());
  acc_avail->set_net_tx_bw(acc_avail->net_tx_bw() + other_avail->net_tx_bw());
  acc_avail->set_net_rx_bw(acc_avail->net_rx_bw() + other_avail->net_rx_bw());
  acc_avail->set_disk_bw(acc_avail->disk_bw() + other_avail->disk_bw());
  // Track the maximum resources available in any dimensions at resources below
  // the accumulator node
  ResourceVector* acc_max =
    accumulator->mutable_max_available_resources_below();
  ResourceVector* other_max = other->mutable_max_available_resources_below();
  acc_max->set_cpu_cores(max(acc_max->cpu_cores(), other_max->cpu_cores()));
  acc_max->set_ram_cap(max(acc_max->ram_cap(), other_max->ram_cap()));
  acc_max->set_net_tx_bw(max(acc_max->net_tx_bw(), other_max->net_tx_bw()));
  acc_max->set_net_rx_bw(max(acc_max->net_rx_bw(), other_max->net_rx_bw()));
  acc_max->set_disk_bw(max(acc_max->disk_bw(), other_max->disk_bw()));
  // Track the minimum resources available in any dimensions at resources below
  // the accumulator node
  ResourceVector* acc_min =
    accumulator->mutable_min_available_resources_below();
  ResourceVector* other_min = other->mutable_min_available_resources_below();
  // Note that we have a special case for zero here, as non-reporting resources
  // would otherwise dominate the min().
  if (fabsl(acc_min->cpu_cores()) < COMPARE_EPS)
    acc_min->set_cpu_cores(other_min->cpu_cores());
  else if (other_min->cpu_cores() > 0.0)
    acc_min->set_cpu_cores(min(acc_min->cpu_cores(), other_min->cpu_cores()));
  if (acc_min->ram_cap() == 0)
    acc_min->set_ram_cap(other_min->ram_cap());
  else if (other_min->ram_cap() > 0)
    acc_min->set_ram_cap(min(acc_min->ram_cap(), other_min->ram_cap()));
  if (acc_min->net_tx_bw() == 0)
    acc_min->set_net_tx_bw(other_min->net_tx_bw());
  else if (other_min->net_tx_bw() > 0)
    acc_min->set_net_tx_bw(min(acc_min->net_tx_bw(), other_min->net_tx_bw()));
  if (acc_min->net_rx_bw() == 0)
    acc_min->set_net_rx_bw(other_min->net_rx_bw());
  else if (other_min->net_rx_bw() > 0)
    acc_min->set_net_rx_bw(min(acc_min->net_rx_bw(), other_min->net_rx_bw()));
  if (acc_min->disk_bw() == 0)
    acc_min->set_disk_bw(other_min->disk_bw());
  else if (other_min->disk_bw() > 0)
    acc_min->set_disk_bw(min(acc_min->disk_bw(), other_min->disk_bw()));
  // Running/idle task count
  accumulator->set_num_running_tasks_below(
      accumulator->num_running_tasks_below() +
      other->num_running_tasks_below());
  accumulator->set_num_slots_below(accumulator->num_slots_below() +
                                   other->num_slots_below());
  // Interference scores
  CoCoInterferenceScores* aiv = accumulator->mutable_coco_interference_scores();
  const CoCoInterferenceScores& oiv = other->coco_interference_scores();
  ResourceStatus* ors =
    FindPtrOrNull(*resource_map_, ResourceIDFromString(other->uuid()));
  CHECK_NOTNULL(ors);
  const ResourceTopologyNodeDescriptor& ortnd = ors->topology_node();
  uint64_t num_children =
    max(static_cast<uint64_t>(ortnd.children_size()), 1UL);
  aiv->set_turtle_penalty(aiv->turtle_penalty() +
      (oiv.turtle_penalty() / num_children));
  aiv->set_sheep_penalty(aiv->sheep_penalty() +
      (oiv.sheep_penalty() / num_children));
  aiv->set_rabbit_penalty(aiv->rabbit_penalty() +
      (oiv.rabbit_penalty() / num_children));
  aiv->set_devil_penalty(aiv->devil_penalty() +
      (oiv.devil_penalty() / num_children));
  // Resource reservations
  ResourceVector* acc_reservation = accumulator->mutable_reserved_resources();
  const ResourceVector& other_reservation = other->reserved_resources();
  acc_reservation->set_cpu_cores(acc_reservation->cpu_cores() +
                                 other_reservation.cpu_cores());
  acc_reservation->set_ram_cap(acc_reservation->ram_cap() +
                               other_reservation.ram_cap());
  acc_reservation->set_net_tx_bw(acc_reservation->net_tx_bw() +
                                 other_reservation.net_tx_bw());
  acc_reservation->set_net_rx_bw(acc_reservation->net_rx_bw() +
                                 other_reservation.net_rx_bw());
  acc_reservation->set_disk_bw(acc_reservation->disk_bw() +
                               other_reservation.disk_bw());
}

int64_t CocoCostModel::ComputeInterferenceScore(ResourceID_t res_id) {
  // Find resource within topology
  VLOG(2) << "Computing interference scores for resources below " << res_id;
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->descriptor();
  const ResourceTopologyNodeDescriptor& rtnd = rs->topology_node();
  // TODO(malte): note that the below implicitly assumes that each leaf runs
  // exactly one task; we may need to revisit this assumption in the future.
  uint64_t num_total_slots_below = rd.num_slots_below();
  uint64_t num_idle_slots_below = num_total_slots_below;
  double scale_factor = 1;
  if (num_total_slots_below > 0) {
    num_idle_slots_below = num_total_slots_below -
      rd.num_running_tasks_below();
    VLOG(2) << num_idle_slots_below << " of " << num_total_slots_below
            << " slots are idle.";
    scale_factor =
      exp(static_cast<double>(num_total_slots_below - num_idle_slots_below) /
          static_cast<double>(num_total_slots_below));
    VLOG(2) << "Scale factor: " << scale_factor;
  }
  uint64_t summed_interference_costs = 0;
  if (num_total_slots_below == 0) {
    // Slots haven't been initialised yet
    return 0;
  } else if (rd.type() == ResourceDescriptor::RESOURCE_PU) {
    // Base case, we're at a PU
    int64_t interference_score = 0;
    RepeatedField<uint64_t> running_tasks = rd.current_running_tasks();
    for (auto& task_id : running_tasks) {
      CoCoInterferenceScores iv;
      GetInterferenceScoreForTask(task_id, &iv);
      interference_score += FlattenInterferenceScore(iv);
    }
    return interference_score;
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
      VLOG(2) << "Interference cost for " << it->resource_desc().uuid()
              << " is " << child_interference_cost;
      summed_interference_costs += child_interference_cost;
    }
  }
  VLOG(2) << "Total aggregate cost: " << summed_interference_costs;
  int64_t interference_cost =
    (scale_factor * summed_interference_costs) - summed_interference_costs;
  VLOG(2) << "After scaling: " << interference_cost;
  return interference_cost;
}

const string CocoCostModel::DebugInfo() const {
  string out;
  out += "<p><b>Maximum capacity in cluster:</b> ";
  out += ResourceVectorToString(max_machine_capacity_, " / ");
  out += "</p>";
  out += "<p><b>Minimum capacity in cluster:</b> ";
  out += ResourceVectorToString(min_machine_capacity_, " / ");
  out += "</p>";
  out += "<h2>Resource load info</h2>";
  out += "<table class=\"table table-bordered\"><tr><th>Resource</th>";
  out += "<th>Tasks below</th>";
  out += "<th colspan=\"4\">Capacity</th>";
  out += "<th colspan=\"4\">Available</th>";
  out += "<th colspan=\"4\">Min below</th>";
  out += "<th colspan=\"4\">Max below</th>";
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
    CHECK_NOTNULL(res_node_desc);
    to_visit.pop();
    const ResourceDescriptor rd = res_node_desc->resource_desc();
    out += "<tr><td>" + rd.uuid() + "</td>";
    out += "<td>" + to_string(rd.num_running_tasks_below()) + "</td>";
    out += "<td>" + ResourceVectorToString(rd.resource_capacity(), "</td><td>")
           + "</td>";
    out += "<td>" + ResourceVectorToString(rd.available_resources(),
                                           "</td><td>") + "</td>";
    out += "<td>" + ResourceVectorToString(rd.min_available_resources_below(),
                                           "</td><td>") + "</td>";
    out += "<td>" + ResourceVectorToString(rd.max_available_resources_below(),
                                           "</td><td>") + "</td>";
    out += "</tr>";
    for (auto rtnd_iter =
         res_node_desc->mutable_children()->pointer_begin();
         rtnd_iter != res_node_desc->mutable_children()->pointer_end();
         ++rtnd_iter) {
      to_visit.push(*rtnd_iter);
    }
  }
  out += "</table>";
  out += "</body></html>";
  return out;
}

const string CocoCostModel::DebugInfoCSV() const {
  string out;
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
    CHECK_NOTNULL(res_node_desc);
    to_visit.pop();
    const ResourceDescriptor rd = res_node_desc->resource_desc();
    out += rd.uuid() + ",";
    out += to_string(rd.type()) + ",";
    out += to_string(rd.num_running_tasks_below()) + ",";
    out += ResourceVectorToString(rd.resource_capacity(), ",") + ",";
    out += ResourceVectorToString(rd.available_resources(), ",") + ",";
    out += ResourceVectorToString(rd.reserved_resources(), ",") + ",";
    out += ResourceVectorToString(rd.min_available_resources_below(),
                                  ",") + ",";
    out += ResourceVectorToString(rd.max_available_resources_below(),
                                  ",") + ",";
    out += "\n";
    for (auto rtnd_iter =
         res_node_desc->mutable_children()->pointer_begin();
         rtnd_iter != res_node_desc->mutable_children()->pointer_end();
         ++rtnd_iter) {
      to_visit.push(*rtnd_iter);
    }
  }
  return out;
}

Cost_t CocoCostModel::FlattenCostVector(CostVector_t cv) {
  // Compute priority dimension and ensure that it always dominates
  int64_t priority_value = cv.priority_ * omega_;
  // Compute the rest of the cost vector
  int64_t accumulator = 0;
  accumulator += cv.cpu_cores_;
  accumulator += cv.ram_cap_;
  accumulator += cv.network_tx_bw_;
  accumulator += cv.network_rx_bw_;
  accumulator += cv.disk_bw_;
  accumulator += cv.machine_type_score_;
  accumulator += cv.interference_score_;
  accumulator += cv.locality_score_;
  if (accumulator > infinity_)
    infinity_ = accumulator + 1;
  return accumulator + priority_value;
}

Cost_t CocoCostModel::FlattenInterferenceScore(
    const CoCoInterferenceScores& iv) {
  Cost_t acc = 0;
  acc += iv.turtle_penalty();
  acc += iv.sheep_penalty();
  acc += iv.rabbit_penalty();
  acc += iv.devil_penalty();
  return acc;
}

const TaskDescriptor& CocoCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

void CocoCostModel::GetInterferenceScoreForTask(
    TaskID_t task_id,
    CoCoInterferenceScores* interference_vector) {
  const TaskDescriptor& td = GetTask(task_id);

  if (td.task_type() == TaskDescriptor::TURTLE) {
    // Turtles don't care about devils, or indeed anything else
    // TOTAL: 20
    interference_vector->set_turtle_penalty(
        interference_vector->turtle_penalty() + FLAGS_penalty_turtle_any);
    interference_vector->set_sheep_penalty(
        interference_vector->sheep_penalty() + FLAGS_penalty_turtle_any);
    interference_vector->set_rabbit_penalty(
        interference_vector->rabbit_penalty() + FLAGS_penalty_turtle_any);
    interference_vector->set_devil_penalty(
        interference_vector->devil_penalty() + FLAGS_penalty_turtle_any);
  } else if (td.task_type() == TaskDescriptor::SHEEP) {
    // Sheep love turtles and rabbits, but dislike devils
    // TOTAL: 36
    interference_vector->set_turtle_penalty(
        interference_vector->turtle_penalty() + FLAGS_penalty_sheep_turtle);
    interference_vector->set_sheep_penalty(
        interference_vector->sheep_penalty() + FLAGS_penalty_sheep_sheep);
    interference_vector->set_rabbit_penalty(
        interference_vector->rabbit_penalty() + FLAGS_penalty_sheep_rabbit);
    interference_vector->set_devil_penalty(
        interference_vector->devil_penalty() + FLAGS_penalty_sheep_devil);
  } else if (td.task_type() == TaskDescriptor::RABBIT) {
    // Rabbits love turtles and sheep, but hate devils and dislike other
    // rabbits
    // TOTAL: 126
    interference_vector->set_turtle_penalty(
        interference_vector->turtle_penalty() + FLAGS_penalty_rabbit_turtle);
    interference_vector->set_sheep_penalty(
        interference_vector->sheep_penalty() + FLAGS_penalty_rabbit_sheep);
    interference_vector->set_rabbit_penalty(
        interference_vector->rabbit_penalty() + FLAGS_penalty_rabbit_rabbit);
    interference_vector->set_devil_penalty(
        interference_vector->devil_penalty() + FLAGS_penalty_rabbit_devil);
  } else if (td.task_type() == TaskDescriptor::DEVIL) {
    // Devils like turtles, hate rabbits, dislike sheep and other devils
    // TOTAL: 140
    interference_vector->set_turtle_penalty(
        interference_vector->turtle_penalty() + FLAGS_penalty_devil_turtle);
    interference_vector->set_sheep_penalty(
        interference_vector->sheep_penalty() + FLAGS_penalty_devil_sheep);
    interference_vector->set_rabbit_penalty(
        interference_vector->rabbit_penalty() + FLAGS_penalty_devil_rabbit);
    interference_vector->set_devil_penalty(
        interference_vector->devil_penalty() + FLAGS_penalty_devil_devil);
  }
}

vector<EquivClass_t>* CocoCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  const TaskDescriptor& td = GetTask(task_id);
  // We have one task agg per job. The id of the aggregator is the hash
  // of the job id.
  EquivClass_t task_agg = static_cast<EquivClass_t>(HashJobID(td));
  equiv_classes->push_back(task_agg);
  task_aggs_.insert(task_agg);
  unordered_map<EquivClass_t, unordered_set<TaskID_t> >::iterator task_ec_it =
    task_ec_to_set_task_id_.find(task_agg);
  if (task_ec_it != task_ec_to_set_task_id_.end()) {
    task_ec_it->second.insert(task_id);
  } else {
    unordered_set<TaskID_t> task_set;
    task_set.insert(task_id);
    CHECK(InsertIfNotPresent(&task_ec_to_set_task_id_, task_agg, task_set));
  }
  return equiv_classes;
}

vector<ResourceID_t>* CocoCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  // TODO(ionel): This method may end up adding many preference arcs.
  // Limit the number of preference arcs it adds.
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  if (ContainsKey(task_aggs_, ec)) {
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
        if (res_node_desc->resource_desc().type() !=
            ResourceDescriptor::RESOURCE_COORDINATOR) {
          TaskFitIndication_t task_fit = TaskFitsUnderResourceAggregate(
              ec, res_node_desc->resource_desc());
          if (task_fit == TASK_ALWAYS_FITS_IN_UNRESERVED ||
              task_fit == TASK_ALWAYS_FITS_IN_AVAILABLE) {
            // We fit under all subordinate resources, so put an arc here and
            // stop exploring the subtree.
            VLOG(2) << "Tasks in EC " << ec << " do fit into resources below "
                    << res_node_desc->resource_desc().uuid();
            // TODO(malte): This is a bit of a hack, since the question whether
            // a resource reservation is treated as strict should be a per-job
            // or per-task property. At the moment, we always treat it as, but
            // the infrastructure here is written to deal with overcommit in
            // principle (the TASK_ALWAYS_FITS_IN_AVAILABLE &&
            // !TASK_ALWAYS_FITS_IN_UNRESERVED case).
            if (task_fit == TASK_ALWAYS_FITS_IN_UNRESERVED)
              prefered_res->push_back(
                  ResourceIDFromString(res_node_desc->resource_desc().uuid()));
            continue;
          } else if (task_fit == TASK_NEVER_FITS) {
            // We don't fit into *any* subordinate resources, so give up on this
            // subtree.
            VLOG(2) << "Tasks in EC " << ec << " definitely do not fit into "
                    << "resources below "
                    << res_node_desc->resource_desc().uuid();
            continue;
          }
          // Neither of the two applies, which implies that we must have one of
          // the TASK_SOMETIMES_FITS_* cases.
          CHECK(task_fit == TASK_SOMETIMES_FITS_IN_AVAILABLE ||
                task_fit == TASK_SOMETIMES_FITS_IN_UNRESERVED);
          VLOG(2) << "Tasks in EC " << ec << " sometimes fit into "
                  << "resources below "
                  << res_node_desc->resource_desc().uuid();
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

vector<ResourceID_t>* CocoCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  // TODO(malte): implement!
  return NULL;
}

vector<EquivClass_t>* CocoCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t tec) {
  // TODO(malte): implement!
  return NULL;
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
ArcDescriptor CocoCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  // Baseline value (based on resource request)
  CostVector_t cost_vector;
  cost_vector.priority_ = td.priority();
  cost_vector.cpu_cores_ = static_cast<uint32_t>(
    omega_ + NormalizeCost(td.resource_request().cpu_cores(),
                           min_machine_capacity_.cpu_cores()));
  cost_vector.ram_cap_ = static_cast<uint32_t>(
    omega_ + NormalizeCost(td.resource_request().ram_cap(),
                           min_machine_capacity_.ram_cap()));
  cost_vector.network_tx_bw_ = static_cast<uint32_t>(
    omega_ + NormalizeCost(td.resource_request().net_tx_bw(),
                           min_machine_capacity_.net_tx_bw()));
  cost_vector.network_rx_bw_ = static_cast<uint32_t>(
    omega_ + NormalizeCost(td.resource_request().net_rx_bw(),
                           min_machine_capacity_.net_rx_bw()));
  cost_vector.disk_bw_ = static_cast<uint32_t>(
    omega_ + NormalizeCost(td.resource_request().disk_bw(),
                           min_machine_capacity_.disk_bw()));
  cost_vector.machine_type_score_ = 1;
  cost_vector.interference_score_ = 1;
  cost_vector.locality_score_ = 0;
  int64_t base_cost = FlattenCostVector(cost_vector);
  uint64_t time_since_submit =
    time_manager_->GetCurrentTimestamp() - td.submit_time();
  // timestamps are in microseconds, but we scale to tenths of a second here in
  // order to keep the costs small
  int64_t wait_time_cost = FLAGS_coco_wait_time_multiplier *
    (time_since_submit / 100000);
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Task " << task_id << "'s cost to unscheduled aggregator:";
    VLOG(2) << "  Baseline vector: ";
    VLOG(2) << "  Flattened: " << base_cost;
    PrintCostVector(cost_vector);
    VLOG(2) << "  Wait time component: " << wait_time_cost;
    VLOG(2) << "  TOTAL: " << (wait_time_cost + base_cost);
  }
  return ArcDescriptor(wait_time_cost + base_cost, 1ULL, 0ULL);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
ArcDescriptor CocoCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t CocoCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  // Tasks may not use the cluster aggregator in the CoCo model
  return infinity_;
}

ArcDescriptor CocoCostModel::TaskToResourceNode(TaskID_t task_id,
                                                ResourceID_t resource_id) {
  // Not used in CoCo, as we don't have direct arcs from tasks to resources;
  // we only connect via TECs.
  LOG(ERROR) << "Arcs from tasks to resource nodes should not be present";
  return ArcDescriptor(0LL, 0ULL, 0ULL);
}

ArcDescriptor CocoCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  // Get the RD for the machine corresponding to this resource
  ResourceID_t destination_res_id = ResourceIDFromString(destination.uuid());
  ResourceStatus* machine_rs =
    FindPtrOrNull(*resource_map_, MachineResIDForResource(destination_res_id));
  CHECK_NOTNULL(machine_rs);
  const ResourceDescriptor& machine_rd = machine_rs->descriptor();
  // Compute resource request dimensions (normalized by machine capacity)
  CostVector_t cost_vector;
  bzero(&cost_vector, sizeof(CostVector_t));
  cost_vector.priority_ = 0;
  if (destination.type() == ResourceDescriptor::RESOURCE_PU) {
    cost_vector.cpu_cores_ =
        NormalizeCost(machine_rd.resource_capacity().cpu_cores() -
                      destination.available_resources().cpu_cores(),
                      machine_rd.resource_capacity().cpu_cores());
  } else if (destination.type() == ResourceDescriptor::RESOURCE_MACHINE) {
    cost_vector.ram_cap_ =
        NormalizeCost(machine_rd.resource_capacity().ram_cap() -
                      destination.available_resources().ram_cap(),
                      machine_rd.resource_capacity().ram_cap());
    cost_vector.network_tx_bw_ =
        NormalizeCost(machine_rd.resource_capacity().net_tx_bw() -
                      destination.available_resources().net_tx_bw(),
                      machine_rd.resource_capacity().net_tx_bw());
    cost_vector.network_rx_bw_ =
        NormalizeCost(machine_rd.resource_capacity().net_rx_bw() -
                      destination.available_resources().net_rx_bw(),
                      machine_rd.resource_capacity().net_rx_bw());
    cost_vector.disk_bw_ =
        NormalizeCost(machine_rd.resource_capacity().disk_bw() -
                      destination.available_resources().disk_bw(),
                      machine_rd.resource_capacity().disk_bw());
  } else {
    // Cost on arcs pointing to resource nodes that are not PUs or
    // MACHINEs is 0.
  }
  // XXX(malte): unimplemented
  cost_vector.machine_type_score_ = 0;
  cost_vector.interference_score_ =
    ComputeInterferenceScore(destination_res_id);
  // XXX(malte): unimplemented
  cost_vector.locality_score_ = 0;
  Cost_t flat_cost = FlattenCostVector(cost_vector);
  if (VLOG_IS_ON(2) && flat_cost > 0) {
    VLOG(2) << "Resource " << source.uuid() << "'s cost to resource "
            << destination.uuid() << ":";
    PrintCostVector(cost_vector);
    VLOG(2) << "  Flattened: " << flat_cost;
  }
  // Return the flattened vector
  return ArcDescriptor(flat_cost, CapacityFromResNodeToParent(destination),
                       0ULL);
}

// The cost from the resource leaf to the sink is 0.
ArcDescriptor CocoCostModel::LeafResourceNodeToSink(ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor CocoCostModel::TaskContinuation(TaskID_t task_id) {
  // TODO(malte): Implement!
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CocoCostModel::TaskPreemption(TaskID_t task_id) {
  // TODO(malte): Implement!
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CocoCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                        EquivClass_t tec) {
  // Set cost from task to TA proportional to its resource requirements
  const TaskDescriptor& td = GetTask(task_id);
  if (!td.has_resource_request()) {
    LOG(ERROR) << "Task " << task_id << " does not have a resource "
               << "specification!";
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  // Compute resource request dimensions (normalized by largest machine)
  CostVector_t cost_vector;
  cost_vector.priority_ = td.priority();
  cost_vector.cpu_cores_ = NormalizeCost(td.resource_request().cpu_cores(),
                                         max_machine_capacity_.cpu_cores());
  cost_vector.ram_cap_ = NormalizeCost(td.resource_request().ram_cap(),
                                       max_machine_capacity_.ram_cap());
  cost_vector.network_tx_bw_ = NormalizeCost(td.resource_request().net_tx_bw(),
                                             max_machine_capacity_.net_tx_bw());
  cost_vector.network_rx_bw_ = NormalizeCost(td.resource_request().net_rx_bw(),
                                             max_machine_capacity_.net_rx_bw());
  cost_vector.disk_bw_ = NormalizeCost(td.resource_request().disk_bw(),
                                       max_machine_capacity_.disk_bw());
  cost_vector.machine_type_score_ = 0;
  cost_vector.interference_score_ = 0;
  cost_vector.locality_score_ = 0;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Task " << task_id << "'s cost to EC " << tec << ":";
    PrintCostVector(cost_vector);
    VLOG(2) << "  Flattened: " << FlattenCostVector(cost_vector);
  }
  // Return the flattened vector
  return ArcDescriptor(FlattenCostVector(cost_vector), 1ULL, 0ULL);
}

ArcDescriptor CocoCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  if (ContainsKey(task_aggs_, ec)) {
    // ec is a TEC, so we have a TEC -> resource aggregate arc
    ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
    CHECK_NOTNULL(rs);
    const ResourceDescriptor& rd = rs->descriptor();
    const ResourceTopologyNodeDescriptor& rtnd = rs->topology_node();
    // Figure out the outgoing capacity by checking the task's resource
    // requirements
    ResourceVector* res_request = FindOrNull(task_ec_to_resource_request_, ec);
    CHECK_NOTNULL(res_request);
    const ResourceVector& res_avail = rd.available_resources();
    uint64_t num_tasks_that_fit = TaskFitCount(*res_request, res_avail);
    // Get the interference score for the task
    unordered_set<TaskID_t>* task_set = FindOrNull(task_ec_to_set_task_id_, ec);
    uint32_t score = 0;
    if (task_set && task_set->size() > 0) {
      // N.B.: This assumes that all tasks in an EC are of the same type.
      TaskID_t sample_task_id = *task_set->begin();
      const TaskDescriptor& td = GetTask(sample_task_id);
      uint64_t num_children =
        max(static_cast<uint64_t>(rtnd.children_size()), 1UL);
      if (td.task_type() == TaskDescriptor::TURTLE) {
        score = rd.coco_interference_scores().turtle_penalty() / num_children;
      } else if (td.task_type() == TaskDescriptor::SHEEP) {
        score = rd.coco_interference_scores().sheep_penalty() / num_children;
      } else if (td.task_type() == TaskDescriptor::RABBIT) {
        score = rd.coco_interference_scores().rabbit_penalty() / num_children;
      } else if (td.task_type() == TaskDescriptor::DEVIL) {
        score = rd.coco_interference_scores().devil_penalty() / num_children;
      }
    }
    VLOG(2) << num_tasks_that_fit << " tasks of TEC " << ec << " fit under "
            << res_id << ", at interference score of " << score;
    return ArcDescriptor(score, num_tasks_that_fit, 0ULL);
  } else {
    LOG(WARNING) << "Unknown EC " << ec << " is not a TEC, so returning "
                 << "zero cost!";
    // No cost; no capacity
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
}

ArcDescriptor CocoCostModel::EquivClassToEquivClass(
    EquivClass_t tec1,
    EquivClass_t tec2) {
  LOG(ERROR) << "Arcs from equiv class to equiv class should not be present";
  return ArcDescriptor(0LL, 0ULL, 0ULL);
}

ResourceID_t CocoCostModel::MachineResIDForResource(ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  while (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_MACHINE) {
    if (rtnd->parent_id().empty()) {
      LOG(FATAL) << "Non-machine resource " << rtnd->resource_desc().uuid()
                 << " has no parent!";
    }
    rs = FindPtrOrNull(*resource_map_, ResourceIDFromString(rtnd->parent_id()));
    rtnd = rs->mutable_topology_node();
  }
  return ResourceIDFromString(rtnd->resource_desc().uuid());
}

Cost_t CocoCostModel::NormalizeCost(double raw_cost, double max_cost) {
  if (omega_ == 0 || fabsl(max_cost) < COMPARE_EPS)
    return 0;
  return (raw_cost / max_cost) * omega_;
}

void CocoCostModel::PrintCostVector(CostVector_t cv) {
  LOG(INFO) << "[ PRIORITY: " << cv.priority_ << ", ";
  LOG(INFO) << "  CPU: " << cv.cpu_cores_ << ", ";
  LOG(INFO) << "  RAM: " << cv.ram_cap_ << ", ";
  LOG(INFO) << "  NET TX: " << cv.network_tx_bw_ << ", ";
  LOG(INFO) << "  NET RX: " << cv.network_rx_bw_ << ", ";
  LOG(INFO) << "  DISK: " << cv.disk_bw_ << ", ";
  LOG(INFO) << "  MACHINE TYPE: " << cv.machine_type_score_ << ", ";
  LOG(INFO) << "  INTERFERENCE: " << cv.interference_score_ << ", ";
  LOG(INFO) << "  LOCALITY: " << cv.locality_score_ << " ]";
}

string CocoCostModel::ResourceVectorToString(
    const ResourceVector& rv,
    const string& delimiter) const {
  stringstream out;
  out << rv.cpu_cores() << delimiter;
  out << rv.ram_cap() << delimiter;
  out << rv.disk_bw() << delimiter;
  out << rv.net_tx_bw() << delimiter;
  out << rv.net_rx_bw();
  return out.str();
}

void CocoCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
  const ResourceDescriptor& rd = rtnd_ptr->resource_desc();
  const ResourceVector& cap = rd.resource_capacity();
  // Check if this machine's capacity is the maximum in any dimension
  if (cap.cpu_cores() > max_machine_capacity_.cpu_cores())
    max_machine_capacity_.set_cpu_cores(cap.cpu_cores());
  if (cap.ram_cap() > max_machine_capacity_.ram_cap())
    max_machine_capacity_.set_ram_cap(cap.ram_cap());
  if (cap.net_tx_bw() > max_machine_capacity_.net_tx_bw())
    max_machine_capacity_.set_net_tx_bw(cap.net_tx_bw());
  if (cap.net_rx_bw() > max_machine_capacity_.net_rx_bw())
    max_machine_capacity_.set_net_rx_bw(cap.net_rx_bw());
  if (cap.disk_bw() > max_machine_capacity_.disk_bw())
    max_machine_capacity_.set_disk_bw(cap.disk_bw());
  // Check if this machine's capacity is the minimum in any capacity
  if (fabsl(min_machine_capacity_.cpu_cores()) < COMPARE_EPS ||
      cap.cpu_cores() < min_machine_capacity_.cpu_cores()) {
    min_machine_capacity_.set_cpu_cores(cap.cpu_cores());
  }
  if (min_machine_capacity_.ram_cap() == 0 ||
      cap.ram_cap() < min_machine_capacity_.ram_cap()) {
    min_machine_capacity_.set_ram_cap(cap.ram_cap());
  }
  if (min_machine_capacity_.net_tx_bw() == 0 ||
      cap.net_tx_bw() < min_machine_capacity_.net_tx_bw()) {
    min_machine_capacity_.set_net_tx_bw(cap.net_tx_bw());
  }
  if (min_machine_capacity_.net_rx_bw() == 0 ||
      cap.net_rx_bw() < min_machine_capacity_.net_rx_bw()) {
    min_machine_capacity_.set_net_rx_bw(cap.net_rx_bw());
  }
  if (min_machine_capacity_.disk_bw() == 0 ||
      cap.disk_bw() < min_machine_capacity_.disk_bw()) {
    min_machine_capacity_.set_disk_bw(cap.disk_bw());
  }
}

void CocoCostModel::AddTask(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  const TaskDescriptor& td = GetTask(task_id);
  for (auto& equiv_class : *equiv_classes) {
    // NOTE: The code assumes that all the task connected to an
    // equivalence class request the same amount of resources.
    InsertIfNotPresent(&task_ec_to_resource_request_, equiv_class,
                       td.resource_request());
  }
  delete equiv_classes;
}

void CocoCostModel::RemoveMachine(ResourceID_t res_id) {
}

void CocoCostModel::RemoveTask(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  for (auto& equiv_class : *equiv_classes) {
    unordered_map<EquivClass_t, unordered_set<TaskID_t> >::iterator set_it =
      task_ec_to_set_task_id_.find(equiv_class);
    if (set_it != task_ec_to_set_task_id_.end()) {
      set_it->second.erase(task_id);
      if (set_it->second.size() == 0) {
        task_ec_to_set_task_id_.erase(equiv_class);
        task_aggs_.erase(equiv_class);
        task_ec_to_resource_request_.erase(equiv_class);
      }
    }
  }
  delete equiv_classes;
}

FlowGraphNode* CocoCostModel::GatherStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    // Cases: 1) TASK -> EQUIV
    //        2) TASK -> RESOURCE
    return accumulator;
  }

  if (accumulator->type_ == FlowNodeType::COORDINATOR) {
    // We're not allowed to scheduled tasks via the cluster aggregator in CoCo.
    // There's no point to update its state.
    return accumulator;
  }

  // Case: (RESOURCE -> RESOURCE)
  // We're inside the resource topology
  ResourceDescriptor* rd_ptr = accumulator->rd_ptr_;
  CHECK_NOTNULL(rd_ptr);
  // Use the KB to find load information and compute available resources
  ResourceID_t machine_res_id =
    MachineResIDForResource(accumulator->resource_id_);
  if (accumulator->type_ == FlowNodeType::PU) {
    // Base case: (PU -> SINK). We are at a PU and we gather the statistics.
    CHECK(other->resource_id_.is_nil());
    // Get the RD for the machine
    /*ResourceStatus* machine_rs_ptr =
      FindPtrOrNull(*resource_map_, machine_res_id);
    CHECK_NOTNULL(machine_rs_ptr);
    ResourceDescriptor* machine_rd_ptr = machine_rs_ptr->mutable_descriptor();*/
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample =
      knowledge_base_->GetLatestStatsForMachine(machine_res_id, &latest_stats);
    if (have_sample) {
      VLOG(2) << "Updating PU " << accumulator->resource_id_ << "'s "
              << "resource stats!";
      // Get the CPU stats for this PU
      string label = rd_ptr->friendly_name();
      uint64_t idx = label.find("PU #");
      if (idx != string::npos) {
        string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
        uint32_t core_id = strtoul(core_id_substr.c_str(), 0, 10);
        float available_cpu_cores =
          latest_stats.cpus_stats(core_id).cpu_capacity() *
          (1.0 - latest_stats.cpus_stats(core_id).cpu_utilization());
        rd_ptr->mutable_available_resources()->set_cpu_cores(
            available_cpu_cores);
        rd_ptr->mutable_max_available_resources_below()->set_cpu_cores(
            available_cpu_cores);
        rd_ptr->mutable_min_available_resources_below()->set_cpu_cores(
            available_cpu_cores);
      }
      // The CPU utilization gets added up automaticaly, so we only set the
      // per-machine properties here
      rd_ptr->mutable_available_resources()->set_ram_cap(
          latest_stats.mem_capacity() * (1.0 - latest_stats.mem_utilization()));
      rd_ptr->mutable_max_available_resources_below()->set_ram_cap(
          latest_stats.mem_capacity() * (1.0 - latest_stats.mem_utilization()));
      rd_ptr->mutable_min_available_resources_below()->set_ram_cap(
          latest_stats.mem_capacity() * (1.0 - latest_stats.mem_utilization()));
      // Running/idle task count
      rd_ptr->set_num_running_tasks_below(rd_ptr->current_running_tasks_size());
      rd_ptr->set_num_slots_below(FLAGS_max_tasks_per_pu);
      // Interference score vectors and resource reservations are accumulated if
      // we have a running task here.
      RepeatedField<uint64_t> running_tasks = rd_ptr->current_running_tasks();
      for (auto& task_id : running_tasks) {
        GetInterferenceScoreForTask(
            task_id,
            rd_ptr->mutable_coco_interference_scores());
        // Get TD for running tasks for reservation
        const TaskDescriptor& td = GetTask(task_id);
        ResourceVector* reserved = rd_ptr->mutable_reserved_resources();
        reserved->set_cpu_cores(reserved->cpu_cores() +
                                td.resource_request().cpu_cores());
        reserved->set_ram_cap(reserved->ram_cap() +
                              td.resource_request().ram_cap());
        reserved->set_disk_bw(reserved->disk_bw() +
                              td.resource_request().disk_bw());
        reserved->set_net_tx_bw(reserved->net_tx_bw() +
                                td.resource_request().net_tx_bw());
        reserved->set_net_rx_bw(reserved->net_rx_bw() +
                                td.resource_request().net_rx_bw());
      }
    }
    return accumulator;
  } else if (accumulator->type_ == FlowNodeType::MACHINE) {
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample =
      knowledge_base_->GetLatestStatsForMachine(accumulator->resource_id_,
                                                &latest_stats);
    if (have_sample) {
      VLOG(2) << "Updating machine " << accumulator->resource_id_ << "'s "
              << "resource stats!";
      rd_ptr->mutable_available_resources()->set_disk_bw(
          rd_ptr->resource_capacity().disk_bw() -
          latest_stats.disk_bw());
      rd_ptr->mutable_max_available_resources_below()->set_disk_bw(
          rd_ptr->resource_capacity().disk_bw() -
          latest_stats.disk_bw());
      rd_ptr->mutable_min_available_resources_below()->set_disk_bw(
          rd_ptr->resource_capacity().disk_bw() -
          latest_stats.disk_bw());
      rd_ptr->mutable_available_resources()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_max_available_resources_below()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_min_available_resources_below()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_available_resources()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
      rd_ptr->mutable_max_available_resources_below()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
      rd_ptr->mutable_min_available_resources_below()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
    }
  }
  if (accumulator->rd_ptr_ && other->rd_ptr_) {
    AccumulateResourceStats(accumulator->rd_ptr_, other->rd_ptr_);
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
  if (rv1.cpu_cores() <= rv2.cpu_cores()) {
    at_least_one_fit = true;
  } else {
    at_least_one_nonfit = true;
    VLOG(2) << "non-fit due to CPU: " << rv1.cpu_cores() << "/"
            << rv2.cpu_cores();
  }
  VLOG(2) << "CPU cores: " << rv1.cpu_cores()
          << ", " << rv2.cpu_cores()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // RAM capacity
  if (rv1.ram_cap() <= rv2.ram_cap()) {
    at_least_one_fit = true;
  } else {
    at_least_one_nonfit = true;
    VLOG(2) << "non-fit due to RAM: " << rv1.ram_cap() << "/"
            << rv2.ram_cap();
  }
  VLOG(2) << "RAM cap: " << rv1.ram_cap()
          << ", " << rv2.ram_cap()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // Disk bandwidth
  if (rv1.disk_bw() <= rv2.disk_bw()) {
    at_least_one_fit = true;
  } else {
    at_least_one_nonfit = true;
    VLOG(2) << "non-fit due to disk: " << rv1.disk_bw() << "/"
            << rv2.disk_bw();
  }
  VLOG(2) << "Disk BW: " << rv1.disk_bw()
          << ", " << rv2.disk_bw()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // Network bandwidth
  if (rv1.net_tx_bw() <= rv2.net_tx_bw()) {
    at_least_one_fit = true;
  } else {
    at_least_one_nonfit = true;
    VLOG(2) << "non-fit due to net tx: " << rv1.net_tx_bw() << "/"
            << rv2.net_tx_bw();
  }
  VLOG(2) << "Net tx BW: " << rv1.net_tx_bw()
          << ", " << rv2.net_tx_bw()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  if (rv1.net_rx_bw() <= rv2.net_rx_bw()) {
    at_least_one_fit = true;
  } else {
    at_least_one_nonfit = true;
    VLOG(2) << "non-fit due to net rx: " << rv1.net_rx_bw() << "/"
            << rv2.net_rx_bw();
  }
  VLOG(2) << "Net rx BW: " << rv1.net_rx_bw()
          << ", " << rv2.net_rx_bw()
          << "; " << at_least_one_fit << "/"
          << at_least_one_nonfit;
  // We must either fit or not fit in at least one dimension!
  CHECK(at_least_one_nonfit || at_least_one_fit);
  if (at_least_one_fit && !at_least_one_nonfit) {
    // rv1 fits into rv2 in ALL dimensions
    VLOG(2) << "Vector WHOLLY FITS";
    return RESOURCE_VECTOR_WHOLLY_FITS;
  } else if (at_least_one_fit && at_least_one_nonfit) {
    // rv1 fits into rv2 in SOME dimensions
    VLOG(2) << "Vector PARTIALLY FITS";
    return RESOURCE_VECTOR_PARTIALLY_FITS;
  } else {
    VLOG(2) << "Vector DOES NOT FIT";
    // rv2 fits into rv2 in NO dimension
    return RESOURCE_VECTOR_DOES_NOT_FIT;
  }
}

void CocoCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  ResourceDescriptor* rd_ptr = accumulator->rd_ptr_;
  CHECK_NOTNULL(rd_ptr);
  rd_ptr->clear_available_resources();
  rd_ptr->clear_reserved_resources();
  rd_ptr->clear_min_available_resources_below();
  rd_ptr->clear_max_available_resources_below();
  rd_ptr->clear_num_running_tasks_below();
  rd_ptr->clear_num_slots_below();
  rd_ptr->clear_coco_interference_scores();
}

uint64_t CocoCostModel::TaskFitCount(const ResourceVector& req,
                                     const ResourceVector& avail) {
  // Note: Do not initialize to MAX_UINT64 because there can be
  // several arcs with max capacity going into a node. This would
  // make the solver's supply values to overflow.
  uint64_t num_tasks = task_map_->size();
  if (fabsl(req.cpu_cores()) > COMPARE_EPS) {
    num_tasks = min(num_tasks,
                    static_cast<uint64_t>(avail.cpu_cores() / req.cpu_cores()));
  }
  if (req.ram_cap() > 0) {
    num_tasks = min(num_tasks, avail.ram_cap() / req.ram_cap());
  }
  if (req.disk_bw() > 0) {
    num_tasks = min(num_tasks, avail.disk_bw() / req.disk_bw());
  }
  if (req.net_tx_bw() > 0) {
    num_tasks = min(num_tasks, avail.net_tx_bw() / req.net_tx_bw());
  }
  if (req.net_rx_bw() > 0) {
    num_tasks = min(num_tasks, avail.net_rx_bw() / req.net_rx_bw());
  }
  return num_tasks;
}

CocoCostModel::TaskFitIndication_t
CocoCostModel::TaskFitsUnderResourceAggregate(
    EquivClass_t tec,
    const ResourceDescriptor& res) {
  ResourceVector* request = FindOrNull(task_ec_to_resource_request_, tec);
  CHECK_NOTNULL(request);
  // TODO(malte): this is a bit of a hack for now; we should move the
  // reservation check into its own method.
  ResourceVector unreserved;
  const ResourceVector& cap = res.resource_capacity();
  const ResourceVector& reserved = res.reserved_resources();
  unreserved.set_cpu_cores(max(cap.cpu_cores() - reserved.cpu_cores(), 0.0f));
  unreserved.set_ram_cap(max(static_cast<uint64_t>(cap.ram_cap()) -
                             static_cast<uint64_t>(reserved.ram_cap()), 0UL));
  unreserved.set_net_tx_bw(
      max(static_cast<uint64_t>(cap.net_tx_bw()) -
          static_cast<uint64_t>(reserved.net_tx_bw()), 0UL));
  unreserved.set_net_rx_bw(
      max(static_cast<uint64_t>(cap.net_rx_bw()) -
          static_cast<uint64_t>(reserved.net_rx_bw()), 0UL));
  unreserved.set_disk_bw(max(static_cast<uint64_t>(cap.disk_bw()) -
                             static_cast<uint64_t>(reserved.disk_bw()), 0UL));
  VLOG(2) << "Unreserved resources under " << res.uuid() << ": "
          << ResourceVectorToString(unreserved, " / ");
  if (CompareResourceVectors(*request, unreserved) ==
      RESOURCE_VECTOR_WHOLLY_FITS) {
    // We fit into unreserved space on *all* subordinate resources.
    return TASK_ALWAYS_FITS_IN_UNRESERVED;
  }
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
  ResourceVectorFitIndication_t fit_under_max =
    CompareResourceVectors(*request, res.max_available_resources_below());
  if (fit_under_max == RESOURCE_VECTOR_WHOLLY_FITS ||
      fit_under_max == RESOURCE_VECTOR_PARTIALLY_FITS) {
    // We fit in available space (but not unreserved space) on *some*
    // (at least one) subordinate resources.
    return TASK_SOMETIMES_FITS_IN_AVAILABLE;
  }
  // Otherwise, we don't fit in *any* subordinate resources.
  return TASK_NEVER_FITS;
}

FlowGraphNode* CocoCostModel::UpdateStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  // TODO(ionel): We need to consider EQUIVALENCE CLASSES as well because
  // we update the arcs from ECs to Resources. Remove the condition one
  // the update arc code is moved to the FlowGraphManager.
  if (!accumulator->IsResourceNode() &&
      !accumulator->IsEquivalenceClassNode()) {
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
  return accumulator;
}

}  // namespace firmament
