// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// WhareMap cost model.

#include "scheduling/flow/wharemap_cost_model.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/utils.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/flow_graph_manager.h"

DECLARE_bool(preemption);

namespace firmament {

WhareMapCostModel::WhareMapCostModel(shared_ptr<ResourceMap_t> resource_map,
                                     shared_ptr<TaskMap_t> task_map,
                                     shared_ptr<KnowledgeBase> knowledge_base,
                                     TimeInterface* time_manager,
                                     DIMACSChangeStats* dimacs_stats)
  : resource_map_(resource_map),
    task_map_(task_map),
    knowledge_base_(knowledge_base),
    time_manager_(time_manager),
    dimacs_stats_(dimacs_stats) {
  // Create the cluster aggregator EC, which all machines are members of.
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  VLOG(1) << "Cluster aggregator EC is " << cluster_aggregator_ec_;
}

WhareMapCostModel::~WhareMapCostModel() {
  // time_manager_ is not owned by the WhareMapCostModel. We don't have to
  // delete it.
  for (auto& psi : psi_map_) {
    delete psi.second;
  }
  for (auto& xi : xi_map_) {
    delete xi.second;
  }
}

Cost_t WhareMapCostModel::AverageFromVec(const vector<uint64_t>& vec) const {
  uint64_t acc = 0ULL;
  for (auto it = vec.begin(); it != vec.end(); ++it) {
    acc += *it;
  }
  return (acc / vec.size());
}

const string WhareMapCostModel::DebugInfo() const {
  string out;
  out += "psi_map_ contents:\n";
  for (auto it = psi_map_.begin(); it != psi_map_.end(); ++it) {
    stringstream ss;
    ss << "  <" << it->first.first << ", " << it->first.second << "> -> "
       << "avg: " << AverageFromVec(*it->second) << ", "
       << "min: " << MinFromVec(*it->second) << ", "
       << "max: " << MaxFromVec(*it->second) << "; ";
    ss << "[";
    for (auto vit = it->second->begin(); vit != it->second->end(); ++vit)
      ss << *vit << ", ";
    ss << "]" << endl;
    out += ss.str();
  }
  out += "xi_map_ contents:\n";
  for (auto it = xi_map_.begin(); it != xi_map_.end(); ++it) {
    stringstream ss;
    ss << "  < <" << it->first.first.first << ", " << it->first.first.second
       << ">, " << it->first.second << "> -> "
       << "avg: " << AverageFromVec(*it->second) << ", "
       << "min: " << MinFromVec(*it->second) << ", "
       << "max: " << MaxFromVec(*it->second) << "; ";
    ss << "[";
    for (auto vit = it->second->begin(); vit != it->second->end(); ++vit)
      ss << *vit << ", ";
    ss << "]" << endl;
    out += ss.str();
  }
  return out;
}

uint64_t WhareMapCostModel::HashWhareMapStats(const WhareMapStats& wms) {
  size_t hash = 42;
  boost::hash_combine(hash, wms.num_idle());
  boost::hash_combine(hash, wms.num_devils());
  boost::hash_combine(hash, wms.num_rabbits());
  boost::hash_combine(hash, wms.num_sheep());
  boost::hash_combine(hash, wms.num_turtles());

  return static_cast<uint64_t>(hash);
}

const TaskDescriptor& WhareMapCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

Cost_t WhareMapCostModel::MaxFromVec(const vector<uint64_t>& vec) const {
  uint64_t cur_max = 0ULL;
  for (auto it = vec.begin(); it != vec.end(); ++it) {
    cur_max = max(cur_max, *it);
  }
  return cur_max;
}

Cost_t WhareMapCostModel::MinFromVec(const vector<uint64_t>& vec) const {
  uint64_t cur_min = UINT64_MAX;
  for (auto it = vec.begin(); it != vec.end(); ++it) {
    cur_min = min(cur_min, *it);
  }
  return cur_min;
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t WhareMapCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  uint64_t now = time_manager_->GetCurrentTimestamp();
  uint64_t time_since_submit = now - td.submit_time();
  // timestamps are in microseconds, but we scale to tenths of a second here in
  // order to keep the costs small
  uint64_t wait_time_centamillis = time_since_submit / 100000;
  // Cost is the max of the average runtime and the wait time, so that the
  // average runtime is a lower bound on the cost.
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  uint64_t avg_pspi =
    knowledge_base_->GetAvgPsPIForTEC(equiv_classes->front());
  uint64_t* best_avg_pspi =
    FindOrNull(best_case_psi_map_, equiv_classes->front());
  uint64_t normalized_avg_pspi = 100ULL;
  if (best_avg_pspi) {
    normalized_avg_pspi = ((avg_pspi * 100) / *best_avg_pspi) + 1;
    VLOG(1) << "Avg PsPI for TEC " << equiv_classes->front() << " is "
            << avg_pspi << ", "
            << (static_cast<double>(avg_pspi / *best_avg_pspi)) << "x best";
  }
  delete equiv_classes;
  return COST_LOWER_BOUND + 1 +
    max(WAIT_TIME_MULTIPLIER * wait_time_centamillis, normalized_avg_pspi);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t WhareMapCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  // No cost in this cost model.
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t WhareMapCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  uint64_t* best_avg_pspi =
    FindOrNull(best_case_psi_map_, equiv_classes->front());
  uint64_t* worst_avg_pspi =
    FindOrNull(worst_case_psi_map_, equiv_classes->front());
  if (!worst_avg_pspi || !best_avg_pspi) {
    // We don't have a current worst-case or best case average PsPI value for
    // this TEC, so we fall back to using the overall average for the TEC or
    // zero.
    // TODO(malte): check if this can ever return a non-zero value when we
    // don't have a value in the worst_case_psi_map_.
    return knowledge_base_->GetAvgPsPIForTEC(equiv_classes->front());
  }
  VLOG(1) << "Worst avg PsPI for TEC " << equiv_classes->front() << " is "
          << *worst_avg_pspi;
  delete equiv_classes;
  return (*worst_avg_pspi * 100) / (*best_avg_pspi);
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
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, destination);
  if (!rs)
    return 0LL;
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  // Below is a somewhat hackish way of making sure that tasks spread out
  // across machines: we assign a baseline cost equal to the core ID for each
  // core. The core ID is extracted from the description string...
  if (rtnd->resource_desc().type() ==  ResourceDescriptor::RESOURCE_PU) {
    string label = rtnd->resource_desc().friendly_name();
    uint64_t idx = label.find("PU #");
    if (idx != string::npos) {
      string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
      int64_t core_id = strtoll(core_id_substr.c_str(), 0, 10);
      return core_id;
    }
  }
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
                                                     EquivClass_t ec) {
  // The cost of scheduling via the cluster aggregator
  if (ec == cluster_aggregator_ec_)
    return TaskToClusterAggCost(task_id);
  else
    // XXX(malte): Implement other EC's costs!
    return 0;
}

pair<Cost_t, int64_t> WhareMapCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  // If ec isn't a task aggregator, we don't need to do anything
  if (task_aggs_.find(ec) == task_aggs_.end()) {
    // ec must be a machine agg or the cluster agg; we don't need
    // any cost here.
    return pair<Cost_t, int64_t>(0LL, -1LL);
  }
  // Otherwise, ec must be a TEC, so we extract the Whare-MCs cost
  // here. Whare-M does not have TEC -> resource arcs, so this won't
  // ever happen with Whare-M only.
  // Get machine for res_id
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  CHECK_EQ(rtnd->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  EquivClass_t* machine_ec = FindOrNull(machine_to_ec_, res_id);
  CHECK_NOTNULL(machine_ec);
  // See if we have a xi_map_ record for this combination
  pair<pair<EquivClass_t, EquivClass_t>, EquivClass_t> ec_stat_pair;
  ec_stat_pair.first.first = ec;
  ec_stat_pair.first.second = HashWhareMapStats(
      rtnd->resource_desc().whare_map_stats());
  ec_stat_pair.second = *machine_ec;
  vector<uint64_t>* xi_vec = FindPtrOrNull(xi_map_, ec_stat_pair);
  if (xi_vec) {
    // Return normalized cost for the projected placement
    // Best case: baseline for normalisation
    uint64_t* best_avg_pspi =
      FindOrNull(best_case_xi_map_, ec);
    CHECK_NOTNULL(best_avg_pspi);
    // Average PsPI for tasks in ec1 on machine of type ec2
    uint64_t avg_for_ec = AverageFromVec(*xi_vec);
    return pair<Cost_t, int64_t>((avg_for_ec * 100) / *best_avg_pspi, -1LL);
  }
  // No record exists, so we return a high cost
  return pair<Cost_t, int64_t>(INT64_MAX, -1LL);
}

Cost_t WhareMapCostModel::EquivClassToEquivClass(EquivClass_t ec1,
                                                 EquivClass_t ec2) {
  pair<EquivClass_t, EquivClass_t> ec_pair(ec1, ec2);
  vector<uint64_t>* pspi_vec = FindPtrOrNull(psi_map_, ec_pair);
  if (pspi_vec) {
    // Best case: baseline for normalisation
    uint64_t* best_avg_pspi =
      FindOrNull(best_case_psi_map_, ec1);
    CHECK_NOTNULL(best_avg_pspi);
    // Average PsPI for tasks in ec1 on machine of type ec2
    uint64_t avg_for_ec = AverageFromVec(*pspi_vec);
    return (avg_for_ec * 100) / *best_avg_pspi;
  }
  return 0LL;
}

vector<EquivClass_t>* WhareMapCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // We have one task EC per program.
  // The ID of the aggregator is the hash of the command line.
  // This (first) EC will be used for the Whare-Map costs: the TEC
  // aggregator is the source of the Whare-M cost arcs, and
  // TaskToUnscheduledAggCost (currently) assumes that the first TEC is
  // the one for which the cost model has statistics.
  EquivClass_t task_agg =
    static_cast<EquivClass_t>(HashCommandLine(*td_ptr));
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
  // We also have one EC per job.
  // The ID of the aggregator is the hash of the job ID.
  EquivClass_t job_agg =
    static_cast<EquivClass_t>(HashJobID(*td_ptr));
  equiv_classes->push_back(job_agg);
  // All tasks also have an arc to the cluster aggregator.
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<EquivClass_t>* WhareMapCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // Get the machine aggregator corresponding to this machine.
  EquivClass_t* ec_class = FindOrNull(machine_to_ec_, res_id);
  if (ec_class) {
    equiv_classes->push_back(*ec_class);
  }
  // Every machine is also in the special cluster aggregator EC
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<ResourceID_t>* WhareMapCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator, and has arcs to all machines.
    // XXX(malte): This is inefficient, as it needlessly adds all the
    // machines every time we call this. To optimize, we can just include
    // the ones for which arcs are missing.
    for (auto it = machine_to_rtnd_.begin();
         it != machine_to_rtnd_.end();
         ++it) {
      prefered_res->push_back(it->first);
    }
  } else if (task_aggs_.find(ec) != task_aggs_.end()) {
    // ec is a task aggregator.
    // Get worst-case cost
    uint64_t* worst_case_pspi = FindOrNull(worst_case_xi_map_, ec);
    uint64_t* best_case_pspi = FindOrNull(best_case_xi_map_, ec);
    // If we don't have a worst-case cost, we use the maximum PsPI value
    // observed for this TEC as an approximation
    Cost_t normed_worst_pspi = knowledge_base_->GetAvgPsPIForTEC(ec);
    // This is the worst-case *average*, so less susceptible to outliers than
    // the default
    if (worst_case_pspi && best_case_pspi)
      normed_worst_pspi = (*worst_case_pspi * 100) / *best_case_pspi;
    if (FLAGS_num_pref_arcs_agg_to_res > 0) {
      // This branch implements the Xi(c_t, L_m, c_m) arcs of Whare-MCs.
      // Iterate over all the machines and choose those to connect from the TEC.
      multimap<Cost_t, ResourceID_t> priority_res;
      for (auto it = machine_to_rtnd_.begin();
           it != machine_to_rtnd_.end();
           ++it) {
        pair<Cost_t, int64_t> cost_and_cap_to_res =
          EquivClassToResourceNode(ec, it->first);
        Cost_t cost_to_res = cost_and_cap_to_res.first;
        ResourceID_t res_id =
          ResourceIDFromString(it->second->resource_desc().uuid());
        if (cost_to_res >= normed_worst_pspi) {
          // This is a poor choice, as the cost is worse than the worst known
          // one; this can be the case if EquivClassToResourceNode returns
          // INT64_MAX, for example. We don't want to add that arc!
          continue;
        }
        if (priority_res.size() < FLAGS_num_pref_arcs_agg_to_res) {
          // We haven't go enough priority machines yet, so add this one
          priority_res.insert(pair<Cost_t, ResourceID_t>(cost_to_res, res_id));
        } else {
          // If this is a better option than one of the high-priority machines
          // we already have, swap it in.
          multimap<Cost_t, ResourceID_t>::reverse_iterator rit =
            priority_res.rbegin();
          if (cost_to_res < rit->first) {
            priority_res.erase(priority_res.find(rit->first));
            priority_res.insert(pair<Cost_t, ResourceID_t>(
                  cost_to_res, res_id));
          }
        }
      }
      for (multimap<Cost_t, ResourceID_t>::iterator it = priority_res.begin();
           it != priority_res.end(); ++it) {
        prefered_res->push_back(it->second);
      }
    }
  } else if (machine_aggs_.find(ec) != machine_aggs_.end()) {
    // ec is a machine aggregator.
    multimap<EquivClass_t, ResourceID_t>::iterator it =
      machine_ec_to_res_id_.find(ec);
    multimap<EquivClass_t, ResourceID_t>::iterator it_to =
      machine_ec_to_res_id_.upper_bound(ec);
    for (; it != it_to; ++it) {
      prefered_res->push_back(it->second);
    }
  } else {
    VLOG(1) << "Ignored unhandled type of equivalence aggregator "
            << "(EC " << ec << ")";
  }
  return prefered_res;
}

vector<TaskID_t>* WhareMapCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<TaskID_t>* prefered_task = new vector<TaskID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator.
    // We add an arc from each task to the cluster aggregator.
    // XXX(malte): This is very slow because it iterates over all tasks; we
    // should instead only return the set of tasks that do not yet have the
    // appropriate arcs.
    for (TaskMap_t::iterator it = task_map_->begin(); it != task_map_->end();
         ++it) {
      // XXX(malte): task_map_ contains ALL tasks ever seen by the system,
      // including those that have completed, failed or are otherwise no longer
      // present in the flow graph. We do some crude filtering here, but clearly
      // we should instead maintain a collection of tasks actually eligible for
      // scheduling.
      if (it->second->state() == TaskDescriptor::RUNNABLE ||
          (FLAGS_preemption &&
           it->second->state() == TaskDescriptor::RUNNING)) {
        VLOG(2) << "Adding arc from task " << it->first << " to EC " << ec
                << "; task in state "
                << ENUM_TO_STRING(TaskDescriptor::TaskState,
                                  it->second->state());
        prefered_task->push_back(it->first);
      }
    }
  } else if (task_aggs_.find(ec) != task_aggs_.end()) {
    // ec is a task aggregator.
    // This is where we add preference arcs from tasks to new equiv class
    // aggregators.
    // XXX(ionel): This is very slow because it iterates over all tasks.
    for (TaskMap_t::iterator it = task_map_->begin(); it != task_map_->end();
         ++it) {
      // TODO(malte): This is a bit inefficient, because it recalculates the TEC
      // vector for each task, and does dynamic memory allocation...
      vector<EquivClass_t>* tec_vec = GetTaskEquivClasses(it->first);
      for (auto tvi = tec_vec->begin(); tvi != tec_vec->end(); ++tvi) {
        if (*tvi == ec) {
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
      delete tec_vec;
    }
  } else if (machine_aggs_.find(ec) != machine_aggs_.end()) {
    // ec is a machine aggregator.
    // This is where we can add arcs form tasks to machine aggregators.
    // We do not need to add any arcs in the WhareMap cost model.
  } else {
    VLOG(1) << "Ignored unhandled type of equivalence aggregator "
            << "(EC " << ec << ")";
  }
  return prefered_task;
}

vector<ResourceID_t>* WhareMapCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  // Tasks do not have preference arcs to resources, but all tasks
  // have arcs to the cluster aggregator. This arc is added in
  // GetIncomingEquivClassPrefArcs(), rather than here, since this
  // method returns a vector of ResourceID_t, i.e. it is used for
  // direct preferences to resources.
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    WhareMapCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  vector<EquivClass_t>* incoming_ec = new vector<EquivClass_t>();
  vector<EquivClass_t>* outgoing_ec = new vector<EquivClass_t>();
  if (tec == cluster_aggregator_ec_) {
    // Cluster aggregator: has no outgoing arcs to other ECs in this
    // cost model. (Could have, e.g., arcs to rack aggregators, though!).
  } else if (task_aggs_.find(tec) != task_aggs_.end()) {
    // Add the machine equivalence classes to the vector.
    for (unordered_set<EquivClass_t>::iterator
           it = machine_aggs_.begin();
         it != machine_aggs_.end();
         ++it) {
      outgoing_ec->push_back(*it);
    }
  } else if (machine_aggs_.find(tec) != machine_aggs_.end()) {
    // Add the task equivalence classes to the vector.
    for (unordered_set<EquivClass_t>::iterator
           it = task_aggs_.begin();
         it != task_aggs_.end();
         ++it) {
      incoming_ec->push_back(*it);
    }
  } else {
    // Nothing to do, ignore
  }
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(incoming_ec, outgoing_ec);
}

void WhareMapCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_EQ(rtnd_ptr->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  size_t hash = 42;
  BFSTraverseResourceProtobufTreeToHash(
      rtnd_ptr, &hash,
      boost::bind(&WhareMapCostModel::ComputeMachineTypeHash, this, _1, _2));
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  // Set the number of cores for the machine.
  EquivClass_t machine_ec = static_cast<EquivClass_t>(hash);
  // Add mapping between task equiv class and resource id.
  machine_ec_to_res_id_.insert(
      pair<EquivClass_t, ResourceID_t>(machine_ec, res_id));
  // Add mapping between resource id and resource topology node.
  InsertIfNotPresent(&machine_to_rtnd_, res_id, rtnd_ptr);
  // Add mapping between resource id and machine equiv class.
  InsertIfNotPresent(&machine_to_ec_, res_id, machine_ec);
  // Add machine to the machine aggregators set.
  machine_aggs_.insert(machine_ec);
}

void WhareMapCostModel::RemoveMachine(ResourceID_t res_id) {
  EquivClass_t* machine_ec = FindOrNull(machine_to_ec_, res_id);
  CHECK_NOTNULL(machine_ec);
  // Remove the machine from the machine ec map.
  multimap<EquivClass_t, ResourceID_t>::iterator it =
    machine_ec_to_res_id_.find(*machine_ec);
  multimap<EquivClass_t, ResourceID_t>::iterator it_to =
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

void WhareMapCostModel::AddTask(TaskID_t task_id) {
  // No-op in the WhareMap cost model
}

ResourceID_t WhareMapCostModel::MachineResIDForResource(ResourceID_t res_id) {
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

void WhareMapCostModel::RecordMECtoPsPIMapping(
    pair<EquivClass_t, EquivClass_t> ec_pair,
    const TaskFinalReport& task_report) {
  // Record the <task EC, machine EC> -> psPI mapping
  vector<uint64_t>* pspi_vec = FindPtrOrNull(psi_map_, ec_pair);
  VLOG(1) << "Runtime: " << task_report.runtime();
  VLOG(1) << "Instructions: " << task_report.instructions();
  if (task_report.instructions() > 0) {
    uint64_t pspi_value =
      (static_cast<uint64_t>(task_report.runtime()) * SECONDS_TO_PICOSECONDS) /
      task_report.instructions();
    if (!pspi_vec) {
      pspi_vec = new vector<uint64_t>();
      InsertIfNotPresent(&psi_map_, ec_pair, pspi_vec);
    }
    pspi_vec->push_back(pspi_value);
    // Now check if this is a new worst-case; if so, record it
    uint64_t new_avg_pspi = AverageFromVec(*pspi_vec);
    uint64_t* cur_best_avg_pspi =
      FindOrNull(best_case_psi_map_, ec_pair.first);
    uint64_t* cur_worst_avg_pspi =
      FindOrNull(worst_case_psi_map_, ec_pair.first);
    // Is this a new worst case?
    if (!cur_worst_avg_pspi || new_avg_pspi > *cur_worst_avg_pspi) {
      InsertOrUpdate(&worst_case_psi_map_, ec_pair.first, new_avg_pspi);
    }
    // Is this a new best case?
     if (!cur_best_avg_pspi || new_avg_pspi > *cur_best_avg_pspi) {
      InsertOrUpdate(&best_case_psi_map_, ec_pair.first, new_avg_pspi);
    }
    VLOG(1) << "Recording a psPi mapping: <" << ec_pair.first << ", "
            << ec_pair.second << "> -> " << pspi_value << ", now have "
            << pspi_vec->size() << " samples.";
  } else {
    LOG(WARNING) << "No instruction count in final report for task "
                 << task_report.task_id() << ", so did not record any "
                 << "information for it!";
  }
}

void WhareMapCostModel::RecordMECAndCoRunnerSetToPsPIMapping(
    pair<EquivClass_t, EquivClass_t> ec_pair,
    const WhareMapStats& wms,
    const TaskFinalReport& task_report) {
  // Record the < <task EC, corunner set>, machine EC> -> psPI mapping
  pair<pair<EquivClass_t, EquivClass_t>, EquivClass_t> stat_ec_pair;
  stat_ec_pair.first.first = ec_pair.first;
  stat_ec_pair.first.second = HashWhareMapStats(wms);
  stat_ec_pair.second = ec_pair.second;
  vector<uint64_t>* pspi_vec = FindPtrOrNull(xi_map_, stat_ec_pair);
  VLOG(1) << "Runtime: " << task_report.runtime();
  VLOG(1) << "Instructions: " << task_report.instructions();
  VLOG(1) << "Co-runners: " << wms.num_idle() << " idle, "
          << wms.num_devils() << " devil, " << wms.num_rabbits()
          << " rabbit, " << wms.num_sheep() << " sheep, "
          << wms.num_turtles() << " turtle";
  if (task_report.instructions() > 0) {
    uint64_t pspi_value =
      (static_cast<uint64_t>(task_report.runtime()) * 1000000000000) /
      task_report.instructions();
    if (!pspi_vec) {
      pspi_vec = new vector<uint64_t>();
      InsertIfNotPresent(&xi_map_, stat_ec_pair, pspi_vec);
    }
    pspi_vec->push_back(pspi_value);
    // Now check if this is a new worst-case; if so, record it
    uint64_t new_avg_pspi = 0ULL;
    for (auto it = pspi_vec->begin();
         it != pspi_vec->end();
         ++it) {
      new_avg_pspi += *it;
    }
    new_avg_pspi /= pspi_vec->size();
    uint64_t* cur_best_avg_pspi =
      FindOrNull(best_case_xi_map_, ec_pair.first);
    uint64_t* cur_worst_avg_pspi =
      FindOrNull(worst_case_xi_map_, ec_pair.first);
    // Is this a new worst case?
    if (!cur_worst_avg_pspi || new_avg_pspi > *cur_worst_avg_pspi) {
      InsertOrUpdate(&worst_case_xi_map_, ec_pair.first, new_avg_pspi);
    }
    // Is this a new best case?
     if (!cur_best_avg_pspi || new_avg_pspi > *cur_best_avg_pspi) {
      InsertOrUpdate(&best_case_xi_map_, ec_pair.first, new_avg_pspi);
    }
    VLOG(1) << "Recording a psPi mapping: <" << ec_pair.first << ", "
            << ec_pair.second << "> -> " << pspi_value << ", now have "
            << pspi_vec->size() << " samples.";
  } else {
    LOG(WARNING) << "No instruction count in final report for task "
                 << task_report.task_id() << ", so did not record any "
                 << "information for it!";
  }
}

void WhareMapCostModel::RemoveTask(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  // Get the TD in order to find the resource
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  // If the task has just successfully finished, we remember how it did.
  // If the removal is a consequence of a failure or an abort, we don't
  // record this, but still clear up the state below.
  if (td->state() == TaskDescriptor::COMPLETED) {
    CHECK_GT(equiv_classes->size(), 1);
    // TODO(malte): We always use the first EC here; consider tracking
    // data for all task ECs
    EquivClass_t tec = equiv_classes->at(0);
    // Get the machine EC that this task was previously running on
    const TaskDescriptor& td = GetTask(task_id);
    CHECK(td.has_scheduled_to_resource());
    ResourceID_t res_id = ResourceIDFromString(td.scheduled_to_resource());
    ResourceID_t machine_res_id = MachineResIDForResource(res_id);
    EquivClass_t* mec = FindOrNull(machine_to_ec_, machine_res_id);
    CHECK_NOTNULL(mec);
    // Add the Whare-M information to the psi_map_
    pair<EquivClass_t, EquivClass_t> ec_pair(tec, *mec);
    CHECK(td.has_final_report());
    RecordMECtoPsPIMapping(ec_pair, td.final_report());
    // Add the Whare-MCs information to the xi_map_
    ResourceStatus* rs_ptr =
      FindPtrOrNull(*resource_map_, machine_res_id);
    CHECK_NOTNULL(rs_ptr);
    ResourceDescriptor* machine_rd = rs_ptr->mutable_descriptor();
    RecordMECAndCoRunnerSetToPsPIMapping(
        ec_pair, machine_rd->whare_map_stats(), td.final_report());
  }
  // Now remove the state we keep for this task
  for (vector<EquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    unordered_map<EquivClass_t, set<TaskID_t> >::iterator set_it =
      task_ec_to_set_task_id_.find(*it);
    if (set_it != task_ec_to_set_task_id_.end()) {
      // Remove the task's ID from the set of tasks in the EC
      set_it->second.erase(task_id);
      // If the EC is now empty, remove it as well
      if (set_it->second.size() == 0) {
        task_ec_to_set_task_id_.erase(*it);
        task_aggs_.erase(*it);
      }
    }
  }
}

void WhareMapCostModel::ComputeMachineTypeHash(
    const ResourceTopologyNodeDescriptor* rtnd_ptr, size_t* hash) {
  boost::hash_combine(*hash, rtnd_ptr->resource_desc().type());
}

FlowGraphNode* WhareMapCostModel::GatherStats(FlowGraphNode* accumulator,
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
      // Base case: (PU -> SINK). We are at a PU and we gather the statistics.
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, accumulator->resource_id_);
      if (!rs_ptr)
        return accumulator;
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      if (!rd_ptr)
        return accumulator;
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
  if (accumulator->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
    if (!other->resource_id_.is_nil() &&
        other->type_ == FlowNodeType::MACHINE) {
      // Case: (EQUIV -> MACHINE).
      // We don't have to do anything.
    } else if (other->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
      // Case: (EQUIV -> EQUIV).
      // We don't have to do anything.
    } else {
      LOG(FATAL) << "Unexpected preference arc from node " << accumulator->id_
                 << " of type " << accumulator->type_ << " to " << other->id_
                 << " of type " << other->type_;
    }
    // TODO(ionel): Update knowledge base.
    return accumulator;
  }
  // Case: (RESOURCE -> RESOURCE)
  ResourceStatus* acc_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  CHECK_NOTNULL(acc_rs_ptr);
  WhareMapStats* wms_acc_ptr =
    acc_rs_ptr->mutable_descriptor()->mutable_whare_map_stats();
  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, other->resource_id_);
  if (!other_rs_ptr)
    return accumulator;
  WhareMapStats* wms_other_ptr =
    other_rs_ptr->mutable_descriptor()->mutable_whare_map_stats();
  if (accumulator->type_ == FlowNodeType::MACHINE) {
    AccumulateWhareMapStats(wms_acc_ptr, wms_other_ptr);
    // TODO(ionel): Update knowledge base.
    return accumulator;
  }
  AccumulateWhareMapStats(wms_acc_ptr, wms_other_ptr);
  return accumulator;
}

FlowGraphNode* WhareMapCostModel::UpdateStats(FlowGraphNode* accumulator,
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
  if (accumulator->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
    if (other->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
      // Case: EQUIV -> EQUIV
    } else if (other->type_ == FlowNodeType::MACHINE) {
      // Case: EQUIV -> MACHINE
    } else {
      LOG(FATAL) << "Unexpected preference arc";
    }
    return accumulator;
  }
  // Case: RESOURCE -> RESOURCE
  FlowGraphArc* arc = FlowGraph::GetArc(accumulator, other);
  CHECK_NOTNULL(arc);
  uint64_t new_cost = ResourceNodeToResourceNodeCost(accumulator->resource_id_,
                                                     other->resource_id_);
  if (arc->cost_ != new_cost) {
    uint64_t old_cost = arc->cost_;
    arc->cost_ = new_cost;
    DIMACSChange *chg = new DIMACSChangeArc(*arc, old_cost);
    chg->set_comment("WhareMap/UpdateStats");
    dimacs_stats_->UpdateStats(CHG_ARC_BETWEEN_RES);
    flow_graph_manager_->AddGraphChange(chg);
  }

  return accumulator;
}

void WhareMapCostModel::AccumulateWhareMapStats(WhareMapStats* accumulator,
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

}  // namespace firmament
