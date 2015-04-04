// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Representation of a Quincy-style scheduling flow graph.

#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <cstdio>
#include <cstdlib>

#include <boost/bind.hpp>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/dimacs_add_node.h"
#include "scheduling/dimacs_change_arc.h"
#include "scheduling/dimacs_new_arc.h"
#include "scheduling/dimacs_remove_node.h"
#include "scheduling/flow_graph.h"
#include "scheduling/cost_models/flow_scheduling_cost_model_interface.h"

DEFINE_bool(preemption, false, "Enable preemption and migration of tasks");
DEFINE_bool(add_root_task_to_graph, true, "Add the job root task to the graph");
DEFINE_bool(randomize_flow_graph_node_ids, false,
            "If true the the flow graph will not generate node ids in order");

namespace firmament {

using machine::topology::TopologyManager;

FlowGraph::FlowGraph(FlowSchedulingCostModelInterface *cost_model,
                     unordered_set<ResourceID_t,
                       boost::hash<boost::uuids::uuid>>* leaf_res_ids)
    : cost_model_(cost_model),
      current_id_(1),
      cluster_agg_node_(NULL),
      leaf_res_ids_(leaf_res_ids) {
  // Add sink and cluster aggregator node
  AddSpecialNodes();
  // We do not randomize the special nodes because the solvers make
  // assumptions about the the id number of the sink node.
  if (FLAGS_randomize_flow_graph_node_ids) {
    PopulateUnusedIds(50);
  }
}

FlowGraph::~FlowGraph() {
  delete cost_model_;
  for (unordered_map<uint64_t, FlowGraphNode*>::iterator it = node_map_.begin();
       it != node_map_.end(); ) {
    unordered_map<uint64_t, FlowGraphNode*>::iterator it_tmp = it;
    ++it;
    DeleteNode(it_tmp->second);
  }
  ResetChanges();
  // XXX(malte): N.B. this leaks memory as we haven't destroyed all of the
  // nodes and arcs in the flow graph (which are allocated on the heap)
}

void FlowGraph::AddGraphChange(DIMACSChange* change) {
	if (change->GetComment().empty()) {
		change->SetComment("AddGraphChange: anonymous caller");
	}
  graph_changes_.push_back(change);
}

void FlowGraph::AddMachine(ResourceTopologyNodeDescriptor* root) {
  UpdateResourceTopology(root);
}

void FlowGraph::AddArcsForTask(FlowGraphNode* task_node,
                               FlowGraphNode* unsched_agg_node,
                               vector<FlowGraphArc*>* task_arcs) {
  // We always have an edge to the cluster aggregator node
  FlowGraphArc* cluster_agg_arc = AddArcInternal(task_node, cluster_agg_node_);
  // Assign cost to the (task -> cluster agg) edge from cost model
  cluster_agg_arc->cost_ =
      cost_model_->TaskToClusterAggCost(task_node->task_id_);
  cluster_agg_arc->cap_upper_bound_ = 1;
  task_arcs->push_back(cluster_agg_arc);

  // We also always have an edge to our job's unscheduled node
  FlowGraphArc* unsched_arc = AddArcInternal(task_node, unsched_agg_node);
  // Add this task's potential flow to the per-job unscheduled
  // aggregator's outgoing edge
  AdjustUnscheduledAggToSinkCapacityGeneratingDelta(task_node->job_id_, 1);
  // Assign cost to the (task -> unscheduled agg) edge from cost model
  unsched_arc->cost_ =
      cost_model_->TaskToUnscheduledAggCost(task_node->task_id_);
  // Set up arc to unscheduled aggregator
  unsched_arc->cap_upper_bound_ = 1;
  task_arcs->push_back(unsched_arc);
  vector<ResourceID_t>* task_pref_arcs =
    cost_model_->GetTaskPreferenceArcs(task_node->task_id_);
  for (vector<ResourceID_t>::iterator it = task_pref_arcs->begin();
       it != task_pref_arcs->end(); ++it) {
    FlowGraphArc* arc_to_res =
      AddArcInternal(task_node, NodeForResourceID(*it));
    arc_to_res->cost_ =
      cost_model_->TaskToResourceNodeCost(task_node->task_id_, *it);
    arc_to_res->cap_upper_bound_ = 1;
    task_arcs->push_back(arc_to_res);
  }
  delete task_pref_arcs;
}

FlowGraphArc* FlowGraph::AddArcInternal(uint64_t src, uint64_t dst) {
  FlowGraphNode* src_node = FindPtrOrNull(node_map_, src);
  CHECK_NOTNULL(src_node);
  FlowGraphNode* dst_node = FindPtrOrNull(node_map_, dst);
  CHECK_NOTNULL(dst_node);
  FlowGraphArc* arc = new FlowGraphArc(src, dst, src_node, dst_node);
  arc_set_.insert(arc);
  src_node->AddArc(arc);
  return arc;
}

FlowGraphArc* FlowGraph::AddArcInternal(FlowGraphNode* src,
                                        FlowGraphNode* dst) {
  FlowGraphArc* arc = new FlowGraphArc(src->id_, dst->id_, src, dst);
  arc_set_.insert(arc);
  src->AddArc(arc);
  return arc;
}

void FlowGraph::AddArcsFromToOtherEquivNodes(EquivClass_t equiv_class,
                                             FlowGraphNode* ec_node) {
  pair<vector<EquivClass_t>*,
       vector<EquivClass_t>*> equiv_class_to_connect =
    cost_model_->GetEquivClassToEquivClassesArcs(equiv_class);
  CHECK_NOTNULL(equiv_class_to_connect.first);
  CHECK_NOTNULL(equiv_class_to_connect.second);
  // Add incoming arcs.
  for (vector<EquivClass_t>::iterator
         it = equiv_class_to_connect.first->begin();
       it != equiv_class_to_connect.first->end(); ++it) {
    FlowGraphNode* ec_src_ptr = FindPtrOrNull(tec_to_node_, *it);
    CHECK_NOTNULL(ec_src_ptr);
    FlowGraphArc* arc = AddArcInternal(ec_src_ptr->id_, ec_node->id_);
    arc->cost_ = cost_model_->EquivClassToEquivClass(*it, equiv_class);
    // TODO(ionel): Set the capacity on the arc to a sensible value.
    arc->cap_upper_bound_ = 1;
    DIMACSChange *chg = new DIMACSNewArc(*arc);
    chg->SetComment("AddArcsFromToOtherEquivNodes: incoming");
    graph_changes_.push_back(chg);
  }
  // Add outgoing arcs.
  for (vector<EquivClass_t>::iterator
         it = equiv_class_to_connect.second->begin();
       it != equiv_class_to_connect.second->end(); ++it) {
    FlowGraphNode* ec_dst_ptr = FindPtrOrNull(tec_to_node_, *it);
    CHECK_NOTNULL(ec_dst_ptr);
    FlowGraphArc* arc = AddArcInternal(ec_node->id_, ec_dst_ptr->id_);
    arc->cost_ = cost_model_->EquivClassToEquivClass(equiv_class, *it);
    // TODO(ionel): Set the capacity on the arc to a sensible value.
    arc->cap_upper_bound_ = 1;
    DIMACSChange *chg = new DIMACSNewArc(*arc);
    chg->SetComment("AddArcsFromToOtherEquivNodes: outgoing");
    graph_changes_.push_back(chg);
  }
  delete equiv_class_to_connect.first;
  delete equiv_class_to_connect.second;
}

FlowGraphNode* FlowGraph::GetUnschedAggForJob(JobID_t job_id) {
  uint64_t* unsched_agg_node_id = FindOrNull(job_unsched_to_node_id_, job_id);
  if (unsched_agg_node_id == NULL) {
    return NULL;
  }
  return Node(*unsched_agg_node_id);
}

void FlowGraph::AddOrUpdateJobNodes(JobDescriptor* jd) {
  // First add an unscheduled aggregator node for this job
  // if none exists alread
  FlowGraphArc* unsched_agg_to_sink_arc;
  FlowGraphNode* unsched_agg_node;
  uint64_t* unsched_agg_node_id = FindOrNull(job_unsched_to_node_id_,
                                             JobIDFromString(jd->uuid()));
  if (!unsched_agg_node_id) {
    unsched_agg_node = AddNodeInternal(NextId());
    unsched_agg_node->type_.set_type(FlowNodeType::JOB_AGGREGATOR);
    string comment;
    spf(&comment, "UNSCHED_AGG_for_%s", jd->uuid().c_str());
    unsched_agg_node->comment_ = comment;
    // ... and connect it directly to the sink
    unsched_agg_to_sink_arc = AddArcInternal(unsched_agg_node, sink_node_);
    unsched_agg_to_sink_arc->cap_upper_bound_ = 0;
    unsched_agg_to_sink_arc->cost_ =
        cost_model_->UnscheduledAggToSinkCost(JobIDFromString(jd->uuid()));
    // Record this for the future in the job <-> node ID lookup table
    CHECK(InsertIfNotPresent(&job_unsched_to_node_id_,
                             JobIDFromString(jd->uuid()),
                             unsched_agg_node->id_));
    // Add new job unscheduled agg to the graph changes.
    vector<FlowGraphArc*> *unsched_arcs = new vector<FlowGraphArc*>();
    unsched_arcs->push_back(unsched_agg_to_sink_arc);

    DIMACSChange *chg = new DIMACSAddNode(*unsched_agg_node,
                                          unsched_arcs);
    chg->SetComment("AddOrUpdateJobNodes: unsched_agg");
    graph_changes_.push_back(chg);
  } else {
    FlowGraphNode** unsched_agg_node_ptr = FindOrNull(node_map_,
                                                      *unsched_agg_node_id);
    unsched_agg_node = *unsched_agg_node_ptr;
    FlowGraphArc** lookup_ptr = FindOrNull(unsched_agg_node->outgoing_arc_map_,
                                           sink_node_->id_);
    CHECK_NOTNULL(lookup_ptr);
    unsched_agg_to_sink_arc = *lookup_ptr;
  }
  // TODO(gustafa): Maybe clear this and just fill it up on every iteration
  // instead of this first time.
  unsched_agg_nodes_.insert(unsched_agg_node->id_);

  // Now add the job's task nodes
  // TODO(malte): This is a simple BFS lashup; maybe we can do better?
  queue<TaskDescriptor*> q;
  if (FLAGS_add_root_task_to_graph) {
    q.push(jd->mutable_root_task());
  } else {
    TaskDescriptor* root_task = jd->mutable_root_task();
    for (RepeatedPtrField<TaskDescriptor>::iterator c_iter =
         root_task->mutable_spawned()->begin();
         c_iter != root_task->mutable_spawned()->end();
         ++c_iter) {
      // We do actually need to push tasks even if they are already completed,
      // failed or running, since they may have children eligible for
      // scheduling.
      q.push(&(*c_iter));
    }
  }

  while (!q.empty()) {
    TaskDescriptor* cur = q.front();
    q.pop();
    // Check if this node has already been added
    uint64_t* tn_ptr = FindOrNull(task_to_nodeid_map_, cur->uid());
    FlowGraphNode* task_node = tn_ptr ? Node(*tn_ptr) : NULL;
    if (cur->state() == TaskDescriptor::RUNNABLE && !task_node) {
      generate_trace_.TaskSubmitted(JobIDFromString(jd->uuid()), cur->uid());
      vector<FlowGraphArc*> *task_arcs = new vector<FlowGraphArc*>();
      task_node = AddNodeInternal(NextId());
      task_node->type_.set_type(FlowNodeType::UNSCHEDULED_TASK);
      // Add the current task's node
      task_node->excess_ = 1;
      task_node->task_id_ = cur->uid();  // set task ID in node
      task_node->job_id_ = JobIDFromString(jd->uuid());
      sink_node_->excess_--;
      task_nodes_.insert(task_node->id_);
      // Insert a record for the node representing this task's ID
      InsertIfNotPresent(&task_to_nodeid_map_, cur->uid(), task_node->id_);
      // Log info
      VLOG(2) << "Adding edges for task " << cur->uid() << "'s node ("
              << task_node->id_ << "); task state is " << cur->state();
      // Arcs for this node
      AddArcsForTask(task_node, unsched_agg_node, task_arcs);
      // Add the new task node to the graph changes

      DIMACSChange *chg = new DIMACSAddNode(*task_node, task_arcs);
      chg->SetComment("AddOrUpdateJobNodes: task node");
      graph_changes_.push_back(chg);

      AddTaskEquivClasses(task_node);
    } else if (cur->state() == TaskDescriptor::RUNNING ||
               cur->state() == TaskDescriptor::ASSIGNED) {
      // The task is already running, so it must have a node already
      //task_node->type_.set_type(FlowNodeType::SCHEDULED_TASK);
    } else if (task_node) {
      VLOG(2) << "Ignoring task " << cur->uid()
              << ", as its node already exists.";
    } else {
      VLOG(2) << "Ignoring task " << cur->uid() << " [" << hex << cur
              << "], which is in state "
              << ENUM_TO_STRING(TaskDescriptor::TaskState, cur->state());
    }
    // Enqueue any existing children of this task
    for (RepeatedPtrField<TaskDescriptor>::iterator c_iter =
         cur->mutable_spawned()->begin();
         c_iter != cur->mutable_spawned()->end();
         ++c_iter) {
      // We do actually need to push tasks even if they are already completed,
      // failed or running, since they may have children eligible for
      // scheduling.
      q.push(&(*c_iter));
    }
  }
  // Set the excess on the unscheduled node to the difference between the
  // maximum number of running tasks for this job and the number of tasks
  // (F_j - N_j in Quincy terms).
  // TODO(malte): Stub -- this currently allows an unlimited number of tasks per
  // job to be scheduled.
  unsched_agg_node->excess_ = 0;
}

FlowGraphNode* FlowGraph::AddNodeInternal(uint64_t id) {
  FlowGraphNode* node = new FlowGraphNode(id);
  CHECK(InsertIfNotPresent(&node_map_, id, node));
  return node;
}

void FlowGraph::AddSpecialNodes() {
  // N.B.: we do NOT create a cluster aggregator node X here, since
  // the root of the resource topology is automatically chosen as the
  // cluster aggregator.
  // Sink node
  sink_node_ = AddNodeInternal(NextId());
  sink_node_->type_.set_type(FlowNodeType::SINK);
  sink_node_->comment_ = "SINK";
  graph_changes_.push_back(new DIMACSAddNode(*sink_node_,
                                             new vector<FlowGraphArc*>()));
}

void FlowGraph::AddResourceEquivClasses(FlowGraphNode* res_node) {
  ResourceID_t res_id = res_node->resource_id_;
  vector<EquivClass_t>* equiv_classes =
    cost_model_->GetResourceEquivClasses(res_id);
  for (vector<EquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    FlowGraphNode* ec_node_ptr = FindPtrOrNull(tec_to_node_, *it);
    if (ec_node_ptr == NULL) {
      // Node for resource equiv class doesn't yet exist.
      AddEquivClassNode(*it);
    } else {
      // Node for resource equiv class already exists. Add arc from it.
      // XXX(ionel): We don't add arcs from tasks or other equiv classes
      // to the resource equiv class when a new resource is connected to it.
      FlowGraphArc* ec_arc =
        AddArcInternal(ec_node_ptr->id_, res_node->id_);
      ec_arc->cap_upper_bound_ = 1;
      ec_arc->cost_ =
        cost_model_->EquivClassToResourceNode(*it, res_id);

      DIMACSChange *chg = new DIMACSNewArc(*ec_arc);
      chg->SetComment("AddResourceEquivClasses");
      graph_changes_.push_back(chg);
    }
  }
  delete equiv_classes;
}

void FlowGraph::AddResourceTopology(
    ResourceTopologyNodeDescriptor* resource_tree) {
  BFSTraverseResourceProtobufTreeReturnRTND(
      resource_tree,
      boost::bind(&FlowGraph::AddResourceNode, this, _1));
}

void FlowGraph::AddResourceNode(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  FlowGraphNode* new_node;
  CHECK_NOTNULL(rtnd_ptr);
  const ResourceTopologyNodeDescriptor& rtnd = *rtnd_ptr;
  // Add the node if it does not already exist
  if (!NodeForResourceID(ResourceIDFromString(rtnd.resource_desc().uuid()))) {
    vector<FlowGraphArc*> *resource_arcs = new vector<FlowGraphArc*>();
    uint64_t id = NextId();
    if (rtnd.resource_desc().has_friendly_name()) {
      VLOG(2) << "Adding node " << id << " for resource "
              << rtnd.resource_desc().uuid() << " ("
              << rtnd.resource_desc().friendly_name() << ")";
    } else {
      VLOG(2) << "Adding node " << id << " for resource "
              << rtnd.resource_desc().uuid();
    }
    new_node = AddNodeInternal(id);
    if (rtnd_ptr->resource_desc().type() == ResourceDescriptor::RESOURCE_PU) {
      new_node->type_.set_type(FlowNodeType::PU);
    } else if (rtnd_ptr->resource_desc().type() ==
               ResourceDescriptor::RESOURCE_MACHINE) {
      new_node->type_.set_type(FlowNodeType::MACHINE);
    } else {
      new_node->type_.set_type(FlowNodeType::UNKNOWN);
    }
    InsertIfNotPresent(&resource_to_nodeid_map_,
                       ResourceIDFromString(rtnd.resource_desc().uuid()),
                       new_node->id_);
    new_node->resource_id_ = ResourceIDFromString(rtnd.resource_desc().uuid());
    if (rtnd.resource_desc().has_friendly_name())
      new_node->comment_ = rtnd.resource_desc().friendly_name();
    // Record the parent if we have one
    if (rtnd.has_parent_id()) {
      // Add arc from parent to us if it doesn't already exist
      FlowGraphNode* parent_node =
        NodeForResourceID(ResourceIDFromString(rtnd.parent_id()));
      CHECK_NOTNULL(parent_node);
      FlowGraphArc** arc = FindOrNull(parent_node->outgoing_arc_map_, id);
      if (!arc) {
        VLOG(2) << "Adding missing arc from parent "
                << parent_node->resource_id_
                << "(" << parent_node->id_ << ") to "
                << rtnd.resource_desc().uuid() << "("  << id << ").";
        // The arc will have a 0 capacity, but it will be updated
        // by the ConfigureResource methods.
        resource_arcs->push_back(AddArcInternal(parent_node->id_, id));
      }
      InsertIfNotPresent(&resource_to_parent_map_,
                         new_node->resource_id_,
                         ResourceIDFromString(rtnd.parent_id()));
    }
    // Add new resource node to the graph changes.
    DIMACSChange *chg = new DIMACSAddNode(*new_node, resource_arcs);
    chg->SetComment("AddResourceNode");
    graph_changes_.push_back(chg);

    if (rtnd_ptr->resource_desc().type() ==
        ResourceDescriptor::RESOURCE_MACHINE) {
      ResourceID_t res_id =
        ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
      generate_trace_.AddMachine(res_id);
      cost_model_->AddMachine(rtnd_ptr);
      AddResourceEquivClasses(new_node);
    }
  } else {
    new_node = NodeForResourceID(
        ResourceIDFromString(rtnd.resource_desc().uuid()));
  }
  // Consider different cases: root node, internal node and leaf node
  if (!rtnd.has_parent_id()) {
    // 1) Root node
    ConfigureResourceRootNode(rtnd, new_node);
  }
  if (rtnd.children_size() > 0) {
    // 2) Node inside the tree with non-zero children (i.e. no leaf node)
    ConfigureResourceBranchNode(rtnd, new_node);
  } else if (rtnd.has_parent_id()) {
    // 3) Leaves of the resource topology; add an arc to the sink node
    ConfigureResourceLeafNode(rtnd, new_node);
  }
}

void FlowGraph::AddEquivClassNode(EquivClass_t ec) {
  VLOG(2) << "Add equiv class " << ec;
  vector<FlowGraphArc*> *ec_arcs = new vector<FlowGraphArc*>();
  // Add the equivalence class flow graph node.
  FlowGraphNode* ec_node = AddNodeInternal(NextId());
  ec_node->type_.set_type(FlowNodeType::EQUIVALENCE_CLASS);
  CHECK(InsertIfNotPresent(&tec_to_node_, ec, ec_node));
  string comment;
  spf(&comment, "EC_AGG_%ju", ec);
  ec_node->comment_ = comment;
  vector<TaskID_t>* task_pref_arcs =
    cost_model_->GetIncomingEquivClassPrefArcs(ec);
  vector<ResourceID_t>* res_pref_arcs =
    cost_model_->GetOutgoingEquivClassPrefArcs(ec);
  // Add the incoming arcs to the equivalence class node.
  for (vector<TaskID_t>::iterator it = task_pref_arcs->begin();
       it != task_pref_arcs->end(); ++it) {
    FlowGraphArc* ec_arc =
      AddArcInternal(NodeForTaskID(*it)->id_, ec_node->id_);
    // XXX(ionel): Increase the capacity if we want to allow for PU sharing.
    ec_arc->cap_upper_bound_ = 1;
    ec_arc->cost_ =
      cost_model_->TaskToEquivClassAggregator(*it, ec);
    ec_arcs->push_back(ec_arc);
  }
  delete task_pref_arcs;
  // Add the outgoing arcs from the equivalence class node.
  for (vector<ResourceID_t>::iterator it = res_pref_arcs->begin();
       it != res_pref_arcs->end(); ++it) {
    FlowGraphArc* ec_arc =
      AddArcInternal(ec_node->id_, NodeForResourceID(*it)->id_);
    // XXX(ionel): Increase the capacity if we want to allow for PU sharing.
    ec_arc->cap_upper_bound_ = 1;
    ec_arc->cost_ =
      cost_model_->EquivClassToResourceNode(ec, *it);
    ec_arcs->push_back(ec_arc);
  }
  delete res_pref_arcs;
  // Add the new equivalence node to the graph changes
  DIMACSChange *chg = new DIMACSAddNode(*ec_node, ec_arcs);
  chg->SetComment("AddEquivClassNode");
  graph_changes_.push_back(chg);
  AddArcsFromToOtherEquivNodes(ec, ec_node);
}

void FlowGraph::AddTaskEquivClasses(FlowGraphNode* task_node) {
  vector<EquivClass_t>* equiv_classes =
    cost_model_->GetTaskEquivClasses(task_node->task_id_);
  for (vector<EquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    FlowGraphNode* ec_node_ptr = FindPtrOrNull(tec_to_node_, *it);
    if (ec_node_ptr == NULL) {
      // Node for task equiv class doesn't yet exist.
      AddEquivClassNode(*it);
    } else {
      // Node for task equiv class already exists. Add arc to it.
      // XXX(ionel): We don't add new arcs from the equivalence class to
      // resource nodes when we add a new arc from a task to the equiv class.
      // We may want to do add some in the future.
      FlowGraphArc* ec_arc =
        AddArcInternal(task_node->id_, ec_node_ptr->id_);
      ec_arc->cap_upper_bound_ = 1;
      ec_arc->cost_ =
        cost_model_->TaskToEquivClassAggregator(task_node->task_id_, *it);

      DIMACSChange *chg = new DIMACSNewArc(*ec_arc);
      chg->SetComment("AddTaskEquivClasses");
      graph_changes_.push_back(chg);
    }
  }
  delete equiv_classes;
}

void FlowGraph::AdjustUnscheduledAggToSinkCapacityGeneratingDelta(
    JobID_t job, int64_t delta) {
  uint64_t* unsched_agg_node_id = FindOrNull(job_unsched_to_node_id_, job);
  CHECK_NOTNULL(unsched_agg_node_id);
  FlowGraphArc** lookup_ptr =
      FindOrNull(Node(*unsched_agg_node_id)->outgoing_arc_map_,
                 sink_node_->id_);
  CHECK_NOTNULL(lookup_ptr);
  FlowGraphArc* unsched_agg_to_sink_arc = *lookup_ptr;
  unsched_agg_to_sink_arc->cap_upper_bound_ += delta;

  DIMACSChange *chg = new DIMACSChangeArc(*unsched_agg_to_sink_arc);
  chg->SetComment("AdjustUnscheduledAggToSinkCapacityGeneratingDelta");
  graph_changes_.push_back(chg);
}

void FlowGraph::AdjustUnscheduledAggArcCosts() {
  unordered_map<JobID_t, uint64_t,
                boost::hash<boost::uuids::uuid> >::iterator it =
      job_unsched_to_node_id_.begin();
  for (; it != job_unsched_to_node_id_.end(); ++it) {
    FlowGraphNode* unsched_node = Node(it->second);
    for (unordered_map<uint64_t, FlowGraphArc*>::iterator
         ait = unsched_node->incoming_arc_map_.begin();
         ait != unsched_node->incoming_arc_map_.end();) {
      unordered_map<uint64_t, FlowGraphArc*>::iterator ait_tmp = ait;
      ++ait;
      FlowGraphArc* arc = ait_tmp->second;
      CHECK_NOTNULL(arc);
      TaskID_t task_id = Node(arc->src_)->task_id_;

      Cost_t new_cost = cost_model_->TaskToUnscheduledAggCost(task_id);
      CHECK_GE(new_cost, 0);
      if ((uint64_t)new_cost != arc->cost_) {
        arc->cost_ = new_cost;
        DIMACSChange *chg = new DIMACSChangeArc(*arc);
        chg->SetComment("AdjustUnscheduledAggArcCosts");
        graph_changes_.push_back(chg);
      }
    }
  }
}

void FlowGraph::ConfigureResourceRootNode(
    const ResourceTopologyNodeDescriptor& rtnd, FlowGraphNode* new_node) {
  // 1) Root node
  // N.B. a root node without parent is always automatically taken as the
  // cluster aggregator.
  new_node->type_.set_type(FlowNodeType::GLOBAL_AGGREGATOR);
  new_node->comment_ = "CLUSTER_AGG";
  // Reset cluster aggregator to this node
  CHECK(cluster_agg_node_ == NULL);
  cluster_agg_node_ = new_node;
}

void FlowGraph::ConfigureResourceBranchNode(
    const ResourceTopologyNodeDescriptor& rtnd, FlowGraphNode* new_node) {
  // Add internal arc from parent
  if (rtnd.has_parent_id()) {
    FlowGraphNode* parent_node = NodeForResourceID(
        ResourceIDFromString(rtnd.parent_id()));
    CHECK(parent_node != NULL) << "Could not find parent node with ID "
                               << rtnd.parent_id();
    // Find the arc from parent node (which should have been added before)
    FlowGraphArc* arc = FindPtrOrNull(parent_node->outgoing_arc_map_,
                                      new_node->id_);
    CHECK_NOTNULL(arc);
    // Set initial capacity to 0; this will be updated as leaves are added
    // below this node!
    arc->cap_upper_bound_ = 0;
    arc->cost_ =
        cost_model_->ResourceNodeToResourceNodeCost(
            parent_node->resource_id_, new_node->resource_id_);
    // XXX(ionel): The new arc has a capacity of 0. This is treated as an arc
    // removal by the DIMACS extended. We add the arc to the graph changes later
    // when we change the capacity.
    // graph_changes_.push_back(new DIMACSChangeArc(*arc));
  } else if (new_node->type_.type() != FlowNodeType::GLOBAL_AGGREGATOR) {
    // Having no parent is only okay if we're the root node
    LOG(FATAL) << "Found child without parent_id set! This will lead to an "
               << "inconsistent flow graph! child ID: "
               << rtnd.resource_desc().uuid();
  }
}

void FlowGraph::ConfigureResourceLeafNode(
    const ResourceTopologyNodeDescriptor& rtnd, FlowGraphNode* new_node) {
  VLOG(2) << "Considering node " << rtnd.resource_desc().uuid()
          << ", which has parent "
          << (rtnd.has_parent_id() ? rtnd.parent_id() : "NONE");
  VLOG(2) << "Adding arc from leaf resource " << rtnd.resource_desc().uuid()
          << " to sink node.";
  if (rtnd.resource_desc().type() != ResourceDescriptor::RESOURCE_PU)
    LOG(FATAL) << "Leaf resource " << rtnd.resource_desc().uuid()
               << " is not a PU! This may yield an unschedulable flow!";
  FlowGraphNode* cur_node = NodeForResourceID(
      ResourceIDFromString(rtnd.resource_desc().uuid()));
  CHECK(cur_node != NULL) << "Could not find leaf node with ID "
                          << rtnd.resource_desc().uuid();
  cur_node->type_.set_type(FlowNodeType::PU);
  FlowGraphArc* arc = AddArcInternal(cur_node->id_, sink_node_->id_);
  arc->cap_upper_bound_ = 1;
  // TODO(malte): change this if support time-sharing
  arc->cost_ =
      cost_model_->LeafResourceNodeToSinkCost(cur_node->resource_id_);
  leaf_nodes_.insert(cur_node->id_);
  leaf_res_ids_->insert(cur_node->resource_id_);

  DIMACSChange *chg = new DIMACSNewArc(*arc);
  chg->SetComment("ConfigureResourceLeafNode");
  graph_changes_.push_back(chg);
  // Add flow capacity to parent nodes until we hit the root node
  FlowGraphNode* parent = cur_node;
  ResourceID_t* parent_id;
  while ((parent_id = FindOrNull(resource_to_parent_map_,
                                 parent->resource_id_)) != NULL) {
    uint64_t cur_id = parent->id_;
    parent = NodeForResourceID(*parent_id);
    FlowGraphArc* arc = FindPtrOrNull(parent->outgoing_arc_map_, cur_id);
    CHECK_NOTNULL(arc);
    VLOG(2) << "Adding capacity on edge from " << *parent_id << " ("
            << parent->id_ << ") to " << cur_id << " ("
            << arc->cap_upper_bound_ << " -> "
            << arc->cap_upper_bound_ + 1 << ")";
    arc->cap_upper_bound_ += 1;

    DIMACSChange *chg = new DIMACSChangeArc(*arc);
    chg->SetComment("ConfigureResourceLeafNode");
    graph_changes_.push_back(chg);
  }
}

bool FlowGraph::CheckNodeType(uint64_t node, FlowNodeType_NodeType type) {
  FlowNodeType_NodeType node_type = Node(node)->type_.type();
  return (node_type == type);
}

void FlowGraph::ChangeArc(FlowGraphArc* arc, uint64_t cap_lower_bound,
                          uint64_t cap_upper_bound, uint64_t cost,
													const char *comment) {
  arc->cap_lower_bound_ = cap_lower_bound;
  arc->cap_upper_bound_ = cap_upper_bound;
  arc->cost_ = cost;
  if (!arc->cap_upper_bound_) {
    DeleteArcGeneratingDelta(arc, comment);
  } else {
  	DIMACSChange *chg = new DIMACSChangeArc(*arc);
  	chg->SetComment(comment);
    graph_changes_.push_back(chg);
  }
}

void FlowGraph::DeleteArcGeneratingDelta(FlowGraphArc* arc, const char *comment) {
  arc->cap_lower_bound_ = 0;
  arc->cap_upper_bound_ = 0;

  DIMACSChange *chg = new DIMACSChangeArc(*arc);
  chg->SetComment(comment);
  graph_changes_.push_back(chg);
  DeleteArc(arc);
}

void FlowGraph::DeleteArc(FlowGraphArc* arc) {
  // Remove the arc from the incoming and outgoing collections.
  arc->src_node_->outgoing_arc_map_.erase(arc->dst_node_->id_);
  arc->dst_node_->incoming_arc_map_.erase(arc->src_node_->id_);
  // First remove various meta-data relating to this arc
  arc_set_.erase(arc);
  // Then delete the arc itself
  delete arc;
}

void FlowGraph::DeleteNode(FlowGraphNode* node, const char *comment) {
  // First remove all outgoing arcs
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
         node->outgoing_arc_map_.begin();
       it != node->outgoing_arc_map_.end();) {
    CHECK_EQ(it->first, it->second->dst_);
    CHECK_EQ(node->id_, it->second->src_);
    CHECK_EQ(it->second->dst_node_->incoming_arc_map_.erase(it->second->src_),
             1);
    unordered_map<uint64_t, FlowGraphArc*>::iterator it_tmp = it;
    ++it;
    DeleteArc(it_tmp->second);
  }
  node->outgoing_arc_map_.clear();
  // Remove all incoming arcs.
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
         node->incoming_arc_map_.begin();
       it != node->incoming_arc_map_.end();) {
    CHECK_EQ(node->id_, it->second->dst_);
    CHECK_EQ(it->first, it->second->src_);
    CHECK_EQ(it->second->src_node_->outgoing_arc_map_.erase(it->second->dst_),
             1);
    unordered_map<uint64_t, FlowGraphArc*>::iterator it_tmp = it;
    ++it;
    DeleteArc(it_tmp->second);
  }
  node->incoming_arc_map_.clear();
  node_map_.erase(node->id_);

  DIMACSChange *chg = new DIMACSRemoveNode(*node);
  chg->SetComment(comment);
  graph_changes_.push_back(chg);
  delete node;
}

void FlowGraph::DeleteResourceNode(FlowGraphNode* res_node, const char *comment) {
  ResourceID_t res_id = res_node->resource_id_;
  ResourceID_t res_id_tmp = res_id;
  resource_to_parent_map_.erase(res_id);
  unused_ids_.push(res_node->id_);
  leaf_nodes_.erase(res_node->id_);
  // This erase is going to delete the res_id. That's why we use
  // res_id_tmp from now onwards.
  leaf_res_ids_->erase(res_id);
  resource_to_nodeid_map_.erase(res_id_tmp);
  DeleteNode(res_node, comment);
  vector<EquivClass_t>* equiv_classes =
    cost_model_->GetResourceEquivClasses(res_id_tmp);
  for (vector<EquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    DeleteOrUpdateIncomingEquivNode(*it, comment);
  }
  delete equiv_classes;
}

void FlowGraph::DeleteOrUpdateIncomingEquivNode
                            (EquivClass_t task_equiv, const char *comment) {
  FlowGraphNode* equiv_node_ptr = FindPtrOrNull(tec_to_node_, task_equiv);
  if (equiv_node_ptr == NULL) {
    // Equiv class node can be NULL because all it's task are running
    // and are directly connected to resource nodes.
    return;
  }
  if (equiv_node_ptr->outgoing_arc_map_.size() == 0) {
    VLOG(2) << "Deleting resource_equiv class: " << task_equiv;
    // The equiv class doesn't have any incoming arcs from tasks.
    // We can remove the node.
    tec_to_node_.erase(task_equiv);
    unused_ids_.push(equiv_node_ptr->id_);
    DeleteNode(equiv_node_ptr, comment);
  } else {
    // TODO(ionel): We may want to reduce the number of outgoing
    // arcs from the equiv class to cores. However, this is not
    // mandatory.
  }
}

void FlowGraph::DeleteOrUpdateOutgoingEquivNode
                            (EquivClass_t task_equiv, const char *comment) {
  FlowGraphNode* equiv_node_ptr = FindPtrOrNull(tec_to_node_, task_equiv);
  if (equiv_node_ptr == NULL) {
    // Equiv class node can be NULL because all it's task are running
    // and are directly connected to resource nodes.
    return;
  }
  if (equiv_node_ptr->incoming_arc_map_.size() == 0) {
    VLOG(2) << "Deleting task_equiv class: " << task_equiv;
    // The equiv class doesn't have any incoming arcs from tasks.
    // We can remove the node.
    tec_to_node_.erase(task_equiv);
    unused_ids_.push(equiv_node_ptr->id_);
    DeleteNode(equiv_node_ptr, comment);
  } else {
    // TODO(ionel): We may want to reduce the number of outgoing
    // arcs from the equiv class to cores. However, this is not
    // mandatory.
  }
}

void FlowGraph::DeleteTaskNode(TaskID_t task_id, const char *comment) {
  uint64_t* node_id = FindOrNull(task_to_nodeid_map_, task_id);
  CHECK_NOTNULL(node_id);
  FlowGraphNode* node = Node(*node_id);
  // Increase the sink's excess and set this node's excess to zero
  node->excess_ = 0;
  sink_node_->excess_++;
  // Find the unscheduled node for this job and decrement its outgoing capacity
  // TODO(malte): this is only relevant if we support preemption; otherwise the
  // capcacity will already have been deducted (as part of PinTaskToNode,
  // currently).
  // Then remove node meta-data
  VLOG(2) << "Deleting task node with id " << node->id_ << ", task id "
          << node->task_id_;
  task_nodes_.erase(node->task_id_);
  unused_ids_.push(node->id_);
  task_to_nodeid_map_.erase(task_id);
  // Then remove the node itself
  DeleteNode(node, comment);
  vector<EquivClass_t>* equiv_classes =
    cost_model_->GetTaskEquivClasses(node->task_id_);
  for (vector<EquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    DeleteOrUpdateOutgoingEquivNode(*it, comment);
  }
  delete equiv_classes;
}

FlowGraphArc* FlowGraph::GetArc(FlowGraphNode* src, FlowGraphNode* dst) {
  unordered_map<uint64_t, FlowGraphArc*>::iterator arc_it =
    src->outgoing_arc_map_.find(dst->id_);
  if (arc_it == src->outgoing_arc_map_.end()) {
    LOG(FATAL) << "Could not find arc";
  }
  return arc_it->second;
}

void FlowGraph::JobCompleted(JobID_t job_id) {
  uint64_t* unsched_node_id = FindOrNull(job_unsched_to_node_id_, job_id);
  CHECK_NOTNULL(unsched_node_id);
  FlowGraphNode* node = Node(*unsched_node_id);
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator
         it = node->incoming_arc_map_.begin();
       it != node->incoming_arc_map_.end();) {
    unordered_map<uint64_t, FlowGraphArc*>::iterator it_tmp = it;
    ++it;
    FlowGraphArc* arc = it_tmp->second;
    CHECK_NOTNULL(arc);
    FlowGraphNode* task_node = arc->src_node_;
    CHECK_NOTNULL(task_node);
    CHECK_EQ(task_node->job_id_, job_id);
    generate_trace_.TaskCompleted(task_node->task_id_);
    DeleteTaskNode(task_node->task_id_, "JobCompleted: task node");
    cost_model_->RemoveTask(task_node->task_id_);
  }
  CHECK_EQ(node->incoming_arc_map_.size(), 0);
  job_unsched_to_node_id_.erase(job_id);
  unused_ids_.push(node->id_);
  DeleteNode(node, "JobCompleted: unsched");
}

uint64_t FlowGraph::NextId() {
  if (FLAGS_randomize_flow_graph_node_ids) {
    if (unused_ids_.empty()) {
      PopulateUnusedIds(current_id_ * 2);
    }
    uint64_t new_id = unused_ids_.front();
    unused_ids_.pop();
    ids_created_.push_back(new_id);
    return new_id;
  } else {
    if (unused_ids_.empty()) {
      ids_created_.push_back(current_id_);
      return current_id_++;
    } else {
      uint64_t new_id = unused_ids_.front();
      unused_ids_.pop();
      ids_created_.push_back(new_id);
      return new_id;
    }
  }
}

FlowGraphNode* FlowGraph::NodeForResourceID(const ResourceID_t& res_id) {
  uint64_t* id = FindOrNull(resource_to_nodeid_map_, res_id);
  // Returns NULL if resource unknown
  if (!id)
    return NULL;
  VLOG(2) << "Resource " << res_id << " is represented by node " << *id;
  FlowGraphNode** node_ptr = FindOrNull(node_map_, *id);
  return (node_ptr ? *node_ptr : NULL);
}

FlowGraphNode* FlowGraph::NodeForTaskID(TaskID_t task_id) {
  uint64_t* id = FindOrNull(task_to_nodeid_map_, task_id);
  // Returns NULL if task unknown
  if (!id)
    return NULL;
  VLOG(2) << "Task " << task_id << " is represented by node " << *id;
  FlowGraphNode** node_ptr = FindOrNull(node_map_, *id);

  return (node_ptr ? *node_ptr : NULL);
}

void FlowGraph::PinTaskToNode(FlowGraphNode* task_node,
                              FlowGraphNode* res_node) {
  // Remove all arcs apart from the task -> resource mapping;
  // note that this effectively disables preemption!
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
         task_node->outgoing_arc_map_.begin();
       it != task_node->outgoing_arc_map_.end(); ) {
    VLOG(2) << "Deleting arc from " << it->second->src_ << " to "
            << it->second->dst_;
    unordered_map<uint64_t, FlowGraphArc*>::iterator it_tmp = it;
    ++it;
    DeleteArcGeneratingDelta(it_tmp->second, "PinTaskToNode");
  }
  // Remove this task's potential flow from the per-job unscheduled
  // aggregator's outgoing edge
  AdjustUnscheduledAggToSinkCapacityGeneratingDelta(task_node->job_id_, -1);
  // Re-add a single arc from the task to the resource node
  FlowGraphArc* new_arc = AddArcInternal(task_node, res_node);
  new_arc->cap_upper_bound_ = 1;

  DIMACSChange *chg = new DIMACSNewArc(*new_arc);
  chg->SetComment("PinTaskToNode");
  graph_changes_.push_back(chg);
}

void FlowGraph::PopulateUnusedIds(uint64_t new_current_id) {
  srand(42);
  vector<uint64_t> ids;
  for (uint64_t index = current_id_; index < new_current_id; ++index) {
    ids.push_back(index);
  }
  random_shuffle(ids.begin(), ids.end());
  for (vector<uint64_t>::iterator it = ids.begin(); it != ids.end(); ++it) {
    unused_ids_.push(*it);
  }
  current_id_ = new_current_id;
}

void FlowGraph::RemoveMachine(ResourceID_t res_id) {
  generate_trace_.RemoveMachine(res_id);
  uint64_t* node_id = FindOrNull(resource_to_nodeid_map_, res_id);
  CHECK_NOTNULL(node_id);
  RemoveMachineSubTree(Node(*node_id));
  cost_model_->RemoveMachine(res_id);
}

void FlowGraph::RemoveMachineSubTree(FlowGraphNode* res_node) {
  while (true) {
    unordered_map<uint64_t, FlowGraphArc*>::iterator
      it = res_node->outgoing_arc_map_.begin();
    if (it == res_node->outgoing_arc_map_.end()) {
      break;
    }
    if (it->second->dst_node_->resource_id_.is_nil()) {
      // The node is not a resource node. We will just delete the arc to it.
      DeleteArc(it->second);
      continue;
    }
    if (it->second->dst_node_->type_.type() == FlowNodeType::PU ||
        it->second->dst_node_->type_.type() == FlowNodeType::MACHINE ||
        it->second->dst_node_->type_.type() == FlowNodeType::UNKNOWN) {
      RemoveMachineSubTree(it->second->dst_node_);
    } else {
      // The node is not a machine related node. We will just delete the arc
      // to it.
      DeleteArc(it->second);
    }
  }
  // We've deleted all its children. Now we can delete the node itself.
  DeleteResourceNode(res_node, "RemoveMachineSubTree");
}

void FlowGraph::TaskCompleted(TaskID_t tid) {
  generate_trace_.TaskCompleted(tid);
  DeleteTaskNode(tid, "TaskCompleted");
  cost_model_->RemoveTask(tid);
}

void FlowGraph::TaskEvicted(TaskID_t tid, ResourceID_t res_id) {
  generate_trace_.TaskEvicted(tid);
  UpdateArcsForEvictedTask(tid, res_id);
  // We do not have to remove the task from the cost model because
  // the task will still exist in the flow graph at the end of
  // UpdateArcsForEvictedTask.
}

void FlowGraph::TaskFailed(TaskID_t tid) {
  generate_trace_.TaskFailed(tid);
  DeleteTaskNode(tid, "TaskFailed");
  cost_model_->RemoveTask(tid);
}

void FlowGraph::TaskKilled(TaskID_t tid) {
  generate_trace_.TaskKilled(tid);
  DeleteTaskNode(tid, "TaskKilled");
  cost_model_->RemoveTask(tid);
}

void FlowGraph::TaskScheduled(TaskID_t tid, ResourceID_t res_id) {
  generate_trace_.TaskScheduled(tid, res_id);
  UpdateArcsForBoundTask(tid, res_id);
}

void FlowGraph::UpdateArcsForBoundTask(TaskID_t tid, ResourceID_t res_id) {
  FlowGraphNode* task_node = NodeForTaskID(tid);
  FlowGraphNode* assigned_res_node = NodeForResourceID(res_id);
  CHECK_NOTNULL(task_node);
  CHECK_NOTNULL(assigned_res_node);
  if (!FLAGS_preemption) {
    // After the task is bound, we now remove all of its edges into the flow
    // graph apart from the bound resource.
    // N.B.: This disables preemption and migration!
    VLOG(2) << "Disabling preemption for " << tid;
    // Disable preemption
    PinTaskToNode(task_node, assigned_res_node);
  }
}

void FlowGraph::UpdateArcsForEvictedTask(TaskID_t task_id,
                                         ResourceID_t res_id) {
  FlowGraphNode* task_node = NodeForTaskID(task_id);
  if (!FLAGS_preemption) {
    // Delete outgoing arcs for running task.
    for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
           task_node->outgoing_arc_map_.begin();
         it != task_node->outgoing_arc_map_.end();) {
      unordered_map<uint64_t, FlowGraphArc*>::iterator it_tmp = it;
      ++it;
      DeleteArc(it_tmp->second);
    }
    // Add back arcs to equiv class node, unscheduled agg and to
    // resource topology agg.
    vector<FlowGraphArc*> *task_arcs = new vector<FlowGraphArc*>();
    uint64_t* unsched_agg_node_id = FindOrNull(job_unsched_to_node_id_,
                                               task_node->job_id_);
    FlowGraphNode** unsched_agg_node_ptr =
      FindOrNull(node_map_, *unsched_agg_node_id);
    CHECK_NOTNULL(unsched_agg_node_ptr);

    AddArcsForTask(task_node, *unsched_agg_node_ptr, task_arcs);
    for (vector<FlowGraphArc*>::iterator it = task_arcs->begin();
         it != task_arcs->end(); ++it) {
    	DIMACSChange *chg = new DIMACSNewArc(**it);
    	chg->SetComment("UpdateArcsForEvictedTask");
      graph_changes_.push_back(chg);
    }
    delete task_arcs;

    AddTaskEquivClasses(task_node);

    // Add this task's potential flow from the per-job unscheduled
    // aggregator's outgoing edge
    AdjustUnscheduledAggToSinkCapacityGeneratingDelta(task_node->job_id_, 1);
  }
}

void FlowGraph::UpdateResourceNode(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  const ResourceTopologyNodeDescriptor& rtnd = *rtnd_ptr;
  ResourceID_t res_id = ResourceIDFromString(rtnd.resource_desc().uuid());
  // First of all, check if this node already exists in our resource topology
  uint64_t* found_node = FindOrNull(resource_to_nodeid_map_, res_id);
  VLOG(2) << "Considering resource " << res_id << ", which is "
          << (found_node ? *found_node : 0);
  if (found_node) {
    // Check if its parent is identical
    if (rtnd.has_parent_id()) {
      ResourceID_t* old_parent_id = FindOrNull(resource_to_parent_map_, res_id);
      ResourceID_t new_parent_id = ResourceIDFromString(rtnd.parent_id());
      // We didn't have a parent or the parent we had was different, so we need
      // to move the node
      if (!old_parent_id || *old_parent_id != new_parent_id) {
        // If not, we need to move it to the new parent
        InsertOrUpdate(&resource_to_parent_map_, res_id, new_parent_id);
        // Remove arc corresponding to the old parent/child relationship
        uint64_t* new_parent_node =
            FindOrNull(resource_to_nodeid_map_, new_parent_id);
        CHECK_NOTNULL(new_parent_node);
        LOG(FATAL) << "Moving resources to new parents not supported yet";
      }
      // Parent is the same as before (and not NULL)
      if (old_parent_id) {
        uint64_t* old_parent_node =
            FindOrNull(resource_to_nodeid_map_, *old_parent_id);
        CHECK_NOTNULL(old_parent_node);
        // TODO(malte): Is there anything we need to do here?
      }
    }
    // Check if any children need adding
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
         child_iter = rtnd_ptr->mutable_children()->pointer_begin();
         child_iter != rtnd_ptr->mutable_children()->pointer_end();
         ++child_iter) {
      uint64_t* child_node =
        FindOrNull(resource_to_nodeid_map_,
                   ResourceIDFromString((*child_iter)->resource_desc().uuid()));
      if (!child_node)
        AddResourceTopology(*child_iter);
    }
  } else {
    // It does not already exist, so add it.
    VLOG(1) << "Adding new resource " << res_id << " to flow graph.";
    // N.B.: We need to ensure we hook in at the right place here by setting the
    // parent ID appropriately if it is not already.
    AddResourceNode(rtnd_ptr);
  }
}

void FlowGraph::UpdateResourceTopology(
    ResourceTopologyNodeDescriptor* resource_tree) {
  // N.B.: This only considers ADDITION of resources currently; if resources
  // are removed from the topology (e.g. due to a failure), they won't
  // disappear via this method.
  BFSTraverseResourceProtobufTreeReturnRTND(
      resource_tree,
      boost::bind(&FlowGraph::UpdateResourceNode, this, _1));
  uint32_t new_num_leaves = 0;
  for (unordered_map<uint64_t, FlowGraphArc*>::const_iterator it =
       cluster_agg_node_->outgoing_arc_map_.begin();
       it != cluster_agg_node_->outgoing_arc_map_.end();
       ++it) {
    new_num_leaves += it->second->cap_upper_bound_;
  }
  VLOG(2) << "Updated resource topology in flow scheduler. New "
          << "number of schedulable leaves: "
          << (new_num_leaves);
}

void FlowGraph::ResetChanges() {
  for (vector<DIMACSChange*>::iterator it = graph_changes_.begin();
       it != graph_changes_.end(); ) {
    vector<DIMACSChange*>::iterator it_tmp = it;
    ++it;
    delete *it_tmp;
  }
  graph_changes_.clear();
  ids_created_.clear();
}

void FlowGraph::ComputeTopologyStatistics(
    FlowGraphNode* node,
    boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> gather) {
  queue<FlowGraphNode*> to_visit;
  set<FlowGraphNode*> processed;
  to_visit.push(node);
  processed.insert(node);
  while (!to_visit.empty()) {
    FlowGraphNode* cur_node = to_visit.front();
    to_visit.pop();
    for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
           cur_node->incoming_arc_map_.begin();
         it != cur_node->incoming_arc_map_.end(); ++it) {
      it->second->src_node_ = gather(it->second->src_node_, cur_node);
      if (processed.find(it->second->src_node_) == processed.end()) {
        to_visit.push(it->second->src_node_);
        processed.insert(it->second->src_node_);
      }
    }
  }
}

}  // namespace firmament
