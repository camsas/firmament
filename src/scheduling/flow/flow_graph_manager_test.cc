// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <gtest/gtest.h>

#include "base/common.h"
#include "misc/map-util.h"
#include "misc/wall_time.h"
#include "misc/utils.h"
#include "scheduling/flow/dimacs_add_node.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/dimacs_remove_node.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_manager.h"
#include "scheduling/flow/flow_graph_node.h"
#include "scheduling/flow/trivial_cost_model.h"

DECLARE_string(flow_scheduling_solver);
DECLARE_uint64(num_pref_arcs_task_to_res);

namespace firmament {

class FlowGraphManagerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  FlowGraphManagerTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
    resource_map_ = shared_ptr<ResourceMap_t>(new ResourceMap_t);
    task_map_ = shared_ptr<TaskMap_t>(new TaskMap_t);
    leaf_res_ids_ =
      new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
    tg_ = new TraceGenerator(&wall_time_);
    graph_manager_ = new FlowGraphManager(
        new TrivialCostModel(resource_map_, task_map_, leaf_res_ids_),
        leaf_res_ids_, &wall_time_, tg_, &dimacs_stats_);
  }

  virtual ~FlowGraphManagerTest() {
    // You can do clean-up work that doesn't throw exceptions here.
    delete leaf_res_ids_;
    delete tg_;
    delete graph_manager_;
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests.
  void CreateSimpleResourceTopo(ResourceTopologyNodeDescriptor *rtn_root) {
    string root_id = to_string(GenerateResourceID("test"));
    rtn_root->mutable_resource_desc()->set_uuid(root_id);
    rtn_root->mutable_resource_desc()->set_type(
        ResourceDescriptor::RESOURCE_MACHINE);
    ResourceTopologyNodeDescriptor* rtn_c1 = rtn_root->add_children();
    string c1_uid = to_string(GenerateResourceID("test-c1"));
    rtn_c1->mutable_resource_desc()->set_uuid(c1_uid);
    rtn_c1->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
    rtn_c1->set_parent_id(root_id);
    ResourceTopologyNodeDescriptor* rtn_c2 = rtn_root->add_children();
    string c2_uid = to_string(GenerateResourceID("test-c2"));
    rtn_c2->mutable_resource_desc()->set_uuid(c2_uid);
    rtn_c2->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
    rtn_c2->set_parent_id(root_id);
  }

  ResourceDescriptor* CreateMachine(ResourceTopologyNodeDescriptor* rtnd_ptr,
                                    string machine_name) {
    ResourceID_t res_id = GenerateResourceID(machine_name);
    ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
    rd_ptr->set_uuid(to_string(res_id));
    rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
    return rd_ptr;
  }

  TaskDescriptor* CreateTask(JobDescriptor* jd_ptr, uint64_t job_id_seed) {
    JobID_t job_id = GenerateJobID(job_id_seed);
    jd_ptr->set_uuid(to_string(job_id));
    jd_ptr->set_name(to_string(job_id));
    TaskDescriptor* td_ptr = jd_ptr->mutable_root_task();
    td_ptr->set_uid(GenerateRootTaskID(*jd_ptr));
    td_ptr->set_job_id(jd_ptr->uuid());
    return td_ptr;
  }

  shared_ptr<ResourceMap_t> resource_map_;
  shared_ptr<TaskMap_t> task_map_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  DIMACSChangeStats dimacs_stats_;
  WallTime wall_time_;
  TraceGenerator* tg_;
  FlowGraphManager* graph_manager_;
};

TEST_F(FlowGraphManagerTest, AddEquivClassNode) {
  uint64_t num_nodes =
    graph_manager_->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager_->graph_change_manager_->flow_graph().NumArcs();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager_->AddEquivClassNode(ec);
  EXPECT_EQ(ec_node->ec_id_, ec);
  EXPECT_EQ(FindPtrOrNull(graph_manager_->tec_to_node_map_, ec), ec_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager_->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager_->graph_change_manager_->flow_graph().NumArcs());
  // Fail if a node for ec has already been added.
  EXPECT_DEATH(graph_manager_->AddEquivClassNode(ec), "");
}

TEST_F(FlowGraphManagerTest, AddResourceNode) {
  uint64_t num_nodes =
    graph_manager_->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager_->graph_change_manager_->flow_graph().NumArcs();
  ResourceID_t res_id = GenerateResourceID("test");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  FlowGraphNode* res_node = graph_manager_->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  EXPECT_EQ(res_node->resource_id_, res_id);
  EXPECT_EQ(res_node->rd_ptr_, rd_ptr);
  EXPECT_EQ(FindPtrOrNull(graph_manager_->resource_to_node_map_,
                          res_node->resource_id_),
            res_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager_->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager_->graph_change_manager_->flow_graph().NumArcs());
  // Fail if the resource was already added.
  EXPECT_DEATH(graph_manager_->AddResourceNode(rd_ptr), "");
  ResourceTopologyNodeDescriptor* rtnd_child_ptr = rtnd.add_children();
  ResourceDescriptor* rd_child_ptr = rtnd_child_ptr->mutable_resource_desc();
  ResourceID_t res_child_id = GenerateResourceID("test-child");
  rd_child_ptr->set_uuid(to_string(res_child_id));
  rd_child_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* res_child_node = graph_manager_->AddResourceNode(rd_child_ptr);
  CHECK_NOTNULL(res_child_node);
  EXPECT_EQ(res_child_node->resource_id_, res_child_id);
  EXPECT_EQ(res_child_node->rd_ptr_, rd_child_ptr);
  EXPECT_EQ(FindPtrOrNull(graph_manager_->resource_to_node_map_,
                          res_child_node->resource_id_),
            res_child_node);
  EXPECT_NE(graph_manager_->leaf_nodes_.find(res_child_node->id_),
            graph_manager_->leaf_nodes_.end());
  EXPECT_NE(graph_manager_->leaf_res_ids_->find(res_child_id),
            graph_manager_->leaf_res_ids_->end());
  EXPECT_EQ(num_nodes + 2,
            graph_manager_->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager_->graph_change_manager_->flow_graph().NumArcs());
}

TEST_F(FlowGraphManagerTest, AddTaskNode) {
  uint64_t num_nodes =
    graph_manager_->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager_->graph_change_manager_->flow_graph().NumArcs();
  int64_t previous_sink_excess = graph_manager_->sink_node_->excess_;
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager_->AddTaskNode(job_id, td_ptr);
  CHECK_NOTNULL(task_node->td_ptr_);
  EXPECT_FALSE(task_node->job_id_.is_nil());
  EXPECT_EQ(task_node->excess_, 1);
  EXPECT_EQ(previous_sink_excess - 1, graph_manager_->sink_node_->excess_);
  EXPECT_EQ(FindPtrOrNull(graph_manager_->task_to_node_map_, td_ptr->uid()),
            task_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager_->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager_->graph_change_manager_->flow_graph().NumArcs());
  // Fail if the task was already added.
  EXPECT_DEATH(graph_manager_->AddTaskNode(job_id, td_ptr), "");
}

TEST_F(FlowGraphManagerTest, AddUnscheduledAggNode) {
  uint64_t num_nodes =
    graph_manager_->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager_->graph_change_manager_->flow_graph().NumArcs();
  JobID_t job_id = GenerateJobID(42);
  FlowGraphNode* unsched_agg_node =
    graph_manager_->AddUnscheduledAggNode(job_id);
  EXPECT_EQ(FindPtrOrNull(graph_manager_->job_unsched_to_node_, job_id),
            unsched_agg_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager_->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager_->graph_change_manager_->flow_graph().NumArcs());
  // Fail if we already added an unscheduled agg for job_id.
  EXPECT_DEATH(graph_manager_->AddUnscheduledAggNode(job_id), "");
}

TEST_F(FlowGraphManagerTest, AddResourceTopologyDFS) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, CapacityBetweenECNodes) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, PinTaskToNode) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, RemoveEquivClassNode) {
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager_->AddEquivClassNode(ec);
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  EXPECT_EQ(graph_manager_->tec_to_node_map_.size(), 1);
  EXPECT_EQ(0, flow_graph.unused_ids_.size());
  graph_manager_->RemoveEquivClassNode(ec_node);
  EXPECT_EQ(graph_manager_->tec_to_node_map_.size(), 0);
  EXPECT_EQ(1, flow_graph.unused_ids_.size());
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_DEATH(graph_manager_->RemoveEquivClassNode(NULL), "");
}

TEST_F(FlowGraphManagerTest, RemoveInvalidECPrefArcs) {
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager_->AddEquivClassNode(ec);
  CHECK_NOTNULL(ec_node);
  EquivClass_t ec_child1 = 43;
  FlowGraphNode* ec_child1_node = graph_manager_->AddEquivClassNode(ec_child1);
  CHECK_NOTNULL(ec_child1_node);
  EquivClass_t ec_child2 = 44;
  FlowGraphNode* ec_child2_node = graph_manager_->AddEquivClassNode(ec_child2);
  CHECK_NOTNULL(ec_child2_node);
  // Add a resource node.
  ResourceID_t res_id = GenerateResourceID("test");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  FlowGraphNode* res_node = graph_manager_->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  EXPECT_EQ(num_arcs, 0);
  // Add preference arcs from EC to its EC children and to the resource.
  graph_manager_->graph_change_manager_->AddArc(
      ec_node, ec_child1_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_EQUIV_CLASS, "test");
  graph_manager_->graph_change_manager_->AddArc(
      ec_node, ec_child2_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_EQUIV_CLASS, "test");
  FlowGraphArc* arc_to_res = graph_manager_->graph_change_manager_->AddArc(
      ec_node, res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_EQUIV_CLASS_TO_RES, "test");
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 3);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 3);
  vector<EquivClass_t> pref_ecs;
  pref_ecs.push_back(ec_child1_node->ec_id_);
  graph_manager_->RemoveInvalidECPrefArcs(*ec_node, pref_ecs,
                                          DEL_ARC_BETWEEN_EQUIV_CLASS);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 2);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 2);
  vector<EquivClass_t> no_pref_ecs;
  graph_manager_->RemoveInvalidECPrefArcs(*ec_node, no_pref_ecs,
                                          DEL_ARC_BETWEEN_EQUIV_CLASS);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 1);
  EXPECT_EQ(ec_node->outgoing_arc_map_.begin()->second, arc_to_res);
}

TEST_F(FlowGraphManagerTest, RemoveInvalidPrefResArcs) {
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager_->AddEquivClassNode(ec);
  CHECK_NOTNULL(ec_node);
  EquivClass_t ec_child = 43;
  FlowGraphNode* ec_child_node = graph_manager_->AddEquivClassNode(ec_child);
  CHECK_NOTNULL(ec_child_node);
  // Add the resource nodes.
  ResourceTopologyNodeDescriptor machine1_rtnd;
  ResourceDescriptor* machine1_rd_ptr =
    CreateMachine(&machine1_rtnd, "machine1");
  FlowGraphNode* machine1_res_node =
    graph_manager_->AddResourceNode(machine1_rd_ptr);
  CHECK_NOTNULL(machine1_res_node);
  ResourceTopologyNodeDescriptor machine2_rtnd;
  ResourceDescriptor* machine2_rd_ptr =
    CreateMachine(&machine2_rtnd, "machine2");
  FlowGraphNode* machine2_res_node =
    graph_manager_->AddResourceNode(machine2_rd_ptr);
  CHECK_NOTNULL(machine2_res_node);
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  EXPECT_EQ(num_arcs, 0);
  // Add preference arcs from EC to the machines and to another ec.
  graph_manager_->graph_change_manager_->AddArc(
      ec_node, machine1_res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_EQUIV_CLASS_TO_RES, "test");
  FlowGraphArc* arc_to_ec = graph_manager_->graph_change_manager_->AddArc(
      ec_node, ec_child_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_EQUIV_CLASS, "test");
  graph_manager_->graph_change_manager_->AddArc(
      ec_node, machine2_res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_EQUIV_CLASS_TO_RES, "test");
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 3);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 3);

  vector<ResourceID_t> pref_res;
  pref_res.push_back(machine2_res_node->resource_id_);
  graph_manager_->RemoveInvalidPrefResArcs(*ec_node, pref_res,
                                           DEL_ARC_EQUIV_CLASS_TO_RES);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 2);
  vector<ResourceID_t> no_pref_res;
  graph_manager_->RemoveInvalidPrefResArcs(*ec_node, no_pref_res,
                                           DEL_ARC_EQUIV_CLASS_TO_RES);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 1);
  EXPECT_EQ(ec_node->outgoing_arc_map_.begin()->second, arc_to_ec);
}

TEST_F(FlowGraphManagerTest, RemoveResourceNode) {
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  ResourceTopologyNodeDescriptor machine_rtnd;
  ResourceDescriptor* rd_ptr = CreateMachine(&machine_rtnd, "test-machine");
  FlowGraphNode* res_node = graph_manager_->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_EQ(FindPtrOrNull(graph_manager_->resource_to_node_map_,
                          res_node->resource_id_),
            res_node);
  EXPECT_EQ(0, flow_graph.unused_ids_.size());
  graph_manager_->RemoveResourceNode(res_node);
  EXPECT_EQ(1, flow_graph.unused_ids_.size());
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  CHECK(FindPtrOrNull(graph_manager_->resource_to_node_map_, res_id) == NULL);
  EXPECT_DEATH(graph_manager_->RemoveResourceNode(NULL), "");
}

TEST_F(FlowGraphManagerTest, RemoveTaskNode) {
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager_->AddTaskNode(job_id, td_ptr);
  CHECK_NOTNULL(task_node);
  EXPECT_EQ(flow_graph.unused_ids_.size(), 0);
  EXPECT_EQ(graph_manager_->sink_node_->excess_, -1);
  graph_manager_->RemoveTaskNode(task_node);
  EXPECT_EQ(graph_manager_->sink_node_->excess_, 0);
  EXPECT_EQ(flow_graph.unused_ids_.size(), 1);
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_DEATH(graph_manager_->RemoveTaskNode(NULL), "");
}

TEST_F(FlowGraphManagerTest, RemoveUnscheduledAggNode) {
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  JobID_t job_id = GenerateJobID(42);
  FlowGraphNode* unsched_agg_node =
    graph_manager_->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_EQ(flow_graph.unused_ids_.size(), 0);
  graph_manager_->RemoveUnscheduledAggNode(job_id);
  EXPECT_EQ(flow_graph.unused_ids_.size(), 1);
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  CHECK(FindPtrOrNull(graph_manager_->job_unsched_to_node_, job_id) == NULL);
  // Fail if we already added an unscheduled agg for job_id.
  job_id = GenerateJobID(47);
  EXPECT_DEATH(graph_manager_->RemoveUnscheduledAggNode(job_id), "");
}

TEST_F(FlowGraphManagerTest, TraverseAndRemoveTopology) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateArcsForScheduledTask) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateChildrenTasks) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateEquivClassNode) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateEquivToEquivArcs) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateEquivToResArcs) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateFlowGraph) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateResourceNode) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateResourceStatsUpToRoot) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateResOutgoingArcs) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateResToSinkArc) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateRunningTaskNode) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateRunningTaskToUnscheduledAggArc) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateTaskNode) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateTaskToEquivArcs) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateTaskToResArcs) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateTaskToUnscheduledAggArc) {
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  // Case when we don't have an unscheduled aggregator node for the job.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager_->AddTaskNode(job_id, td_ptr);
  graph_manager_->UpdateTaskToUnscheduledAggArc(task_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 2);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  FlowGraphArc* task_to_unsched_arc =
    task_node->outgoing_arc_map_.begin()->second;
  EXPECT_EQ(task_to_unsched_arc->cap_lower_bound_, 0);
  EXPECT_EQ(task_to_unsched_arc->cap_upper_bound_, 1);

  // Case when we have an unscheduled aggregator for the job.
  JobDescriptor test_job2;
  td_ptr = CreateTask(&test_job2, 47);
  job_id = JobIDFromString(td_ptr->job_id());
  task_node = graph_manager_->AddTaskNode(job_id, td_ptr);
  FlowGraphNode* unsched_agg_node =
    graph_manager_->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 4);
  graph_manager_->UpdateTaskToUnscheduledAggArc(task_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 4);
  EXPECT_EQ(flow_graph.NumArcs(), 2);
  task_to_unsched_arc = task_node->outgoing_arc_map_.begin()->second;
  EXPECT_EQ(task_to_unsched_arc->cap_lower_bound_, 0);
  EXPECT_EQ(task_to_unsched_arc->cap_upper_bound_, 1);

  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, UpdateUnscheduledAggNode) {
  const FlowGraph& flow_graph =
    graph_manager_->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  EXPECT_DEATH(graph_manager_->UpdateUnscheduledAggNode(NULL, 0), "");
  JobID_t job_id = GenerateJobID(42);
  FlowGraphNode* unsched_agg_node =
    graph_manager_->AddUnscheduledAggNode(job_id);
  EXPECT_EQ(FindPtrOrNull(graph_manager_->job_unsched_to_node_, job_id),
            unsched_agg_node);
  EXPECT_EQ(flow_graph.NumArcs(), 0);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 1);
  EXPECT_DEATH(graph_manager_->UpdateUnscheduledAggNode(unsched_agg_node, 0),
               "");
  // This call to update ends up adding the arc.
  graph_manager_->UpdateUnscheduledAggNode(unsched_agg_node, 1);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(graph_manager_->sink_node_->incoming_arc_map_.size(), 1);
  FlowGraphArc* arc_to_unsched =
    unsched_agg_node->outgoing_arc_map_.begin()->second;
  EXPECT_EQ(arc_to_unsched->src_node_, unsched_agg_node);
  EXPECT_EQ(unsched_agg_node->outgoing_arc_map_.size(), 1);
  EXPECT_EQ(arc_to_unsched->dst_node_, graph_manager_->sink_node_);
  EXPECT_EQ(arc_to_unsched->cap_upper_bound_, 1);
  // Update the arc again.
  graph_manager_->UpdateUnscheduledAggNode(unsched_agg_node, 1);
  EXPECT_EQ(graph_manager_->sink_node_->incoming_arc_map_.size(), 1);
  EXPECT_EQ(unsched_agg_node->outgoing_arc_map_.size(), 1);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(arc_to_unsched->cap_upper_bound_, 2);
  // Update the arc so that its capacity is zero.
  graph_manager_->UpdateUnscheduledAggNode(unsched_agg_node, -2);
  EXPECT_EQ(graph_manager_->sink_node_->incoming_arc_map_.size(), 1);
  EXPECT_EQ(unsched_agg_node->outgoing_arc_map_.size(), 1);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(arc_to_unsched->cap_upper_bound_, 0);
}

// TEST_F(FlowGraphManagerTest, AddOrUpdateJobNodes) {
//   ResourceTopologyNodeDescriptor rtn_root;
//   CreateSimpleResourceTopo(&rtn_root);
//   graph_manager_->AddResourceTopology(&rtn_root);
//   // Now generate a job and add it
//   JobID_t jid = GenerateJobID();
//   JobDescriptor test_job;
//   test_job.set_uuid(to_string(jid));
//   TaskDescriptor* rt = test_job.mutable_root_task();
//   rt->set_state(TaskDescriptor::RUNNABLE);
//   rt->set_uid(GenerateRootTaskID(test_job));
//   rt->set_job_id(test_job.uuid());
//   CHECK(InsertIfNotPresent(task_map_.get(), rt->uid(), rt));
//   uint32_t num_changes = graph_manager_->graph_changes_.size();
//   vector<JobDescriptor*> jd_ptr_vect;
//   jd_ptr_vect.push_back(&test_job);
//   graph_manager_->AddOrUpdateJobNodes(jd_ptr_vect);
//   // This should add one new node for the agg, one arc change from agg to
//   // sink, one new node for the root task, one new node for the
//   // equivalence class and one new node for the cluster equiv class.
//   CHECK_EQ(graph_manager_->graph_changes_.size(), num_changes + 6);
//   DIMACSAddNode* unsched_agg =
//     static_cast<DIMACSAddNode*>(graph_manager_->graph_changes_[num_changes]);
//   DIMACSChangeArc* arc_to_sink =
//     static_cast<DIMACSChangeArc*>(
//       graph_manager_->graph_changes_[num_changes + 1]);
//   DIMACSAddNode* root_task =
//     static_cast<DIMACSAddNode*>(graph_manager_->graph_changes_[num_changes + 2]);
//   DIMACSAddNode* equiv_class =
//     static_cast<DIMACSAddNode*>(graph_manager_->graph_changes_[num_changes + 3]);

//   CHECK_EQ(arc_to_sink->cap_upper_bound_, 1);
//   // Arc to sink.
//   CHECK_EQ(unsched_agg->arc_additions_.size(), 1);
//   // Arc to unscheduled aggregator and to topology.
//   CHECK_EQ(root_task->arc_additions_.size(), 2);
//   // The equiv class won't have any arcs
//   CHECK_EQ(equiv_class->arc_additions_.size(), 0);
// }

// TEST_F(FlowGraphManagerTest, AddOrUpdateResourceNode) {
//   ResourceTopologyNodeDescriptor rtn_root;
//   CreateSimpleResourceTopo(&rtn_root);
//   graph_manager_->AddResourceTopology(&rtn_root);
//   string root_id = rtn_root.mutable_resource_desc()->uuid();
//   uint32_t num_changes = graph_manager_->graph_changes_.size();
//   // Add a new core node.
//   ResourceTopologyNodeDescriptor* core_node = rtn_root.add_children();
//   string core_uid = to_string(GenerateResourceID());
//   core_node->mutable_resource_desc()->set_uuid(core_uid);
//   core_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
//   core_node->set_parent_id(root_id);
//   graph_manager_->AddOrUpdateResourceNode(core_node);
//   // Check if the number of changes is correct. One change for adding the new
//   // node, another change for adding an arc from the node to the sink and
//   // another change for update the arc from the parent to the core.
//   CHECK_EQ(graph_manager_->graph_changes_.size(), num_changes + 3);
// }

// TEST_F(FlowGraphManagerTest, DeleteReAddResourceTopoAndJob) {
//   // We have to set the solver not to be cs2 so that we actually adjust
//   // NumNodes when we delete a node.
//   FLAGS_flow_scheduling_solver = "flowlessly";
//   ResourceTopologyNodeDescriptor rtn_root;
//   CreateSimpleResourceTopo(&rtn_root);
//   uint64_t num_nodes =
//     graph_manager_->graph_change_manager_->flow_graph().NumNodes();
//   uint64_t num_arcs =
//     graph_manager_->graph_change_manager_->flow_graph().NumArcs();
//   graph_manager_->AddResourceTopology(&rtn_root);
//   // Now generate a job and add it
//   JobID_t jid = GenerateJobID();
//   JobDescriptor test_job;
//   test_job.set_uuid(to_string(jid));
//   TaskDescriptor* rt = test_job.mutable_root_task();
//   rt->set_state(TaskDescriptor::RUNNABLE);
//   rt->set_uid(GenerateRootTaskID(test_job));
//   rt->set_job_id(test_job.uuid());
//   CHECK(InsertIfNotPresent(task_map_.get(), rt->uid(), rt));
//   vector<JobDescriptor*> jd_ptr_vect;
//   jd_ptr_vect.push_back(&test_job);
//   graph_manager_->AddOrUpdateJobNodes(jd_ptr_vect);
//   // Three resource nodes, plus one task, plus unsched aggregator,
//   // plus cluster aggregator EC, plus task EC
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes + 7);
//   // Two internal to topology, two to sink, one from cluster agg EC to
//   // root resource, one from task to cluster agg, one from task to
//   // preferred ec, one from prefered ec to resource, one from task to
//   // unscheduled aggregator, one from unscheduled aggregator to sink
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs + 10);
//   // Job "finishes"
//   rt->set_state(TaskDescriptor::COMPLETED);
//   graph_manager_->JobCompleted(jid);
//   // Three resource nodes (cluster agg EC has been deleted, as it no longer
//   // has any outgoing arcs).
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes + 3);
//   // Two internal to topology, two to sink
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs + 4);
//   for (auto it = rtn_root.mutable_children()->begin();
//        it != rtn_root.mutable_children()->end();
//        ++it) {
//     FlowGraphNode* n =
//       graph_manager_->NodeForResourceID(
//           ResourceIDFromString(it->resource_desc().uuid()));
//     graph_manager_->DeleteResourceNode(n);
//   }
//   FlowGraphNode* n =
//     graph_manager_->NodeForResourceID(
//         ResourceIDFromString(rtn_root.resource_desc().uuid()));
//   graph_manager_->DeleteResourceNode(n);
//   // Everything should be as in the beginning
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes);
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs);
//   graph_manager_->AddResourceTopology(&rtn_root);
//   // Three resource nodes, plus cluster aggregator EC
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes + 4);
//   // Two internal to topology, two to sink, one from cluster agg EC to
//   // root resource
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs + 5);
// }

// TEST_F(FlowGraphManagerTest, DeleteResourceNode) {
//   ResourceTopologyNodeDescriptor rtn_root;
//   CreateSimpleResourceTopo(&rtn_root);
//   graph_manager_->AddResourceTopology(&rtn_root);
//   string root_id = rtn_root.mutable_resource_desc()->uuid();
//   // Add a new core node.
//   ResourceTopologyNodeDescriptor* core_node = rtn_root.add_children();
//   string core_uid = to_string(GenerateResourceID());
//   core_node->mutable_resource_desc()->set_uuid(core_uid);
//   core_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
//   core_node->set_parent_id(root_id);
//   graph_manager_->AddOrUpdateResourceNode(core_node);
//   uint32_t num_changes = graph_manager_->graph_changes_.size();
//   // Delete the core node.
//   graph_manager_->DeleteResourceNode(graph_manager_->NodeForResourceID(
//        ResourceIDFromString(core_node->resource_desc().uuid())));
//   CHECK_EQ(graph_manager_->graph_changes_.size(), num_changes + 1);
// }

// TEST_F(FlowGraphManagerTest, DeleteReAddResourceTopo) {
//   // We have to set the solver not to be cs2 so that we actually adjust
//   // NumNodes when we delete a node.
//   ResourceTopologyNodeDescriptor rtn_root;
//   CreateSimpleResourceTopo(&rtn_root);
//   uint64_t num_nodes =
//     graph_manager_->graph_change_manager_->flow_graph().NumNodes();
//   uint64_t num_arcs =
//     graph_manager_->graph_change_manager_->flow_graph().NumArcs();
//   graph_manager_->AddResourceTopology(&rtn_root);
//   // Three resource nodes, plus cluster aggregator EC
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes + 4);
//   // Two internal to topology, two to sink, one from cluster agg EC to
//   // root resource
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs + 5);
//   for (auto it = rtn_root.mutable_children()->begin();
//        it != rtn_root.mutable_children()->end();
//        ++it) {
//     FlowGraphNode* n =
//       graph_manager_->NodeForResourceID(
//           ResourceIDFromString(it->resource_desc().uuid()));
//     graph_manager_->DeleteResourceNode(n);
//   }
//   // Still have cluster agg and topology root here
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes + 2);
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs + 1);
//   FlowGraphNode* n =
//     graph_manager_->NodeForResourceID(
//         ResourceIDFromString(rtn_root.resource_desc().uuid()));
//   graph_manager_->DeleteResourceNode(n);
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes);
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs);
//   graph_manager_->AddResourceTopology(&rtn_root);
//   // Three resource nodes, plus cluster aggregator EC
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumNodes(),
//            num_nodes + 4);
//   // Two internal to topology, two to sink, one from cluster agg EC to
//   // root resource
//   CHECK_EQ(graph_manager_->graph_change_manager_->flow_graph().NumArcs(),
//            num_arcs + 5);
// }

// // Add simple resource topology to graph
// TEST_F(FlowGraphManagerTest, SimpleResourceTopo) {
//   ResourceTopologyNodeDescriptor rtn_root;
//   CreateSimpleResourceTopo(&rtn_root);
//   graph_manager_->AddResourceTopology(&rtn_root);
// }

// // Test correct increment/decrement of unscheduled aggregator capacities.
// TEST_F(FlowGraphManagerTest, UnschedAggCapacityAdjustment) {
//   ResourceTopologyNodeDescriptor rtn_root;
//   CreateSimpleResourceTopo(&rtn_root);
//   graph_manager_->AddResourceTopology(&rtn_root);
//   // Now generate a job and add it
//   JobID_t jid = GenerateJobID();
//   JobDescriptor test_job;
//   test_job.set_uuid(to_string(jid));
//   TaskDescriptor* rt = test_job.mutable_root_task();
//   rt->set_state(TaskDescriptor::RUNNABLE);
//   rt->set_uid(GenerateRootTaskID(test_job));
//   rt->set_job_id(test_job.uuid());
//   CHECK(InsertIfNotPresent(task_map_.get(), rt->uid(), rt));
//   vector<JobDescriptor*> jd_ptr_vect;
//   jd_ptr_vect.push_back(&test_job);
//   graph_manager_->AddOrUpdateJobNodes(jd_ptr_vect);
//   // Grab the unscheduled aggregator for the new job
//   FlowGraphNode* unsched_agg_node =
//     FindPtrOrNull(graph_manager_->job_unsched_to_node_, jid);
//   CHECK_NOTNULL(unsched_agg_node);
//   FlowGraphArc* unsched_agg_to_sink_arc = FindPtrOrNull(
//       unsched_agg_node->outgoing_arc_map_,
//       graph_manager_->sink_node_->id_);
//   CHECK_NOTNULL(unsched_agg_to_sink_arc);
//   CHECK_EQ(unsched_agg_to_sink_arc->cap_upper_bound_, 1);
//   // Now pin the root task to the first resource leaf
//   FlowGraphNode* root_task_node =
//       FindPtrOrNull(graph_manager_->task_to_node_map_,
//                     test_job.root_task().uid());
//   CHECK_NOTNULL(root_task_node);
//   FlowGraphNode* resource_node =
//       FindPtrOrNull(graph_manager_->resource_to_node_map_, ResourceIDFromString(
//           rtn_root.mutable_children()->Get(0).resource_desc().uuid()));
//   CHECK_NOTNULL(resource_node);
//   graph_manager_->PinTaskToNode(root_task_node, resource_node);
//   // The unscheduled aggregator's outbound capacity should have been
//   // decremented.
//   CHECK_EQ(unsched_agg_to_sink_arc->cap_upper_bound_, 0);
// }

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
