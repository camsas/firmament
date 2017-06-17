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

#include <gmock/gmock.h>
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
#include "scheduling/flow/mock_cost_model.h"
#include "scheduling/flow/trivial_cost_model.h"
#include "scheduling/flow/void_cost_model.h"

DECLARE_string(flow_scheduling_solver);
DECLARE_uint64(num_pref_arcs_task_to_res);

using ::testing::_;

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
  }

  virtual ~FlowGraphManagerTest() {
    // You can do clean-up work that doesn't throw exceptions here.
    delete leaf_res_ids_;
    delete tg_;
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
  }

  FlowGraphManager* CreateGraphManagerUsingTrivialCost() {
    return new FlowGraphManager(
        new TrivialCostModel(resource_map_, task_map_, leaf_res_ids_),
        leaf_res_ids_, &wall_time_, tg_, &dimacs_stats_);
  }

  FlowGraphManager* CreateGraphManagerUsingVoidCost() {
    return new FlowGraphManager(
        new VoidCostModel(resource_map_, task_map_),
        leaf_res_ids_, &wall_time_, tg_, &dimacs_stats_);
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
};

TEST_F(FlowGraphManagerTest, AddOrUpdateJobNodes) {
  // TODO(ionel): Implement!
}

TEST_F(FlowGraphManagerTest, AddEquivClassNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  uint64_t num_nodes =
    graph_manager->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager->graph_change_manager_->flow_graph().NumArcs();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  EXPECT_EQ(ec_node->ec_id_, ec);
  EXPECT_EQ(FindPtrOrNull(graph_manager->tec_to_node_map_, ec), ec_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager->graph_change_manager_->flow_graph().NumArcs());
  // Fail if a node for ec has already been added.
  EXPECT_DEATH(graph_manager->AddEquivClassNode(ec), "");
}

TEST_F(FlowGraphManagerTest, AddResourceNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  uint64_t num_nodes =
    graph_manager->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager->graph_change_manager_->flow_graph().NumArcs();
  ResourceID_t res_id = GenerateResourceID("test");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  FlowGraphNode* res_node = graph_manager->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  EXPECT_EQ(res_node->resource_id_, res_id);
  EXPECT_EQ(res_node->rd_ptr_, rd_ptr);
  EXPECT_EQ(FindPtrOrNull(graph_manager->resource_to_node_map_,
                          res_node->resource_id_),
            res_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager->graph_change_manager_->flow_graph().NumArcs());
  // Fail if the resource was already added.
  EXPECT_DEATH(graph_manager->AddResourceNode(rd_ptr), "");
  ResourceTopologyNodeDescriptor* rtnd_child_ptr = rtnd.add_children();
  ResourceDescriptor* rd_child_ptr = rtnd_child_ptr->mutable_resource_desc();
  ResourceID_t res_child_id = GenerateResourceID("test-child");
  rd_child_ptr->set_uuid(to_string(res_child_id));
  rd_child_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* res_child_node = graph_manager->AddResourceNode(rd_child_ptr);
  CHECK_NOTNULL(res_child_node);
  EXPECT_EQ(res_child_node->resource_id_, res_child_id);
  EXPECT_EQ(res_child_node->rd_ptr_, rd_child_ptr);
  EXPECT_EQ(FindPtrOrNull(graph_manager->resource_to_node_map_,
                          res_child_node->resource_id_),
            res_child_node);
  EXPECT_NE(graph_manager->leaf_nodes_.find(res_child_node->id_),
            graph_manager->leaf_nodes_.end());
  EXPECT_NE(graph_manager->leaf_res_ids_->find(res_child_id),
            graph_manager->leaf_res_ids_->end());
  EXPECT_EQ(num_nodes + 2,
            graph_manager->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager->graph_change_manager_->flow_graph().NumArcs());
}

TEST_F(FlowGraphManagerTest, AddTaskNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  uint64_t num_nodes =
    graph_manager->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager->graph_change_manager_->flow_graph().NumArcs();
  int64_t previous_sink_excess = graph_manager->sink_node_->excess_;
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  CHECK_NOTNULL(task_node->td_ptr_);
  EXPECT_FALSE(task_node->job_id_.is_nil());
  EXPECT_EQ(task_node->excess_, 1);
  EXPECT_EQ(previous_sink_excess - 1, graph_manager->sink_node_->excess_);
  EXPECT_EQ(FindPtrOrNull(graph_manager->task_to_node_map_, td_ptr->uid()),
            task_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager->graph_change_manager_->flow_graph().NumArcs());
  // Fail if the task was already added.
  EXPECT_DEATH(graph_manager->AddTaskNode(job_id, td_ptr), "");
}

TEST_F(FlowGraphManagerTest, AddUnscheduledAggNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  uint64_t num_nodes =
    graph_manager->graph_change_manager_->flow_graph().NumNodes();
  uint64_t num_arcs =
    graph_manager->graph_change_manager_->flow_graph().NumArcs();
  JobID_t job_id = GenerateJobID(42);
  FlowGraphNode* unsched_agg_node =
    graph_manager->AddUnscheduledAggNode(job_id);
  EXPECT_EQ(FindPtrOrNull(graph_manager->job_unsched_to_node_, job_id),
            unsched_agg_node);
  EXPECT_EQ(num_nodes + 1,
            graph_manager->graph_change_manager_->flow_graph().NumNodes());
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs,
            graph_manager->graph_change_manager_->flow_graph().NumArcs());
  // Fail if we already added an unscheduled agg for job_id.
  EXPECT_DEATH(graph_manager->AddUnscheduledAggNode(job_id), "");
}

TEST_F(FlowGraphManagerTest, AddResourceTopologyDFS) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  // Create simple resource topology.
  ResourceTopologyNodeDescriptor rtnd;
  ResourceID_t root_res_id = GenerateResourceID("test");
  rtnd.mutable_resource_desc()->set_uuid(to_string(root_res_id));
  rtnd.mutable_resource_desc()->set_type(
      ResourceDescriptor::RESOURCE_COORDINATOR);
  ResourceTopologyNodeDescriptor* rtn_machine = rtnd.add_children();
  ResourceID_t machine_res_id = GenerateResourceID("machine");
  rtn_machine->mutable_resource_desc()->set_uuid(to_string(machine_res_id));
  rtn_machine->mutable_resource_desc()->set_type(
      ResourceDescriptor::RESOURCE_MACHINE);
  rtn_machine->set_parent_id(to_string(root_res_id));
  ResourceTopologyNodeDescriptor* rtn_core = rtnd.add_children();
  ResourceID_t core_res_id = GenerateResourceID("test-core");
  rtn_core->mutable_resource_desc()->set_uuid(to_string(core_res_id));
  rtn_core->mutable_resource_desc()->set_type(
      ResourceDescriptor::RESOURCE_PU);
  rtn_core->set_parent_id(to_string(root_res_id));

  EXPECT_EQ(flow_graph.NumArcs(), 0);
  EXPECT_CALL(mock_cost_model, AddMachine(_)).Times(1);
  ON_CALL(mock_cost_model, ResourceNodeToResourceNode(_, _))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, ResourceNodeToResourceNode(_, _)).Times(2);
  ON_CALL(mock_cost_model, LeafResourceNodeToSink(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, LeafResourceNodeToSink(_)).Times(1);
  graph_manager->AddResourceTopologyDFS(&rtnd);
  EXPECT_EQ(flow_graph.NumArcs(), 3);
  FlowGraphNode* root_node = graph_manager->NodeForResourceID(root_res_id);
  CHECK_NOTNULL(root_node);
  FlowGraphNode* machine_node =
    graph_manager->NodeForResourceID(machine_res_id);
  CHECK_NOTNULL(machine_node);
  FlowGraphNode* core_node = graph_manager->NodeForResourceID(core_res_id);
  CHECK_NOTNULL(core_node);
  EXPECT_EQ(root_node->outgoing_arc_map_.size(), 2);
  EXPECT_EQ(machine_node->incoming_arc_map_.size(), 1);
  EXPECT_EQ(machine_node->outgoing_arc_map_.size(), 0);
  EXPECT_EQ(core_node->incoming_arc_map_.size(), 1);
  EXPECT_EQ(core_node->outgoing_arc_map_.size(), 1);
  EXPECT_EQ(root_node->rd_ptr_->num_slots_below(), 1);
  EXPECT_EQ(root_node->rd_ptr_->num_running_tasks_below(), 0);
  EXPECT_EQ(core_node->incoming_arc_map_.begin()->second->cap_upper_bound_, 1);
  EXPECT_EQ(machine_node->incoming_arc_map_.begin()->second->cap_upper_bound_,
            1);
}

TEST_F(FlowGraphManagerTest, PinTaskToNode) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  EXPECT_CALL(mock_cost_model, AddTask(_)).Times(1);
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  FlowGraphNode* unsched_agg_node =
    graph_manager->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  // Add arc between task and unsched_agg.
  graph_manager->graph_change_manager_->AddArc(
      task_node, unsched_agg_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_TO_UNSCHED, "test");
  // Add arc between unsched_agg and sink.
  FlowGraphArc* arc_from_unsched = graph_manager->graph_change_manager_->AddArc(
      unsched_agg_node, graph_manager->sink_node_, 0, 1, 1,
      FlowGraphArcType::OTHER, ADD_ARC_FROM_UNSCHED, "test");
  // Add EC.
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  CHECK_NOTNULL(ec_node);
  // Connect the task to the EC node
  graph_manager->graph_change_manager_->AddArc(
      task_node, ec_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_TASK_TO_EQUIV_CLASS, "test");
  // Add PUs.
  ResourceTopologyNodeDescriptor pu1_rtnd;
  ResourceDescriptor* pu1_rd_ptr = CreateMachine(&pu1_rtnd, "pu1");
  pu1_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu1_node = graph_manager->AddResourceNode(pu1_rd_ptr);
  CHECK_NOTNULL(pu1_node);
  ResourceTopologyNodeDescriptor pu2_rtnd;
  ResourceDescriptor* pu2_rd_ptr = CreateMachine(&pu2_rtnd, "pu");
  pu2_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu2_node = graph_manager->AddResourceNode(pu2_rd_ptr);
  CHECK_NOTNULL(pu2_node);
  // Connect the task node to the PUs.
  FlowGraphArc* running_arc = graph_manager->graph_change_manager_->AddArc(
      task_node, pu1_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_TASK_TO_RES, "test");
  graph_manager->graph_change_manager_->AddArc(
      task_node, pu2_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_TASK_TO_RES, "test");
  CHECK_EQ(task_node->outgoing_arc_map_.size(), 4);
  CHECK_EQ(arc_from_unsched->cap_upper_bound_, 1);
  ON_CALL(mock_cost_model, TaskContinuation(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskContinuation(_)).Times(2);
  ON_CALL(mock_cost_model, UnscheduledAggToSink(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, UnscheduledAggToSink(_)).Times(2);
  // The task node has a preference arc to resource node to which we pin the
  // task.
  graph_manager->PinTaskToNode(task_node, pu1_node);
  CHECK_EQ(task_node->outgoing_arc_map_.size(), 1);
  CHECK_EQ(FindPtrOrNull(graph_manager->task_to_running_arc_,
                         task_node->td_ptr_->uid()),
           running_arc);
  CHECK_EQ(running_arc->cost_, 42);
  CHECK_EQ(running_arc->type_, RUNNING);
  CHECK_EQ(arc_from_unsched->cap_upper_bound_, 0);

  graph_manager->task_to_running_arc_.erase(task_node->td_ptr_->uid());
  // The task node does not have a preference arc to resource node to which we
  // pin the task.
  graph_manager->PinTaskToNode(task_node, pu2_node);
  CHECK_EQ(task_node->outgoing_arc_map_.size(), 1);
  FlowGraphArc* new_running_arc = task_node->outgoing_arc_map_.begin()->second;
  CHECK_EQ(new_running_arc->dst_node_->id_, pu2_node->id_);
}

TEST_F(FlowGraphManagerTest, PurgeUnconnectedEquivClassNodes) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  const FlowGraph& flow_graph =
      graph_manager->graph_change_manager_->flow_graph();
  // Create task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  // Add EC.
  EquivClass_t ec1 = 42;
  FlowGraphNode* ec_node1 = graph_manager->AddEquivClassNode(ec1);
  CHECK_NOTNULL(ec_node1);
  // Connect the task to the EC node.
  graph_manager->graph_change_manager_->AddArc(
      task_node, ec_node1, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_TASK_TO_EQUIV_CLASS, "test");
  // Add another EC node, do not connect to any task.
  EquivClass_t ec2 = 43;
  FlowGraphNode* ec_node2 = graph_manager->AddEquivClassNode(ec2);
  CHECK_NOTNULL(ec_node2);
  EXPECT_EQ(graph_manager->tec_to_node_map_.size(), 2);
  EXPECT_EQ(0UL, flow_graph.unused_ids_.size());
  graph_manager->PurgeUnconnectedEquivClassNodes();
  // Verify purging of unconnected EC node.
  EXPECT_EQ(graph_manager->tec_to_node_map_.size(), 1);
  EXPECT_EQ(1UL, flow_graph.unused_ids_.size());
}

TEST_F(FlowGraphManagerTest, RemoveEquivClassNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  EXPECT_EQ(graph_manager->tec_to_node_map_.size(), 1);
  EXPECT_EQ(0, flow_graph.unused_ids_.size());
  graph_manager->RemoveEquivClassNode(ec_node);
  EXPECT_EQ(graph_manager->tec_to_node_map_.size(), 0);
  EXPECT_EQ(1, flow_graph.unused_ids_.size());
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_DEATH(graph_manager->RemoveEquivClassNode(NULL), "");
}

TEST_F(FlowGraphManagerTest, RemoveInvalidECPrefArcs) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  CHECK_NOTNULL(ec_node);
  EquivClass_t ec_child1 = 43;
  FlowGraphNode* ec_child1_node = graph_manager->AddEquivClassNode(ec_child1);
  CHECK_NOTNULL(ec_child1_node);
  EquivClass_t ec_child2 = 44;
  FlowGraphNode* ec_child2_node = graph_manager->AddEquivClassNode(ec_child2);
  CHECK_NOTNULL(ec_child2_node);
  // Add a resource node.
  ResourceID_t res_id = GenerateResourceID("test");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  FlowGraphNode* res_node = graph_manager->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  EXPECT_EQ(num_arcs, 0);
  // Add preference arcs from EC to its EC children and to the resource.
  graph_manager->graph_change_manager_->AddArc(
      ec_node, ec_child1_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_EQUIV_CLASS, "test");
  graph_manager->graph_change_manager_->AddArc(
      ec_node, ec_child2_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_EQUIV_CLASS, "test");
  FlowGraphArc* arc_to_res = graph_manager->graph_change_manager_->AddArc(
      ec_node, res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_EQUIV_CLASS_TO_RES, "test");
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 3);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 3);
  vector<EquivClass_t> pref_ecs;
  pref_ecs.push_back(ec_child1_node->ec_id_);
  graph_manager->RemoveInvalidECPrefArcs(*ec_node, pref_ecs,
                                          DEL_ARC_BETWEEN_EQUIV_CLASS);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 2);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 2);
  vector<EquivClass_t> no_pref_ecs;
  graph_manager->RemoveInvalidECPrefArcs(*ec_node, no_pref_ecs,
                                          DEL_ARC_BETWEEN_EQUIV_CLASS);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 1);
  EXPECT_EQ(ec_node->outgoing_arc_map_.begin()->second, arc_to_res);
}

TEST_F(FlowGraphManagerTest, RemoveInvalidPrefResArcs) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  CHECK_NOTNULL(ec_node);
  EquivClass_t ec_child = 43;
  FlowGraphNode* ec_child_node = graph_manager->AddEquivClassNode(ec_child);
  CHECK_NOTNULL(ec_child_node);
  // Add the resource nodes.
  ResourceTopologyNodeDescriptor machine1_rtnd;
  ResourceDescriptor* machine1_rd_ptr =
    CreateMachine(&machine1_rtnd, "machine1");
  FlowGraphNode* machine1_res_node =
    graph_manager->AddResourceNode(machine1_rd_ptr);
  CHECK_NOTNULL(machine1_res_node);
  ResourceTopologyNodeDescriptor machine2_rtnd;
  ResourceDescriptor* machine2_rd_ptr =
    CreateMachine(&machine2_rtnd, "machine2");
  FlowGraphNode* machine2_res_node =
    graph_manager->AddResourceNode(machine2_rd_ptr);
  CHECK_NOTNULL(machine2_res_node);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  EXPECT_EQ(num_arcs, 0);
  // Add preference arcs from EC to the machines and to another ec.
  graph_manager->graph_change_manager_->AddArc(
      ec_node, machine1_res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_EQUIV_CLASS_TO_RES, "test");
  FlowGraphArc* arc_to_ec = graph_manager->graph_change_manager_->AddArc(
      ec_node, ec_child_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_EQUIV_CLASS, "test");
  graph_manager->graph_change_manager_->AddArc(
      ec_node, machine2_res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_EQUIV_CLASS_TO_RES, "test");
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 3);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 3);

  vector<ResourceID_t> pref_res;
  pref_res.push_back(machine2_res_node->resource_id_);
  graph_manager->RemoveInvalidPrefResArcs(*ec_node, pref_res,
                                           DEL_ARC_EQUIV_CLASS_TO_RES);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 2);
  vector<ResourceID_t> no_pref_res;
  graph_manager->RemoveInvalidPrefResArcs(*ec_node, no_pref_res,
                                           DEL_ARC_EQUIV_CLASS_TO_RES);
  EXPECT_EQ(flow_graph.NumArcs(), num_arcs + 1);
  EXPECT_EQ(ec_node->outgoing_arc_map_.begin()->second, arc_to_ec);
}

TEST_F(FlowGraphManagerTest, RemoveResourceNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  ResourceTopologyNodeDescriptor machine_rtnd;
  ResourceDescriptor* rd_ptr = CreateMachine(&machine_rtnd, "test-machine");
  FlowGraphNode* res_node = graph_manager->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_EQ(FindPtrOrNull(graph_manager->resource_to_node_map_,
                          res_node->resource_id_),
            res_node);
  EXPECT_EQ(0, flow_graph.unused_ids_.size());
  graph_manager->RemoveResourceNode(res_node);
  EXPECT_EQ(1, flow_graph.unused_ids_.size());
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  CHECK(FindPtrOrNull(graph_manager->resource_to_node_map_, res_id) == NULL);
  EXPECT_DEATH(graph_manager->RemoveResourceNode(NULL), "");
}

TEST_F(FlowGraphManagerTest, RemoveTaskHelper) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager = new FlowGraphManager(
      &mock_cost_model, leaf_res_ids_, &wall_time_, tg_, &dimacs_stats_);
  const FlowGraph& flow_graph =
      graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_task_nodes = graph_manager->task_to_node_map_.size();
  uint64_t num_running_arcs = graph_manager->task_to_running_arc_.size();
  // Add a task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  EXPECT_CALL(mock_cost_model, AddTask(_)).Times(1);
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  CHECK_NOTNULL(task_node);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  // Add a PU to the graph.
  ResourceTopologyNodeDescriptor pu_rtnd;
  ResourceDescriptor* pu_rd_ptr = CreateMachine(&pu_rtnd, "pu");
  pu_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu_node = graph_manager->AddResourceNode(pu_rd_ptr);
  CHECK_NOTNULL(pu_node);
  // Add running arc between task and PU.
  FlowGraphArc* arc_to_res = graph_manager->graph_change_manager_->AddArc(
      task_node, pu_node, 1, 1, 1, FlowGraphArcType::RUNNING,
      ADD_ARC_RUNNING_TASK, "test");
  CHECK(InsertIfNotPresent(&graph_manager->task_to_running_arc_,
                           task_node->td_ptr_->uid(), arc_to_res));
  EXPECT_EQ(FindPtrOrNull(graph_manager->task_to_running_arc_,
                          task_node->td_ptr_->uid()),
            arc_to_res);
  // PU node should have incoming arc.
  EXPECT_EQ(pu_node->incoming_arc_map_.size(), 1);
  EXPECT_EQ(flow_graph.unused_ids_.size(), 0);
  EXPECT_EQ(graph_manager->task_to_running_arc_.size(), num_running_arcs + 1);
  EXPECT_EQ(graph_manager->task_to_node_map_.size(), num_task_nodes + 1);
  // Remove task and verify.
  EXPECT_CALL(mock_cost_model, RemoveTask(_)).Times(1);
  graph_manager->RemoveTaskHelper(td_ptr->uid());
  EXPECT_EQ(flow_graph.unused_ids_.size(), 1);
  EXPECT_EQ(graph_manager->task_to_running_arc_.size(), num_running_arcs);
  EXPECT_EQ(graph_manager->task_to_node_map_.size(), num_task_nodes);
  // PU node doesn't have any incoming arcs left.
  EXPECT_EQ(pu_node->incoming_arc_map_.size(), 0);
}

TEST_F(FlowGraphManagerTest, RemoveTaskNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  CHECK_NOTNULL(task_node);
  EXPECT_EQ(flow_graph.unused_ids_.size(), 0);
  EXPECT_EQ(graph_manager->sink_node_->excess_, -1);
  graph_manager->RemoveTaskNode(task_node);
  EXPECT_EQ(graph_manager->sink_node_->excess_, 0);
  EXPECT_EQ(flow_graph.unused_ids_.size(), 1);
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_DEATH(graph_manager->RemoveTaskNode(NULL), "");
}

TEST_F(FlowGraphManagerTest, RemoveUnscheduledAggNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_arcs = flow_graph.NumArcs();
  JobID_t job_id = GenerateJobID(42);
  FlowGraphNode* unsched_agg_node =
    graph_manager->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  EXPECT_EQ(flow_graph.unused_ids_.size(), 0);
  graph_manager->RemoveUnscheduledAggNode(job_id);
  EXPECT_EQ(flow_graph.unused_ids_.size(), 1);
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs, flow_graph.NumArcs());
  CHECK(FindPtrOrNull(graph_manager->job_unsched_to_node_, job_id) == NULL);
  // Fail if we already added an unscheduled agg for job_id.
  job_id = GenerateJobID(47);
  EXPECT_DEATH(graph_manager->RemoveUnscheduledAggNode(job_id), "");
}

TEST_F(FlowGraphManagerTest, TaskScheduled) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  uint64_t num_arcs =
    graph_manager->graph_change_manager_->flow_graph().NumArcs();
  // Add a task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  CHECK_NOTNULL(task_node);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  FlowGraphNode* unsched_agg_node =
      graph_manager->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  // Add arc between task and unsched_agg.
  graph_manager->graph_change_manager_->AddArc(task_node, unsched_agg_node, 0,
                                               1, 1, FlowGraphArcType::OTHER,
                                               ADD_ARC_TO_UNSCHED, "test");
  // Add arc between unsched_agg and sink.
  FlowGraphArc* arc_from_unsched = graph_manager->graph_change_manager_->AddArc(
      unsched_agg_node, graph_manager->sink_node_, 0, 1, 1,
      FlowGraphArcType::OTHER, ADD_ARC_FROM_UNSCHED, "test");
  CHECK_NOTNULL(arc_from_unsched);
  // Add a PU to the graph.
  ResourceTopologyNodeDescriptor pu_rtnd;
  ResourceDescriptor* pu_rd_ptr = CreateMachine(&pu_rtnd, "pu");
  pu_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu_node = graph_manager->AddResourceNode(pu_rd_ptr);
  CHECK_NOTNULL(pu_node);
  // Schedule the task to PU.
  graph_manager->TaskScheduled(td_ptr->uid(),
                               ResourceIDFromString(pu_rd_ptr->uuid()));
  FlowGraphArc* arc_to_res =
      graph_manager->graph_change_manager_->mutable_flow_graph()->GetArc(
          task_node, pu_node);
  CHECK_NOTNULL(arc_to_res);
  // Verify task's running arc.
  EXPECT_EQ(arc_to_res->type_, 1);
  EXPECT_EQ(arc_to_res->src_node_, task_node);
  EXPECT_EQ(arc_to_res->dst_node_, pu_node);
  EXPECT_EQ(FindPtrOrNull(graph_manager->task_to_running_arc_, td_ptr->uid()),
            arc_to_res);
  // Check the method doesn't add any new arcs.
  EXPECT_EQ(num_arcs + 2,
            graph_manager->graph_change_manager_->flow_graph().NumArcs());
}

TEST_F(FlowGraphManagerTest, TraverseAndRemoveTopology) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  ResourceTopologyNodeDescriptor machine_rtnd;
  ResourceDescriptor* machine_rd_ptr =
    CreateMachine(&machine_rtnd, "machine");
  FlowGraphNode* machine_node = graph_manager->AddResourceNode(machine_rd_ptr);
  CHECK_NOTNULL(machine_node);
  ResourceTopologyNodeDescriptor pu1_rtnd;
  ResourceDescriptor* pu1_rd_ptr = CreateMachine(&pu1_rtnd, "pu1");
  pu1_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu1_node = graph_manager->AddResourceNode(pu1_rd_ptr);
  CHECK_NOTNULL(pu1_node);
  ResourceTopologyNodeDescriptor pu2_rtnd;
  ResourceDescriptor* pu2_rd_ptr = CreateMachine(&pu2_rtnd, "pu");
  pu2_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu2_node = graph_manager->AddResourceNode(pu2_rd_ptr);
  CHECK_NOTNULL(pu2_node);
  // Connect the task node to the PUs.
  graph_manager->graph_change_manager_->AddArc(
      machine_node, pu1_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_RES, "test");
  graph_manager->graph_change_manager_->AddArc(
      machine_node, pu2_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_RES, "test");
  CHECK_EQ(flow_graph.NumArcs(), 2);
  CHECK_EQ(flow_graph.unused_ids_.size(), 0);
  set<uint64_t> pus_removed;
  EXPECT_CALL(mock_cost_model, RemoveMachine(_)).Times(1);
  graph_manager->TraverseAndRemoveTopology(machine_node, &pus_removed);
  CHECK_EQ(flow_graph.NumArcs(), 0);
  CHECK_EQ(flow_graph.unused_ids_.size(), 3);
  CHECK_EQ(pus_removed.size(), 2);
}

TEST_F(FlowGraphManagerTest, UpdateAllCostsToUnscheduledAggs) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager = new FlowGraphManager(
      &mock_cost_model, leaf_res_ids_, &wall_time_, tg_, &dimacs_stats_);
  // Add a job.
  JobDescriptor test_job1;
  TaskDescriptor* td_ptr1 = CreateTask(&test_job1, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr1->uid(), td_ptr1);
  JobID_t job_id1 = JobIDFromString(td_ptr1->job_id());
  EXPECT_CALL(mock_cost_model, AddTask(_)).Times(1);
  FlowGraphNode* task_node1 = graph_manager->AddTaskNode(job_id1, td_ptr1);
  FlowGraphNode* unsched_agg_node1 =
      graph_manager->AddUnscheduledAggNode(job_id1);
  CHECK_NOTNULL(unsched_agg_node1);
  // Add arc between task and unsched_agg1.
  FlowGraphArc* arc_task_to_unsched1 =
      graph_manager->graph_change_manager_->AddArc(
          task_node1, unsched_agg_node1, 0, 1, 1, FlowGraphArcType::OTHER,
          ADD_ARC_TO_UNSCHED, "test1");

  // Adding another job.
  JobDescriptor test_job2;
  TaskDescriptor* td_ptr2 = CreateTask(&test_job2, 43);
  InsertIfNotPresent(task_map_.get(), td_ptr2->uid(), td_ptr2);
  JobID_t job_id2 = JobIDFromString(td_ptr2->job_id());
  EXPECT_CALL(mock_cost_model, AddTask(_)).Times(1);
  FlowGraphNode* task_node2 = graph_manager->AddTaskNode(job_id2, td_ptr2);
  FlowGraphNode* unsched_agg_node2 =
      graph_manager->AddUnscheduledAggNode(job_id2);
  CHECK_NOTNULL(unsched_agg_node2);
  // Add arc between task and unsched_agg2.
  FlowGraphArc* arc_task_to_unsched2 =
      graph_manager->graph_change_manager_->AddArc(
          task_node2, unsched_agg_node2, 0, 1, 1, FlowGraphArcType::OTHER,
          ADD_ARC_TO_UNSCHED, "test2");
  // Update all costs to unsched aggs and verify updated costs.
  ON_CALL(mock_cost_model, TaskToUnscheduledAgg(_))
      .WillByDefault(testing::Return(ArcDescriptor(45LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(2);
  graph_manager->UpdateAllCostsToUnscheduledAggs();
  CHECK_EQ(arc_task_to_unsched1->cost_, 45);
  CHECK_EQ(arc_task_to_unsched2->cost_, 45);
}

TEST_F(FlowGraphManagerTest, UpdateArcsForScheduledTask) {
  FLAGS_preemption = true;
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_arcs =
    graph_manager->graph_change_manager_->flow_graph().NumArcs();
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  EXPECT_CALL(mock_cost_model, AddTask(_)).Times(1);
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  FlowGraphNode* unsched_agg_node =
    graph_manager->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  // Add arc between task and unsched_agg.
  FlowGraphArc* arc_task_to_unsched =
    graph_manager->graph_change_manager_->AddArc(
      task_node, unsched_agg_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_TO_UNSCHED, "test");
  // Add a PU to the graph.
  ResourceTopologyNodeDescriptor pu_rtnd;
  ResourceDescriptor* pu_rd_ptr = CreateMachine(&pu_rtnd, "pu");
  pu_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu_node = graph_manager->AddResourceNode(pu_rd_ptr);
  CHECK_NOTNULL(pu_node);
  // Call to update the running arc.
  ON_CALL(mock_cost_model, TaskContinuation(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskContinuation(_)).Times(1);
  // Call to update the arc to unsched.
  ON_CALL(mock_cost_model, TaskPreemption(_))
    .WillByDefault(testing::Return(ArcDescriptor(43LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskPreemption(_)).Times(1);
  graph_manager->UpdateArcsForScheduledTask(task_node, pu_node);
  CHECK_EQ(flow_graph.NumArcs(), num_arcs + 2);
  FlowGraphArc* arc_to_res =
    graph_manager->graph_change_manager_->mutable_flow_graph()->GetArc(
        task_node, pu_node);
  CHECK_EQ(arc_to_res->cost_, 42);
  // The lower bound must be zero because otherwise we would force the task to
  // run.
  CHECK_EQ(arc_to_res->cap_lower_bound_, 0);
  CHECK_EQ(arc_task_to_unsched->cost_, 43);
}

TEST_F(FlowGraphManagerTest, UpdateChildrenTasks) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  uint64_t num_nodes =
    graph_manager->graph_change_manager_->flow_graph().NumNodes();
  JobDescriptor test_job;
  TaskDescriptor* root_td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(root_td_ptr->job_id());
  FlowGraphNode* root_task_node =
    graph_manager->AddTaskNode(job_id, root_td_ptr);
  CHECK_NOTNULL(root_task_node);
  // Add an unscheduled aggregator.
  graph_manager->UpdateTaskToUnscheduledAggArc(root_task_node);
  // Add children tasks.
  TaskDescriptor* failed_child_td_ptr = root_td_ptr->add_spawned();
  failed_child_td_ptr->set_uid(GenerateTaskID(*root_td_ptr));
  failed_child_td_ptr->set_job_id(to_string(job_id));
  failed_child_td_ptr->set_state(TaskDescriptor::FAILED);
  TaskDescriptor* runnable_child_td_ptr = root_td_ptr->add_spawned();
  runnable_child_td_ptr->set_uid(GenerateTaskID(*root_td_ptr));
  runnable_child_td_ptr->set_job_id(to_string(job_id));
  runnable_child_td_ptr->set_state(TaskDescriptor::RUNNABLE);
  CHECK_EQ(flow_graph.NumNodes(), num_nodes + 2);
  graph_manager->UpdateChildrenTasks(root_td_ptr, &node_queue, &marked_nodes);
  // We've created a node for the runnable task.
  CHECK_EQ(flow_graph.NumNodes(), num_nodes + 3);
  // Added both tasks to the queue.
  CHECK_EQ(node_queue.size(), 2);
  // We've only marked the runnable task.
  CHECK_EQ(marked_nodes.size(), 1);
}

TEST_F(FlowGraphManagerTest, UpdateEquivClassNode) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  EXPECT_EQ(ec_node->ec_id_, ec);
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  EXPECT_DEATH(
       graph_manager->UpdateEquivClassNode(NULL, &node_queue, &marked_nodes),
       "");
  EXPECT_CALL(mock_cost_model, GetEquivClassToEquivClassesArcs(_)).Times(1);
  EXPECT_CALL(mock_cost_model, GetOutgoingEquivClassPrefArcs(_)).Times(1);
  graph_manager->UpdateEquivClassNode(ec_node, &node_queue, &marked_nodes);
  CHECK_EQ(flow_graph.NumArcs(), 0);
}

TEST_F(FlowGraphManagerTest, UpdateEquivToEquivArcs) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  EXPECT_EQ(ec_node->ec_id_, ec);
  EquivClass_t ec_child1 = 43;
  FlowGraphNode* ec_child1_node = graph_manager->AddEquivClassNode(ec_child1);
  CHECK_NOTNULL(ec_child1_node);
  EquivClass_t ec_child2 = 44;
  FlowGraphNode* ec_child2_node = graph_manager->AddEquivClassNode(ec_child2);
  CHECK_NOTNULL(ec_child2_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 3);
  EXPECT_EQ(flow_graph.NumArcs(), 0);
  // Both ECs are prefered.
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  vector<EquivClass_t>* pref_ecs = new vector<EquivClass_t>();
  pref_ecs->push_back(ec_child1);
  pref_ecs->push_back(ec_child2);
  ON_CALL(mock_cost_model, GetEquivClassToEquivClassesArcs(_))
    .WillByDefault(testing::Return(pref_ecs));
  EXPECT_CALL(mock_cost_model, GetEquivClassToEquivClassesArcs(_))
    .Times(1);
  ON_CALL(mock_cost_model, EquivClassToEquivClass(_, _))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, EquivClassToEquivClass(_, _)).Times(2);
  graph_manager->UpdateEquivToEquivArcs(ec_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 2);
  // No prefered ECs.
  vector<EquivClass_t>* no_pref_ecs = new vector<EquivClass_t>();
  ON_CALL(mock_cost_model, GetEquivClassToEquivClassesArcs(_))
    .WillByDefault(testing::Return(no_pref_ecs));
  EXPECT_CALL(mock_cost_model, GetEquivClassToEquivClassesArcs(_))
    .Times(1);
  graph_manager->UpdateEquivToEquivArcs(ec_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 0);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 0);
}

TEST_F(FlowGraphManagerTest, UpdateEquivToResArcs) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  EquivClass_t ec = 42;
  FlowGraphNode* ec_node = graph_manager->AddEquivClassNode(ec);
  EXPECT_EQ(ec_node->ec_id_, ec);
  // Add the resource nodes.
  ResourceTopologyNodeDescriptor machine1_rtnd;
  ResourceDescriptor* machine1_rd_ptr =
    CreateMachine(&machine1_rtnd, "machine1");
  FlowGraphNode* machine1_res_node =
    graph_manager->AddResourceNode(machine1_rd_ptr);
  CHECK_NOTNULL(machine1_res_node);
  ResourceTopologyNodeDescriptor machine2_rtnd;
  ResourceDescriptor* machine2_rd_ptr =
    CreateMachine(&machine2_rtnd, "machine2");
  FlowGraphNode* machine2_res_node =
    graph_manager->AddResourceNode(machine2_rd_ptr);
  CHECK_NOTNULL(machine2_res_node);
  // Both machines are prefered.
  vector<ResourceID_t>* pref_res = new vector<ResourceID_t>();
  pref_res->push_back(machine1_res_node->resource_id_);
  pref_res->push_back(machine2_res_node->resource_id_);
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  ON_CALL(mock_cost_model, GetOutgoingEquivClassPrefArcs(_))
    .WillByDefault(testing::Return(pref_res));
  EXPECT_CALL(mock_cost_model, GetOutgoingEquivClassPrefArcs(_))
    .Times(1);
  ON_CALL(mock_cost_model, EquivClassToResourceNode(_, _))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, EquivClassToResourceNode(_, _))
    .Times(2);
  graph_manager->UpdateEquivToResArcs(ec_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 2);
  // No prefered ECs.
  vector<ResourceID_t>* no_pref_res = new vector<ResourceID_t>();
  ON_CALL(mock_cost_model, GetOutgoingEquivClassPrefArcs(_))
    .WillByDefault(testing::Return(no_pref_res));
  EXPECT_CALL(mock_cost_model, GetOutgoingEquivClassPrefArcs(_))
    .Times(1);
  graph_manager->UpdateEquivToResArcs(ec_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 0);
  EXPECT_EQ(ec_node->outgoing_arc_map_.size(), 0);
}

TEST_F(FlowGraphManagerTest, UpdateResourceStatsUpToRoot) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = CreateMachine(&rtnd, "res");
  FlowGraphNode* res_node = graph_manager->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  ResourceTopologyNodeDescriptor child_rtnd;
  ResourceDescriptor* child_rd_ptr = CreateMachine(&child_rtnd, "child_res");
  FlowGraphNode* child_res_node = graph_manager->AddResourceNode(child_rd_ptr);
  CHECK_NOTNULL(child_res_node);
  // Connect the two resources.
  FlowGraphArc* arc_res_to_res = graph_manager->graph_change_manager_->AddArc(
      res_node, child_res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_RES, "test");
  CHECK(InsertIfNotPresent(&graph_manager->node_to_parent_node_map_,
                           child_res_node, res_node));
  CHECK_EQ(rd_ptr->num_slots_below(), 0);
  CHECK_EQ(rd_ptr->num_running_tasks_below(), 0);
  CHECK_EQ(arc_res_to_res->cap_upper_bound_, 1);
  graph_manager->UpdateResourceStatsUpToRoot(child_res_node, 1, 1, 1);
  CHECK_EQ(child_rd_ptr->num_slots_below(), 0);
  CHECK_EQ(child_rd_ptr->num_running_tasks_below(), 0);
  CHECK_EQ(rd_ptr->num_slots_below(), 1);
  CHECK_EQ(rd_ptr->num_running_tasks_below(), 1);
  CHECK_EQ(arc_res_to_res->cap_upper_bound_, 2);
}

TEST_F(FlowGraphManagerTest, UpdateResOutgoingArcs) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = CreateMachine(&rtnd, "res");
  FlowGraphNode* res_node = graph_manager->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  res_node->type_ = FlowNodeType::PU;
  ResourceTopologyNodeDescriptor child_rtnd;
  ResourceDescriptor* child_rd_ptr = CreateMachine(&child_rtnd, "child_res");
  FlowGraphNode* child_res_node = graph_manager->AddResourceNode(child_rd_ptr);
  CHECK_NOTNULL(child_res_node);
  // Connect the two resources.
  FlowGraphArc* arc_res_to_res = graph_manager->graph_change_manager_->AddArc(
      res_node, child_res_node, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_BETWEEN_RES, "test");
  // Connect the top resource to the sink.
  graph_manager->graph_change_manager_->AddArc(
      res_node, graph_manager->sink_node_, 0, 1, 1, FlowGraphArcType::OTHER,
      ADD_ARC_RES_TO_SINK, "test");
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  ON_CALL(mock_cost_model, ResourceNodeToResourceNode(_, _))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, ResourceNodeToResourceNode(_, _)).Times(1);
  ON_CALL(mock_cost_model, LeafResourceNodeToSink(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, LeafResourceNodeToSink(_)).Times(1);
  graph_manager->UpdateResOutgoingArcs(res_node, &node_queue, &marked_nodes);
  CHECK_EQ(node_queue.size(), 1);
  CHECK_EQ(marked_nodes.size(), 1);
  CHECK_EQ(arc_res_to_res->cost_, 42);
}

TEST_F(FlowGraphManagerTest, UpdateResToSinkArc) {
  MockCostModel mock_cost_model;
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = CreateMachine(&rtnd, "res");
  FlowGraphNode* res_node = graph_manager->AddResourceNode(rd_ptr);
  CHECK_NOTNULL(res_node);
  // The resource is not a PU.
  EXPECT_DEATH(graph_manager->UpdateResToSinkArc(res_node), "");
  res_node->type_ = FlowNodeType::PU;
  // Check we're making a call to the cost model.
  ON_CALL(mock_cost_model, LeafResourceNodeToSink(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, LeafResourceNodeToSink(_)).Times(1);
  graph_manager->UpdateResToSinkArc(res_node);
}

TEST_F(FlowGraphManagerTest, UpdateRunningTaskNode) {
  MockCostModel mock_cost_model;
  EXPECT_CALL(mock_cost_model, AddTask(_)) .Times(1);
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  // No running arc for the task.
  EXPECT_DEATH(
      graph_manager->UpdateRunningTaskNode(task_node, false, NULL, NULL), "");
  ON_CALL(mock_cost_model, TaskToUnscheduledAgg(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(1);
  ON_CALL(mock_cost_model, TaskPreemption(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskPreemption(_)).Times(1);
  // Add an unscheduled aggregator.
  graph_manager->UpdateTaskToUnscheduledAggArc(task_node);
  // Add a PU to the graph.
  ResourceTopologyNodeDescriptor pu_rtnd;
  ResourceDescriptor* pu_rd_ptr = CreateMachine(&pu_rtnd, "pu");
  pu_rd_ptr->set_type(ResourceDescriptor::RESOURCE_PU);
  FlowGraphNode* pu_node = graph_manager->AddResourceNode(pu_rd_ptr);
  CHECK_NOTNULL(pu_node);
  // Add running arc.
  FlowGraphArc* arc_to_res = graph_manager->graph_change_manager_->AddArc(
      task_node, pu_node, 1, 1, 1, FlowGraphArcType::RUNNING,
      ADD_ARC_RUNNING_TASK, "test");
  CHECK(InsertIfNotPresent(&graph_manager->task_to_running_arc_,
                           task_node->td_ptr_->uid(), arc_to_res));
  ON_CALL(mock_cost_model, TaskContinuation(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskContinuation(_)).Times(1);
  graph_manager->UpdateRunningTaskNode(task_node, false, NULL, NULL);
}

TEST_F(FlowGraphManagerTest, UpdateRunningTaskToUnscheduledAggArc) {
  MockCostModel mock_cost_model;
  EXPECT_CALL(mock_cost_model, AddTask(_)) .Times(1);
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  FLAGS_preemption = true;
  // We haven't yet added an unscheduled agg node for this task.
  EXPECT_DEATH(graph_manager->UpdateRunningTaskToUnscheduledAggArc(task_node),
               "");
  FlowGraphNode* unsched_agg_node =
    graph_manager->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  // We don't yet have an arc to the unsched agg node.
  EXPECT_DEATH(graph_manager->UpdateRunningTaskToUnscheduledAggArc(task_node),
               "");
  ON_CALL(mock_cost_model, TaskToUnscheduledAgg(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(1);
  graph_manager->UpdateTaskToUnscheduledAggArc(task_node);
  ON_CALL(mock_cost_model, TaskPreemption(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskPreemption(_)).Times(1);
  graph_manager->UpdateRunningTaskToUnscheduledAggArc(task_node);
}

TEST_F(FlowGraphManagerTest, UpdateTaskNode) {
  MockCostModel mock_cost_model;
  EXPECT_CALL(mock_cost_model, AddTask(_)) .Times(1);
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNING);
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  // Tries to update running task but there's no running arc.
  EXPECT_DEATH(
      graph_manager->UpdateTaskNode(task_node, &node_queue, &marked_nodes), "");
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 1);
  EXPECT_EQ(flow_graph.NumArcs(), 0);
  // Updates a runnable task.
  task_node->td_ptr_->set_state(TaskDescriptor::RUNNABLE);
  ON_CALL(mock_cost_model, TaskToUnscheduledAgg(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(1);
  EXPECT_CALL(mock_cost_model, GetTaskPreferenceArcs(_)).Times(1);
  EXPECT_CALL(mock_cost_model, GetTaskEquivClasses(_)).Times(1);
  graph_manager->UpdateTaskNode(task_node, &node_queue, &marked_nodes);
  // The code added an unscheduled aggregator node and an arc to id.
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 2);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_DEATH(graph_manager->UpdateTaskNode(NULL, &node_queue, &marked_nodes),
               "");
}

TEST_F(FlowGraphManagerTest, UpdateTaskToEquivArcs) {
  MockCostModel mock_cost_model;
  EXPECT_CALL(mock_cost_model, AddTask(_)).Times(1);
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  ON_CALL(mock_cost_model, TaskToUnscheduledAgg(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(1);
  graph_manager->UpdateTaskToUnscheduledAggArc(task_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 2);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  // Add the equivalence class nodes.
  EquivClass_t ec1 = 42;
  FlowGraphNode* ec1_node = graph_manager->AddEquivClassNode(ec1);
  CHECK_NOTNULL(ec1_node);
  EquivClass_t ec2 = 43;
  FlowGraphNode* ec2_node = graph_manager->AddEquivClassNode(ec2);
  CHECK_NOTNULL(ec2_node);
  // Both ECs are prefered.
  vector<EquivClass_t>* pref_ecs = new vector<EquivClass_t>();
  pref_ecs->push_back(ec1);
  pref_ecs->push_back(ec2);
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  ON_CALL(mock_cost_model, GetTaskEquivClasses(_))
    .WillByDefault(testing::Return(pref_ecs));
  EXPECT_CALL(mock_cost_model, GetTaskEquivClasses(_))
    .Times(1);
  ON_CALL(mock_cost_model, TaskToEquivClassAggregator(_, _))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToEquivClassAggregator(_, _)).Times(2);
  graph_manager->UpdateTaskToEquivArcs(task_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 3);
  // No prefered ECs.
  vector<EquivClass_t>* no_pref_ecs = new vector<EquivClass_t>();
  ON_CALL(mock_cost_model, GetTaskEquivClasses(_))
    .WillByDefault(testing::Return(no_pref_ecs));
  EXPECT_CALL(mock_cost_model, GetTaskEquivClasses(_))
    .Times(1);
  graph_manager->UpdateTaskToEquivArcs(task_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(task_node->outgoing_arc_map_.size(), 1);
}

TEST_F(FlowGraphManagerTest, UpdateTaskToResArcs) {
  MockCostModel mock_cost_model;
  EXPECT_CALL(mock_cost_model, AddTask(_)) .Times(1);
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  ON_CALL(mock_cost_model, TaskToUnscheduledAgg(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(1);
  graph_manager->UpdateTaskToUnscheduledAggArc(task_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 2);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  // Add the resource nodes.
  ResourceTopologyNodeDescriptor machine1_rtnd;
  ResourceDescriptor* machine1_rd_ptr =
    CreateMachine(&machine1_rtnd, "machine1");
  FlowGraphNode* machine1_res_node =
    graph_manager->AddResourceNode(machine1_rd_ptr);
  CHECK_NOTNULL(machine1_res_node);
  ResourceTopologyNodeDescriptor machine2_rtnd;
  ResourceDescriptor* machine2_rd_ptr =
    CreateMachine(&machine2_rtnd, "machine2");
  FlowGraphNode* machine2_res_node =
    graph_manager->AddResourceNode(machine2_rd_ptr);
  CHECK_NOTNULL(machine2_res_node);
  // Both machines are prefered.
  vector<ResourceID_t>* pref_res = new vector<ResourceID_t>();
  pref_res->push_back(machine1_res_node->resource_id_);
  pref_res->push_back(machine2_res_node->resource_id_);
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  ON_CALL(mock_cost_model, GetTaskPreferenceArcs(_))
    .WillByDefault(testing::Return(pref_res));
  EXPECT_CALL(mock_cost_model, GetTaskPreferenceArcs(_))
    .Times(1);
  ON_CALL(mock_cost_model, TaskToResourceNode(_, _))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToResourceNode(_, _))
    .Times(2);
  graph_manager->UpdateTaskToResArcs(task_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 3);
  // No machines are prefered.
  vector<ResourceID_t>* no_pref_res = new vector<ResourceID_t>();
  ON_CALL(mock_cost_model, GetTaskPreferenceArcs(_))
    .WillByDefault(testing::Return(no_pref_res));
  EXPECT_CALL(mock_cost_model, GetTaskPreferenceArcs(_))
    .Times(1);
  graph_manager->UpdateTaskToResArcs(task_node, &node_queue, &marked_nodes);
  EXPECT_EQ(marked_nodes.size(), 2);
  EXPECT_EQ(node_queue.size(), 2);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(task_node->outgoing_arc_map_.size(), 1);
}

TEST_F(FlowGraphManagerTest, UpdateTaskToUnscheduledAggArc) {
  MockCostModel mock_cost_model;
  EXPECT_CALL(mock_cost_model, AddTask(_)) .Times(2);
  FlowGraphManager* graph_manager =
    new FlowGraphManager(&mock_cost_model, leaf_res_ids_, &wall_time_, tg_,
                         &dimacs_stats_);
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  // Case when we don't have an unscheduled aggregator node for the job.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  FlowGraphNode* task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  ON_CALL(mock_cost_model, TaskToUnscheduledAgg(_))
    .WillByDefault(testing::Return(ArcDescriptor(42LL, 1ULL, 0ULL)));
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(1);
  graph_manager->UpdateTaskToUnscheduledAggArc(task_node);
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
  task_node = graph_manager->AddTaskNode(job_id, td_ptr);
  FlowGraphNode* unsched_agg_node =
    graph_manager->AddUnscheduledAggNode(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 4);
  EXPECT_CALL(mock_cost_model, TaskToUnscheduledAgg(_)).Times(1);
  graph_manager->UpdateTaskToUnscheduledAggArc(task_node);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 4);
  EXPECT_EQ(flow_graph.NumArcs(), 2);
  task_to_unsched_arc = task_node->outgoing_arc_map_.begin()->second;
  EXPECT_EQ(task_to_unsched_arc->cap_lower_bound_, 0);
  EXPECT_EQ(task_to_unsched_arc->cap_upper_bound_, 1);
}

TEST_F(FlowGraphManagerTest, UpdateUnscheduledAggNode) {
  FlowGraphManager* graph_manager = CreateGraphManagerUsingTrivialCost();
  const FlowGraph& flow_graph =
    graph_manager->graph_change_manager_->flow_graph();
  uint64_t num_nodes = flow_graph.NumNodes();
  EXPECT_DEATH(graph_manager->UpdateUnscheduledAggNode(NULL, 0), "");
  JobID_t job_id = GenerateJobID(42);
  FlowGraphNode* unsched_agg_node =
    graph_manager->AddUnscheduledAggNode(job_id);
  EXPECT_EQ(FindPtrOrNull(graph_manager->job_unsched_to_node_, job_id),
            unsched_agg_node);
  EXPECT_EQ(flow_graph.NumArcs(), 0);
  EXPECT_EQ(flow_graph.NumNodes(), num_nodes + 1);
  EXPECT_DEATH(graph_manager->UpdateUnscheduledAggNode(unsched_agg_node, 0),
               "");
  // This call to update ends up adding the arc.
  graph_manager->UpdateUnscheduledAggNode(unsched_agg_node, 1);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(graph_manager->sink_node_->incoming_arc_map_.size(), 1);
  FlowGraphArc* arc_to_unsched =
    unsched_agg_node->outgoing_arc_map_.begin()->second;
  EXPECT_EQ(arc_to_unsched->src_node_, unsched_agg_node);
  EXPECT_EQ(unsched_agg_node->outgoing_arc_map_.size(), 1);
  EXPECT_EQ(arc_to_unsched->dst_node_, graph_manager->sink_node_);
  EXPECT_EQ(arc_to_unsched->cap_upper_bound_, 1);
  // Update the arc again.
  graph_manager->UpdateUnscheduledAggNode(unsched_agg_node, 1);
  EXPECT_EQ(graph_manager->sink_node_->incoming_arc_map_.size(), 1);
  EXPECT_EQ(unsched_agg_node->outgoing_arc_map_.size(), 1);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(arc_to_unsched->cap_upper_bound_, 2);
  // Update the arc so that its capacity is zero.
  graph_manager->UpdateUnscheduledAggNode(unsched_agg_node, -2);
  EXPECT_EQ(graph_manager->sink_node_->incoming_arc_map_.size(), 1);
  EXPECT_EQ(unsched_agg_node->outgoing_arc_map_.size(), 1);
  EXPECT_EQ(flow_graph.NumArcs(), 1);
  EXPECT_EQ(arc_to_unsched->cap_upper_bound_, 0);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
