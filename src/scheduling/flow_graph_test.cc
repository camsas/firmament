// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Tests for flow graph.

#include <gtest/gtest.h>

#include <vector>

#include "base/common.h"
#include "misc/utils.h"
#include "scheduling/dimacs_add_node.h"
#include "scheduling/dimacs_change_arc.h"
#include "scheduling/dimacs_new_arc.h"
#include "scheduling/dimacs_remove_node.h"
#include "scheduling/flow_graph.h"
#include "scheduling/trivial_cost_model.h"

DECLARE_uint64(num_pref_arcs_task_to_res);

namespace firmament {

// The fixture for testing the FlowGraph container class.
class FlowGraphTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  FlowGraphTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
  }

  virtual ~FlowGraphTest() {
    // You can do clean-up work that doesn't throw exceptions here.
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
    string root_id = to_string(GenerateUUID());
    rtn_root->mutable_resource_desc()->set_uuid(root_id);
    ResourceTopologyNodeDescriptor* rtn_c1 = rtn_root->add_children();
    string c1_uid = to_string(GenerateUUID());
    rtn_c1->mutable_resource_desc()->set_uuid(c1_uid);
    rtn_c1->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
    rtn_c1->set_parent_id(root_id);
    ResourceTopologyNodeDescriptor* rtn_c2 = rtn_root->add_children();
    string c2_uid = to_string(GenerateUUID());
    rtn_c2->mutable_resource_desc()->set_uuid(c2_uid);
    rtn_c2->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
    rtn_c2->set_parent_id(root_id);
  }
};

// Tests arc addition to node.
TEST_F(FlowGraphTest, AddArcToNode) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  uint64_t init_node_count = g.NumNodes();
  FlowGraphNode* n0 = g.AddNodeInternal(g.NextId());
  FlowGraphNode* n1 = g.AddNodeInternal(g.NextId());
  FlowGraphArc* arc = g.AddArcInternal(n0->id_, n1->id_);
  CHECK_EQ(g.NumNodes(), init_node_count + 2);
  CHECK_EQ(g.NumArcs(), 1);
  CHECK_EQ(n0->outgoing_arc_map_[n1->id_], arc);
}

TEST_F(FlowGraphTest, AddOrUpdateJobNodes) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  g.AddResourceTopology(&rtn_root);
  // Now generate a job and add it
  JobID_t jid = GenerateJobID();
  JobDescriptor test_job;
  test_job.set_uuid(to_string(jid));
  TaskDescriptor* rt = test_job.mutable_root_task();
  rt->set_state(TaskDescriptor::RUNNABLE);
  rt->set_uid(GenerateRootTaskID(test_job));
  rt->set_job_id(test_job.uuid());
  CHECK(InsertIfNotPresent(task_map.get(), rt->uid(), rt));
  uint32_t num_changes = g.graph_changes_.size();
  uint32_t num_ids = g.ids_created_.size();
  g.AddOrUpdateJobNodes(&test_job);
  // This should add one new node for the agg, one arc change from agg to
  // sink, one new node for the root task, one new node for the
  // equivalence class.
  CHECK_EQ(g.graph_changes_.size(), num_changes + 4);
  uint64_t sink_graph_id = g.ids_created_[0];
  DIMACSAddNode* unsched_agg =
    static_cast<DIMACSAddNode*>(g.graph_changes_[num_changes]);
  DIMACSChangeArc* arc_to_sink =
    static_cast<DIMACSChangeArc*>(g.graph_changes_[num_changes + 1]);
  DIMACSAddNode* root_task =
    static_cast<DIMACSAddNode*>(g.graph_changes_[num_changes + 2]);
  DIMACSAddNode* equiv_class =
    static_cast<DIMACSAddNode*>(g.graph_changes_[num_changes + 3]);

  // Check the id of the source equals the id of the unsched agg node.
  CHECK_EQ(arc_to_sink->src_, g.ids_created_[num_ids]);
  CHECK_EQ(arc_to_sink->dst_, sink_graph_id);
  CHECK_EQ(arc_to_sink->cap_upper_bound_, 1);
  // Arc to sink.
  CHECK_EQ(unsched_agg->arcs_->size(), 1);
  // Arc to unscheduled aggregator and to topology.
  CHECK_EQ(root_task->arcs_->size(), 2);
  CHECK_EQ(equiv_class->arcs_->size(), 1 + FLAGS_num_pref_arcs_task_to_res);
}

TEST_F(FlowGraphTest, AddResourceNode) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  g.AddResourceTopology(&rtn_root);
  string root_id = rtn_root.mutable_resource_desc()->uuid();
  uint32_t num_changes = g.graph_changes_.size();
  // Add a new core node.
  ResourceTopologyNodeDescriptor* core_node = rtn_root.add_children();
  string core_uid = to_string(GenerateUUID());
  core_node->mutable_resource_desc()->set_uuid(core_uid);
  core_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  core_node->set_parent_id(root_id);
  uint64_t sink_graph_id = g.ids_created_[0];
  uint64_t root_graph_id = g.ids_created_[1];
  g.AddResourceNode(core_node);
  uint64_t new_core_graph_id = g.ids_created_[g.ids_created_.size() - 1];
  // Check if the number of changes is correct. One change for adding the new
  // node, another change for adding an arc from the node to the sink and
  // another change for update the arc from the parent to the core.
  CHECK_EQ(g.graph_changes_.size(), num_changes + 3);
  DIMACSAddNode* add_node =
    static_cast<DIMACSAddNode*>(g.graph_changes_[num_changes]);
  CHECK_EQ(add_node->node_.id_, new_core_graph_id);
  DIMACSNewArc* arc_to_sink =
    static_cast<DIMACSNewArc*>(g.graph_changes_[num_changes + 1]);
  CHECK_EQ(arc_to_sink->src_, new_core_graph_id);
  CHECK_EQ(arc_to_sink->dst_, sink_graph_id);
  DIMACSChangeArc* arc_to_parent =
    static_cast<DIMACSChangeArc*>(g.graph_changes_[num_changes + 2]);
  CHECK_EQ(arc_to_parent->src_, root_graph_id);
  CHECK_EQ(arc_to_parent->dst_, new_core_graph_id);

  // Add two new PUs.
  ResourceTopologyNodeDescriptor* memory_node = rtn_root.add_children();
  string memory_uid = to_string(GenerateUUID());
  memory_node->mutable_resource_desc()->set_uuid(memory_uid);
  memory_node->set_parent_id(root_id);
  ResourceTopologyNodeDescriptor* pu1_node = memory_node->add_children();
  string pu1_uid = to_string(GenerateUUID());
  pu1_node->mutable_resource_desc()->set_uuid(pu1_uid);
  pu1_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  pu1_node->set_parent_id(memory_uid);
  ResourceTopologyNodeDescriptor* pu2_node = memory_node->add_children();
  string pu2_uid = to_string(GenerateUUID());
  pu2_node->mutable_resource_desc()->set_uuid(pu2_uid);
  pu2_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  pu2_node->set_parent_id(memory_uid);
  // Add non-leaf node.
  g.AddResourceNode(memory_node);
  uint64_t non_leaf_node_id = g.ids_created_[g.ids_created_.size() - 1];
  DIMACSAddNode* non_leaf_node =
    static_cast<DIMACSAddNode*>(g.graph_changes_[g.graph_changes_.size() - 1]);
  CHECK_EQ(non_leaf_node->node_.id_, non_leaf_node_id);
  // Arc to parent.
  CHECK_EQ(non_leaf_node->arcs_->size(), 1);
}

// Change an arc and check it gets added to changes.
TEST_F(FlowGraphTest, ChangeArc) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  uint64_t n0_id = g.NextId();
  FlowGraphNode* n0 = g.AddNodeInternal(n0_id);
  uint64_t n1_id = g.NextId();
  FlowGraphNode* n1 = g.AddNodeInternal(n1_id);
  FlowGraphArc* arc = g.AddArcInternal(n0->id_, n1->id_);
  uint32_t num_changes = g.graph_changes_.size();
  g.ChangeArc(arc, 0, 100, 42);
  CHECK_EQ(arc->cost_, 42);
  CHECK_EQ(arc->cap_lower_bound_, 0);
  CHECK_EQ(arc->cap_upper_bound_, 100);
  CHECK_EQ(arc->src_, n0->id_);
  CHECK_EQ(arc->dst_, n1->id_);
  CHECK_EQ(g.graph_changes_.size(), num_changes + 1);
  uint64_t num_arcs = g.NumArcs();
  // This should delete the arc.
  g.ChangeArc(arc, 0, 0, 42);
  CHECK_EQ(g.NumArcs(), num_arcs - 1);
  // Should have only added a change for the arc we removed.
  CHECK_EQ(g.graph_changes_.size(), num_changes + 2);
  DIMACSChangeArc* chg_arc =
    static_cast<DIMACSChangeArc*>(g.graph_changes_[num_changes]);
  CHECK_EQ(chg_arc->src_, n0_id);
  CHECK_EQ(chg_arc->dst_, n1_id);
  CHECK_EQ(chg_arc->cap_lower_bound_, 0);
  CHECK_EQ(chg_arc->cap_upper_bound_, 100);
  CHECK_EQ(chg_arc->cost_, 42);
  DIMACSChangeArc* del_arc =
    static_cast<DIMACSChangeArc*>(g.graph_changes_[num_changes + 1]);
  CHECK_EQ(del_arc->src_, n0_id);
  CHECK_EQ(del_arc->dst_, n1_id);
  CHECK_EQ(del_arc->cap_lower_bound_, 0);
  CHECK_EQ(del_arc->cap_upper_bound_, 0);
}

TEST_F(FlowGraphTest, DeleteResourceNode) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  g.AddResourceTopology(&rtn_root);
  string root_id = rtn_root.mutable_resource_desc()->uuid();
  // Add a new core node.
  ResourceTopologyNodeDescriptor* core_node = rtn_root.add_children();
  string core_uid = to_string(GenerateUUID());
  core_node->mutable_resource_desc()->set_uuid(core_uid);
  core_node->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  core_node->set_parent_id(root_id);
  g.AddResourceNode(core_node);
  uint64_t new_core_graph_id = g.ids_created_[g.ids_created_.size() - 1];
  uint32_t num_changes = g.graph_changes_.size();
  // Delete the core node.
  g.DeleteResourceNode(g.NodeForResourceID(
       ResourceIDFromString(core_node->resource_desc().uuid())));
  CHECK_EQ(g.graph_changes_.size(), num_changes + 1);
  DIMACSRemoveNode* rem_node =
    static_cast<DIMACSRemoveNode*>(g.graph_changes_[num_changes]);
  CHECK_EQ(rem_node->node_id_, new_core_graph_id);
}

TEST_F(FlowGraphTest, ResetChanges) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  FlowGraphNode* n0 = g.AddNodeInternal(g.NextId());
  FlowGraphNode* n1 = g.AddNodeInternal(g.NextId());
  g.AddArcInternal(n0->id_, n1->id_);
  CHECK_EQ(g.graph_changes_.size(), 1);
  g.ResetChanges();
  CHECK_EQ(g.graph_changes_.size(), 0);
}

// Add simple resource topology to graph
TEST_F(FlowGraphTest, SimpleResourceTopo) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  g.AddResourceTopology(&rtn_root);
}

// Test correct increment/decrement of unscheduled aggregator capacities.
TEST_F(FlowGraphTest, UnschedAggCapacityAdjustment) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  FlowGraph g(new TrivialCostModel(task_map, leaf_res_ids), leaf_res_ids);
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  g.AddResourceTopology(&rtn_root);
  // Now generate a job and add it
  JobID_t jid = GenerateJobID();
  JobDescriptor test_job;
  test_job.set_uuid(to_string(jid));
  TaskDescriptor* rt = test_job.mutable_root_task();
  rt->set_state(TaskDescriptor::RUNNABLE);
  rt->set_uid(GenerateRootTaskID(test_job));
  rt->set_job_id(test_job.uuid());
  CHECK(InsertIfNotPresent(task_map.get(), rt->uid(), rt));
  g.AddOrUpdateJobNodes(&test_job);
  // Grab the unscheduled aggregator for the new job
  uint64_t* unsched_agg_node_id = FindOrNull(g.job_unsched_to_node_id_, jid);
  CHECK_NOTNULL(unsched_agg_node_id);
  FlowGraphArc** lookup_ptr =
      FindOrNull(g.Node(*unsched_agg_node_id)->outgoing_arc_map_,
                 g.sink_node_->id_);
  CHECK_NOTNULL(lookup_ptr);
  FlowGraphArc* unsched_agg_to_sink_arc = *lookup_ptr;
  CHECK_EQ(unsched_agg_to_sink_arc->cap_upper_bound_, 1);
  // Now pin the root task to the first resource leaf
  uint64_t* root_task_node_id =
      FindOrNull(g.task_to_nodeid_map_, test_job.root_task().uid());
  CHECK_NOTNULL(root_task_node_id);
  uint64_t* resource_node_id =
      FindOrNull(g.resource_to_nodeid_map_, ResourceIDFromString(
          rtn_root.mutable_children()->Get(0).resource_desc().uuid()));
  CHECK_NOTNULL(resource_node_id);
  g.PinTaskToNode(g.Node(*root_task_node_id), g.Node(*resource_node_id));
  // The unscheduled aggregator's outbound capacity should have been
  // decremented.
  CHECK_EQ(unsched_agg_to_sink_arc->cap_upper_bound_, 0);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
