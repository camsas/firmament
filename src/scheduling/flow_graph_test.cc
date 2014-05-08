// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Tests for DIMACS exporter for CS2 solver.

#include <gtest/gtest.h>

#include <vector>

#include "base/common.h"
#include "misc/utils.h"
#include "scheduling/flow_graph.h"

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
    rtn_c1->set_parent_id(root_id);
    rtn_root->mutable_resource_desc()->add_children(c1_uid);
    ResourceTopologyNodeDescriptor* rtn_c2 = rtn_root->add_children();
    string c2_uid = to_string(GenerateUUID());
    rtn_c2->mutable_resource_desc()->set_uuid(c2_uid);
    rtn_c2->set_parent_id(root_id);
    rtn_root->mutable_resource_desc()->add_children(c2_uid);
  }
};

// Tests arc addition to node.
TEST_F(FlowGraphTest, AddArcToNode) {
  FlowGraph g(FlowSchedulingCostModelType::COST_MODEL_TRIVIAL);
  uint64_t init_node_count = g.NumNodes();
  FlowGraphNode* n0 = g.AddNodeInternal(g.next_id());
  FlowGraphNode* n1 = g.AddNodeInternal(g.next_id());
  FlowGraphArc* arc = g.AddArcInternal(n0->id_, n1->id_);
  CHECK_EQ(g.NumNodes(), init_node_count + 2);
  CHECK_EQ(g.NumArcs(), 1);
  CHECK_EQ(n0->outgoing_arc_map_[n1->id_], arc);
}

// Add simple resource topology to graph
TEST_F(FlowGraphTest, SimpleResourceTopo) {
  FlowGraph g(FlowSchedulingCostModelType::COST_MODEL_TRIVIAL);
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  g.AddResourceTopology(&rtn_root, 2);
}

// Test correct increment/decrement of unscheduled aggregator capacities.
TEST_F(FlowGraphTest, UnschedAggCapacityAdjustment) {
  FlowGraph g(FlowSchedulingCostModelType::COST_MODEL_TRIVIAL);
  ResourceTopologyNodeDescriptor rtn_root;
  CreateSimpleResourceTopo(&rtn_root);
  g.AddResourceTopology(&rtn_root, 2);
  // Now generate a job and add it
  JobID_t jid = GenerateJobID();
  JobDescriptor test_job;
  test_job.mutable_root_task()->set_state(TaskDescriptor::RUNNABLE);
  test_job.set_uuid(to_string(jid));
  g.AddJobNodes(&test_job);
  // Grab the unscheduled aggregator for the new job
  uint64_t* unsched_agg_node_id = FindOrNull(g.job_to_nodeid_map_, jid);
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
