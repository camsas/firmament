// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Tests for flow graph.

#include <gtest/gtest.h>

#include <vector>

#include "base/common.h"
#include "scheduling/flow/flow_graph.h"

DECLARE_string(flow_scheduling_solver);

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
};

// Tests arc addition to node.
TEST_F(FlowGraphTest, AddArcToNode) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  FlowGraph graph;
  uint64_t init_node_count = graph.NumNodes();
  FlowGraphNode* n0 = graph.AddNode();
  FlowGraphNode* n1 = graph.AddNode();
  FlowGraphArc* arc = graph.AddArc(n0->id_, n1->id_);
  CHECK_EQ(graph.NumNodes(), init_node_count + 2);
  CHECK_EQ(graph.NumArcs(), 1);
  CHECK_EQ(n0->outgoing_arc_map_[n1->id_], arc);
}

// Change an arc and check it gets added to changes.
TEST_F(FlowGraphTest, ChangeArc) {
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  FlowGraph graph;
  FlowGraphNode* n0 = graph.AddNode();
  FlowGraphNode* n1 = graph.AddNode();
  FlowGraphArc* arc = graph.AddArc(n0->id_, n1->id_);
  graph.ChangeArc(arc, 0, 100, 42);
  CHECK_EQ(arc->cost_, 42);
  CHECK_EQ(arc->cap_lower_bound_, 0);
  CHECK_EQ(arc->cap_upper_bound_, 100);
  CHECK_EQ(arc->src_, n0->id_);
  CHECK_EQ(arc->dst_, n1->id_);
  uint64_t num_arcs = graph.NumArcs();
  // This should delete the arc.
  graph.ChangeArc(arc, 0, 0, 42);
  CHECK_EQ(graph.NumArcs(), num_arcs - 1);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
