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
  graph.DeleteArc(arc);
  CHECK_EQ(graph.NumArcs(), num_arcs - 1);
}

// Tests add nodes by passing node pointer to FlowGraphNode instead of node ID.
TEST_F(FlowGraphTest, AddArcToNode2) {
  FlowGraph fgraph;
  FlowGraphNode* node0 = fgraph.AddNode();
  FlowGraphNode* node1 = fgraph.AddNode();
  FlowGraphArc* arc = fgraph.AddArc(node0, node1);
  fgraph.ChangeArc(arc, 0, 100, 42);
  CHECK_EQ(fgraph.NumArcs(), 1);
  fgraph.DeleteArc(arc);
  fgraph.DeleteNode(node0);
  fgraph.DeleteNode(node1);
}

// Tests change arc cost.
TEST_F(FlowGraphTest, ChangeArcCost) {
  FlowGraph fgraph;
  FlowGraphNode* node0 = fgraph.AddNode();
  FlowGraphNode* node1 = fgraph.AddNode();
  FlowGraphArc* arc = fgraph.AddArc(node0, node1);
  fgraph.ChangeArc(arc, 0, 100, 42);
  CHECK_EQ(fgraph.NumArcs(), 1);
  CHECK_EQ(arc->cost_, 42);
  fgraph.ChangeArcCost(arc, 44);
  CHECK_EQ(arc->cost_, 44);
  fgraph.DeleteArc(arc);
  fgraph.DeleteNode(node0);
  fgraph.DeleteNode(node1);
}

// Tests get arc.
TEST_F(FlowGraphTest, GetArc) {
  FlowGraph fgraph;
  FlowGraphNode* node0 = fgraph.AddNode();
  FlowGraphNode* node1 = fgraph.AddNode();
  FlowGraphArc* arc = fgraph.AddArc(node0, node1);
  FlowGraphArc* get_arc = fgraph.GetArc(node0, node1);
  CHECK_EQ(get_arc->src_, node0->id_);
  CHECK_EQ(get_arc->dst_, node1->id_);
  fgraph.DeleteArc(arc);
  fgraph.DeleteNode(node0);
  fgraph.DeleteNode(node1);
}

}  // namespace firmament

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
