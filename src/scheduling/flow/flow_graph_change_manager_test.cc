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

#include <gtest/gtest.h>

#include "scheduling/flow/dimacs_add_node.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/dimacs_remove_node.h"
#include "scheduling/flow/flow_graph_change_manager.h"

namespace firmament {

class FlowGraphChangeManagerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  FlowGraphChangeManagerTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
    change_manager_ = new FlowGraphChangeManager(&dimacs_stats_);
  }

  virtual ~FlowGraphChangeManagerTest() {
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

  DIMACSChangeStats dimacs_stats_;
  FlowGraphChangeManager* change_manager_;
};

TEST_F(FlowGraphChangeManagerTest, AddGraphChange) {
  FlowGraphNode node1(1);
  FlowGraphNode node2(2);
  FlowGraphArc arc12(1, 2, 0, 1, 42, &node1, &node2);
  change_manager_->AddGraphChange(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->AddGraphChange(
      new DIMACSAddNode(node2, vector<FlowGraphArc*>()));
  change_manager_->AddGraphChange(new DIMACSNewArc(arc12));
  EXPECT_EQ(change_manager_->graph_changes_.size(), 3);
}

TEST_F(FlowGraphChangeManagerTest, MergeChangesToSameArc) {
  FlowGraphNode node1(1);
  FlowGraphNode node2(2);
  FlowGraphArc arc12(1, 2, 0, 1, 42, &node1, &node2);
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node2, vector<FlowGraphArc*>()));
  // The following two arc changes should be merged into one.
  change_manager_->graph_changes_.push_back(new DIMACSNewArc(arc12));
  // Change the arc we've just added.
  arc12.cap_upper_bound_ = 2;
  arc12.cost_ = 43;
  change_manager_->graph_changes_.push_back(new DIMACSChangeArc(arc12, 42));
  change_manager_->graph_changes_.push_back(new DIMACSRemoveNode(node1));
  // Add a new node that reuses the id of the node we've just removed.
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->graph_changes_.push_back(new DIMACSNewArc(arc12));
  EXPECT_EQ(change_manager_->graph_changes_.size(), 7);
  change_manager_->MergeChangesToSameArc();
  EXPECT_EQ(change_manager_->graph_changes_.size(), 6);
  DIMACSNewArc* new_arc =
    dynamic_cast<DIMACSNewArc*>(change_manager_->graph_changes_[2]);
  EXPECT_EQ(new_arc->src_, 1);
  EXPECT_EQ(new_arc->dst_, 2);
  EXPECT_EQ(new_arc->cap_upper_bound_, 2);
  EXPECT_EQ(new_arc->cost_, 43);
}

TEST_F(FlowGraphChangeManagerTest, PurgeChangesBeforeNodeRemoval) {
  FlowGraphNode node1(1);
  FlowGraphNode node2(2);
  FlowGraphArc arc12(1, 2, 0, 1, 42, &node1, &node2);
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node2, vector<FlowGraphArc*>()));
  // The following change should be purged because we latter remove one of
  // the node it connects.
  change_manager_->graph_changes_.push_back(new DIMACSNewArc(arc12));
  change_manager_->graph_changes_.push_back(new DIMACSRemoveNode(node1));
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->graph_changes_.push_back(new DIMACSNewArc(arc12));
  EXPECT_EQ(change_manager_->graph_changes_.size(), 6);
  change_manager_->PurgeChangesBeforeNodeRemoval();
  EXPECT_EQ(change_manager_->graph_changes_.size(), 5);
}

TEST_F(FlowGraphChangeManagerTest, RemoveDuplicateChanges) {
  FlowGraphNode node1(1);
  FlowGraphNode node2(2);
  FlowGraphArc arc12(1, 2, 0, 1, 42, &node1, &node2);
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node2, vector<FlowGraphArc*>()));
  change_manager_->graph_changes_.push_back(new DIMACSNewArc(arc12));
  change_manager_->graph_changes_.push_back(new DIMACSChangeArc(arc12, 42));
  // Add duplicate change.
  change_manager_->graph_changes_.push_back(new DIMACSChangeArc(arc12, 42));
  change_manager_->graph_changes_.push_back(new DIMACSRemoveNode(node1));
  change_manager_->graph_changes_.push_back(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->graph_changes_.push_back(new DIMACSNewArc(arc12));
  // Add again change to arc (1,2), but this one should not be removed.
  change_manager_->graph_changes_.push_back(new DIMACSChangeArc(arc12, 42));
  EXPECT_EQ(change_manager_->graph_changes_.size(), 9);
  change_manager_->RemoveDuplicateChanges();
  EXPECT_EQ(change_manager_->graph_changes_.size(), 8);
}

TEST_F(FlowGraphChangeManagerTest, ResetChanges) {
  FlowGraphNode node1(1);
  FlowGraphNode node2(2);
  FlowGraphArc arc12(1, 2, 0, 1, 42, &node1, &node2);
  change_manager_->AddGraphChange(
      new DIMACSAddNode(node1, vector<FlowGraphArc*>()));
  change_manager_->AddGraphChange(
      new DIMACSAddNode(node2, vector<FlowGraphArc*>()));
  change_manager_->AddGraphChange(new DIMACSNewArc(arc12));
  EXPECT_EQ(change_manager_->graph_changes_.size(), 3);
  change_manager_->ResetChanges();
  EXPECT_EQ(change_manager_->graph_changes_.size(), 0);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
