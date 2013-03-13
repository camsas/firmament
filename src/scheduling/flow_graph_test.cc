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
namespace misc {

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
};

// Tests allocation of an empty envelope and puts an integer into it (using
// memcopy internally).
TEST_F(FlowGraphTest, SimpleGraphOutput) {
  FlowGraph g;
  ResourceTopologyNodeDescriptor rtn_root;
  string root_id = to_string(GenerateUUID());
  rtn_root.mutable_resource_desc()->set_uuid(root_id);
  ResourceTopologyNodeDescriptor* rtn_c1 = rtn_root.add_children();
  string c1_uid = to_string(GenerateUUID());
  rtn_c1->mutable_resource_desc()->set_uuid(c1_uid);
  rtn_c1->set_parent_id(root_id);
  rtn_root.mutable_resource_desc()->add_children(c1_uid);
  ResourceTopologyNodeDescriptor* rtn_c2 = rtn_root.add_children();
  string c2_uid = to_string(GenerateUUID());
  rtn_c2->mutable_resource_desc()->set_uuid(c2_uid);
  rtn_c2->set_parent_id(root_id);
  rtn_root.mutable_resource_desc()->add_children(c2_uid);
  g.AddResourceTopology(&rtn_root);
}

}  // namespace misc
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
