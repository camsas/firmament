// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Task graph class unit tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "base/task_desc.pb.h"
#include "base/task_graph.h"

namespace firmament {

class TaskGraphTest : public ::testing::Test {
 protected:
  TaskGraphTest() {
    // Set-up the sample task graph: root task with two children
    //
    //      RT
    //  +---+---+
    // ST1     ST2
    //
    sample_tg_root_desc_.set_uid(12345ULL);
    TaskDescriptor* st1 = sample_tg_root_desc_.add_spawned();
    st1->set_uid(9999ULL);
    TaskDescriptor* st2 = sample_tg_root_desc_.add_spawned();
    st2->set_uid(1111ULL);
  }

  virtual ~TaskGraphTest() {
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

  // Objects declared here can be used by all tests in the test case for Worker.
  TaskDescriptor sample_tg_root_desc_;
};

// Initialize a task graph from a protobuf
TEST_F(TaskGraphTest, CreateTGTest) {
  FLAGS_v = 2;
  VLOG(1) << sample_tg_root_desc_.DebugString();
  TaskGraph sample_tg(&sample_tg_root_desc_);
  // Descriptor stored in root node should be the same as root task descriptor
  CHECK_EQ(sample_tg.root_node_->descriptor(), &sample_tg_root_desc_);
}

// Retrieve a task's parent (who spawned it).
TEST_F(TaskGraphTest, GetParentTest) {
  TaskGraph sample_tg(&sample_tg_root_desc_);
  CHECK_EQ(sample_tg.root_node_->mutable_parent(), static_cast<TaskGraphNode*>(NULL));
  CHECK_EQ(sample_tg.ParentOf(&sample_tg_root_desc_),
           static_cast<TaskDescriptor*>(NULL));
}

// Retrieve a task's children (tasks spawned by it).
TEST_F(TaskGraphTest, GetChildrenTest) {
  TaskGraph sample_tg(&sample_tg_root_desc_);
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
