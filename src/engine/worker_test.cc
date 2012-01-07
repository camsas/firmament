// TODO: header

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "engine/worker.h"

using namespace firmament;

DECLARE_string(platform);

namespace {

// The fixture for testing class Worker.
class WorkerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  WorkerTest() {
    // You can do set-up work for each test here.
  }

  virtual ~WorkerTest() {
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
  Worker w_;
};

// Tests that the Foo::Bar() method does Abc.
TEST_F(WorkerTest, TestMethod) {
  EXPECT_EQ(0, w_.Test());
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

