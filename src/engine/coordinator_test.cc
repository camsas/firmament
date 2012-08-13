// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Coordinator class unit tests.

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "engine/coordinator.h"

DECLARE_string(platform);

namespace {

using firmament::Coordinator;
using firmament::GetPlatformID;
using firmament::PlatformID;

// The fixture for testing class Coordinator.
class CoordinatorTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  CoordinatorTest() {
    // You can do set-up work for each test here.
  }

  virtual ~CoordinatorTest() {
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

  // Objects declared here can be used by all tests in the test case for
  // Coordinator.
};

// Tests that the platform gets set correctly when instantiating a worker.
TEST_F(CoordinatorTest, PlatformSetTest) {
  FLAGS_platform = "PL_UNIX";
  FLAGS_v = 1;
  Coordinator test_coordinator(GetPlatformID(FLAGS_platform));
  // We expect this worker to have been configured as a UNIX worker.
  EXPECT_EQ(test_coordinator.platform_id(), firmament::PL_UNIX);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
