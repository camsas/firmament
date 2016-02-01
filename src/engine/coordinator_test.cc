// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Coordinator class unit tests.

#include <stdint.h>
#include <iostream>

#include <gtest/gtest.h>

#include "base/common.h"
#include "engine/coordinator.h"

#ifdef __HTTP_UI__
DECLARE_bool(http_ui);
#endif

namespace {

using firmament::Coordinator;

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
TEST_F(CoordinatorTest, HTTPUIStartStopTest) {
  FLAGS_platform = "PL_UNIX";
#ifdef __HTTP_UI__
  FLAGS_http_ui = true;
#endif
  FLAGS_v = 2;
  Coordinator test_coordinator();
  // Hold on for 1 second
  boost::this_thread::sleep(boost::posix_time::seconds(1));
  test_coordinator.Shutdown("test end");
}


}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
