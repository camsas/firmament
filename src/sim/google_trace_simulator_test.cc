// The Firmament project
// Copyright (c) 2015-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Tests for the Google trace simulator.

#include <gtest/gtest.h>

#include "sim/google_trace_simulator.h"

namespace firmament {
namespace sim {

class GoogleTraceSimulatorTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body is empty.

  GoogleTraceSimulatorTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
  }

  virtual ~GoogleTraceSimulatorTest() {
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

TEST(GoogleTraceSimulatorTest, Empty) {
  EXPECT_EQ(1, 1);
}

} // namespace sim
} // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_logtostderr = true;
  FLAGS_stderrthreshold = 0;
  return RUN_ALL_TESTS();
}
