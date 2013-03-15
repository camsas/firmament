// The Firmament project
// Copyright (c) 2011-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// ProcFS machine unit tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "platforms/common.pb.h"
#include "platforms/unix/common.h"
#include "platforms/unix/procfs_machine.h"
#include "base/machine_perf_statistics_sample.pb.h"

using firmament::common::InitFirmament;

namespace firmament {
namespace platform_unix {

class ProcFSMachineTest : public ::testing::Test {
 protected:

  ProcFSMachineTest()
    : pfsm_() { // 0.1s polling freq
    // You can do set-up work for each test here.
  }

  virtual ~ProcFSMachineTest() {
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
  ProcFSMachine pfsm_;
};

// Tests retrieval of simple process statistics.
TEST_F(ProcFSMachineTest, CreateStatistics) {
  MachinePerfStatisticsSample* stats = new MachinePerfStatisticsSample;
  pfsm_.CreateStatistics(stats);
  //  CHECK_EQ(stats->get_total_ram(0), 1);
  //  CHECK_EQ(stats->get_free_ram(0), 1);
}

}  // namespace platform_unix
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
