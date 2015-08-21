// The Firmament project
// Copyright (c) 2011-2013 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// ProcFS machine unit tests.

#include <gtest/gtest.h>

#include <vector>
#include <unistd.h>

#include "base/common.h"
#include "base/machine_perf_statistics_sample.pb.h"
#include "platforms/common.pb.h"
#include "platforms/unix/common.h"
#include "platforms/unix/procfs_machine.h"

using firmament::common::InitFirmament;

DEFINE_uint64(heartbeat_interval, 1000000,
              "Heartbeat interval in microseconds.");

namespace firmament {
namespace platform_unix {

class ProcFSMachineTest : public ::testing::Test {
 protected:
  ProcFSMachineTest()
    : pfsm_() { // 0.1s polling freq
    FLAGS_v = 2;
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
  // This is more of a joke, but it does the job for testing.
  // Print stats to manually check.
  MachinePerfStatisticsSample* stats = new MachinePerfStatisticsSample;
  // We need to wait for a bit to collect some data
  sleep(1);
  pfsm_.CreateStatistics(stats);
  VLOG(1) << stats->DebugString();
  delete stats;
  sleep(5);
}

// Tests retrieval of machine resource capacities.
TEST_F(ProcFSMachineTest, GetCapacities) {
  // This is more of a joke, but it does the job for testing.
  // Print capacities to manually check, since we don't know what machine
  // this will run on!
  ResourceVector caps;
  pfsm_.GetMachineCapacity(&caps);
  VLOG(1) << caps.DebugString();
  sleep(5);
}

}  // namespace platform_unix
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  //  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
