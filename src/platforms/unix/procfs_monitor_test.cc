// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// ProcFS monitor unit tests.

#include <gtest/gtest.h>

#include <boost/thread.hpp>

#include "base/common.h"
#include "platforms/common.pb.h"
#include "platforms/unix/common.h"
#include "platforms/unix/procfs_monitor.h"

using firmament::common::InitFirmament;

namespace firmament {
namespace platform_unix {

// The fixture for testing the stream socket messaging adapter.
class ProcFSMonitorTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  ProcFSMonitorTest()
    : pfsm_(100000UL)  { // 0.1s polling freq
    // You can do set-up work for each test here.
  }

  virtual ~ProcFSMonitorTest() {
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
  ProcFSMonitor pfsm_;
};

// Tests retrieval of simple process statistics.
TEST_F(ProcFSMonitorTest, SimpleProcessStatsTest) {
  pid_t pid = getpid();
  const ProcFSMonitor::ProcessStatistics_t* stats =
      pfsm_.ProcessInformation(pid, NULL);
  CHECK_EQ(stats->pid, pid);
  malloc(getpagesize());
  // Should have more pages allocated now...
  CHECK_LT(stats->rss, pfsm_.ProcessInformation(pid, NULL)->rss);
}

// Tests retrieval of simple process statistics.
TEST_F(ProcFSMonitorTest, SimpleSysInfoTest) {
  FLAGS_v = 2;
  boost::thread t(&ProcFSMonitor::Run, &pfsm_);
  sleep(1);
  pfsm_.Stop();
  t.join();
}

// Tests retrieval of simple process statistics.
TEST_F(ProcFSMonitorTest, SchedStatsTest) {
  pid_t pid = getpid();
  const ProcFSMonitor::ProcessStatistics_t* stats =
      pfsm_.ProcessInformation(pid, NULL);
  CHECK_EQ(stats->pid, pid);
  sleep(1);
  CHECK_LT(stats->sched_run_ticks,
           pfsm_.ProcessInformation(pid, NULL)->sched_run_ticks);
}


}  // namespace platform_unix
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
