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

// ProcFS monitor unit tests.

#include <gtest/gtest.h>

#include <boost/thread.hpp>

#include <sys/mman.h>
#include <unistd.h>

#include "base/common.h"
#include "platforms/unix/common.h"
#include "platforms/unix/procfs_monitor.h"

using firmament::common::InitFirmament;

DEFINE_uint64(heartbeat_interval, 1000000,
              "Heartbeat interval in microseconds.");

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
  usleep(1000);
  char* p = (char*)mmap(0, getpagesize() * 10, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
  CHECK_NOTNULL(p);
  for (int i = 0; i < getpagesize() * 10; ++i) {
    p[i] = 42;  // demand-page
  }
  usleep(1000);
  // Should have more pages allocated now...
  const ProcFSMonitor::ProcessStatistics_t* new_stats =
    pfsm_.ProcessInformation(pid, NULL);
  CHECK_LT(stats->vsize, new_stats->vsize);
  munmap(p, getpagesize() * 10);
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
