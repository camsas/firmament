// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// SimpleScheduler class unit tests.

#include <gtest/gtest.h>

#include "base/common.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "misc/map-util.h"
#include "engine/simple_scheduler.h"

namespace firmament {
namespace scheduler {

using common::pb_to_set;
using scheduler::SimpleScheduler;

// The fixture for testing class SimpleScheduler.
class SimpleSchedulerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SimpleSchedulerTest() {
    // You can do set-up work for each test here.
  }

  virtual ~SimpleSchedulerTest() {
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
  // SimpleScheduler.
};

// Tests that the lazy graph reduction algorithm correctly identifies runnable
// tasks.
TEST_F(SimpleSchedulerTest, LazyGraphReductionTest) {
  FLAGS_v = 2;
  // Simple, plain, 1-task job (base case)
  JobDescriptor* test_job = new JobDescriptor();
  SimpleScheduler sched;
  shared_ptr<TaskDescriptor> rtp(test_job->mutable_root_task());
  set<string> output_ids(pb_to_set(test_job->output_ids()));
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched.LazyGraphReduction(output_ids, rtp);
  // The root task should be runnable
  CHECK_EQ(runnable_tasks.size(), 1);
}

// Tests correct operation of the RunnableTasksForJob wrapper.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForJob) {
  FLAGS_v = 2;
  // Simple, plain, 1-task job (base case)
  JobDescriptor test_job;
  SimpleScheduler sched;
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched.RunnableTasksForJob(test_job);
  // The root task should be runnable
  CHECK_EQ(runnable_tasks.size(), 1);
}

// Find runnable tasks for a slightly more elaborate task graph.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForComplexJob) {
  FLAGS_v = 2;
  // Simple, plain, 1-task job (base case)
  JobDescriptor test_job;
  TaskDescriptor* td1 = test_job.mutable_root_task()->add_spawned();
  TaskDescriptor* td2 = test_job.mutable_root_task()->add_spawned();
  VLOG(1) << td1 << td2;
  SimpleScheduler sched;
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched.RunnableTasksForJob(test_job);
  // Two tasks should be runnable: those spawned by the root task.
  CHECK_EQ(runnable_tasks.size(), 2);
}



}  // namespace scheduler
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
