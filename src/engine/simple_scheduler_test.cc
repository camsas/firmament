// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// SimpleScheduler class unit tests.

#include <set>

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

  SimpleSchedulerTest() :
    job_map_(new JobMap_t),
    res_map_(new ResourceMap_t) {
    // You can do set-up work for each test here.
    FLAGS_v = 3;
  }

  virtual ~SimpleSchedulerTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
    res_map_->clear();
    job_map_->clear();
    sched_.reset(new SimpleScheduler(job_map_, res_map_, ""));
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for
  // SimpleScheduler.
  scoped_ptr<SimpleScheduler> sched_;
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<ResourceMap_t> res_map_;
};

// Tests that the lazy graph reduction algorithm correctly identifies runnable
// tasks.
TEST_F(SimpleSchedulerTest, LazyGraphReductionTest) {
  // Simple, plain, 1-task job (base case)
  shared_ptr<JobDescriptor> test_job(new JobDescriptor);
  shared_ptr<TaskDescriptor> rtp(new TaskDescriptor);
  set<DataObjectID_t> output_ids(pb_to_set(test_job->output_ids()));
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched_->LazyGraphReduction(output_ids, rtp);
  // The root task should be runnable
  CHECK_EQ(runnable_tasks.size(), 1);
  // The only runnable task should be equivalent to the root task we pushed in.
  CHECK_EQ(*runnable_tasks.begin(), rtp);
}

// Tests correct operation of the RunnableTasksForJob wrapper.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForJob) {
  // Simple, plain, 1-task job (base case)
  shared_ptr<JobDescriptor> test_job(new JobDescriptor);
  VLOG(1) << "got here, job is: " << test_job->DebugString();
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched_->RunnableTasksForJob(test_job);
  // The root task should be runnable
  CHECK_EQ(runnable_tasks.size(), 1);
}

// Find runnable tasks for a slightly more elaborate task graph.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForComplexJob) {
  // Somewhat more complex job with 3 tasks.
  shared_ptr<JobDescriptor> test_job(new JobDescriptor);
  TaskDescriptor* td1 = test_job->mutable_root_task()->add_spawned();
  td1->set_uid(1);
  ReferenceDescriptor* d0_td1 = td1->add_dependencies();
  d0_td1->set_type(ReferenceDescriptor::CONCRETE);
  TaskDescriptor* td2 = test_job->mutable_root_task()->add_spawned();
  ReferenceDescriptor* d0_td2 = td2->add_dependencies();
  d0_td2->set_type(ReferenceDescriptor::FUTURE);
  td2->set_uid(2);
  VLOG(1) << "got here, job is: " << test_job->DebugString();
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched_->RunnableTasksForJob(test_job);
  // Two tasks should be runnable: those spawned by the root task.
  CHECK_EQ(runnable_tasks.size(), 2);
}



}  // namespace scheduler
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
