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
#include "misc/utils.h"
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
    res_map_(new ResourceMap_t),
    obj_map_(new DataObjectMap_t) {
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
    obj_map_->clear();
    sched_.reset(new SimpleScheduler(job_map_, res_map_, obj_map_, ""));
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  void PrintRunnableTasks(set<shared_ptr<TaskDescriptor> > runnable_tasks) {
    int i = 0;
    for (set<shared_ptr<TaskDescriptor> >::const_iterator t_iter =
         runnable_tasks.begin();
         t_iter != runnable_tasks.end();
         ++t_iter) {
      VLOG(1) << "Runnable task " << i << ": " << (*t_iter)->uid()
              << " [at " << *t_iter << "]";
      ++i;
    }
  }

  // Objects declared here can be used by all tests in the test case for
  // SimpleScheduler.
  scoped_ptr<SimpleScheduler> sched_;
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<ResourceMap_t> res_map_;
  shared_ptr<DataObjectMap_t> obj_map_;
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

// Tests lookup of a reference in the object table.
TEST_F(SimpleSchedulerTest, ObjectIDToReferenceDescLookup) {
  ReferenceDescriptor rd;
  rd.set_id(1234);
  rd.set_type(ReferenceDescriptor::CONCRETE);
  CHECK(InsertIfNotPresent(obj_map_.get(), rd.id(), rd));
  shared_ptr<ReferenceInterface> ref = sched_->ReferenceForID(rd.id());
  VLOG(1) << *ref;
  CHECK_EQ(ref->id(), rd.id());
}

// Tests lookup of a data object's producing task via the object table.
TEST_F(SimpleSchedulerTest, ProducingTaskLookup) {
  ReferenceDescriptor rd;
  rd.set_id(1234);
  rd.set_type(ReferenceDescriptor::CONCRETE);
  rd.set_producing_task(1);
  CHECK(InsertIfNotPresent(obj_map_.get(), rd.id(), rd));
  shared_ptr<TaskDescriptor> td = sched_->ProducingTaskForDataObjectID(rd.id());
  VLOG(1) << td->DebugString();
  CHECK_EQ(td->uid(), 1);
}

// Find runnable tasks for a slightly more elaborate task graph.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForComplexJob) {
  // Somewhat more complex job with 3 tasks.
  shared_ptr<JobDescriptor> test_job(new JobDescriptor);
  test_job->mutable_root_task()->set_uid(0);
  test_job->mutable_root_task()->set_name("root_task");
  // add spawned task #1
  TaskDescriptor* td1 = test_job->mutable_root_task()->add_spawned();
  td1->set_uid(GenerateTaskID(test_job->root_task()));
  ReferenceDescriptor* d0_td1 = td1->add_outputs();
  d0_td1->set_id(GenerateDataObjectID(*td1));
  d0_td1->set_type(ReferenceDescriptor::CONCRETE);
  d0_td1->set_producing_task(td1->uid());
  // add spawned task #2
  TaskDescriptor* td2 = test_job->mutable_root_task()->add_spawned();
  td2->set_uid(GenerateTaskID(test_job->root_task()));
  ReferenceDescriptor* d0_td2 = td2->add_outputs();
  d0_td2->set_id(GenerateDataObjectID(*td2));
  d0_td2->set_type(ReferenceDescriptor::FUTURE);
  d0_td2->set_producing_task(td2->uid());
  test_job->add_output_ids(d0_td2->id());
  test_job->add_output_ids(d0_td1->id());
  // put concrete input ref of td1 into object table
  InsertIfNotPresent(obj_map_.get(), d0_td1->id(), *d0_td1);
  // put future input ref of td2 into object table
  InsertIfNotPresent(obj_map_.get(), d0_td2->id(), *d0_td2);
  VLOG(1) << "got here, job is: " << test_job->DebugString();
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched_->RunnableTasksForJob(test_job);
  PrintRunnableTasks(runnable_tasks);
  // Two tasks should be runnable: those spawned by the root task.
  CHECK_EQ(runnable_tasks.size(), 2);
}

// Find runnable tasks for a slightly more elaborate task graph.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForComplexJob2) {
  // Somewhat more complex job with 3 tasks.
  shared_ptr<JobDescriptor> test_job(new JobDescriptor);
  test_job->mutable_root_task()->set_uid(0);
  test_job->mutable_root_task()->set_name("root_task");
  ReferenceDescriptor* o0_rt = test_job->mutable_root_task()->add_outputs();
  o0_rt->set_id(GenerateDataObjectID(test_job->root_task()));
  o0_rt->set_type(ReferenceDescriptor::FUTURE);
  o0_rt->set_producing_task(test_job->root_task().uid());
  // add spawned task #1
  TaskDescriptor* td1 = test_job->mutable_root_task()->add_spawned();
  td1->set_uid(GenerateTaskID(test_job->root_task()));
  ReferenceDescriptor* o0_td1 = td1->add_outputs();
  o0_td1->set_id(GenerateDataObjectID(*td1));
  o0_td1->set_type(ReferenceDescriptor::FUTURE);
  o0_td1->set_producing_task(td1->uid());
  ReferenceDescriptor* d0_td1 = td1->add_dependencies();
  d0_td1->set_id(o0_rt->id());
  d0_td1->set_type(ReferenceDescriptor::FUTURE);
  d0_td1->set_producing_task(test_job->root_task().uid());
  test_job->add_output_ids(o0_td1->id());
  test_job->add_output_ids(d0_td1->id());
  // put concrete input ref of td1 into object table
  InsertIfNotPresent(obj_map_.get(), o0_td1->id(), *o0_td1);
  // put future input ref of td2 into object table
  InsertIfNotPresent(obj_map_.get(), d0_td1->id(), *d0_td1);
  VLOG(1) << "got here, job is: " << test_job->DebugString();
  set<shared_ptr<TaskDescriptor> > runnable_tasks =
      sched_->RunnableTasksForJob(test_job);
  // Two tasks should be runnable: those spawned by the root task.
  PrintRunnableTasks(runnable_tasks);
  CHECK_EQ(runnable_tasks.size(), 2);
}




}  // namespace scheduler
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
