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

// SimpleScheduler class unit tests.

#include <set>

#include <gtest/gtest.h>

#include "base/common.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "misc/map-util.h"
#include "misc/wall_time.h"
#include "misc/utils.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/simple/simple_scheduler.h"
#include "storage/simple_object_store.h"

namespace firmament {
namespace scheduler {

using common::pb_to_set;
using scheduler::SimpleScheduler;
using store::SimpleObjectStore;

// The fixture for testing class SimpleScheduler.
class SimpleSchedulerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  SimpleSchedulerTest() :
    job_map_(new JobMap_t),
    res_map_(new ResourceMap_t),
    obj_store_(new store::SimpleObjectStore(GenerateResourceID())),
    task_map_(new TaskMap_t) {
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
    obj_store_->Flush();
    WallTime wall_time;
    sched_.reset(new SimpleScheduler(job_map_, res_map_, &res_topo_,
                                     obj_store_, task_map_,
                                     shared_ptr<KnowledgeBase>(),
                                     shared_ptr<TopologyManager>(), NULL, NULL,
                                     GenerateResourceID(), "test",
                                     &wall_time, NULL));
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  void AddJobsTasksToTaskMap(JobDescriptor* jd) {
    AddTaskToTaskMap(jd->mutable_root_task());
  }

  void AddTaskToTaskMap(TaskDescriptor* tdp) {
    // Add root task to task map (this would have been done by the coordinator
    // in the real system)
    CHECK(InsertIfNotPresent(task_map_.get(), tdp->uid(), tdp));
    if (tdp->spawned_size() > 0) {
      for (RepeatedPtrField<TaskDescriptor>::iterator t_iter =
           tdp->mutable_spawned()->begin();
           t_iter != tdp->mutable_spawned()->end();
           ++t_iter)
        AddTaskToTaskMap(&(*t_iter));
    }
  }

  // Objects declared here can be used by all tests in the test case for
  // SimpleScheduler.
  scoped_ptr<SimpleScheduler> sched_;
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<ResourceMap_t> res_map_;
  ResourceTopologyNodeDescriptor res_topo_;
  shared_ptr<store::SimpleObjectStore> obj_store_;
  shared_ptr<TaskMap_t> task_map_;
};

// Tests that the lazy graph reduction algorithm correctly identifies runnable
// tasks.
TEST_F(SimpleSchedulerTest, LazyGraphReductionTest) {
  // Simple, plain, 1-task job (base case)
  JobDescriptor* test_job = new JobDescriptor;
  JobID_t job_id = GenerateJobID();
  test_job->set_uuid(to_string(job_id));
  TaskDescriptor* rtp = test_job->mutable_root_task();
  rtp->set_uid(GenerateRootTaskID(*test_job));
  rtp->set_state(TaskDescriptor::CREATED);
  rtp->set_job_id(to_string(job_id));
  unordered_set<DataObjectID_t*> output_ids(DataObjectIDsFromProtobuf(
      test_job->output_ids()));
  AddTaskToTaskMap(rtp);
  sched_->LazyGraphReduction(output_ids, rtp, job_id);
  // The root task should be runnable
  EXPECT_EQ(sched_->runnable_tasks_[job_id].size(), 1UL);
  // The only runnable task should be equivalent to the root task we pushed in.
  EXPECT_EQ(*(sched_->runnable_tasks_[job_id].begin()), rtp->uid());
  delete test_job;
}

// Tests correct operation of the ComputeRunnableTasksForJob wrapper.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForJob) {
  // Simple, plain, 1-task job (base case)
  JobDescriptor* test_job = new JobDescriptor;
  JobID_t job_id = GenerateJobID();
  test_job->set_uuid(to_string(job_id));
  TaskDescriptor* rtp = test_job->mutable_root_task();
  rtp->set_uid(GenerateRootTaskID(*test_job));
  rtp->set_state(TaskDescriptor::CREATED);
  rtp->set_job_id(to_string(job_id));
  AddJobsTasksToTaskMap(test_job);
  unordered_set<TaskID_t> runnable_tasks =
    sched_->ComputeRunnableTasksForJob(test_job);
  // The root task should be runnable
  EXPECT_EQ(runnable_tasks.size(), 1UL);
  delete test_job;
}

// Tests lookup of a reference in the object table.
TEST_F(SimpleSchedulerTest, ObjectIDToReferenceDescLookup) {
  ReferenceDescriptor rd;
  rd.set_id("feedcafedeadbeeffeedcafedeadbeef");
  rd.set_type(ReferenceDescriptor::CONCRETE);
  DataObjectID_t doid(DataObjectIDFromProtobuf(rd.id()));
  CHECK(!obj_store_->AddReference(doid, &rd));
  unordered_set<ReferenceInterface*> refs = sched_->ReferencesForID(doid);
  EXPECT_EQ(*(*refs.begin())->id().name_str(), rd.id());
}

// Tests lookup of a data object's producing task via the object table.
TEST_F(SimpleSchedulerTest, ProducingTaskLookup) {
  JobID_t job_id = GenerateJobID();
  TaskDescriptor td;
  td.set_uid(1);
  td.set_job_id(to_string(job_id));
  AddTaskToTaskMap(&td);
  ReferenceDescriptor rd;
  rd.set_id("feedcafedeadbeeffeedcafedeadbeef");
  rd.set_type(ReferenceDescriptor::CONCRETE);
  rd.set_producing_task(1);
  DataObjectID_t doid(DataObjectIDFromProtobuf(rd.id()));
  CHECK(!obj_store_->AddReference(doid, &rd));
  unordered_set<TaskDescriptor*> tdps =
      sched_->ProducingTasksForDataObjectID(doid, job_id);
  CHECK_EQ(tdps.size(), 1);
  VLOG(1) << (*tdps.begin())->DebugString();
  EXPECT_EQ((*tdps.begin())->uid(), 1UL);
}

// Find runnable tasks for a slightly more elaborate task graph.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForComplexJob) {
  // Somewhat more complex job with 3 tasks.
  JobDescriptor* test_job = new JobDescriptor;
  JobID_t job_id = GenerateJobID();
  test_job->set_uuid(to_string(job_id));
  TaskDescriptor* rtp = test_job->mutable_root_task();
  rtp->set_uid(GenerateRootTaskID(*test_job));
  rtp->set_state(TaskDescriptor::CREATED);
  rtp->set_job_id(to_string(job_id));
  // add spawned task #1
  TaskDescriptor* td1 = test_job->mutable_root_task()->add_spawned();
  td1->set_uid(GenerateTaskID(test_job->root_task()));
  td1->set_job_id(to_string(job_id));
  ReferenceDescriptor* d0_td1 = td1->add_outputs();
  DataObjectID_t d0_td1_o1 = GenerateDataObjectID(*td1);
  d0_td1->set_id(d0_td1_o1.name_bytes(), DIOS_NAME_BYTES);
  d0_td1->set_type(ReferenceDescriptor::CONCRETE);
  d0_td1->set_producing_task(td1->uid());
  // add spawned task #2
  TaskDescriptor* td2 = test_job->mutable_root_task()->add_spawned();
  td2->set_uid(GenerateTaskID(test_job->root_task()));
  td2->set_job_id(to_string(job_id));
  ReferenceDescriptor* d0_td2 = td2->add_outputs();
  DataObjectID_t d0_td2_o1 = GenerateDataObjectID(*td2);
  d0_td2->set_id(d0_td2_o1.name_bytes(), DIOS_NAME_BYTES);
  d0_td2->set_type(ReferenceDescriptor::FUTURE);
  d0_td2->set_producing_task(td2->uid());
  test_job->add_output_ids(d0_td2->id());
  test_job->add_output_ids(d0_td1->id());
  // put concrete input ref of td1 into object table
  DataObjectID_t d0_td1_id(DataObjectIDFromProtobuf(d0_td1->id()));
  DataObjectID_t d0_td2_id(DataObjectIDFromProtobuf(d0_td2->id()));
  CHECK(!obj_store_->AddReference(d0_td1_id, d0_td1));
  // put future input ref of td2 into object table
  CHECK(!obj_store_->AddReference(d0_td2_id, d0_td2));
  VLOG(1) << "got here, job is: " << test_job->DebugString();
  AddJobsTasksToTaskMap(test_job);
  unordered_set<TaskID_t> runnable_tasks =
    sched_->ComputeRunnableTasksForJob(test_job);
  // Three tasks should be runnable: those spawned by the root task, and the
  // root task itself.
  EXPECT_EQ(runnable_tasks.size(), 3UL);
  delete test_job;
}

// Find runnable tasks for a slightly more elaborate task graph.
TEST_F(SimpleSchedulerTest, FindRunnableTasksForComplexJob2) {
  // Somewhat more complex job with 3 tasks.
  JobDescriptor* test_job = new JobDescriptor;
  JobID_t job_id = GenerateJobID();
  test_job->set_uuid(to_string(job_id));
  test_job->mutable_root_task()->set_uid(GenerateRootTaskID(*test_job));
  test_job->mutable_root_task()->set_name("root_task");
  test_job->mutable_root_task()->set_job_id(to_string(job_id));
  ReferenceDescriptor* o0_rt = test_job->mutable_root_task()->add_outputs();
  o0_rt->set_id(GenerateDataObjectID(test_job->root_task()).name_bytes(),
                DIOS_NAME_BYTES);
  o0_rt->set_type(ReferenceDescriptor::CONCRETE);
  o0_rt->set_producing_task(test_job->root_task().uid());
  // add spawned task #1
  TaskDescriptor* td1 = test_job->mutable_root_task()->add_spawned();
  td1->set_uid(GenerateTaskID(test_job->root_task()));
  td1->set_job_id(to_string(job_id));
  ReferenceDescriptor* o0_td1 = td1->add_outputs();
  o0_td1->set_id(GenerateDataObjectID(*td1).name_bytes(), DIOS_NAME_BYTES);
  o0_td1->set_type(ReferenceDescriptor::FUTURE);
  o0_td1->set_producing_task(td1->uid());
  ReferenceDescriptor* d0_td1 = td1->add_dependencies();
  d0_td1->set_id(o0_rt->id());
  d0_td1->set_type(ReferenceDescriptor::CONCRETE);
  d0_td1->set_producing_task(test_job->root_task().uid());
  test_job->add_output_ids(o0_td1->id());
  test_job->add_output_ids(d0_td1->id());
  // put concrete input ref of td1 into object table
  DataObjectID_t o0_td1_id(DataObjectIDFromProtobuf(o0_td1->id()));
  CHECK(!obj_store_->AddReference(o0_td1_id, o0_td1));
  // put future input ref of td2 into object table
  DataObjectID_t d0_td1_id(DataObjectIDFromProtobuf(d0_td1->id()));
  CHECK(!obj_store_->AddReference(d0_td1_id, d0_td1));
  VLOG(1) << "got here, job is: " << test_job->DebugString();
  AddJobsTasksToTaskMap(test_job);
  unordered_set<TaskID_t> runnable_tasks =
    sched_->ComputeRunnableTasksForJob(test_job);
  // Two tasks should be runnable: those spawned by the root task.
  EXPECT_EQ(runnable_tasks.size(), 2UL);
  delete test_job;
}

}  // namespace scheduler
}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
