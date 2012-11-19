// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive simple-minded queue-based scheduler.

#ifndef FIRMAMENT_ENGINE_SIMPLE_SCHEDULER_H
#define FIRMAMENT_ENGINE_SIMPLE_SCHEDULER_H

#include <set>
#include <string>

#include "base/common.h"
#include "base/types.h"
#include "base/reference_interface.h"
#include "base/job_desc.pb.h"
#include "base/task_desc.pb.h"
#include "engine/scheduler_interface.h"

namespace firmament {
namespace scheduler {

class SimpleScheduler : public SchedulerInterface {
 public:
  SimpleScheduler(shared_ptr<JobMap_t> job_map,
                  shared_ptr<ResourceMap_t> resource_map,
                  shared_ptr<DataObjectMap_t> object_map,
                  shared_ptr<TaskMap_t> task_map,
                  const string& coordinator_uri);
  const set<TaskID_t>& RunnableTasksForJob(JobDescriptor* job_desc);
  uint64_t ScheduleJob(JobDescriptor* job_desc);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<SimpleScheduler>";
  }

 protected:
  void BindTaskToResource(shared_ptr<TaskDescriptor> task_desc,
//                          shared_ptr<ResourceDescriptor> res_desc);
                          ResourceDescriptor* res_desc);
  const ResourceID_t* FindResourceForTask(shared_ptr<TaskDescriptor> task_desc);

 private:
  // Unit tests
  FRIEND_TEST(SimpleSchedulerTest, LazyGraphReductionTest);
  FRIEND_TEST(SimpleSchedulerTest, ObjectIDToReferenceDescLookup);
  FRIEND_TEST(SimpleSchedulerTest, ProducingTaskLookup);
  void DebugPrintRunnableTasks();
  const set<TaskID_t>& LazyGraphReduction(
      const set<DataObjectID_t>& output_ids,
      TaskDescriptor* root_task);
  shared_ptr<ReferenceInterface> ReferenceForID(DataObjectID_t id);
  shared_ptr<TaskDescriptor> ProducingTaskForDataObjectID(DataObjectID_t id);
  // Cached sets of runnable and blocked tasks; these are updated on each
  // execution of LazyGraphReduction
  set<TaskID_t> runnable_tasks_;
  set<shared_ptr<TaskDescriptor> > blocked_tasks_;
  // Initialized to hold the URI of the (currently unique) coordinator this
  // scheduler is associated with. This is passed down to the executor and to
  // tasks so that they can find the coordinator at runtime.
  const string coordinator_uri_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_SIMPLE_SCHEDULER_H
