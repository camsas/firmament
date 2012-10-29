// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive simple-minded queue-based scheduler.

#ifndef FIRMAMENT_ENGINE_SIMPLE_SCHEDULER_H
#define FIRMAMENT_ENGINE_SIMPLE_SCHEDULER_H

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
                  shared_ptr<ResourceMap_t> resource_map);
  const set<shared_ptr<TaskDescriptor> >& RunnableTasksForJob(
      shared_ptr<JobDescriptor> job_desc);
  uint64_t ScheduleJob(shared_ptr<JobDescriptor> job_desc);
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
  const set<shared_ptr<TaskDescriptor> >& LazyGraphReduction(
      const set<ReferenceID_t>& output_ids,
      shared_ptr<TaskDescriptor> root_task);
  shared_ptr<ReferenceInterface> ReferenceForID(ReferenceID_t id);
  shared_ptr<TaskDescriptor> ProducingTaskForReferenceID(ReferenceID_t id);
  // Cached sets of runnable and blocked tasks; these are updated on each
  // execution of LazyGraphReduction
  set<shared_ptr<TaskDescriptor> > runnable_tasks_;
  set<shared_ptr<TaskDescriptor> > blocked_tasks_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_SIMPLE_SCHEDULER_H
