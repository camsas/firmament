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
  SimpleScheduler();
  void AddJob();
  const set<shared_ptr<TaskDescriptor> > RunnableTasksForJob(
      JobDescriptor& job_desc);
  void FindResourceForTask();
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<SimpleScheduler>";
  }
 protected:
 private:
  // Unit tests
  FRIEND_TEST(SimpleSchedulerTest, LazyGraphReductionTest);
  const set<shared_ptr<TaskDescriptor> > LazyGraphReduction(
      const set<string>& output_ids,
      shared_ptr<TaskDescriptor> root_task);
  shared_ptr<ReferenceInterface> ReferenceFromDescriptor(const ReferenceDescriptor& desc);
  shared_ptr<ReferenceInterface> ReferenceForID(const string& id);
  shared_ptr<TaskDescriptor> TaskForOutputID(const string& id);
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_SIMPLE_SCHEDULER_H
