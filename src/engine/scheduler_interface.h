// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// The scheduler interface assumed by the engine.

#ifndef FIRMAMENT_ENGINE_SCHEDULER_INTERFACE_H
#define FIRMAMENT_ENGINE_SCHEDULER_INTERFACE_H

#include "base/job_desc.pb.h"
#include "misc/printable_interface.h"

namespace firmament {
namespace scheduler {

class SchedulerInterface : public PrintableInterface {
  // XXX(malte): stub
 public:
  void AddJob();
  void RunnableTasksForJob(const JobDescriptor& job_desc);
  void FindResourceForTask();
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_SCHEDULER_INTERFACE_H
