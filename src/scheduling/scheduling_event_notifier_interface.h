// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// An event notifier interface. If the notifier is passed to the event driven
// scheduler then the methods defined in this interfaces are called events
// happen.

#ifndef FIRMAMENT_SCHEDULING_SCHEDULING_EVENT_NOTIFIER_INTERFACE_H
#define FIRMAMENT_SCHEDULING_SCHEDULING_EVENT_NOTIFIER_INTERFACE_H

#include "base/types.h"

namespace firmament {
namespace scheduler {

class SchedulingEventNotifierInterface {
 public:
  virtual ~SchedulingEventNotifierInterface() {}
  virtual void OnJobCompletion(JobID_t job_id) = 0;
  virtual void OnTaskCompletion(TaskDescriptor* td_ptr,
                                ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskEviction(TaskDescriptor* td_ptr,
                              ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskFailure(TaskDescriptor* td_ptr,
                             ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskMigration(TaskDescriptor* td_ptr,
                               ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskPlacement(TaskDescriptor* td_ptr,
                               ResourceDescriptor* rd_ptr) = 0;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_SCHEDULING_EVENT_NOTIFIER_INTERFACE_H
