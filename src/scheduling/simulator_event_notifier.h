// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_SIMULATOR_EVENT_NOTIFIER_H
#define FIRMAMENT_SCHEDULING_SIMULATOR_EVENT_NOTIFIER_H

#include "scheduling/event_notifier_interface.h"

namespace firmament {
namespace scheduler {

class SimulatorEventNotifier : public EventNotifierInterface {
  void OnJobCompletion(JobID_t job_id);
  void OnTaskCompletion(TaskDescriptor* td_ptr,
                        ResourceDescriptor* rd_ptr);
  void OnTaskEviction(TaskDescriptor* td_ptr,
                      ResourceDescriptor* rd_ptr);
  void OnTaskFailure(TaskDescriptor* td_ptr,
                     ResourceDescriptor* rd_ptr);
  void OnTaskMigration(TaskDescriptor* td_ptr,
                       ResourceDescriptor* rd_ptr);
  void OnTaskPlacement(TaskDescriptor* td_ptr,
                       ResourceDescriptor* rd_ptr);
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_SIMULATOR_EVENT_NOTIFIER_H
