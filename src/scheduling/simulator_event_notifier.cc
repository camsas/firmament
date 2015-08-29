// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/simulator_event_notifier.h"

#include "scheduling/event_notifier_interface.h"

namespace firmament {
namespace scheduler {

void SimulatorEventNotifier::OnJobCompletion(JobID_t job_id) {
  // TODO(ionel): Implement!
}

void SimulatorEventNotifier::OnTaskCompletion(TaskDescriptor* td_ptr,
                                              ResourceDescriptor* rd_ptr) {
  // TODO(ionel): Implement!
}

void SimulatorEventNotifier::OnTaskEviction(TaskDescriptor* td_ptr,
                                            ResourceDescriptor* rd_ptr) {
  // TODO(ionel): Implement!
}

void SimulatorEventNotifier::OnTaskFailure(TaskDescriptor* td_ptr,
                                           ResourceDescriptor* rd_ptr) {
  // TODO(ionel): Implement!
}

void SimulatorEventNotifier::OnTaskMigration(TaskDescriptor* td_ptr,
                                             ResourceDescriptor* rd_ptr) {
  // TODO(ionel): Implement!
}

void SimulatorEventNotifier::OnTaskPlacement(TaskDescriptor* td_ptr,
                                             ResourceDescriptor* rd_ptr) {
  // TODO(ionel): Implement!
}

}  // namespace scheduler
}  // namespace firmament
