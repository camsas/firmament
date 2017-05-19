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
  virtual void OnJobRemoval(JobID_t job_id) = 0;
  virtual void OnSchedulingDecisionsCompletion(uint64_t scheduler_start_time,
                                               uint64_t scheduler_runtime) = 0;
  virtual void OnTaskCompletion(TaskDescriptor* td_ptr,
                                ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskEviction(TaskDescriptor* td_ptr,
                              ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskFailure(TaskDescriptor* td_ptr,
                             ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskMigration(TaskDescriptor* td_ptr,
                               ResourceDescriptor* old_rd_ptr,
                               ResourceDescriptor* rd_ptr) = 0;
  virtual void OnTaskPlacement(TaskDescriptor* td_ptr,
                               ResourceDescriptor* rd_ptr) = 0;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_SCHEDULING_EVENT_NOTIFIER_INTERFACE_H
