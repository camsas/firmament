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

#ifndef FIRMAMENT_SCHEDULING_FIRMAMENT_SCHEDULER_SERVICE_H
#define FIRMAMENT_SCHEDULING_FIRMAMENT_SCHEDULER_SERVICE_H

#include <grpc++/grpc++.h>

#include "scheduling/firmament_scheduler.pb.h"
#include "scheduling/firmament_scheduler.grpc.pb.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace firmament {

class FirmamentSchedulerServiceImpl final :
  public FirmamentScheduler::Service {
 public:
  FirmamentSchedulerServiceImpl() {};
  ~FirmamentSchedulerServiceImpl() {};

  Status Schedule(ServerContext* context,
                  const ScheduleRequest* request,
                  SchedulingDeltas* reply) override;
  Status TaskCompleted(ServerContext* context,
                       const TaskDescriptor* td_ptr,
                       TaskCompletedResponse* reply) override;
  Status TaskFailed(ServerContext* context,
                    const TaskDescriptor* td_ptr,
                    TaskFailedResponse* reply) override;
  Status TaskRemoved(ServerContext* context,
                     const TaskDescriptor* td_ptr,
                     TaskRemovedResponse* reply) override;
  Status TaskSubmitted(ServerContext* context,
                       const TaskDescriptor* td_ptr,
                       TaskSubmittedResponse* reply) override;
  Status NodeAdded(ServerContext* context,
                   const ResourceDescriptor* td_ptr,
                   NodeAddedResponse* reply) override;
  Status NodeFailed(ServerContext* context,
                   const ResourceDescriptor* td_ptr,
                    NodeFailedResponse* reply) override;
  Status NodeRemoved(ServerContext* context,
                     const ResourceDescriptor* td_ptr,
                     NodeRemovedResponse* reply) override;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FIRMAMENT_SCHEDULER_SERVICE_H
