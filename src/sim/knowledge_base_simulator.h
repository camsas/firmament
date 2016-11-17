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

#ifndef FIRMAMENT_SIM_KNOWLEDGE_BASE_SIMULATOR_H
#define FIRMAMENT_SIM_KNOWLEDGE_BASE_SIMULATOR_H

#include "scheduling/knowledge_base.h"

#include "scheduling/data_layer_manager_interface.h"
#include "sim/trace_utils.h"

namespace firmament {
namespace sim {

class KnowledgeBaseSimulator : public KnowledgeBase {
 public:
  KnowledgeBaseSimulator();
  KnowledgeBaseSimulator(DataLayerManagerInterface* data_layer_manager);

  void AddMachineSample(
      uint64_t current_simulation_time,
      ResourceDescriptor* rd_ptr,
      const unordered_map<TaskID_t, ResourceDescriptor*>& task_id_to_rd);
  void EraseTraceTaskStats(TaskID_t task_id);
  uint64_t GetRuntimeForTask(TaskID_t task_id);
  void PopulateTaskFinalReport(TaskDescriptor* td_ptr, TaskFinalReport* report);
  void SetTaskType(TaskDescriptor* td_ptr);
  void SetTraceTaskStats(TaskID_t task_id, const TraceTaskStats& task_stat);

 private:
  unordered_map<TaskID_t, TraceTaskStats> task_stats_;
};

} // namespace sim
} // namespace firmament

#endif  // FIRMAMENT_SIM_KNOWLEDGE_BASE_SIMULATOR_H
