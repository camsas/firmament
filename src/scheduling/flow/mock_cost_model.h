/*
 * Firmament
 * Copyright (c) Ionel Gog <ionel.gog@cl.cam.ac.uk>
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

#ifndef FIRMAMENT_SCHEDULING_FLOW_MOCK_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_FLOW_MOCK_COST_MODEL_H

#include "scheduling/flow/cost_model_interface.h"

#include <gmock/gmock.h>

namespace firmament {

class MockCostModel : public CostModelInterface {
 public:
  MOCK_METHOD1(TaskToUnscheduledAggCost, Cost_t(TaskID_t task_id));
  MOCK_METHOD1(UnscheduledAggToSinkCost, Cost_t(JobID_t job_id));
  MOCK_METHOD2(TaskToResourceNodeCost,
               Cost_t(TaskID_t task_id, ResourceID_t res_id));
  MOCK_METHOD2(ResourceNodeToResourceNodeCost,
               Cost_t(const ResourceDescriptor& src,
                      const ResourceDescriptor& dst));
  MOCK_METHOD1(LeafResourceNodeToSinkCost, Cost_t(ResourceID_t res_id));
  MOCK_METHOD1(TaskContinuationCost, Cost_t(TaskID_t task_id));
  MOCK_METHOD1(TaskPreemptionCost, Cost_t(TaskID_t task_id));
  MOCK_METHOD2(TaskToEquivClassAggregator,
               Cost_t(TaskID_t task_id, EquivClass_t ec));
  MOCK_METHOD2(EquivClassToResourceNode,
               pair<Cost_t, uint64_t>(EquivClass_t ec, ResourceID_t res_id));
  MOCK_METHOD2(EquivClassToEquivClass,
               pair<Cost_t, uint64_t>(EquivClass_t ec1, EquivClass_t ec2));
  MOCK_METHOD1(GetTaskEquivClasses, vector<EquivClass_t>*(TaskID_t task_id));
  MOCK_METHOD1(GetOutgoingEquivClassPrefArcs,
               vector<ResourceID_t>*(EquivClass_t ec));
  MOCK_METHOD1(GetTaskPreferenceArcs, vector<ResourceID_t>*(TaskID_t task_id));
  MOCK_METHOD1(GetEquivClassToEquivClassesArcs,
              vector<EquivClass_t>*(EquivClass_t ec));
  MOCK_METHOD1(AddMachine, void(ResourceTopologyNodeDescriptor* rtnd_ptr));
  MOCK_METHOD1(AddTask, void(TaskID_t task_id));
  MOCK_METHOD1(RemoveMachine, void(ResourceID_t res_id));
  MOCK_METHOD1(RemoveTask, void(TaskID_t task_id));
  MOCK_METHOD2(GatherStats,
               FlowGraphNode*(FlowGraphNode* acc, FlowGraphNode* other));
  MOCK_METHOD1(PrepareStats, void(FlowGraphNode* acc));
  MOCK_METHOD2(UpdateStats,
               FlowGraphNode*(FlowGraphNode* acc, FlowGraphNode* other));
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_MOCK_COST_MODEL_H
