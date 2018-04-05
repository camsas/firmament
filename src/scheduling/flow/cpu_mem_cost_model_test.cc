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

// Tests for cpu memory cost model.

#include "scheduling/flow/cpu_mem_cost_model.h"
#include <gtest/gtest.h>
#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/label_utils.h"

DECLARE_uint64(max_multi_arcs_for_cpu);
DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

// The fixture for testing the CpuMemCostModel.
class CpuMemCostModelTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  CpuMemCostModelTest() {
    // You can do set-up work for each test here.
    resource_map_.reset(new ResourceMap_t);
    task_map_.reset(new TaskMap_t);
    knowledge_base_.reset(new KnowledgeBase);
    cost_model = new CpuMemCostModel(resource_map_, task_map_, knowledge_base_);
  }

  virtual ~CpuMemCostModelTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
    delete cost_model;
  }

  // TODO(shivramsrivastava): Copied from CreateTask() func from
  // flow_grap_manager_test. Need to write common unit test utility module.
  TaskDescriptor* CreateTask(JobDescriptor* jd_ptr, uint64_t job_id_seed) {
    JobID_t job_id = GenerateJobID(job_id_seed);
    jd_ptr->set_uuid(to_string(job_id));
    jd_ptr->set_name(to_string(job_id));
    TaskDescriptor* td_ptr = jd_ptr->mutable_root_task();
    td_ptr->set_uid(GenerateRootTaskID(*jd_ptr));
    td_ptr->set_job_id(jd_ptr->uuid());
    return td_ptr;
  }

  CpuMemCostModel* cost_model;
  boost::shared_ptr<ResourceMap_t> resource_map_;
  boost::shared_ptr<TaskMap_t> task_map_;
  boost::shared_ptr<KnowledgeBase> knowledge_base_;
};

TEST_F(CpuMemCostModelTest, AccumulateResourceStats) {
  ResourceDescriptor machine_rd;
  ResourceDescriptor pu1_rd;
  ResourceDescriptor pu2_rd;
  ResourceVector* pu1_available_resources =
      pu1_rd.mutable_available_resources();
  ResourceVector* pu2_available_resources =
      pu2_rd.mutable_available_resources();
  ResourceVector* machine_available_resources =
      machine_rd.mutable_available_resources();
  pu1_available_resources->set_cpu_cores(900.0);
  pu2_available_resources->set_cpu_cores(800.0);
  pu1_rd.set_num_running_tasks_below(5);
  pu2_rd.set_num_running_tasks_below(10);
  pu1_rd.set_num_slots_below(45);
  pu2_rd.set_num_slots_below(40);
  cost_model->AccumulateResourceStats(&machine_rd, &pu1_rd);
  cost_model->AccumulateResourceStats(&machine_rd, &pu2_rd);
  EXPECT_FLOAT_EQ(1700.0, machine_available_resources->cpu_cores());
  EXPECT_EQ(15UL, machine_rd.num_running_tasks_below());
  EXPECT_EQ(85UL, machine_rd.num_slots_below());
}

TEST_F(CpuMemCostModelTest, AddMachine) {
  ResourceID_t res_id = GenerateResourceID("test");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  cost_model->AddMachine(&rtnd);
  EXPECT_EQ(1UL, cost_model->ecs_for_machines_.size());
  unordered_map<ResourceID_t, vector<EquivClass_t>,
                boost::hash<ResourceID_t>>::iterator it =
      cost_model->ecs_for_machines_.find(res_id);
  EXPECT_NE(cost_model->ecs_for_machines_.end(), it);
  EXPECT_EQ(FLAGS_max_multi_arcs_for_cpu, it->second.size());
  // Remove test machine.
  cost_model->RemoveMachine(res_id);
}

TEST_F(CpuMemCostModelTest, AddTask) {
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 42);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(10.0);
  cost_model->AddTask(task_id);
  unordered_map<TaskID_t, CpuMemCostVector_t>::iterator it =
      cost_model->task_resource_requirement_.find(task_id);
  EXPECT_NE(cost_model->task_resource_requirement_.end(), it);
  EXPECT_FLOAT_EQ(10.0, it->second.cpu_cores_);
  EXPECT_EQ(1UL, cost_model->task_resource_requirement_.size());
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->erase(task_id);
}

TEST_F(CpuMemCostModelTest, EquivClassToEquivClass) {
  // Create Task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 43);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
  td_ptr->mutable_resource_request()->set_ram_cap(1000);
  cost_model->AddTask(task_id);
  // Get task equivalence classes.
  vector<EquivClass_t>* equiv_classes =
      cost_model->GetTaskEquivClasses(task_id);
  // Create machine and its equivalence classes.
  ResourceID_t res_id = GenerateResourceID("test1");
  ResourceTopologyNodeDescriptor rtnd;
  ResourceDescriptor* rd_ptr = rtnd.mutable_resource_desc();
  rd_ptr->set_uuid(to_string(res_id));
  rd_ptr->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceVector* resource_capacity = rd_ptr->mutable_resource_capacity();
  ResourceVector* available_resources = rd_ptr->mutable_available_resources();
  resource_capacity->set_cpu_cores(1000.0);
  resource_capacity->set_ram_cap(32000);
  available_resources->set_cpu_cores(500.0);
  available_resources->set_ram_cap(16000);
  ResourceStatus resource_status =
      ResourceStatus(rd_ptr, &rtnd, rd_ptr->friendly_name(), 0);
  ResourceStatus* rs_ptr = &resource_status;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));
  cost_model->AddMachine(&rtnd);
  unordered_map<ResourceID_t, vector<EquivClass_t>,
                boost::hash<ResourceID_t>>::iterator it =
      cost_model->ecs_for_machines_.find(res_id);
  EXPECT_EQ(res_id, it->first);
  vector<EquivClass_t> machine_equiv_classes = it->second;
  // Calculate cost of arc between main EC and first machine EC.
  ArcDescriptor arc_cost1 = cost_model->EquivClassToEquivClass(
      (*equiv_classes)[0], machine_equiv_classes[0]);
  // Calculate cost of arc between main EC and second machine EC.
  ArcDescriptor arc_cost2 = cost_model->EquivClassToEquivClass(
      (*equiv_classes)[0], machine_equiv_classes[1]);
  EXPECT_EQ(1000, arc_cost1.cost_);
  EXPECT_EQ(1051, arc_cost2.cost_);
  // Cost of arc between main EC and first machine EC should be less than
  // cost of arc between main EC and second machine EC.
  EXPECT_LT(arc_cost1.cost_, arc_cost2.cost_);
  // Clean up.
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->find(task_id);
  delete equiv_classes;
  // Remove test machine.
  cost_model->RemoveMachine(res_id);
  cost_model->resource_map_.get()->erase(res_id);
}

TEST_F(CpuMemCostModelTest, GetEquivClassToEquivClassesArcs) {
  // Create Task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 44);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
  td_ptr->mutable_resource_request()->set_ram_cap(1000);
  cost_model->AddTask(task_id);
  // Get task equivalence classes.
  vector<EquivClass_t>* equiv_classes =
      cost_model->GetTaskEquivClasses(task_id);
  // Create machine1 and its equivalence classes.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceVector* resource_capacity1 = rd_ptr1->mutable_resource_capacity();
  ResourceVector* available_resources1 = rd_ptr1->mutable_available_resources();
  resource_capacity1->set_cpu_cores(1000.0);
  resource_capacity1->set_ram_cap(32000);
  available_resources1->set_cpu_cores(500.0);
  available_resources1->set_ram_cap(16000);
  ResourceStatus resource_status1 =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status1;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  cost_model->AddMachine(&rtnd1);
  // Create machine2 and its equivalence classes.
  ResourceID_t res_id2 = GenerateResourceID("Machine2");
  ResourceTopologyNodeDescriptor rtnd2;
  ResourceDescriptor* rd_ptr2 = rtnd2.mutable_resource_desc();
  rd_ptr2->set_friendly_name("Machine2");
  rd_ptr2->set_uuid(to_string(res_id2));
  rd_ptr2->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceVector* resource_capacity2 = rd_ptr2->mutable_resource_capacity();
  ResourceVector* available_resources2 = rd_ptr2->mutable_available_resources();
  resource_capacity2->set_cpu_cores(1000.0);
  resource_capacity2->set_ram_cap(32000);
  available_resources2->set_cpu_cores(500.0);
  available_resources2->set_ram_cap(16000);
  ResourceStatus resource_status2 =
      ResourceStatus(rd_ptr2, &rtnd2, rd_ptr2->friendly_name(), 0);
  ResourceStatus* rs_ptr2 = &resource_status2;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id2, rs_ptr2));
  cost_model->AddMachine(&rtnd2);
  vector<EquivClass_t>* equiv_to_equiv_arcs =
      cost_model->GetEquivClassToEquivClassesArcs((*equiv_classes)[0]);
  // Since machine1 & machine2 can fit 16 tasks each, we expect no. of
  // EquivClassToEquivClassesArcs to be 32.
  EXPECT_EQ(32U, equiv_to_equiv_arcs->size());
  // Clean up.
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->find(task_id);
  delete equiv_classes;
  // Remove test machine1.
  cost_model->RemoveMachine(res_id1);
  cost_model->resource_map_.get()->erase(res_id1);
  // Remove test machine1.
  cost_model->RemoveMachine(res_id2);
  cost_model->resource_map_.get()->erase(res_id2);
  delete equiv_to_equiv_arcs;
}

TEST_F(CpuMemCostModelTest, GatherStats) {
  // Create machine Machine1.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceStatus resource_status1 =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status1;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  // Create PU node PU1.
  ResourceTopologyNodeDescriptor* pu_rtnd =
      rs_ptr1->mutable_topology_node()->add_children();
  ResourceID_t res_id2 = GenerateResourceID("PU1");
  ResourceDescriptor* rd_ptr2 = pu_rtnd->mutable_resource_desc();
  rd_ptr2->set_friendly_name("Machine1_PU #0");
  rd_ptr2->set_uuid(to_string(res_id2));
  rd_ptr2->set_type(ResourceDescriptor::RESOURCE_PU);
  rd_ptr2->add_current_running_tasks(1);
  rd_ptr2->add_current_running_tasks(2);
  pu_rtnd->set_parent_id(to_string(res_id1));
  ResourceStatus resource_status2 =
      ResourceStatus(rd_ptr2, pu_rtnd, rd_ptr2->friendly_name(), 0);
  ResourceStatus* rs_ptr2 = &resource_status2;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id2, rs_ptr2));
  // Add the test machine sample of machine to knowledge base.
  ResourceStats machine_test_stats;
  machine_test_stats.set_resource_id(to_string(res_id1));
  machine_test_stats.add_cpus_stats();
  CpuStats* pu_test_stats = machine_test_stats.mutable_cpus_stats(0);
  pu_test_stats->set_cpu_utilization(0.5);
  pu_test_stats->set_cpu_capacity(1000);
  machine_test_stats.set_mem_capacity(32000);
  machine_test_stats.set_mem_utilization(0.5);
  cost_model->knowledge_base_->AddMachineSample(machine_test_stats);
  // Create flow graph nodes.
  FlowGraphNode* machine_node = new FlowGraphNode(1);
  machine_node->type_ = FlowNodeType::MACHINE;
  machine_node->rd_ptr_ = rd_ptr1;
  machine_node->resource_id_ = res_id1;
  FlowGraphNode* pu_node = new FlowGraphNode(2);
  pu_node->type_ = FlowNodeType::PU;
  pu_node->rd_ptr_ = rd_ptr2;
  pu_node->resource_id_ = res_id2;
  FlowGraphNode* sink_node = new FlowGraphNode(-1);
  sink_node->type_ = FlowNodeType::SINK;
  // Test GatherStats from sink to PU.
  cost_model->GatherStats(pu_node, sink_node);
  EXPECT_FLOAT_EQ(500.0, rd_ptr2->available_resources().cpu_cores());
  EXPECT_EQ(2U, rd_ptr2->num_running_tasks_below());
  EXPECT_EQ(FLAGS_max_tasks_per_pu, rd_ptr2->num_slots_below());
  // Test GatherStats from PU to Machine.
  cost_model->GatherStats(machine_node, pu_node);
  EXPECT_FLOAT_EQ(500.0, rd_ptr1->available_resources().cpu_cores());
  EXPECT_EQ(16000U, rd_ptr1->available_resources().ram_cap());
  // Clean up.
  delete machine_node;
  delete pu_node;
  delete sink_node;
  cost_model->resource_map_.get()->erase(res_id1);
  cost_model->resource_map_.get()->erase(res_id2);
}

TEST_F(CpuMemCostModelTest, GetOutgoingEquivClassPrefArcs) {
  // Create machine1 and its equivalence classes.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceStatus resource_status =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  cost_model->AddMachine(&rtnd1);
  unordered_map<ResourceID_t, vector<EquivClass_t>,
                boost::hash<ResourceID_t>>::iterator it =
      cost_model->ecs_for_machines_.find(res_id1);
  EXPECT_EQ(res_id1, it->first);
  vector<EquivClass_t> machine_equiv_classes = it->second;
  vector<ResourceID_t>* equiv_class_outgoing_arcs =
      cost_model->GetOutgoingEquivClassPrefArcs(machine_equiv_classes[0]);
  // We expect only 1 arc from machine equivalence class to machine.
  EXPECT_EQ(1U, equiv_class_outgoing_arcs->size());
  // Clean up.
  // Remove test machine1.
  cost_model->RemoveMachine(res_id1);
  cost_model->resource_map_.get()->erase(res_id1);
  delete equiv_class_outgoing_arcs;
}

TEST_F(CpuMemCostModelTest, GetTaskEquivClasses) {
  // Create Task.
  JobDescriptor test_job;
  TaskDescriptor* td_ptr = CreateTask(&test_job, 45);
  InsertIfNotPresent(cost_model->task_map_.get(), td_ptr->uid(), td_ptr);
  TaskID_t task_id = td_ptr->uid();
  td_ptr->mutable_resource_request()->set_cpu_cores(20.0);
  td_ptr->mutable_resource_request()->set_ram_cap(1000);
  cost_model->AddTask(task_id);
  // Get task equivalence classes.
  vector<EquivClass_t>* equiv_classes =
      cost_model->GetTaskEquivClasses(task_id);
  // We expect only 1 equivalence class for corresponding to task resource
  // request.
  EXPECT_EQ(1U, equiv_classes->size());
  // Clean up.
  // Remove test task.
  cost_model->RemoveTask(task_id);
  cost_model->task_map_.get()->find(task_id);
  delete equiv_classes;
}

TEST_F(CpuMemCostModelTest, MachineResIDForResource) {
  // Create machine1 and its equivalence classes.
  ResourceID_t res_id1 = GenerateResourceID("Machine1");
  ResourceTopologyNodeDescriptor rtnd1;
  ResourceDescriptor* rd_ptr1 = rtnd1.mutable_resource_desc();
  rd_ptr1->set_friendly_name("Machine1");
  rd_ptr1->set_uuid(to_string(res_id1));
  rd_ptr1->set_type(ResourceDescriptor::RESOURCE_MACHINE);
  ResourceStatus resource_status1 =
      ResourceStatus(rd_ptr1, &rtnd1, rd_ptr1->friendly_name(), 0);
  ResourceStatus* rs_ptr1 = &resource_status1;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id1, rs_ptr1));
  cost_model->AddMachine(&rtnd1);
  // Create PU node.
  ResourceTopologyNodeDescriptor* pu_rtnd =
      rs_ptr1->mutable_topology_node()->add_children();
  ResourceID_t res_id2 = GenerateResourceID("PU1");
  ResourceDescriptor* rd_ptr2 = pu_rtnd->mutable_resource_desc();
  rd_ptr2->set_friendly_name("PU1");
  rd_ptr2->set_uuid(to_string(res_id2));
  rd_ptr2->set_type(ResourceDescriptor::RESOURCE_PU);
  pu_rtnd->set_parent_id(to_string(res_id1));
  ResourceStatus resource_status2 =
      ResourceStatus(rd_ptr2, pu_rtnd, rd_ptr2->friendly_name(), 0);
  ResourceStatus* rs_ptr2 = &resource_status2;
  CHECK(InsertIfNotPresent(resource_map_.get(), res_id2, rs_ptr2));
  ResourceID_t machine_res_id = cost_model->MachineResIDForResource(res_id2);
  EXPECT_EQ(machine_res_id, res_id1);
  // Clean up.
  // Remove test machine1.
  cost_model->RemoveMachine(res_id1);
  cost_model->resource_map_.get()->erase(res_id1);
  // Remove test pu1.
  // Not removing PU since its not added as machine.
  cost_model->resource_map_.get()->erase(res_id2);
}

}  // namespace firmament

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
