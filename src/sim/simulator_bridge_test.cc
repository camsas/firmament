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

// Tests for the simulator bridge.

#include <gtest/gtest.h>

#include "misc/utils.h"
#include "sim/google_trace_loader.h"
#include "sim/simulated_wall_time.h"
#include "sim/simulator_bridge.h"
#include "sim/trace_utils.h"

DECLARE_string(machine_tmpl_file);
DEFINE_string(scheduler, "flow", "The scheduler to use for tests.");

namespace firmament {
namespace sim {

class SimulatorBridgeTest : public ::testing::Test {
 protected:
  SimulatorBridgeTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
    FLAGS_scheduler = "flow";
    FLAGS_machine_tmpl_file = "../../tests/testdata/mach_8pus.pbin";
    simulated_time_ = new SimulatedWallTime();
    event_manager_ = new EventManager(simulated_time_);
    bridge_ = new SimulatorBridge(event_manager_, simulated_time_);
    loader_ = new GoogleTraceLoader(event_manager_);
  }

  virtual ~SimulatorBridgeTest() {
    delete bridge_;
    delete event_manager_;
    delete loader_;
  }

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  SimulatedWallTime* simulated_time_;
  EventManager* event_manager_;
  SimulatorBridge* bridge_;
  GoogleTraceLoader* loader_;
};

TEST_F(SimulatorBridgeTest, AddMachine) {
  CHECK_EQ(bridge_->resource_map_->size(), 1);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 0);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 0);
  // Add first machine.
  bridge_->AddMachine(1);
  CHECK_EQ(bridge_->resource_map_->size(), 10);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 1);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 8);
  // Add second machine.
  bridge_->AddMachine(2);
  CHECK_EQ(bridge_->resource_map_->size(), 19);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 2);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 16);
}

TEST_F(SimulatorBridgeTest, AddTask) {
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  CHECK_EQ(bridge_->job_map_->size(), 0);
  CHECK_EQ(bridge_->trace_job_id_to_jd_.size(), 0);
  CHECK_EQ(bridge_->task_map_->size(), 0);
  CHECK_EQ(bridge_->task_id_to_identifier_.size(), 0);
  CHECK_EQ(bridge_->trace_task_id_to_td_.size(), 0);
  // Add the first task.
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_SUBMIT);
  event_desc.set_requested_ram(1024);
  event_desc.set_requested_cpu_cores(1000);
  bridge_->AddTask(trace_task_id, event_desc);
  TaskDescriptor* td1_ptr =
    FindPtrOrNull(bridge_->trace_task_id_to_td_, trace_task_id);
  CHECK_NOTNULL(td1_ptr);
  CHECK_EQ(td1_ptr->state(), TaskDescriptor::CREATED);
  CHECK_EQ(bridge_->job_map_->size(), 1);
  CHECK_EQ(ResourceIDFromString(td1_ptr->job_id()),
           bridge_->job_map_->begin()->first);
  CHECK_EQ(bridge_->trace_job_id_to_jd_.size(), 1);
  CHECK_EQ(bridge_->task_map_->size(), 1);
  CHECK_EQ(bridge_->task_id_to_identifier_.size(), 1);
  CHECK_EQ(bridge_->trace_task_id_to_td_.size(), 1);
  // Add the second task.
  trace_task_id.task_index = 2;
  bridge_->AddTask(trace_task_id, event_desc);
  TaskDescriptor* td2_ptr =
    FindPtrOrNull(bridge_->trace_task_id_to_td_, trace_task_id);
  CHECK_NOTNULL(td2_ptr);
  CHECK_EQ(td2_ptr->state(), TaskDescriptor::CREATED);
  CHECK_EQ(bridge_->job_map_->size(), 1);
  CHECK_EQ(ResourceIDFromString(td1_ptr->job_id()),
           bridge_->job_map_->begin()->first);
  CHECK_EQ(bridge_->trace_job_id_to_jd_.size(), 1);
  CHECK_EQ(bridge_->task_map_->size(), 2);
  CHECK_EQ(bridge_->task_id_to_identifier_.size(), 2);
  CHECK_EQ(bridge_->trace_task_id_to_td_.size(), 2);
}

TEST_F(SimulatorBridgeTest, OnJobCompletion) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_SUBMIT);
  event_desc.set_requested_ram(1024);
  event_desc.set_requested_cpu_cores(1000);
  bridge_->AddMachine(1);
  bridge_->AddTask(trace_task_id, event_desc);
  TaskDescriptor* td_ptr =
    FindPtrOrNull(bridge_->trace_task_id_to_td_, trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->job_num_tasks_, trace_task_id.job_id, 1));
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_,
                           GenerateTaskIDFromTraceIdentifier(trace_task_id),
                           10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  bridge_->OnTaskCompletion(td_ptr, pu_rd_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  bridge_->OnJobCompletion(job_id);
  // We've erased the root task from the task_map_.
  CHECK_EQ(bridge_->task_map_->size(), 0);
  CHECK(FindOrNull(*bridge_->job_map_, job_id) == NULL);
  CHECK(FindOrNull(bridge_->trace_job_id_to_jd_, trace_task_id.job_id) == NULL);
  CHECK(FindOrNull(bridge_->job_num_tasks_, trace_task_id.job_id) == NULL);
  CHECK(FindOrNull(bridge_->job_id_to_trace_job_id_, job_id) == NULL);
}

TEST_F(SimulatorBridgeTest, OnTaskCompletion) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_SUBMIT);
  event_desc.set_requested_ram(1024);
  event_desc.set_requested_cpu_cores(1000);
  bridge_->AddMachine(1);
  bridge_->AddTask(trace_task_id, event_desc);
  TaskDescriptor* td_ptr =
    FindPtrOrNull(bridge_->trace_task_id_to_td_, trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->job_num_tasks_, trace_task_id.job_id, 1));
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_,
                           GenerateTaskIDFromTraceIdentifier(trace_task_id),
                           10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  bridge_->OnTaskCompletion(td_ptr, pu_rd_ptr);
  CHECK(FindOrNull(bridge_->task_runtime_,
                   GenerateTaskIDFromTraceIdentifier(trace_task_id)) == NULL);
  CHECK(FindOrNull(bridge_->trace_task_id_to_td_, trace_task_id) == NULL);
  CHECK(FindOrNull(bridge_->task_id_to_identifier_, td_ptr->uid()) == NULL);
  uint64_t* num_tasks =
    FindOrNull(bridge_->job_num_tasks_, trace_task_id.job_id);
  CHECK_EQ(*num_tasks, 0);
  // We don't erase the task from the task_map_.
  CHECK_EQ(bridge_->task_map_->size(), 1);
}

TEST_F(SimulatorBridgeTest, OnTaskEviction) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_SUBMIT);
  event_desc.set_requested_ram(1024);
  event_desc.set_requested_cpu_cores(1000);
  bridge_->AddMachine(1);
  bridge_->AddTask(trace_task_id, event_desc);
  TaskDescriptor* td_ptr =
    FindPtrOrNull(bridge_->trace_task_id_to_td_, trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_,
                           GenerateTaskIDFromTraceIdentifier(trace_task_id),
                           10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  bridge_->OnTaskEviction(td_ptr, pu_rd_ptr);
  // Check that the end event has been removed.
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), UINT64_MAX);
  // Check that the start time has been reset.
  CHECK_EQ(td_ptr->start_time(), 0);
}

TEST_F(SimulatorBridgeTest, OnTaskMigration) {
  // TODO(ionel): Implement!
}

TEST_F(SimulatorBridgeTest, OnTaskPlacement) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_SUBMIT);
  event_desc.set_requested_ram(1024);
  event_desc.set_requested_cpu_cores(1000);
  bridge_->AddMachine(1);
  bridge_->AddTask(trace_task_id, event_desc);
  TaskDescriptor* td_ptr =
    FindPtrOrNull(bridge_->trace_task_id_to_td_, trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_,
                           GenerateTaskIDFromTraceIdentifier(trace_task_id),
                           10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  // Check that the end event has been added.
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  CHECK_EQ(td_ptr->start_time(), 0);
}

TEST_F(SimulatorBridgeTest, RemoveMachine) {
  CHECK_EQ(bridge_->resource_map_->size(), 1);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 0);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 0);
  bridge_->AddMachine(1);
  CHECK_EQ(bridge_->resource_map_->size(), 10);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 1);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 8);
  bridge_->RemoveMachine(1);
  CHECK_EQ(bridge_->resource_map_->size(), 1);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 0);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 0);
}

} // namespace sim
} // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_logtostderr = true;
  FLAGS_stderrthreshold = 0;
  return RUN_ALL_TESTS();
}
