// The Firmament project
// Copyright (c) 2015-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Tests for the Google trace bridge.

#include <gtest/gtest.h>

#include "misc/utils.h"
#include "sim/trace-extract/google_trace_bridge.h"
#include "sim/trace-extract/google_trace_loader.h"

DECLARE_string(machine_tmpl_file);

namespace firmament {
namespace sim {

class GoogleTraceBridgeTest : public ::testing::Test {
 protected:
  GoogleTraceBridgeTest()
    : event_manager_(new GoogleTraceEventManager()),
      bridge_(new GoogleTraceBridge("", event_manager_)),
      loader_(new GoogleTraceLoader("")) {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
    FLAGS_machine_tmpl_file = "../../../../tests/testdata/machine_topo.pbin";
  }

  virtual ~GoogleTraceBridgeTest() {
    //    delete bridge_;
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

  GoogleTraceEventManager* event_manager_;
  GoogleTraceBridge* bridge_;
  GoogleTraceLoader* loader_;
};

TEST_F(GoogleTraceBridgeTest, AddMachine) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  loader_->LoadMachineTemplate(&machine_tmpl);
  CHECK_EQ(bridge_->resource_map_->size(), 1);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 0);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 0);
  // Add first machine.
  bridge_->AddMachine(machine_tmpl, 1);
  CHECK_EQ(bridge_->resource_map_->size(), 24);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 1);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 8);
  // Add second machine.
  bridge_->AddMachine(machine_tmpl, 2);
  CHECK_EQ(bridge_->resource_map_->size(), 47);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 2);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 16);
}

TEST_F(GoogleTraceBridgeTest, AddTask) {
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  CHECK_EQ(bridge_->job_map_->size(), 0);
  CHECK_EQ(bridge_->trace_job_id_to_jd_.size(), 0);
  CHECK_EQ(bridge_->task_map_->size(), 0);
  CHECK_EQ(bridge_->task_id_to_identifier_.size(), 0);
  CHECK_EQ(bridge_->trace_task_id_to_td_.size(), 0);
  // Add the first task.
  TaskDescriptor* td1_ptr = bridge_->AddTask(trace_task_id);
  CHECK_NOTNULL(td1_ptr);
  CHECK_EQ(td1_ptr->state(), TaskDescriptor::CREATED);
  CHECK_EQ(bridge_->job_map_->size(), 1);
  CHECK_EQ(ResourceIDFromString(td1_ptr->job_id()),
           bridge_->job_map_->begin()->first);
  CHECK_EQ(bridge_->trace_job_id_to_jd_.size(), 1);
  CHECK_EQ(bridge_->task_map_->size(), 2);
  CHECK_EQ(bridge_->task_id_to_identifier_.size(), 1);
  CHECK_EQ(bridge_->trace_task_id_to_td_.size(), 1);
  // Add the second task.
  trace_task_id.task_index = 2;
  TaskDescriptor* td2_ptr = bridge_->AddTask(trace_task_id);
  CHECK_NOTNULL(td2_ptr);
  CHECK_EQ(td2_ptr->state(), TaskDescriptor::CREATED);
  CHECK_EQ(bridge_->job_map_->size(), 1);
  CHECK_EQ(ResourceIDFromString(td1_ptr->job_id()),
           bridge_->job_map_->begin()->first);
  CHECK_EQ(bridge_->trace_job_id_to_jd_.size(), 1);
  CHECK_EQ(bridge_->task_map_->size(), 3);
  CHECK_EQ(bridge_->task_id_to_identifier_.size(), 2);
  CHECK_EQ(bridge_->trace_task_id_to_td_.size(), 2);
}

TEST_F(GoogleTraceBridgeTest, OnJobCompletion) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  loader_->LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  TaskDescriptor* td_ptr = bridge_->AddTask(trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->job_num_tasks_, trace_task_id.job_id, 1));
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_, trace_task_id, 10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  bridge_->OnTaskCompletion(td_ptr, pu_rd_ptr);
  JobID_t job_id = JobIDFromString(td_ptr->job_id());
  bridge_->OnJobCompletion(job_id);
  // We've erased the root task from the task_map_.
  CHECK_EQ(bridge_->task_map_->size(), 1);
  CHECK(FindOrNull(*bridge_->job_map_, job_id) == NULL);
  CHECK(FindOrNull(bridge_->trace_job_id_to_jd_, trace_task_id.job_id) == NULL);
  CHECK(FindOrNull(bridge_->job_num_tasks_, trace_task_id.job_id) == NULL);
  CHECK(FindOrNull(bridge_->job_id_to_trace_job_id_, job_id) == NULL);
}

TEST_F(GoogleTraceBridgeTest, OnTaskCompletion) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  loader_->LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  TaskDescriptor* td_ptr = bridge_->AddTask(trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->job_num_tasks_, trace_task_id.job_id, 1));
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_, trace_task_id, 10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  bridge_->OnTaskCompletion(td_ptr, pu_rd_ptr);
  CHECK(FindOrNull(bridge_->task_runtime_, trace_task_id) == NULL);
  CHECK(FindOrNull(bridge_->trace_task_id_to_td_, trace_task_id) == NULL);
  CHECK(FindOrNull(bridge_->task_id_to_identifier_, td_ptr->uid()) == NULL);
  uint64_t* num_tasks =
    FindOrNull(bridge_->job_num_tasks_, trace_task_id.job_id);
  CHECK_EQ(*num_tasks, 0);
  // We don't erase the task from the task_map_.
  CHECK_EQ(bridge_->task_map_->size(), 2);
}

TEST_F(GoogleTraceBridgeTest, OnTaskEviction) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  loader_->LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  TaskDescriptor* td_ptr = bridge_->AddTask(trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_, trace_task_id, 10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  bridge_->OnTaskEviction(td_ptr, pu_rd_ptr);
  // Check that the end event has been removed.
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), UINT64_MAX);
  // Check that the start time has been reset.
  CHECK_EQ(td_ptr->start_time(), 0);
}

TEST_F(GoogleTraceBridgeTest, OnTaskMigration) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, OnTaskPlacement) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  loader_->LoadMachineTemplate(&machine_tmpl);
  TraceTaskIdentifier trace_task_id;
  trace_task_id.job_id = 1;
  trace_task_id.task_index = 1;
  TaskDescriptor* td_ptr = bridge_->AddTask(trace_task_id);
  ResourceDescriptor* pu_rd_ptr = bridge_->machine_res_id_pus_.begin()->second;
  CHECK(InsertIfNotPresent(&bridge_->task_runtime_, trace_task_id, 10));
  bridge_->OnTaskPlacement(td_ptr, pu_rd_ptr);
  // Check that the end event has been added.
  CHECK_EQ(event_manager_->GetTimeOfNextEvent(), 10);
  CHECK_EQ(td_ptr->start_time(), 0);
}

TEST_F(GoogleTraceBridgeTest, RemoveMachine) {
  ResourceTopologyNodeDescriptor machine_tmpl;
  loader_->LoadMachineTemplate(&machine_tmpl);
  CHECK_EQ(bridge_->resource_map_->size(), 1);
  CHECK_EQ(bridge_->trace_machine_id_to_rtnd_.size(), 0);
  CHECK_EQ(bridge_->machine_res_id_pus_.size(), 0);
  bridge_->AddMachine(machine_tmpl, 1);
  CHECK_EQ(bridge_->resource_map_->size(), 24);
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
