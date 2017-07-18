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

// Tests for DIMACS exporter for CS2 solver.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <fcntl.h>

#include <vector>
#include <map>

#include <boost/bind.hpp>

#include "base/common.h"
#include "misc/trace_generator.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/wall_time.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/dimacs_exporter.h"
#include "scheduling/flow/flow_graph_manager.h"
#include "scheduling/flow/trivial_cost_model.h"

namespace firmament {

// The fixture for testing the DIMACSExporter container class.
class DIMACSExporterTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  DIMACSExporterTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 1;
  }

  virtual ~DIMACSExporterTest() {
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
  }

  void reset_uuid(ResourceTopologyNodeDescriptor* rtnd) {
    string old_parent_id = rtnd->parent_id();
    rtnd->set_parent_id(*FindOrNull(uuid_conversion_map_, rtnd->parent_id()));
    string new_uuid = to_string(GenerateResourceID());
    VLOG(2) << "Resetting UUID for " << rtnd->resource_desc().uuid()
            << " to " << new_uuid << ", parent is " << rtnd->parent_id()
            << ", was " << old_parent_id;
    InsertOrUpdate(&uuid_conversion_map_, rtnd->resource_desc().uuid(),
                   new_uuid);
    rtnd->mutable_resource_desc()->set_uuid(new_uuid);
  }
  // Objects declared here can be used by all tests.
  map<string, string> uuid_conversion_map_;
  // Enable access from tests
  FRIEND_TEST(DIMACSExporterTest, LargeGraph);
  FRIEND_TEST(DIMACSExporterTest, ScalabilityTestGraphs);
};

// Tests allocation of an empty envelope and puts an integer into it (using
// memcopy internally).
TEST_F(DIMACSExporterTest, SimpleGraphOutput) {
  shared_ptr<ResourceMap_t> resource_map =
    shared_ptr<ResourceMap_t>(new ResourceMap_t);
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  DIMACSChangeStats dimacs_stats;
  WallTime wall_time;
  TraceGenerator trace_generator(&wall_time);
  FlowGraphManager flow_graph_manager(
      new TrivialCostModel(resource_map, task_map, leaf_res_ids), leaf_res_ids,
      &wall_time, &trace_generator, &dimacs_stats);
  // Test resource topology
  ResourceTopologyNodeDescriptor rtn_root;
  string root_id = to_string(GenerateResourceID("test"));
  rtn_root.mutable_resource_desc()->set_uuid(root_id);
  rtn_root.mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
  ResourceTopologyNodeDescriptor* rtn_c1 = rtn_root.add_children();
  string c1_uid = to_string(GenerateResourceID("test-c1"));
  rtn_c1->mutable_resource_desc()->set_uuid(c1_uid);
  rtn_c1->set_parent_id(root_id);
  rtn_c1->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  ResourceTopologyNodeDescriptor* rtn_c2 = rtn_root.add_children();
  string c2_uid = to_string(GenerateResourceID("test-c2"));
  rtn_c2->mutable_resource_desc()->set_uuid(c2_uid);
  rtn_c2->set_parent_id(root_id);
  rtn_c2->mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_PU);
  // Test job
  JobDescriptor jd;
  jd.set_uuid(to_string(GenerateJobID()));
  TaskDescriptor* rt = jd.mutable_root_task();
  rt->set_uid(GenerateRootTaskID(jd));
  rt->set_state(TaskDescriptor::RUNNABLE);
  rt->set_job_id(jd.uuid());
  TaskDescriptor* ct1 = rt->add_spawned();
  ct1->set_uid(GenerateTaskID(*rt));
  ct1->set_state(TaskDescriptor::RUNNABLE);
  ct1->set_job_id(jd.uuid());
  TaskDescriptor* ct2 = rt->add_spawned();
  ct2->set_uid(GenerateTaskID(*rt));
  ct2->set_state(TaskDescriptor::RUNNABLE);
  ct2->set_job_id(jd.uuid());
  CHECK(InsertIfNotPresent(task_map.get(), rt->uid(), rt));
  CHECK(InsertIfNotPresent(task_map.get(), ct1->uid(), ct1));
  CHECK(InsertIfNotPresent(task_map.get(), ct2->uid(), ct2));
  // Add resources and job to flow graph
  flow_graph_manager.AddResourceTopology(&rtn_root);
  vector<JobDescriptor*> jd_ptr_vect;
  jd_ptr_vect.push_back(&jd);
  flow_graph_manager.AddOrUpdateJobNodes(jd_ptr_vect);
  // Export
  DIMACSExporter exp;
  FILE* debug_out_file;
  CHECK((debug_out_file = fopen("test.dm", "w")) != NULL);
  exp.Export(flow_graph_manager.graph_change_manager_->flow_graph(),
             debug_out_file);
  fclose(debug_out_file);
  delete leaf_res_ids;
}

// Runs the graph export for a single simulated graph (somewhat simplified),
// with the following parameters:
//  - 2500 machines
//  - 100 jobs of 100 tasks each
//  - 20 preference edges per task
TEST_F(DIMACSExporterTest, LargeGraph) {
  shared_ptr<ResourceMap_t> resource_map =
    shared_ptr<ResourceMap_t>(new ResourceMap_t);
  shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
    new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
  DIMACSChangeStats dimacs_stats;
  WallTime wall_time;
  TraceGenerator trace_generator(&wall_time);
  FlowGraphManager flow_graph_manager(
      new TrivialCostModel(resource_map, task_map, leaf_res_ids), leaf_res_ids,
      &wall_time, &trace_generator, &dimacs_stats);
  // Test resource topology
  ResourceTopologyNodeDescriptor machine_tmpl;
  int fd = open("../tests/testdata/machine_topo.pbin", O_RDONLY);
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);
  // Create N machines
  uint64_t n = 2500;
  ResourceTopologyNodeDescriptor rtn_root;
  ResourceID_t root_uuid = GenerateResourceID("test");
  rtn_root.mutable_resource_desc()->set_uuid(to_string(root_uuid));
  rtn_root.mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
  InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                     to_string(root_uuid));
  for (uint64_t i = 0; i < n; ++i) {
    ResourceTopologyNodeDescriptor* child = rtn_root.add_children();
    child->CopyFrom(machine_tmpl);
    child->set_parent_id(rtn_root.resource_desc().uuid());
    DFSTraverseResourceProtobufTreeReturnRTND(
        child, boost::bind(&DIMACSExporterTest::reset_uuid, this, _1));
  }
  VLOG(1) << "Added " << n << " machines.";
  // Add resources and job to flow graph
  flow_graph_manager.AddResourceTopology(&rtn_root);
  // Test job
  uint64_t j = 100;
  uint64_t t = 100;
  const vector<uint64_t> leaf_ids(flow_graph_manager.leaf_node_ids().begin(),
                                  flow_graph_manager.leaf_node_ids().end());
  uint32_t seed = static_cast<uint32_t>(time(NULL));
  for (uint64_t i = 0; i < j; ++i) {
    JobDescriptor* jd = new JobDescriptor;
    jd->set_uuid(to_string(GenerateJobID()));
    TaskDescriptor* rt = jd->mutable_root_task();
    string bin;
    spf(&bin, "%jd", rand_r(&seed));
    rt->set_binary(bin);
    rt->set_uid(GenerateRootTaskID(*jd));
    rt->set_job_id(jd->uuid());
    CHECK(InsertIfNotPresent(task_map.get(), rt->uid(), rt));
    for (uint64_t k = 1; k < t; ++k) {
      TaskDescriptor* ct = rt->add_spawned();
      ct->set_uid(GenerateTaskID(*rt));
      ct->set_state(TaskDescriptor::RUNNABLE);
      ct->set_job_id(jd->uuid());
      CHECK(InsertIfNotPresent(task_map.get(), ct->uid(), ct));
    }
    vector<JobDescriptor*> jd_ptr_vect;
    jd_ptr_vect.push_back(jd);
    flow_graph_manager.AddOrUpdateJobNodes(jd_ptr_vect);
  }
  VLOG(1) << "Added " << j*t << " tasks in " << j << " jobs (" << t
          << " tasks each).";
  // Export
  DIMACSExporter exp;
  FILE* debug_out_file;
  CHECK((debug_out_file = fopen("/tmp/test.dm", "w")) != NULL);
  exp.Export(flow_graph_manager.graph_change_manager_->flow_graph(),
             debug_out_file);
  fclose(debug_out_file);
  delete leaf_res_ids;
}

// Outputs simplified flow graphs for up to 8092 jobs (in power-of-two steps)
// into /tmp/testYYY.dm, where YYY is the number of jobs. Edit the use of f
// below to scale the number of machines instead.
// XXX(malte): Reduced the number of machines and jobs by two orders of
// magnitude here, so that the tests don't take forever when run in batch mode.
// Adapt as required :)
TEST_F(DIMACSExporterTest, ScalabilityTestGraphs) {
  shared_ptr<ResourceMap_t> resource_map =
    shared_ptr<ResourceMap_t>(new ResourceMap_t);
  for (uint64_t f = 1; f < 100; f *= 2) {
    shared_ptr<TaskMap_t> task_map = shared_ptr<TaskMap_t>(new TaskMap_t);
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids =
      new unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>;
    DIMACSChangeStats dimacs_stats;
    WallTime wall_time;
    TraceGenerator trace_generator(&wall_time);
    FlowGraphManager flow_graph_manager(
        new TrivialCostModel(resource_map, task_map, leaf_res_ids),
        leaf_res_ids, &wall_time, &trace_generator, &dimacs_stats);
    // Test resource topology
    ResourceTopologyNodeDescriptor machine_tmpl;
    int fd = open("../tests/testdata/machine_topo.pbin", O_RDONLY);
    machine_tmpl.ParseFromFileDescriptor(fd);
    close(fd);
    // Create N machines
    uint64_t n = 120;
    ResourceTopologyNodeDescriptor rtn_root;
    ResourceID_t root_uuid = GenerateResourceID("test");
    rtn_root.mutable_resource_desc()->set_uuid(to_string(root_uuid));
    rtn_root.mutable_resource_desc()->set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
    InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                       to_string(root_uuid));
    for (uint64_t i = 0; i < n; ++i) {
      ResourceTopologyNodeDescriptor* child = rtn_root.add_children();
      child->CopyFrom(machine_tmpl);
      child->set_parent_id(rtn_root.resource_desc().uuid());
      DFSTraverseResourceProtobufTreeReturnRTND(
          child, boost::bind(&DIMACSExporterTest::reset_uuid, this, _1));
    }
    VLOG(1) << "Added " << n << " machines.";
    // Add resources and job to flow graph
    flow_graph_manager.AddResourceTopology(&rtn_root);
    // Test job
    uint64_t j = f;
    uint64_t t = 100;
    const vector<uint64_t> leaf_ids(flow_graph_manager.leaf_node_ids().begin(),
                                    flow_graph_manager.leaf_node_ids().end());
    uint32_t seed = static_cast<uint32_t>(time(NULL));
    for (uint64_t i = 0; i < j; ++i) {
      JobDescriptor* jd = new JobDescriptor;
      jd->set_uuid(to_string(GenerateJobID()));
      TaskDescriptor* rt = jd->mutable_root_task();
      string bin;
      spf(&bin, "%jd", rand_r(&seed));
      rt->set_binary(bin);
      rt->set_uid(GenerateRootTaskID(*jd));
      rt->set_job_id(jd->uuid());
      CHECK(InsertIfNotPresent(task_map.get(), rt->uid(), rt));
      for (uint64_t k = 1; k < t; ++k) {
        TaskDescriptor* ct = rt->add_spawned();
        ct->set_uid(GenerateTaskID(*rt));
        ct->set_state(TaskDescriptor::RUNNABLE);
        ct->set_job_id(jd->uuid());
        CHECK(InsertIfNotPresent(task_map.get(), ct->uid(), ct));
      }
      vector<JobDescriptor*> jd_ptr_vect;
      jd_ptr_vect.push_back(jd);
      flow_graph_manager.AddOrUpdateJobNodes(jd_ptr_vect);
    }
    // Export
    DIMACSExporter exp;
    FILE* debug_out_file;
    string outname;
    spf(&outname, "/tmp/test%jd.dm", f);
    VLOG(1) << "Output written to " << outname;
    CHECK((debug_out_file = fopen(outname.c_str(), "w")) != NULL);
    exp.Export(flow_graph_manager.graph_change_manager_->flow_graph(),
               debug_out_file);
    fclose(debug_out_file);
    delete leaf_res_ids;
  }
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
