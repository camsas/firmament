// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Tests for DIMACS exporter for CS2 solver.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <fcntl.h>

#include <vector>
#include <map>

#include <boost/bind.hpp>

#include "base/common.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"
#include "scheduling/dimacs_exporter.h"
#include "scheduling/flow_graph.h"

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
    string new_uuid = to_string(GenerateUUID());
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
  FlowGraph g(FlowSchedulingCostModelType::COST_MODEL_TRIVIAL);
  // Test resource topology
  ResourceTopologyNodeDescriptor rtn_root;
  string root_id = to_string(GenerateUUID());
  rtn_root.mutable_resource_desc()->set_uuid(root_id);
  ResourceTopologyNodeDescriptor* rtn_c1 = rtn_root.add_children();
  string c1_uid = to_string(GenerateUUID());
  rtn_c1->mutable_resource_desc()->set_uuid(c1_uid);
  rtn_c1->set_parent_id(root_id);
  rtn_root.mutable_resource_desc()->add_children(c1_uid);
  ResourceTopologyNodeDescriptor* rtn_c2 = rtn_root.add_children();
  string c2_uid = to_string(GenerateUUID());
  rtn_c2->mutable_resource_desc()->set_uuid(c2_uid);
  rtn_c2->set_parent_id(root_id);
  rtn_root.mutable_resource_desc()->add_children(c2_uid);
  // Test job
  JobDescriptor jd;
  jd.set_uuid(to_string(GenerateJobID()));
  TaskDescriptor* rt = jd.mutable_root_task();
  rt->set_uid(GenerateRootTaskID(jd));
  TaskDescriptor* ct1 = rt->add_spawned();
  ct1->set_uid(GenerateTaskID(*rt));
  TaskDescriptor* ct2 = rt->add_spawned();
  ct2->set_uid(GenerateTaskID(*rt));
  // Add resources and job to flow graph
  g.AddResourceTopology(&rtn_root, 2);
  g.AddJobNodes(&jd);
  // Export
  DIMACSExporter exp;
  exp.Export(g);
  exp.Flush("test.dm");
}

// Runs the graph export for a single simulated graph (somewhat simplified),
// with the following parameters:
//  - 2500 machines
//  - 100 jobs of 100 tasks each
//  - 20 preference edges per task
TEST_F(DIMACSExporterTest, LargeGraph) {
  FlowGraph g(FlowSchedulingCostModelType::COST_MODEL_TRIVIAL);
  // Test resource topology
  ResourceTopologyNodeDescriptor machine_tmpl;
  int fd = open("../tests/testdata/machine_topo.pbin", O_RDONLY);
  machine_tmpl.ParseFromFileDescriptor(fd);
  close(fd);
  // Create N machines
  uint64_t n = 2500;
  ResourceTopologyNodeDescriptor rtn_root;
  ResourceID_t root_uuid = GenerateUUID();
  rtn_root.mutable_resource_desc()->set_uuid(to_string(root_uuid));
  InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                     to_string(root_uuid));
  for (uint64_t i = 0; i < n; ++i) {
    ResourceTopologyNodeDescriptor* child = rtn_root.add_children();
    child->CopyFrom(machine_tmpl);
    child->set_parent_id(rtn_root.resource_desc().uuid());
    TraverseResourceProtobufTreeReturnRTND(
        child, boost::bind(&DIMACSExporterTest::reset_uuid, this, _1));
  }
  VLOG(1) << "Added " << n << " machines.";
  // Add resources and job to flow graph
  g.AddResourceTopology(&rtn_root, n*8);
  // Test job
  uint64_t j = 100;
  uint64_t t = 100;
  const vector<uint64_t> leaf_ids(g.leaf_node_ids().begin(),
                                  g.leaf_node_ids().end());
  unsigned int seed = time(NULL);
  for (uint64_t i = 0; i < j; ++i) {
    JobDescriptor jd;
    jd.set_uuid(to_string(GenerateJobID()));
    TaskDescriptor* rt = jd.mutable_root_task();
    string bin;
    spf(&bin, "%jd", rand_r(&seed));
    rt->set_binary(bin);
    rt->set_uid(GenerateRootTaskID(jd));
    for (uint64_t k = 1; k < t; ++k) {
      TaskDescriptor* ct = rt->add_spawned();
      ct->set_uid(GenerateTaskID(*rt));
      ct->set_state(TaskDescriptor::RUNNABLE);
    }
    g.AddJobNodes(&jd);
  }
  VLOG(1) << "Added " << j*t << " tasks in " << j << " jobs (" << t
          << " tasks each).";
  // Add preferences for each task
  const unordered_set<uint64_t>& task_ids = g.task_node_ids();
  uint64_t p = 20;
  for (unordered_set<uint64_t>::const_iterator t_iter = task_ids.begin();
       t_iter != task_ids.end();
       ++t_iter) {
    for (uint64_t i = 0; i < p; i++) {
      uint64_t target = leaf_ids[rand_r(&seed) % leaf_ids.size()];
      g.AddArcInternal(*t_iter, target);
      VLOG(3) << "Added random preference arc from task node " << *t_iter
              << " to PU node " << target;
    }
  }
  VLOG(1) << "Wrote " << (task_ids.size() * p) << " preference arcs.";
  // Export
  DIMACSExporter exp;
  exp.Export(g);
  string outname = "/tmp/test.dm";
  VLOG(1) << "Output written to " << outname;
  exp.Flush(outname);
}

// Outputs simplified flow graphs for up to 8092 jobs (in power-of-two steps)
// into /tmp/testYYY.dm, where YYY is the number of jobs. Edit the use of f
// below to scale the number of machines instead.
// XXX(malte): Reduced the number of machines and jobs by two orders of
// magnitude here, so that the tests don't take forever when run in batch mode.
// Adapt as required :)
TEST_F(DIMACSExporterTest, ScalabilityTestGraphs) {
  for (uint64_t f = 1; f < 100; f *= 2) {
    FlowGraph g(FlowSchedulingCostModelType::COST_MODEL_TRIVIAL);
    // Test resource topology
    ResourceTopologyNodeDescriptor machine_tmpl;
    int fd = open("../tests/testdata/machine_topo.pbin", O_RDONLY);
    machine_tmpl.ParseFromFileDescriptor(fd);
    close(fd);
    // Create N machines
    uint64_t n = 120;
    ResourceTopologyNodeDescriptor rtn_root;
    ResourceID_t root_uuid = GenerateUUID();
    rtn_root.mutable_resource_desc()->set_uuid(to_string(root_uuid));
    InsertIfNotPresent(&uuid_conversion_map_, to_string(root_uuid),
                       to_string(root_uuid));
    for (uint64_t i = 0; i < n; ++i) {
      ResourceTopologyNodeDescriptor* child = rtn_root.add_children();
      child->CopyFrom(machine_tmpl);
      child->set_parent_id(rtn_root.resource_desc().uuid());
      TraverseResourceProtobufTreeReturnRTND(
          child, boost::bind(&DIMACSExporterTest::reset_uuid, this, _1));
    }
    VLOG(1) << "Added " << n << " machines.";
    // Add resources and job to flow graph
    g.AddResourceTopology(&rtn_root, n*8);
    // Test job
    uint64_t j = f;
    uint64_t t = 100;
    const vector<uint64_t> leaf_ids(g.leaf_node_ids().begin(),
                                    g.leaf_node_ids().end());
    unsigned int seed = time(NULL);
    for (uint64_t i = 0; i < j; ++i) {
      JobDescriptor jd;
      jd.set_uuid(to_string(GenerateJobID()));
      TaskDescriptor* rt = jd.mutable_root_task();
      string bin;
      spf(&bin, "%jd", rand_r(&seed));
      rt->set_binary(bin);
      rt->set_uid(GenerateRootTaskID(jd));
      for (uint64_t k = 1; k < t; ++k) {
        TaskDescriptor* ct = rt->add_spawned();
        ct->set_uid(GenerateTaskID(*rt));
        ct->set_state(TaskDescriptor::RUNNABLE);
      }
      g.AddJobNodes(&jd);
    }
    VLOG(1) << "Added " << j*t << " tasks in " << j << " jobs (" << t
            << " tasks each).";
    // Add preferences for each task
    const unordered_set<uint64_t>& task_ids = g.task_node_ids();
    uint64_t p = 20;
    for (unordered_set<uint64_t>::const_iterator t_iter = task_ids.begin();
         t_iter != task_ids.end();
         ++t_iter) {
      for (uint64_t i = 0; i < p; i++) {
        uint64_t target = leaf_ids[rand_r(&seed) % leaf_ids.size()];
        g.AddArcInternal(*t_iter, target);
        VLOG(3) << "Added random preference arc from task node " << *t_iter
                << " to PU node " << target;
      }
    }
    VLOG(1) << "Wrote " << (task_ids.size() * p) << " preference arcs.";
    // Export
    DIMACSExporter exp;
    exp.Export(g);
    string outname;
    spf(&outname, "/tmp/test%jd.dm", f);
    VLOG(1) << "Output written to " << outname;
    exp.Flush(outname);
  }
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
