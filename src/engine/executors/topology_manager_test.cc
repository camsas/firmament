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

// Topology manager test class.

#include <gtest/gtest.h>

#include <fcntl.h>

#include "base/common.h"
#include "engine/executors/topology_manager.h"
#include "misc/utils.h"

namespace firmament {

using machine::topology::TopologyManager;

// The fixture for testing class TopologyManager.
class TopologyManagerTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  TopologyManagerTest() {
    // You can do set-up work for each test here.
  }

  virtual ~TopologyManagerTest() {
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

  // Objects declared here can be used by all tests in the test case for
  // TopologyManager.
};

// Tests that we can parse a synthetic topology.
TEST_F(TopologyManagerTest, ParseSyntheticTopology) {
  FLAGS_v = 1;
  TopologyManager t;
  // Load a synthetic machine with:
  //  * 2 NUMA nodes,
  //  * 2 sockets in each of them,
  //  * 2 physical cores with private L2 per socket,
  //  * 2 threads per core (sharing L2).
  string topo_description = "NUMANode:2 Socket:2 Cache:3 Core:1 PU:2";
  hwloc_topology_t topology;
  hwloc_topology_init(&topology);
  t.LoadAndParseSyntheticTopology(topo_description, topology);
  ResourceTopologyNodeDescriptor res_desc;
  t.AsProtobuf(topology, &res_desc);
  LOG(INFO) << res_desc.DebugString();
  FILE* fd = fopen("/tmp/mach_test.pbin", "w");
  CHECK(res_desc.SerializePartialToFileDescriptor(fileno(fd)));
  fflush(fd);
  fclose(fd);
}

// Tests that we can parse a synthetic topology.
TEST_F(TopologyManagerTest, GetNumPUs) {
  FLAGS_v = 2;
  TopologyManager t;
  VLOG(1) << "PUs: " << t.NumProcessingUnits();
  t.DebugPrintRawTopology();
}

// Tests topology extraction to protobuf.
TEST_F(TopologyManagerTest, GetResourceTreeAsPB) {
  FLAGS_v = 2;
  TopologyManager t;
  ResourceTopologyNodeDescriptor res_desc;
  t.AsProtobuf(&res_desc);
}

// Tests that subsequent calls to AsProtobuf return a consistent view of the
// topology, and do not generate spurious new UUIDs.
TEST_F(TopologyManagerTest, CheckResourceTreeConsistency) {
  FLAGS_v = 2;
  TopologyManager t;
  ResourceTopologyNodeDescriptor res_desc1;
  ResourceTopologyNodeDescriptor res_desc2;
  t.AsProtobuf(&res_desc1);
  t.AsProtobuf(&res_desc2);
  // Check that the two resource trees are identical
  CHECK_EQ(res_desc1.DebugString(),
           res_desc2.DebugString());
}

// Tests pinning to the root resource of a resource tree
TEST_F(TopologyManagerTest, TestTrivialResourceBinding) {
  FLAGS_v = 2;
  TopologyManager t;
  ResourceTopologyNodeDescriptor res_desc;
  t.AsProtobuf(&res_desc);
  t.BindSelfToResource(ResourceIDFromString(
      res_desc.resource_desc().uuid()));
}

// Tests pinning to the root resource of a resource tree
TEST_F(TopologyManagerTest, TestBindToPUResource) {
  FLAGS_v = 2;
  TopologyManager t;
  ResourceTopologyNodeDescriptor res_desc;
  t.AsProtobuf(&res_desc);
  ResourceTopologyNodeDescriptor* rtnd_ptr = &res_desc;
  // Deep dive to find first PU
  while (rtnd_ptr->children_size() > 0 &&
         rtnd_ptr->resource_desc().type() != ResourceDescriptor::RESOURCE_PU) {
    rtnd_ptr = rtnd_ptr->mutable_children(0);
  }
  t.BindSelfToResource(ResourceIDFromString(
      rtnd_ptr->resource_desc().uuid()));
}

}  // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  firmament::common::InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
