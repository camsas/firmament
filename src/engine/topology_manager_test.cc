// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Topology manager test class.

#include <gtest/gtest.h>

#include "base/common.h"
#include "engine/topology_manager.h"

namespace {

using firmament::machine::topology::TopologyManager;

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
  t.LoadAndParseSyntheticTopology("n:2 2 2 1 p:2");
  t.DebugPrintRawTopology();
}

// Tests that we can parse a synthetic topology.
TEST_F(TopologyManagerTest, GetNumPUs) {
  FLAGS_v = 2;
  TopologyManager t;
  //t.LoadAndParseTopology();
  VLOG(1) << "PUs: " << t.NumProcessingUnits();
  t.DebugPrintRawTopology();
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  firmament::common::InitFirmament(argc, argv);
  return RUN_ALL_TESTS();
}
