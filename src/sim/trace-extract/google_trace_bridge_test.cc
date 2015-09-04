// The Firmament project
// Copyright (c) 2015-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Tests for the Google trace bridge.

#include <gtest/gtest.h>

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
  bridge_->AddMachine(machine_tmpl, 1);
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, AddMachineSamples) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, AddTask) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, GetMachinePUs) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, OnJobCompletion) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, OnTaskCompletion) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, OnTaskEviction) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, OnTaskFailure) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, OnTaskMigration) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, OnTaskPlacement) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, RemoveMachine) {
  // TODO(ionel): Implement!
}

TEST_F(GoogleTraceBridgeTest, RemoveResourceNodeFromParentChildrenList) {
  // TODO(ionel): Implement!
}

} // namespace sim
} // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_logtostderr = true;
  FLAGS_stderrthreshold = 0;
  return RUN_ALL_TESTS();
}
