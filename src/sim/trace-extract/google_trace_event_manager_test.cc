// The Firmament project
// Copyright (c) 2015-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Tests for the Google trace event manager.

#include <gtest/gtest.h>

#include "sim/trace-extract/google_trace_event_manager.h"

namespace firmament {
namespace sim {

class GoogleTraceEventManagerTest : public ::testing::Test {
 protected:
  GoogleTraceEventManagerTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
  }

  virtual ~GoogleTraceEventManagerTest() {
  }

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }
};

TEST(GoogleTraceEventManagerTest, AddEvent) {
  GoogleTraceEventManager event_manager;
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  event_manager.AddEvent(2, event_desc);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 2);
  event_manager.AddEvent(3, event_desc);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 2);
  event_manager.AddEvent(1, event_desc);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 1);
}

TEST(GoogleTraceEventManagerTest, GetNextEvent) {
  GoogleTraceEventManager event_manager;
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  event_manager.AddEvent(2, event_desc);
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  event_manager.AddEvent(2, event_desc);
  CHECK_EQ(event_manager.GetNextEvent().first, 2);
  CHECK_EQ(event_manager.GetNextEvent().first, 2);
}

TEST(GoogleTraceEventManagerTest, RemoveTaskEndRuntimeEvent) {
  EXPECT_EQ(1, 1);
  GoogleTraceEventManager event_manager;
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  event_desc.set_job_id(1);
  event_desc.set_task_index(1);
  event_manager.AddEvent(2, event_desc);
  event_desc.set_task_index(2);
  event_manager.AddEvent(2, event_desc);
  TraceTaskIdentifier task_identifier;
  task_identifier.job_id = 1;
  task_identifier.task_index = 1;
  event_manager.RemoveTaskEndRuntimeEvent(task_identifier, 2);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 2);
  task_identifier.task_index = 3;
  event_manager.RemoveTaskEndRuntimeEvent(task_identifier, 2);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 2);
  task_identifier.task_index = 2;
  event_manager.RemoveTaskEndRuntimeEvent(task_identifier, 2);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), UINT64_MAX);
}

} // namespace sim
} // namespace firmament

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_logtostderr = true;
  FLAGS_stderrthreshold = 0;
  return RUN_ALL_TESTS();
}
