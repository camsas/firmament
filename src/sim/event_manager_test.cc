// The Firmament project
// Copyright (c) 2015-2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Tests for the event manager.

#include <gtest/gtest.h>

#include "sim/event_manager.h"
#include "sim/simulated_wall_time.h"

DEFINE_string(scheduler, "flow", "The scheduler to use for tests.");

namespace firmament {
namespace sim {

class EventManagerTest : public ::testing::Test {
 protected:
  EventManagerTest() {
    // You can do set-up work for each test here.
    FLAGS_v = 2;
  }

  virtual ~EventManagerTest() {
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

TEST(EventManagerTest, AddEvent) {
  SimulatedWallTime simulated_time;
  EventManager event_manager(&simulated_time);
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  event_manager.AddEvent(2, event_desc);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 2);
  event_manager.AddEvent(3, event_desc);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 2);
  event_manager.AddEvent(1, event_desc);
  CHECK_EQ(event_manager.GetTimeOfNextEvent(), 1);
}

TEST(EventManagerTest, GetNextEvent) {
  SimulatedWallTime simulated_time;
  EventManager event_manager(&simulated_time);
  EventDescriptor event_desc;
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  event_manager.AddEvent(2, event_desc);
  event_desc.set_type(EventDescriptor::TASK_END_RUNTIME);
  event_manager.AddEvent(2, event_desc);
  CHECK_EQ(event_manager.GetNextEvent().first, 2);
  CHECK_EQ(event_manager.GetNextEvent().first, 2);
}

TEST(EventManagerTest, RemoveTaskEndRuntimeEvent) {
  SimulatedWallTime simulated_time;
  EventManager event_manager(&simulated_time);
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
