// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Synthetic trace loader.

#ifndef FIRMAMENT_SIM_SYNTHETIC_TRACE_LOADER_H
#define FIRMAMENT_SIM_SYNTHETIC_TRACE_LOADER_H

#include "sim/event_manager.h"
#include "sim/trace_loader.h"

namespace firmament {
namespace sim {

class SyntheticTraceLoader : public TraceLoader {
 public:
  explicit SyntheticTraceLoader(EventManager* event_manager);
  void LoadJobsNumTasks(unordered_map<uint64_t, uint64_t>* job_num_tasks);
  void LoadMachineEvents(multimap<uint64_t, EventDescriptor>* machine_events);
  bool LoadTaskEvents(uint64_t events_up_to_time,
                      unordered_map<uint64_t, uint64_t>* job_num_tasks);
  void LoadTaskUtilizationStats(
      unordered_map<TaskID_t, TraceTaskStats>* task_id_to_stats);
  void LoadTasksRunningTime(
      unordered_map<TaskID_t, uint64_t>* task_runtime);
 private:
  void GetNumberOfSlots(const ResourceTopologyNodeDescriptor& rtnd,
                        uint64_t* num_slots);
  uint64_t NumTasksAtBeginning();

  uint64_t last_generated_job_id_;
  uint64_t num_slots_per_machine_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SYNTHETIC_TRACE_LOADER_H
