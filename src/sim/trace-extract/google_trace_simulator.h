// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H

#include <fstream>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/flow_graph.h"
#include "scheduling/quincy_cost_model.h"
#include "sim/trace-extract/event_desc.pb.h"

namespace firmament {
namespace sim {

struct TaskIdentifier {
  uint64_t job_id;
  uint64_t task_index;

  bool operator==(const TaskIdentifier& other) const {
    return job_id == other.job_id && task_index == other.task_index;
  }
};

struct TaskIdentifierHasher {
  size_t operator()(const TaskIdentifier& key) const {
    return hash<uint64_t>()(key.job_id) * 17 + hash<uint64_t>()(key.task_index);
  }
};

struct MachineEvent {
  uint64_t machine_id;
  int32_t event_type;
};

class GoogleTraceSimulator {
 public:
  explicit GoogleTraceSimulator(string& trace_path);
  void Run();

 private:
  ResourceID_t AddMachineToTopologyAndResetUuid(const ResourceTopologyNodeDescriptor& machine_tmpl,
                                                uint64_t machine_id,
                                                ResourceTopologyNodeDescriptor* new_machine);
  TaskDescriptor* AddNewTask(
      FlowGraph* flow_graph, TaskIdentifier task_identifier,
      unordered_map<uint64_t, TaskIdentifier>* flow_id_to_task_id);

  TaskDescriptor* AddTaskToJob(JobDescriptor* jd_ptr);

  void ApplyMachineEvents(uint64_t last_time, uint64_t cur_time,
                          FlowGraph* flow_graph,
                          const ResourceTopologyNodeDescriptor& machine_tmpl);

  // Compute the number of events of a particular type withing each time interval.
  void BinTasksByEventType(uint64_t event_type, ofstream& out_file);

  void LoadInitialJobs(int64_t max_jobs);
  void LoadInitialMachines(int64_t max_num);
  void LoadInitialTasks();
  // Loads all the machine events and returns a multimap timestamp -> event.
  void LoadMachineEvents();
  // Loads all the task runtimes and returns map task_identifier -> runtime.
  unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>& LoadTasksRunningTime();

  // Read the jobs that are running or waiting to be scheduled at the beginning of the simulation.
  // Read num_jobs if it's specified.
  uint64_t ReadJobsFile(set<uint64_t>* jobs, int64_t num_jobs);
  // Read the machines that are alive at the beginning of the simulation.
  // Read num_machines if it's specified.
  uint64_t ReadMachinesFile(set<uint64_t>* machines, int64_t num_machines);
  // Read the tasks events from the beginning of the trace that are related to the jobs passed
  // in as an argument.
  uint64_t ReadInitialTasksFile();

  void PopulateJob(JobDescriptor* jd, uint64_t job_id);
  // The resource topology is build from the same protobuf file. The function changes the
  // uuids to make sure that there's no two identical uuids.
  void ResetUuid(ResourceTopologyNodeDescriptor* rtnd, const string& hostname,
                 const string& root_uuid);

  void ReplayTrace(FlowGraph* flow_graph);

  void UpdateFlowGraph(FlowGraph* flow_graph, map<uint64_t, uint64_t>* task_mappings,
                       unordered_map<TaskIdentifier, uint64_t, TaskIdentifierHasher>& task_runtime,
                       multimap<uint64_t, TaskDescriptor*>& task_end_runtime);

  EventDescriptor_EventType TranslateMachineEventToEventType(int32_t machine_event);

  // Map used to convert between the new uuids assigned to the machine nodes and
  // the old uuids read from the machine topology file.
  unordered_map<string, string> uuid_conversion_map_;

  // Map used to convert between the google trace job_ids and the Firmament job descriptors.
  unordered_map<uint64_t, JobDescriptor*> job_id_to_jd_;
  // Map used to convert between the google trace machine_ids and the Firmament resource
  // descriptors.
  unordered_map<uint64_t, ResourceID_t> machine_id_to_res_id_;
  // Map used to convert between the google trace task_ids and the Firmament task descriptors.
  unordered_map<TaskIdentifier, TaskDescriptor*, TaskIdentifierHasher> task_id_to_td_;

  // The map storing the simulator events. Maps from timestamp to simulator event.
  multimap<uint64_t, EventDescriptor*> events_;

  string trace_path_;
  // The root node of the machine topology.
  ResourceTopologyNodeDescriptor rtn_root_;

};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_SIMULATOR_H
